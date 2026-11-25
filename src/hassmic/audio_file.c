/* PC backend for protocol tests: capture loops $HASSMIC_CAP (raw 16 kHz mono s16le, silence if unset) at real-time
 * speed; playback is appended to $HASSMIC_PLAY (default /tmp/hassmic_play.raw).
 * HASSMIC_FAKE_ECHO=<ms>:<dB>: our sounds come back in the capture <ms> after they start, <dB> under their level, with
 * two reflections: what Amazon's echo canceller leaves of them on an Echo (earsup.h). */
#include "resample.h"
#include "audio.h"
#include "realtime.h"
#include <math.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

static FILE *cap, *play;
static short block[800];
static unsigned play_rate, play_ch;
static uint64_t play_echo_cursor;           /* simulated playback position, in playback frames */
void (*audio_earcon_started)(unsigned lead_ms);

#define ECHO_LEN (16000 * 4)
static pthread_mutex_t echo_mu = PTHREAD_MUTEX_INITIALIZER;
static float echo[ECHO_LEN];                /* by capture sample, modulo */
static unsigned long long cap_pos;          /* samples cap_read has handed out */

static unsigned echo_add(const short *pcm, size_t samples, unsigned rate, uint64_t *cursor)
{
    const char *e = getenv("HASSMIC_FAKE_ECHO"); int ms; float db;
    int enabled = e && sscanf(e, "%d:%f", &ms, &db) == 2;
    if (!rate || (!enabled && !cursor)) return 0;
    float g = enabled ? powf(10, db / 20) / 32768 : 0;
    pthread_mutex_lock(&echo_mu);
    uint64_t at = cap_pos, first_play = 0, first_cap = 0;
    size_t n = samples * (uint64_t)CAP_RATE / rate;
    unsigned queued = 0;
    if (cursor) {
        /* Playback writes can be shorter than a capture block. Continue at the previous block's end
         * if the simulated capture clock has not reached it, keeping sample phase across writes. */
        uint64_t now = cap_pos * rate / CAP_RATE;
        first_play = *cursor > now ? *cursor : now;
        queued = (unsigned)(first_play - now);
        *cursor = first_play + samples;
        first_cap = (first_play * CAP_RATE + rate - 1) / rate;
        n = (*cursor * CAP_RATE + rate - 1) / rate - first_cap;
        at = first_cap;
    }
    if (enabled) {
        at += (uint64_t)ms * (CAP_RATE / 1000);
        for (size_t j = 0; j < n && j < ECHO_LEN / 2; j++) {
            uint64_t phase = (first_cap + j) * rate - first_play * CAP_RATE;
            size_t i = (size_t)(phase / CAP_RATE);
            float sample = pcm[i];
            if (cursor && i + 1 < samples)
                sample += (pcm[i + 1] - sample) * (phase % CAP_RATE) / CAP_RATE;
            float v = sample * g;
            echo[(at + j) % ECHO_LEN] += v;
            echo[(at + j + 48) % ECHO_LEN] += 0.5f * v;   /* reflections at 3 and 10 ms */
            echo[(at + j + 160) % ECHO_LEN] += 0.25f * v;
        }
    }
    pthread_mutex_unlock(&echo_mu);
    return queued;
}

int cap_open(void)
{
    const char *p = getenv("HASSMIC_CAP");
    cap = p ? fopen(p, "rb") : NULL;
    return p && !cap ? -1 : 0;
}

int cap_read(const void **data)
{
    size_t n = 0;
    memset(block, 0, sizeof block);
    if (cap) {
        n = fread(block, 2, 800, cap);
        if (n < 800) rewind(cap);
    }
    usleep(50000);
    pthread_mutex_lock(&echo_mu);
    for (int i = 0; i < 800; i++) {
        float *e = &echo[(cap_pos + i) % ECHO_LEN], v = block[i] + *e * 32768;
        block[i] = (short)(v > 32767 ? 32767 : v < -32768 ? -32768 : lrintf(v)); *e = 0;
    }
    cap_pos += 800;
    pthread_mutex_unlock(&echo_mu);
    *data = block;
    return sizeof block;
}

void cap_close(void) { if (cap) fclose(cap); cap = NULL; }

int play_open(unsigned rate, unsigned channels)
{
    const char *p = getenv("HASSMIC_PLAY");
    play = fopen(p ? p : "/tmp/hassmic_play.raw", "ab");
    play_rate = rate; play_ch = channels; play_echo_cursor = 0;
    return play ? 0 : -1;
}

static _Atomic float tts_gain = 1;          /* same level as the mixer would get, also shown in the log */
void audio_tts_gain(float g)
{
    if (fabsf(g - atomic_load(&tts_gain)) > 0.0005f) fprintf(stderr, "audio: tts gain %.3f\n", g);
    atomic_store(&tts_gain, g);
}

static int write_reply(FILE *out, unsigned rate, unsigned channels, uint64_t *cursor, const void *data, size_t len)
{
    if (!out) return -1;
    float gain = atomic_load(&tts_gain);
    int16_t reference[REALTIME_RATE / 50];
    if (audio_realtime_rendered && rate == REALTIME_RATE && channels == 1 && len &&
        !(len % sizeof *reference) && len <= sizeof reference) {
        size_t frames = len / sizeof *reference;
        memcpy(reference, data, len);
        if (gain < 0.9995f)
            for (size_t i = 0; i < frames; i++) reference[i] = (int16_t)lrintf(reference[i] * gain);
        if (fwrite(reference, sizeof *reference, frames, out) != frames) return -1;
        unsigned queued = echo_add(reference, frames, rate, cursor);
        audio_realtime_rendered(reference, frames, queued);
    } else if (gain < 0.9995f)
        for (size_t i = 0; i + 1 < len; i += 2) {
            short v;
            memcpy(&v, (const char *)data + i, 2);
            v = (short)lrintf(v * gain);
            if (fwrite(&v, 2, 1, out) != 1) return -1;
        }
    else if (fwrite(data, 1, len, out) != len) return -1;
    usleep((useconds_t)(len * 1e6 / (2.0 * channels * rate)));
    return 0;
}

int play_write(const void *data, size_t len)
{
    return write_reply(play, play_rate, play_ch, &play_echo_cursor, data, len);
}

void play_close(int drain) { (void)drain; if (play) fclose(play); play = NULL; }
int play_close_realtime(int (*keep_draining)(void *), void *arg)
{
    int drain = keep_draining(arg);
    play_close(drain);
    return drain;
}

/* Explicitly enabled by tests. Separate raw files model call playback and the DSP's echo-clean capture. */
static FILE *realtime_voip, *realtime_hfp;
static uint64_t realtime_voip_cursor;

int realtime_voip_supported(void)
{
    const char *enabled = getenv("HASSMIC_FAKE_VOIP");
    return enabled && *enabled && strcmp(enabled, "0");
}

int realtime_voip_open(void)
{
    if (realtime_voip) return 0;
    if (!realtime_voip_supported()) return -1;
    const char *path = getenv("HASSMIC_FAKE_VOIP_PLAY");
    realtime_voip = fopen(path ? path : "/tmp/hassmic_realtime_voip.raw", "ab");
    if (!realtime_voip) return -1;
    setvbuf(realtime_voip, NULL, _IONBF, 0);
    realtime_voip_cursor = 0;
    return 0;
}

int realtime_voip_write(const void *data, size_t len)
{
    if (len % sizeof(int16_t)) return -1;
    return write_reply(realtime_voip, REALTIME_RATE, 1, &realtime_voip_cursor, data, len);
}

int realtime_voip_flush(void)
{
    if (!realtime_voip || fflush(realtime_voip)) return -1;
    realtime_voip_cursor = 0;
    return 0;
}

int realtime_voip_drain(int (*keep_draining)(void *), void *arg)
{
    if (!realtime_voip || !keep_draining(arg)) return 0;
    /* Writes are paced, so the model has already played the submitted buffers. */
    return 1;
}

void realtime_voip_close(void)
{
    if (!realtime_voip) return;
    fclose(realtime_voip);
    realtime_voip = NULL;
}

int realtime_hfp_open(void)
{
    if (realtime_hfp) return 0;
    const char *path = getenv("HASSMIC_FAKE_HFP");
    if (!realtime_voip_supported() || !path) return -1;
    realtime_hfp = fopen(path, "rb");
    if (!realtime_hfp) return -1;
    setvbuf(realtime_hfp, NULL, _IONBF, 0);
    return 0;
}

int realtime_hfp_read(const void **data)
{
    static short samples[CAP_RATE / 25];         /* Cannoli delivers 40 ms blocks */
    if (!realtime_hfp) return -1;
    const size_t count = sizeof samples / sizeof *samples;
    size_t n = fread(samples, sizeof *samples, count, realtime_hfp);
    if (ferror(realtime_hfp)) return -1;
    if (n < count) { memset(samples + n, 0, (count - n) * sizeof *samples); rewind(realtime_hfp); }
    usleep(40000);
    *data = samples;
    return sizeof samples;
}

void realtime_hfp_close(void)
{
    if (!realtime_hfp) return;
    fclose(realtime_hfp);
    realtime_hfp = NULL;
}

void play_earcon(const short *pcm, size_t samples, unsigned rate)
{
    int peak = 0;                               /* for the tests: how loud (separate volumes turn our sounds down) */
    for (size_t i = 0; i < samples; i++) if (abs(pcm[i]) > peak) peak = abs(pcm[i]);
    echo_add(pcm, samples, rate, NULL);
    if (audio_earcon_started) audio_earcon_started(0);
    fprintf(stderr, "earcon: %zu samples @ %u Hz, peak %d\n", samples, rate, peak);
}

void play_earcon_while(const short *pcm, size_t samples, unsigned rate, int (*go)(void))
{
    int peak = 0;
    for (size_t i = 0; i < samples; i++) if (abs(pcm[i]) > peak) peak = abs(pcm[i]);
    fprintf(stderr, "earcon: %zu samples @ %u Hz, until stopped, peak %d\n", samples, rate, peak);
    long ms = 0, len = (long)(samples * 1000ULL / (rate ? rate : 48000));
    for (; ms < len && go(); ms += 20) usleep(20000);
    if (ms < len) fprintf(stderr, "earcon: cut at %ld ms\n", ms);       /* for the tests: what stopped it stopped it at once */
}

/* Music streams on the PC: files, written at real-time speed through a modelled 200 ms device buffer.  The music's
 * buffer drains HASSMIC_FAKE_MUSIC_PPM faster (or slower: < 0) than real time: an output clock that is off, as the
 * Echo Show 5's (~350 ppm), for Sendspin's sync; HASSMIC_FAKE_MUSIC_BURST_MS of it vanish from the queue at once as it
 * starts and stay hidden: AudioTrack's buffer behind audio_android.c's slots (the Show 5's: ~137 ms). */
struct stream { FILE *f; unsigned bps; long long t0, written; double speed; long long burst_us; float g; };   /* g: the gain now (gliding) */
static struct stream music, bt, voip;
static long long raw_us(void) { struct timespec ts; clock_gettime(CLOCK_MONOTONIC_RAW, &ts); return (long long)ts.tv_sec * 1000000 + ts.tv_nsec / 1000; }

static int s_open(struct stream *s, const char *env, const char *def, unsigned rate, unsigned channels)
{
    const char *p = getenv(env);
    s->f = fopen(p ? p : def, "ab");
    s->bps = rate * channels * 2; s->t0 = 0; s->written = 0; s->g = -1;      /* -1: the gain of the first write */
    return s->f ? 0 : -1;
}

static long long s_queued_us(struct stream *s)
{
    if (!s->f || !s->t0) return 0;
    long long played = (long long)((double)(raw_us() - s->t0) * (s->speed ? s->speed : 1)), q = s->written * 1000000 / s->bps - played;
    if (q < 0) { s->t0 = 0; s->written = 0; q = 0; }            /* ran dry */
    return q > s->burst_us ? q - s->burst_us : 0;              /* what the track holds is not seen */
}

static int s_write_raw(struct stream *s, const void *data, size_t len);
/* tests: the gain into the file as the mixer would get it, and into the log when it changes */
static float music_gain = 1;
void audio_music_gain(float g)
{
    if (fabsf(g - music_gain) > 0.0005f) fprintf(stderr, "audio: music gain %.3f\n", g);
    music_gain = g;
}
static float call_gain = 1;
void audio_call_gain(float g)
{
    if (fabsf(g - call_gain) > 0.0005f) fprintf(stderr, "audio: call gain %.3f\n", g);
    call_gain = g;
}

static int s_write(struct stream *s, const void *data, size_t len)
{
    if (!s->f) return -1;
    char buf[8192]; float gain = s == &voip ? call_gain : music_gain;
    if (s->g < 0) s->g = gain;
    if (s->g == 1 && gain == 1) return s_write_raw(s, data, len);
    for (size_t off = 0; off < len; ) {                          /* as the mixer would get it: gliding (resample.c) */
        size_t n = len - off < sizeof buf ? len - off : sizeof buf;
        gain_glide(buf, (const char *)data + off, n, s->bps / 2, &s->g, gain);
        if (s_write_raw(s, buf, n)) return -1;
        off += n;
    }
    return 0;
}

static int s_write_raw(struct stream *s, const void *data, size_t len)
{
    if (!s->f) return -1;
    s_queued_us(s);
    if (!s->t0) s->t0 = raw_us();
    fwrite(data, 1, len, s->f); fflush(s->f); s->written += len;
    while (s_queued_us(s) > 200000) usleep(2000);
    return 0;
}

static void s_close(struct stream *s) { if (s->f) fclose(s->f); s->f = NULL; }

int       music_open(unsigned rate, unsigned channels)
{
    const char *ppm = getenv("HASSMIC_FAKE_MUSIC_PPM"), *burst = getenv("HASSMIC_FAKE_MUSIC_BURST_MS");
    int r = s_open(&music, "HASSMIC_MUSIC", "/tmp/hassmic_music.raw", rate, channels);
    music.speed = ppm ? 1 + atof(ppm) * 1e-6 : 1;
    music.burst_us = burst ? atoll(burst) * 1000 : 0;
    return r;
}
long long music_prime_us(void) { return music.burst_us ? 150000 : 0; }   /* as audio_android.c's, where there is a burst */
long long music_queued_us(void) { return s_queued_us(&music); }
int       music_write(const void *data, size_t len) { return s_write(&music, data, len); }
void      music_close(void) { s_close(&music); }
int       bt_open(unsigned rate, unsigned channels) { return s_open(&bt, "HASSMIC_BT", "/tmp/hassmic_bt.raw", rate, channels); }
long long bt_queued_us(void) { return s_queued_us(&bt); }
int       bt_write(const void *data, size_t len) { return s_write(&bt, data, len); }
void      bt_close(void) { s_close(&bt); }
int       voip_open(unsigned rate, unsigned channels) { return s_open(&voip, "HASSMIC_VOIP", "/tmp/hassmic_voip.raw", rate, channels); }
long long voip_queued_us(void) { return s_queued_us(&voip); }
/* $HASSMIC_VOIP_FAIL: while that file exists the Voip stream fails, as a mixer stream can (call.c drops micHfp then) */
int       voip_write(const void *data, size_t len)
{
    const char *fail = getenv("HASSMIC_VOIP_FAIL");
    if (fail && access(fail, F_OK) == 0) return -1;
    return s_write(&voip, data, len);
}
void      voip_close(void) { s_close(&voip); }

/* A call's microphone: $HASSMIC_CALL_CAP looped at real-time speed, as the capture (none without it, as on Android).
 * While the file $HASSMIC_CALL_CAP_STALL exists, each read waits 1.5 s and brings nothing, as the mixer's does when the
 * stream stalls (audio_mixer.c) */
static FILE *callcap; static short callblock[800];
int  callmic_present(void) { return getenv("HASSMIC_CALL_CAP") != NULL; }
int  callmic_open(void) { const char *p = getenv("HASSMIC_CALL_CAP"); if (!callcap && p) callcap = fopen(p, "rb"); return callcap ? 0 : -1; }
int  callmic_read(const void **data)
{
    if (!callcap) return -1;
    const char *stall = getenv("HASSMIC_CALL_CAP_STALL");
    if (stall && access(stall, F_OK) == 0) { usleep(1500000); return 0; }
    size_t n = fread(callblock, 2, 800, callcap);
    if (n < 800) { memset(callblock + n, 0, (800 - n) * 2); rewind(callcap); }
    usleep(50000);
    *data = callblock;
    return sizeof callblock;
}
void callmic_close(void) { if (callcap) fclose(callcap); callcap = NULL; }

/* The Bluetooth speaker's route: nothing to move on the PC; btout.c's sockets and encoder run as on the device */
int  audio_a2dp_route(int on, uint64_t addr, const char *name) { (void)addr; (void)name; fprintf(stderr, "btout: output %s\n", on ? "to the speaker" : "to the Echo"); return 0; }
int  audio_a2dp_lost(uint64_t addr) { (void)addr; return 0; }
void audio_a2dp_reset(void) {}
int  audio_a2dp_aipc(void) { return 0; }

/* Volumes: in memory; unset = what the caller expects (as a mixer that cannot be asked).  Tests: the mixer's volumes
 * as files in HASSMIC_FAKE_MIXER instead, one per property as audio_mixer.c names them */
static int vols[3] = { -1, -1, -1 };
static const char *const vol_props[] = { "MainVolume", "TTSVolume", "Mute" };

static const char *fake_mixer(enum vol which, char *file, size_t n)
{
    const char *d = getenv("HASSMIC_FAKE_MIXER");
    if (!d) return NULL;
    snprintf(file, n, "%s/%s", d, vol_props[which]);
    return file;
}

int vol_read(enum vol which, int fallback)
{
    char file[300]; int v = fallback, x;
    if (fake_mixer(which, file, sizeof file)) {
        FILE *f = fopen(file, "r");
        if (f) { if (fscanf(f, "%d", &x) == 1 && x >= 0 && x <= 100) v = x; fclose(f); }
        return v;
    }
    return vols[which] < 0 ? fallback : vols[which];
}

void vol_write(enum vol which, int v)
{
    char file[300];
    if (fake_mixer(which, file, sizeof file)) { FILE *f = fopen(file, "w"); if (f) { fprintf(f, "%d\n", v); fclose(f); } return; }
    vols[which] = v;
}

/* No front end on the PC.  HASSMIC_FAKE_AFE=<ms>:<voice>:<ambient> plays one that answers the wake word's energies
 * after <ms> (an Echo Show 5's took 240 and 380 ms, issue #77), for SIGUSR1's wake word too (wake_none.c) */
void afe_listening(int on) { if (getenv("HASSMIC_FAKE_LISTEN")) fprintf(stderr, "front end: listening %d\n", on); }
void afe_stream_stopped(void) { if (getenv("HASSMIC_FAKE_AFE")) fprintf(stderr, "front end: stream stopped\n"); }
int  afe_arbitration(long ts, long te, char *json, size_t n)
{
    const char *e = getenv("HASSMIC_FAKE_AFE"); int ms; double voice, ambient;
    (void)ts; (void)te;
    if (n) json[0] = 0;
    if (!e || sscanf(e, "%d:%lf:%lf", &ms, &voice, &ambient) != 3) return 0;
    usleep((useconds_t)ms * 1000);
    snprintf(json, n, "{\"voiceEnergy\":%.0f,\"ambientEnergy\":%.0f}", voice, ambient);
    return 1;
}
int  afe_eq_get(char *json, size_t n) { if (n) json[0] = 0; return 0; }
void afe_eq_set(const char *json) { (void)json; }
