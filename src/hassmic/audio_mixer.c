/* Device backend: capture micAsr and play on the TTS stream through libmixerAPI, so the AEC reference stays intact. */
#include "resample.h"
#include "audio.h"
#include "board.h"
#include "realtime.h"
#include <dlfcn.h>
#include <limits.h>
#include <math.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include "mixer_api.h"

static MixerHandle cap, play;
static int cap_held;

int cap_open(void)
{
    cap = MixerOpenRec(MIXER_REC_ASR);
    if (!cap) return -1;
    fprintf(stderr, "audio: micAsr rate=%u ch=%u bits=%u\n", MixerGetRate(cap), MixerGetNumCh(cap), MixerGetSampleSizeBits(cap));
    return 0;
}

/* The micAsr stream never pauses while it is healthy (a block every few ms, silence included).  Seen 2026-09-25 on the
 * installed Echo: hassmic sat for hours on a stream the mixer had given up on ("InCapture-GetReadBuff:
 * reason=EmptyQueueHungUp" in logcat, every read NULL, wake word and capture dump dead, buttons still fine); a restart of
 * hassmic alone brought it back.  So after STALL_READS empty reads in a row (up to 1.5 s each) the stream is reopened. */
#define STALL_READS 3

int cap_read(const void **data)
{
    static int empty;
    int status = 0; unsigned n = 0;
    if (cap_held) { MixerReleaseBufRec(cap); cap_held = 0; }
    void *p = cap ? MixerGetBufRec(cap, &status, &n) : NULL;
    if (!p) {
        if (++empty >= STALL_READS) {
            fprintf(stderr, "audio: micAsr delivers nothing (status %d), reopening\n", status);
            if (cap) MixerClose(cap);
            cap = NULL;
            if (cap_open() < 0) { fprintf(stderr, "audio: micAsr reopen failed, trying again\n"); sleep(1); }
            empty = 0;
        }
        return 0;
    }
    empty = 0; cap_held = 1; *data = p;
    return n;
}

void cap_close(void)
{
    if (cap_held) MixerReleaseBufRec(cap);
    if (cap) MixerClose(cap);
    cap = NULL; cap_held = 0;
}

static atomic_int music_q16 = 65536, tts_q16 = 65536, call_q16 = 65536;     /* audio_*_gain; 1 = 65536 */
static int q16(float g) { return (int)lroundf((g < 0 ? 0 : g > 1 ? 1 : g) * 65536); }
void audio_music_gain(float g) { atomic_store(&music_q16, q16(g)); }
void audio_tts_gain(float g) { atomic_store(&tts_q16, q16(g)); }
void audio_call_gain(float g) { atomic_store(&call_q16, q16(g)); }

/* Every writer hands whole samples (decoders, WAV in whole frames, Home Assistant's and Wyoming's chunks), and the
 * callers cut at whole samples; a lone odd byte (a buffer of 1) passes unscaled rather than as whatever the buffer held.
 * The gain glides (resample.c gain_glide), from the target at each open: a step from one buffer to the next clicked */
static float gain_of(atomic_int *q) { return (float)atomic_load(q) / 65536; }
static float play_g; static unsigned play_per_s;
static MixerHandle realtime_voip;
static int (*voip_resume)(MixerHandle);
static float realtime_voip_g;
static int realtime_voip_drained;
enum voip_owner { VOIP_FREE, VOIP_DROP_IN, VOIP_REALTIME };
static enum voip_owner active_voip_owner;
static pthread_mutex_t voip_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t voip_available = PTHREAD_COND_INITIALIZER;

static int claim_voip(enum voip_owner owner)
{
    pthread_mutex_lock(&voip_lock);
    /* Drop In asks the core to stop the Realtime session after starting its workers. */
    if (owner == VOIP_DROP_IN && active_voip_owner == VOIP_REALTIME) {
        struct timespec until;
        if (!clock_gettime(CLOCK_REALTIME, &until)) {
            until.tv_sec += 2;
            while (active_voip_owner == VOIP_REALTIME &&
                   !pthread_cond_timedwait(&voip_available, &voip_lock, &until)) {}
        }
    }
    int acquired = active_voip_owner == VOIP_FREE;
    if (acquired) active_voip_owner = owner;
    pthread_mutex_unlock(&voip_lock);
    return acquired;
}

static void release_voip(void)
{
    pthread_mutex_lock(&voip_lock);
    active_voip_owner = VOIP_FREE;
    pthread_cond_signal(&voip_available);
    pthread_mutex_unlock(&voip_lock);
}

int play_open(unsigned rate, unsigned channels)
{
    play = MixerOpenPlay(rate, channels, 16, MIXER_PLAY_TTS);
    play_g = gain_of(&tts_q16); play_per_s = rate * channels;
    return play ? 0 : -1;
}

static int write_reply(MixerHandle link, unsigned per_s, float *gain, const void *data, size_t len, int realtime)
{
    if (!link) return -1;
    const char *p = data;
    int16_t reference[REALTIME_RATE / 50];         /* core_realtime_audio submits at most 20 ms */
    while (len) {
        int status = 0; unsigned capb = 0;
        char *buf = MixerGetBufPlay(link, &status, &capb);
        if (!buf) return -1;
        unsigned n = (len < capb ? len : capb) & ~1u;              /* whole samples: the gain works on s16 */
        if (realtime && !n) { MixerReleaseBufPlay(link, 0); return -1; }
        if (!n) n = len < capb ? len : capb;
        gain_glide(buf, p, n, per_s, gain, gain_of(&tts_q16));
        int tap = audio_realtime_rendered && n && !(n % sizeof *reference) && n <= sizeof reference;
        unsigned queued = UINT_MAX;
        if (tap) {
            memcpy(reference, buf, n);           /* the shared slot can be recycled after release */
            queued = MixerGetNumBytes(link);     /* measure after any wait for the writable slot */
        }
        int released = MixerReleaseBufPlay(link, n);
        if (realtime && released < 0) return -1;
        if (tap && released == 0)
            audio_realtime_rendered(reference, n / sizeof *reference,
                                     queued == UINT_MAX || queued % sizeof *reference ? UINT_MAX : queued / sizeof *reference);
        p += n; len -= n;
    }
    return 0;
}

int play_write(const void *data, size_t len)
{
    return write_reply(play, play_per_s, &play_g, data, len, audio_realtime_rendered != NULL);
}

void play_close(int drain)
{
    if (!play) return;
    if (drain) MixerDrain(play); else MixerFlush(play);
    MixerClose(play);
    play = NULL;
}

static int drain_reply(MixerHandle link, int (*keep_draining)(void *), void *arg)
{
    /* MixerDrain starts draining. Closing the shared stream too soon drops its last samples. */
    if (keep_draining(arg) && MixerDrain(link) >= 0) {
        for (unsigned elapsed = 0; elapsed < 2000 && keep_draining(arg); elapsed += 10) {
            unsigned queued = MixerGetNumBytes(link);
            if (!queued) return 1;
            if (queued == UINT_MAX) break;
            usleep(10000);
        }
    }
    if (keep_draining(arg)) fprintf(stderr, "play: realtime mixer did not drain\n");
    return 0;
}

int play_close_realtime(int (*keep_draining)(void *), void *arg)
{
    if (!play) return 1;
    int drained = drain_reply(play, keep_draining, arg);
    if (!drained) MixerFlush(play);
    MixerClose(play);
    play = NULL;
    return drained;
}

int realtime_voip_supported(void) { return !strcmp(board.codename, "cannoli"); }

int realtime_voip_open(void)
{
    if (realtime_voip) return 0;
    if (!realtime_voip_supported()) return -1;
    /* Resume is only needed for native Realtime on Cannoli. Other mixers and the build stubs need no export. */
    voip_resume = (int (*)(MixerHandle))dlsym(RTLD_DEFAULT, "MixerResume");
    if (!voip_resume || !claim_voip(VOIP_REALTIME)) return -1;
    realtime_voip = MixerOpenPlay(REALTIME_RATE, 1, 16, MIXER_PLAY_VOIP);
    if (!realtime_voip) { release_voip(); return -1; }
    realtime_voip_g = gain_of(&tts_q16);
    realtime_voip_drained = 0;
    return 0;
}

int realtime_voip_write(const void *data, size_t len)
{
    if (!realtime_voip || len % sizeof(int16_t)) return -1;
    if (!len) return 0;
    if (realtime_voip_drained) {
        if (voip_resume(realtime_voip) != 1) return -1;
        realtime_voip_drained = 0;
    }
    return write_reply(realtime_voip, REALTIME_RATE, &realtime_voip_g, data, len, 1);
}

int realtime_voip_flush(void)
{
    /* Cannoli's Flush and Resume return true when the daemon reaches the requested state. */
    if (!realtime_voip || MixerFlush(realtime_voip) != 1 || voip_resume(realtime_voip) != 1) return -1;
    realtime_voip_g = gain_of(&tts_q16);
    realtime_voip_drained = 0;
    return 0;
}

int realtime_voip_drain(int (*keep_draining)(void *), void *arg)
{
    if (!realtime_voip) return 0;
    int drained = drain_reply(realtime_voip, keep_draining, arg);
    realtime_voip_drained = drained;
    return drained;
}

void realtime_voip_close(void)
{
    if (!realtime_voip) return;
    MixerFlush(realtime_voip);
    MixerClose(realtime_voip);
    realtime_voip = NULL;
    release_voip();
}

/* Music streams (Sendspin, Bluetooth speaker).  The mixer's buffer fill is not exposed in a documented way, so the queue
 * is modelled: bytes written minus bytes that real time has consumed since the stream started draining (it drains at
 * exactly the sample rate, and MixerGetBufPlay blocks once its ring is full, which keeps the model honest). */
struct stream { MixerHandle h; unsigned bps; long long t0, written; atomic_int *gain; float g; };   /* g: the glide's now */
static struct stream music = { .gain = &music_q16 }, bt = { .gain = &music_q16 }, voip = { .gain = &call_q16 };

static long long raw_us(void)
{
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
    return (long long)ts.tv_sec * 1000000 + ts.tv_nsec / 1000;
}

static int s_open(struct stream *s, unsigned rate, unsigned channels, const char *type)
{
    s->h = MixerOpenPlay(rate, channels, 16, type);
    s->bps = rate * channels * 2; s->t0 = 0; s->written = 0; s->g = gain_of(s->gain);
    return s->h ? 0 : -1;
}

static long long s_queued_us(struct stream *s)
{
    if (!s->h || !s->t0) return 0;
    long long q = s->written * 1000000 / s->bps - (raw_us() - s->t0);
    if (q < 0) { s->t0 = 0; s->written = 0; q = 0; }            /* ran dry: the model restarts with the next write */
    return q;
}

static int s_write(struct stream *s, const void *data, size_t len)
{
    const char *p = data;
    if (!s->h) return -1;
    s_queued_us(s);
    if (!s->t0) s->t0 = raw_us();
    while (len) {
        int status = 0; unsigned capb = 0;
        char *buf = MixerGetBufPlay(s->h, &status, &capb);
        if (!buf) return -1;
        unsigned n = (len < capb ? len : capb) & ~1u;              /* whole samples: the gain works on s16 */
        if (!n) n = len < capb ? len : capb;
        gain_glide(buf, p, n, s->bps / 2, &s->g, gain_of(s->gain));
        MixerReleaseBufPlay(s->h, n);
        p += n; len -= n; s->written += n;
    }
    return 0;
}

static void s_close(struct stream *s)
{
    if (!s->h) return;
    MixerFlush(s->h); MixerClose(s->h);
    s->h = NULL;
}

int       music_open(unsigned rate, unsigned channels) { return s_open(&music, rate, channels, MIXER_PLAY_MUSIC); }
long long music_queued_us(void) { return s_queued_us(&music); }
int       music_write(const void *data, size_t len) { return s_write(&music, data, len); }
void      music_close(void) { s_close(&music); }
long long music_prime_us(void) { return 0; }                /* the queue is a model at playback speed: no burst */
int       bt_open(unsigned rate, unsigned channels) { return s_open(&bt, rate, channels, MIXER_PLAY_MUSIC); }
long long bt_queued_us(void) { return s_queued_us(&bt); }
int       bt_write(const void *data, size_t len) { return s_write(&bt, data, len); }
void      bt_close(void) { s_close(&bt); }
int voip_open(unsigned rate, unsigned channels)
{
    if (!claim_voip(VOIP_DROP_IN)) return -1;
    int rc = s_open(&voip, rate, channels, MIXER_PLAY_VOIP);
    if (rc) release_voip();
    return rc;
}
long long voip_queued_us(void) { return s_queued_us(&voip); }
int       voip_write(const void *data, size_t len) { return s_write(&voip, data, len); }
void voip_close(void)
{
    if (!voip.h) return;
    s_close(&voip);
    release_voip();
}

/* micHfp: a stream of its own beside micAsr (the mixer gives each type to one client).  Only where it was measured to
 * carry the call path (board.call_mic): the Echo 2 and the Echo Dot 5 have one in AFE.cfg, untried */
struct hfp_capture { MixerHandle link; int held; };
static struct hfp_capture callmic, realtime_hfp;
static atomic_int hfp_claimed;

static int hfp_format(MixerHandle link)
{
    /* The mixer can report -1 until the stream starts. Refuse a format if it reports a different one. */
    int rate = (int)MixerGetRate(link), ch = (int)MixerGetNumCh(link), bits = (int)MixerGetSampleSizeBits(link);
    if ((rate > 0 && rate != CAP_RATE) || (ch > 0 && ch != 1) || (bits > 0 && bits != 16)) {
        fprintf(stderr, "audio: micHfp is %d Hz, %d ch, %d bits: not ours\n", rate, ch, bits);
        return -1;
    }
    return 0;
}

static void hfp_close(struct hfp_capture *capture)
{
    if (!capture->link) return;
    if (capture->held) MixerReleaseBufRec(capture->link);
    MixerClose(capture->link);
    capture->link = NULL; capture->held = 0;
    atomic_store(&hfp_claimed, 0);
}

static int hfp_open(struct hfp_capture *capture)
{
    if (capture->link) return 0;
    if (atomic_exchange(&hfp_claimed, 1)) return -1;
    capture->link = MixerOpenRec(MIXER_REC_HFP);
    if (!capture->link) { atomic_store(&hfp_claimed, 0); return -1; }
    if (hfp_format(capture->link)) { hfp_close(capture); return -1; }
    return 0;
}

static int hfp_read(struct hfp_capture *capture, const void **data)
{
    int status = 0; unsigned n = 0;
    if (!capture->link) return -1;
    if (capture->held) { MixerReleaseBufRec(capture->link); capture->held = 0; }
    void *p = MixerGetBufRec(capture->link, &status, &n);
    if (!p) return 0;
    capture->held = 1; *data = p;
    return (int)n;
}

int  callmic_present(void) { return board.call_mic; }
int  callmic_open(void) { return hfp_open(&callmic); }
int  callmic_read(const void **data) { return hfp_read(&callmic, data); }
void callmic_close(void) { hfp_close(&callmic); }

static int realtime_hfp_checked;
int realtime_hfp_open(void)
{
    realtime_hfp_checked = 0;
    return realtime_voip_supported() ? hfp_open(&realtime_hfp) : -1;
}
int realtime_hfp_read(const void **data)
{
    int n = hfp_read(&realtime_hfp, data);
    if (n <= 0) return n;
    if (n & 1) return -1;
    if (!realtime_hfp_checked) {
        if (hfp_format(realtime_hfp.link)) return -1;
        realtime_hfp_checked = 1;
    }
    return n;
}
void realtime_hfp_close(void) { hfp_close(&realtime_hfp); }

static int always(void) { return 1; }
void (*audio_earcon_started)(unsigned lead_ms);

void play_earcon(const short *pcm, size_t samples, unsigned rate) { play_earcon_while(pcm, samples, rate, always); }

void play_earcon_while(const short *pcm, size_t samples, unsigned rate, int (*go)(void))
{
    /* MixerDrain returns at once, it does not wait for the queue.  Closing right after it unlinks the ring while the mixer's
     * discovery thread (inotify on /data/mixer_streams) may not have opened it yet: the mixer then logs shmOpenFailed and the
     * sound is lost (seen on ~40 % of button sounds).  So wait for the mixer to start consuming, then for the queue to run
     * out, and only then close.  A stream the mixer never picks up is closed and opened again. */
    long duration_ms = (long)(samples * 1000ULL / (rate ? rate : 48000));
    for (int attempt = 0; attempt < 4; attempt++) {
        MixerHandle h = NULL; const char *p = (const char *)pcm; size_t len = samples * 2; int tries; long t = 0;
        for (tries = 0; tries < 25 && !(h = MixerOpenPlay(rate, 1, 16, MIXER_PLAY_EARCON)); tries++) usleep(20000);
        if (!h) { fprintf(stderr, "earcon: MixerOpenPlay failed\n"); return; }
        while (len) {
            int status = 0; unsigned capb = 0;
            if (!go()) { MixerFlush(h); MixerClose(h); return; }
            char *buf = MixerGetBufPlay(h, &status, &capb);
            if (!buf) { fprintf(stderr, "earcon: MixerGetBufPlay failed, status %d\n", status); MixerClose(h); return; }
            unsigned n = len < capb ? len : capb;
            memcpy(buf, p, n);
            MixerReleaseBufPlay(h, n);
            p += n; len -= n;
        }
        unsigned q0 = MixerGetNumBytes(h);
        while (t < 400 && MixerGetNumBytes(h) >= q0) { usleep(20000); t += 20; }
        if (MixerGetNumBytes(h) >= q0) {
            fprintf(stderr, "earcon: the mixer did not pick up the stream, opening again\n");
            MixerClose(h); usleep(50000);
            continue;
        }
        if (audio_earcon_started) audio_earcon_started(0);
        while (t < duration_ms + 1000 && MixerGetNumBytes(h) > 0 && go()) { usleep(20000); t += 20; }
        if (!go()) MixerFlush(h);
        MixerClose(h);
        return;
    }
    fprintf(stderr, "earcon: giving up\n");
}

/* Volumes: the mixer's own per stream type, set and read through the stock tools (it keeps them across reboots) */
#include "core.h"
static const char *const vol_props[] = { "MainVolume", "TTSVolume", "Mute" };

int vol_read(enum vol which, int fallback)
{
    char out[1024], *line, *save; int v = fallback, x;
    char *argv[] = { "/system/bin/audio_manager_get_prop", (char *)vol_props[which], NULL };
    core_run(argv, out, sizeof out);
    for (line = strtok_r(out, "\n", &save); line; line = strtok_r(NULL, "\n", &save))
        if (sscanf(line, "%d", &x) == 1 && x >= 0 && x <= 100) v = x;
    return v;
}

void vol_write(enum vol which, int v)
{
    char val[8];
    snprintf(val, sizeof val, "%d", v);
    char *argv[] = { "/system/bin/audio_manager_set_prop", (char *)vol_props[which], val, NULL };
    core_spawn(argv);
}

/* The front end: LIPC properties of com.doppler.lasp, through the stock tools (FINDINGS.md "Listening mode") */
void afe_listening(int on)
{
    char *argv[] = { "/system/bin/lipc-set-prop", "-i", "com.doppler.lasp", "LASP_CMD_SET_LISTENING_MODE", on ? "1" : "0", NULL };
    core_spawn(argv);
}

void afe_stream_stopped(void)
{
    char *argv[] = { "/system/bin/lipc-set-prop", "-i", "com.doppler.lasp", "LASP_CMD_NOTIFY_ASR_STREAM_STOPPED", "1", NULL };
    core_spawn(argv);
}

/* both tools in one shell: 150 ms on the device */
int afe_arbitration(long ts, long te, char *json, size_t n)
{
    char cmd[400];
    snprintf(cmd, sizeof cmd, "/system/bin/lipc-set-prop -s com.doppler.lasp LASP_CMD_SET_WAKEWORD_METADATA "
             "'{\"timestamp_before_ww_start\":%ld,\"timestamp_before_ww_end\":%ld}' && "
             "/system/bin/lipc-get-prop -s com.doppler.lasp LASP_CMD_REQUEST_ARBITRATION_JSON", ts, te);
    char *argv[] = { "/system/bin/sh", "-c", cmd, NULL };
    core_run(argv, json, n);
    return json[0] != 0;
}

int afe_eq_get(char *json, size_t n)
{
    char *argv[] = { "/system/bin/lipc-get-prop", "-s", "com.doppler.lasp", "LASP_CMD_GET_USER_EQ_INFO", NULL };
    core_run(argv, json, n);
    return json[0] != 0;
}

void afe_eq_set(const char *json)
{
    char *argv[] = { "/system/bin/lipc-set-prop", "-s", "com.doppler.lasp", "LASP_CMD_SET_USER_EQ_INFO", (char *)json, NULL };
    core_spawn(argv);
}

/* The Bluetooth speaker's route (btout.c, docs/re-a2dp-source.md): "1:<address>" in the LIPC string property
 * com.doppler.audiod A2DPSourceConnect moves the mixer's single output from the Echo's speaker to its A2DP HAL, "0:..."
 * back.  Everything mixed goes there (music, replies, earcons), with the Echo's volume and equalizer applied, as with
 * stock.  On every change the mixer asks btmanagerd's AIPC service for the speaker's name (btout.c answers). */
static long long mono_us(void) { struct timespec t; clock_gettime(CLOCK_MONOTONIC, &t); return (long long)t.tv_sec * 1000000 + t.tv_nsec / 1000; }

int audio_a2dp_route(int on, uint64_t addr, const char *name)
{
    char v[16], out[64];
    (void)name;
    snprintf(v, sizeof v, "%d:%012llx", on, (unsigned long long)addr);     /* exactly 12 digits: the mixer copies 6 bytes */
    char *set[] = { "/system/bin/lipc-set-prop", "-s", "com.doppler.audiod", "A2DPSourceConnect", v, NULL };
    char *get[] = { "/system/bin/lipc-get-prop", "-s", "com.doppler.audiod", "OutputDevice", NULL };
    long long t = mono_us();
    core_run(set, out, sizeof out);
    /* the mixer's playback thread switches a moment later.  Asked once at least (the line below logs its answer), then
     * until 5 s from the start of the set: a lipc that hangs takes core_run's 20 s each time */
    for (int i = 0; i < 20 && (i == 0 || mono_us() - t < 5000000); i++) {
        core_run(get, out, sizeof out);
        if (!strncmp(out, on ? "BT##" : "SPEAKER##", on ? 4 : 9)) break;
        usleep(100000);
    }
    out[strcspn(out, "\r\n")] = 0;
    fprintf(stderr, "btout: mixer output %s (%s, %lld ms)\n", on ? "to the speaker" : "to the Echo", out, (mono_us() - t) / 1000);
    return 0;
}

int audio_a2dp_lost(uint64_t addr) { (void)addr; return 0; }   /* the mixer keeps its route */
int audio_a2dp_aipc(void) { return 1; }

/* left on the speaker by a hassmic that is gone: back to the Echo (core_volume has the mixer's level back already) */
void audio_a2dp_reset(void)
{
    char out[64]; char *get[] = { "/system/bin/lipc-get-prop", "-s", "com.doppler.audiod", "OutputDevice", NULL };
    core_run(get, out, sizeof out);
    if (!strncmp(out, "BT##", 4)) audio_a2dp_route(0, 0, "");
}
