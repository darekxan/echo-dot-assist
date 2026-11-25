/* OpenAI speech-to-speech sessions. The audio threads only enqueue or play PCM;
 * one worker owns each WebSocket, turn state and optional Home Assistant tools. */
#include "realtime.h"
#include "realtime_config.h"
#include "realtime_echo.h"
#include "realtime_mcp.h"
#include "realtime_net.h"
#include "audio.h"
#include "board.h"
#include "clock.h"
#include "core.h"
#include "hash.h"
#include "json.h"
#include <math.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define MIC_FRAMES (CAP_RATE * 12)
#define MIC_CHUNK (CAP_RATE / 50)
#define SESSION_UPDATE_MS 7000
#define SESSION_MAX_MS (9 * 60 * 1000)
#define RESPONSE_TIMEOUT_MS 45000
#define PLAYBACK_TIMEOUT_MS 50000
#define ECHO_TAIL_MS 160
#define MAX_AUDIO_PARTS 64
#define HFP_STALE_MS 250
#define ASR_LEAD_FRAMES CAP_RATE
#define RECOVERED_TURN_FRAMES (CAP_RATE * 8)
#define ASR_ACK_MS 2000

enum hfp_state { HFP_OFF, HFP_WAITING, HFP_STARTING, HFP_ACTIVE, HFP_FAILED };

struct asr_recovery_audio {
    size_t next, count;
    int16_t history[ASR_LEAD_FRAMES];
    int16_t turn[RECOVERED_TURN_FRAMES];      /* ASR lead-in followed by live HFP */
};

struct realtime_session {
    struct realtime_config config;
    unsigned run;
    size_t head, count;
    int interrupt, overflow, dual_mic, reply_started, voip_playback;
    enum hfp_state hfp;
    long long hfp_at;
    struct asr_recovery_audio *asr;
    int16_t mic[MIC_FRAMES];
};

struct audio_part {
    char item[160];
    int index;
    size_t frames;
};

struct mic_resampler {
    int have_sample, phase;
    int16_t last_sample;
};

enum recovery_phase { RECOVERY_OFF, RECOVERY_CAPTURE, RECOVERY_ORIGINAL, RECOVERY_CREATE, RECOVERY_DELETE };

struct asr_recovery_turn {
    enum recovery_phase phase;
    unsigned sequence;
    size_t lead, count, queued_hfp;
    int has_original, overflow, hfp_lost;
    uint64_t splice_at;
    long long end_ms, deadline;
    char original[160], replacement[64], event[64];
};

struct conversation {
    struct realtime_session *session;
    struct ws ws;
    struct realtime_mcp mcp;
    struct mic_resampler resampler;
    uint64_t sent_samples;
    struct asr_recovery_turn recovery;
    int hearing, accepted, wants_response, responding, cancel_sent;
    int playing, draining, cutting, closed;
    int hfp, voip_hfp;                    /* source and call processing of the last PCM sent */
    struct audio_part audio[MAX_AUDIO_PARTS];
    size_t audio_count;
    long long active_ms, response_ms, playback_ms, echo_until, loud_until;
};

enum session_end {
    END_CONVERSATION, END_IDLE, END_LOCAL_STOP, END_SESSION_LIMIT,
    /* The remaining reasons are failures reported to the core. */
    END_CONNECT_FAILED, END_SETUP_FAILED, END_RESPONSE_TIMEOUT, END_PLAYBACK_TIMEOUT,
    END_PLAYBACK_FAILED, END_MIC_OVERFLOW, END_SEND_FAILED, END_RECEIVE_FAILED, END_EVENT_FAILED, END_RECOVERY_TIMEOUT,
};

static const char *const session_end_name[] = {
    [END_CONNECT_FAILED] = "connect_failed",
    [END_SETUP_FAILED] = "setup_failed",
    [END_RESPONSE_TIMEOUT] = "response_timeout",
    [END_PLAYBACK_TIMEOUT] = "playback_timeout",
    [END_PLAYBACK_FAILED] = "playback_failed",
    [END_MIC_OVERFLOW] = "mic_overflow",
    [END_SEND_FAILED] = "send_failed",
    [END_RECEIVE_FAILED] = "receive_failed",
    [END_EVENT_FAILED] = "response_event_failed",
    [END_RECOVERY_TIMEOUT] = "asr_recovery_timeout",
};

static pthread_mutex_t mic_lock = PTHREAD_MUTEX_INITIALIZER;
static struct realtime_session *active;     /* only the worker frees it */
static unsigned hfp_worker_run;             /* one native micHfp worker owns the recorder */

static long long now_ms(void)
{
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    return (long long)now.tv_sec * 1000 + now.tv_nsec / 1000000;
}

static int live(struct realtime_session *session)
{
    pthread_mutex_lock(&mic_lock);
    int result = active == session;
    pthread_mutex_unlock(&mic_lock);
    return result;
}

int realtime_available(unsigned slot)
{
    struct realtime_config config;
    realtime_config_get(&config);
    int enabled = slot < 2 && config.backend[slot] == REALTIME_OPENAI && config.api_key[0];
    realtime_config_clear(&config);
    return enabled;
}

static void enqueue_mic(struct realtime_session *session, const int16_t *pcm, size_t frames)
{
    if (frames > MIC_FRAMES - session->count) session->overflow = 1;
    else {
        size_t tail = (session->head + session->count) % MIC_FRAMES;
        size_t first = MIC_FRAMES - tail;
        if (first > frames) first = frames;
        memcpy(session->mic + tail, pcm, first * sizeof *pcm);
        memcpy(session->mic, pcm + first, (frames - first) * sizeof *pcm);
        session->count += frames;
    }
}

static void remember_asr(struct asr_recovery_audio *asr, const int16_t *pcm, size_t frames)
{
    if (frames > ASR_LEAD_FRAMES) {
        pcm += frames - ASR_LEAD_FRAMES;
        frames = ASR_LEAD_FRAMES;
    }
    size_t first = ASR_LEAD_FRAMES - asr->next;
    if (first > frames) first = frames;
    memcpy(asr->history + asr->next, pcm, first * sizeof *pcm);
    memcpy(asr->history, pcm + first, (frames - first) * sizeof *pcm);
    asr->next = (asr->next + frames) % ASR_LEAD_FRAMES;
    asr->count += frames;
    if (asr->count > ASR_LEAD_FRAMES) asr->count = ASR_LEAD_FRAMES;
}

static int uses_hfp(const struct realtime_session *session)    /* mic_lock held */
{
    return session->reply_started && session->config.microphone_mode != REALTIME_MIC_ASR &&
           session->hfp == HFP_ACTIVE;
}

static void clear_mic(struct realtime_session *session)
{
    session->head = session->count = 0;
}

static void hfp_failed(struct realtime_session *session, const char *reason)    /* mic_lock held */
{
    if (uses_hfp(session)) clear_mic(session);
    session->hfp = HFP_FAILED;
    fprintf(stderr, "realtime: micHfp %s run=%u, using micAsr\n", reason, session->run);
}

void realtime_audio(unsigned run, const int16_t *pcm, size_t frames)
{
    pthread_mutex_lock(&mic_lock);
    if (active && active->run == run) {
        if (active->hfp == HFP_ACTIVE && now_ms() - active->hfp_at >= HFP_STALE_MS)
            hfp_failed(active, "stalled");
        struct asr_recovery_audio *asr = active->asr;
        if (asr) remember_asr(asr, pcm, frames);
        if (!uses_hfp(active)) enqueue_mic(active, pcm, frames);
    }
    pthread_mutex_unlock(&mic_lock);
}

int realtime_wants_voip(unsigned run)
{
    pthread_mutex_lock(&mic_lock);
    int wanted = active && active->run == run && active->config.playback_stream == REALTIME_PLAYBACK_VOIP;
    pthread_mutex_unlock(&mic_lock);
    return wanted;
}

static int hfp_running(unsigned run)    /* mic_lock held */
{
    return active && active->run == run && (active->hfp == HFP_STARTING || active->hfp == HFP_ACTIVE);
}

static void start_hfp(struct realtime_session *session);    /* mic_lock held */

static void *read_hfp(void *arg)
{
    unsigned run = (unsigned)(uintptr_t)arg;
    if (realtime_hfp_open() == 0) {
        for (;;) {
            pthread_mutex_lock(&mic_lock);
            int running = hfp_running(run);
            pthread_mutex_unlock(&mic_lock);
            if (!running) break;
            const void *pcm;
            int bytes = realtime_hfp_read(&pcm);
            if (bytes <= 0 || bytes % 2) break;
            pthread_mutex_lock(&core_lock);
            pthread_mutex_lock(&mic_lock);
            if (hfp_running(run)) {
                active->hfp_at = now_ms();
                if (active->hfp == HFP_STARTING) {
                    active->hfp = HFP_ACTIVE;
                    if (uses_hfp(active)) clear_mic(active);
                }
                if (!core_muted() && uses_hfp(active)) enqueue_mic(active, pcm, (size_t)bytes / 2);
            }
            pthread_mutex_unlock(&mic_lock);
            pthread_mutex_unlock(&core_lock);
        }
        realtime_hfp_close();
    }
    pthread_mutex_lock(&mic_lock);
    if (hfp_running(run)) hfp_failed(active, "unavailable");
    hfp_worker_run = 0;
    if (active && active->hfp == HFP_WAITING) start_hfp(active);
    pthread_mutex_unlock(&mic_lock);
    return NULL;
}

static void start_hfp(struct realtime_session *session)
{
    if (!session->dual_mic || (session->hfp != HFP_OFF && session->hfp != HFP_WAITING)) return;
    if (hfp_worker_run) {
        session->hfp = HFP_WAITING;
        return;
    }
    pthread_t thread;
    session->hfp = HFP_STARTING;
    hfp_worker_run = session->run;
    if (pthread_create(&thread, NULL, read_hfp, (void *)(uintptr_t)session->run)) {
        hfp_worker_run = 0;
        hfp_failed(session, "unavailable");
    } else pthread_detach(thread);
}

void realtime_reply_playback(unsigned run, int voip)
{
    pthread_mutex_lock(&mic_lock);
    if (active && active->run == run) {
        int was_hfp = uses_hfp(active);
        /* Some firmware may only deliver the call mic once Voip is open. */
        if (voip && !active->voip_playback && active->hfp == HFP_FAILED) {
            active->hfp = HFP_OFF;
            start_hfp(active);
        }
        active->reply_started = 1;
        active->voip_playback = voip;
        if (!was_hfp && uses_hfp(active)) clear_mic(active);
    }
    pthread_mutex_unlock(&mic_lock);
}

void realtime_stop_hfp(unsigned run)
{
    pthread_mutex_lock(&mic_lock);
    if (active && active->run == run && active->dual_mic && active->hfp != HFP_FAILED)
        hfp_failed(active, "released");
    pthread_mutex_unlock(&mic_lock);
}

void realtime_stop(unsigned run)
{
    pthread_mutex_lock(&mic_lock);
    if (active && active->run == run) active = NULL;
    pthread_mutex_unlock(&mic_lock);
}

void realtime_interrupt(unsigned run)
{
    pthread_mutex_lock(&mic_lock);
    if (active && active->run == run) active->interrupt = 1;
    pthread_mutex_unlock(&mic_lock);
}

static int mic_read(struct conversation *c, int16_t pcm[MIC_CHUNK], int *interrupt)
{
    struct realtime_session *session = c->session;
    pthread_mutex_lock(&mic_lock);
    size_t frames = 0;
    *interrupt = session->interrupt;
    session->interrupt = 0;
    int failed = session->overflow;
    if (active == session && !failed && session->count >= MIC_CHUNK) {
        c->hfp = uses_hfp(session);
        c->voip_hfp = c->hfp && session->voip_playback;
        frames = MIC_CHUNK;
        size_t first = MIC_FRAMES - session->head;
        if (first > frames) first = frames;
        memcpy(pcm, session->mic + session->head, first * sizeof *pcm);
        memcpy(pcm + first, session->mic, (frames - first) * sizeof *pcm);
        session->head = (session->head + frames) % MIC_FRAMES;
        session->count -= frames;
    }
    pthread_mutex_unlock(&mic_lock);
    return failed ? -1 : (int)frames;
}

static int send_text(struct ws *ws, const char *text)
{
    return ws_send(ws, WS_TEXT, text, strlen(text));
}

static void turn_detection_json(char *out, size_t cap, const struct realtime_config *cfg)
{
    /* The server detects turns while HassMic owns response creation and interruption. */
    const char *control = "\"create_response\":false,\"interrupt_response\":false";
    if (!strcmp(cfg->turn_detection, "semantic_vad")) {
        snprintf(out, cap, "{\"type\":\"semantic_vad\",%s,\"eagerness\":\"%s\"}",
                 control, cfg->semantic_eagerness);
    } else {
        snprintf(out, cap, "{\"type\":\"server_vad\",%s,\"threshold\":%u.%02u,"
                 "\"prefix_padding_ms\":%u,\"silence_duration_ms\":%u}", control,
                 cfg->vad_threshold / 100, cfg->vad_threshold % 100,
                 cfg->vad_prefix_padding_ms, cfg->vad_silence_duration_ms);
    }
}

static int supports_reasoning_effort(const char *model)
{
    static const char *const legacy[] = {
        "gpt-realtime", "gpt-realtime-mini", "gpt-realtime-1.5",
        "gpt-realtime-2025-08-28", "gpt-realtime-mini-2025-10-06", "gpt-realtime-mini-2025-12-15",
    };
    for (size_t i = 0; i < sizeof legacy / sizeof legacy[0]; i++)
        if (!strcmp(model, legacy[i])) return 0;
    return 1;
}

static int send_config(struct ws *ws, const struct realtime_config *cfg, const char *tools)
{
    static const char defaults[] =
        "You are a voice assistant in a home. Keep spoken replies short. Use the home tools for "
        "device requests when available. When the user ends the conversation, call end_conversation without a farewell.";
    static const char tool_rules[] =
        "\nFor the current local time or date, call get_local_time before answering. Use its fresh result."
        "\nWhen a request needs tools, call them silently and give one spoken answer after the last result.";
    static const char local_tools[] =
        "{\"type\":\"function\",\"name\":\"end_conversation\",\"description\":"
        "\"Close the voice conversation quietly when the user is done.\","
        "\"parameters\":{\"type\":\"object\",\"properties\":{}}},"
        "{\"type\":\"function\",\"name\":\"get_local_time\",\"description\":"
        "\"Read the device's current local date, time and UTC offset.\","
        "\"parameters\":{\"type\":\"object\",\"properties\":{}}}";
    char instructions[sizeof cfg->instructions + sizeof defaults + sizeof tool_rules + 100];
    char prompt[sizeof instructions * 6 + 3], voice[sizeof cfg->voice * 6 + 3];
    snprintf(instructions, sizeof instructions, "%s\n%s%s%s",
             cfg->instructions[0] ? cfg->instructions : defaults,
             cfg->language[0] ? "Reply in the language with this language tag: " :
             "Reply in the language the user speaks.", cfg->language, tool_rules);
    if (json_esc(prompt, sizeof prompt, instructions) < 0 || json_esc(voice, sizeof voice, cfg->voice) < 0)
        return -1;
    char turn[256], limit[40] = "", reasoning[80] = "", noise[64] = "";
    turn_detection_json(turn, sizeof turn, cfg);
    if (cfg->max_output_tokens)
        snprintf(limit, sizeof limit, ",\"max_output_tokens\":%u", cfg->max_output_tokens);
    if (cfg->reasoning_effort[0] && supports_reasoning_effort(cfg->model))
        snprintf(reasoning, sizeof reasoning, ",\"reasoning\":{\"effort\":\"%s\"}", cfg->reasoning_effort);
    if (!strcmp(cfg->noise_reduction, "off"))
        snprintf(noise, sizeof noise, ",\"noise_reduction\":null");
    else if (cfg->noise_reduction[0])
        snprintf(noise, sizeof noise, ",\"noise_reduction\":{\"type\":\"%s\"}", cfg->noise_reduction);
    size_t cap = strlen(prompt) + strlen(voice) + strlen(tools) + sizeof local_tools +
                 strlen(turn) + strlen(limit) + strlen(reasoning) + strlen(noise) + 512;
    char *text = malloc(cap);
    if (!text) return -1;
    int n = snprintf(text, cap,
        "{\"type\":\"session.update\",\"session\":{\"type\":\"realtime\","
        "\"instructions\":%s,\"output_modalities\":[\"audio\"]%s%s,"
        "\"audio\":{\"input\":{\"format\":{\"type\":\"audio/pcm\",\"rate\":%d},"
        "\"turn_detection\":%s%s},"
        "\"output\":{\"format\":{\"type\":\"audio/pcm\",\"rate\":%d},\"voice\":%s}},"
        "\"tool_choice\":\"auto\",\"tools\":[%s%s%s]}}",
        prompt, limit, reasoning, REALTIME_RATE, turn, noise, REALTIME_RATE, voice,
        local_tools, *tools ? "," : "", tools);
    int rc = n < 0 || (size_t)n >= cap ? -1 : send_text(ws, text);
    free(text);
    return rc;
}

static int receive_json(struct ws *ws, int wait_ms, struct json *json)
{
    int opcode;
    uint8_t *data;
    size_t len;
    int rc = rt_ws_recv(ws, wait_ms, &opcode, &data, &len);
    if (rc <= 0) return rc;
    if (opcode != WS_TEXT) return -1;
    if (memchr(data, 0, len) || json_parse(json, (const char *)data, len)) {
        json_free(json);
        return -1;
    }
    return 1;
}

static int event_item_id(const struct json *j, char *out, size_t cap)
{
    int id = json_get(j, 0, "item_id");
    return json_type(j, id) == J_STR && (size_t)(j->t[id].end - j->t[id].start) < cap &&
           json_str(j, id, out, cap) && *out;
}

static int configure(struct ws *ws, const struct realtime_config *cfg, const char *tools,
                     struct realtime_session *session)
{
    if (send_config(ws, cfg, tools)) return -1;
    long long end = now_ms() + SESSION_UPDATE_MS;
    while ((!session || live(session)) && now_ms() < end) {
        struct json json = {0};
        int rc = receive_json(ws, 100, &json);
        if (rc < 0) return -1;
        if (!rc) continue;
        int type = json_get(&json, 0, "type");
        int updated = json_streq(&json, type, "session.updated");
        int error = json_streq(&json, type, "error");
        json_free(&json);
        if (updated || error) return updated ? 0 : -1;
    }
    return -1;
}

int realtime_test_openai(const struct realtime_config *cfg)
{
    struct ws ws;
    if (!cfg->api_key[0] || rt_ws_connect(&ws, cfg->endpoint, cfg->model, cfg->api_key, NULL)) return -1;
    int rc = configure(&ws, cfg, "", NULL);
    ws_close(&ws);
    return rc;
}

/* Linear 3:2 conversion. Keep the phase across mixer chunks so clocks and
 * samples stay continuous, even when input ends on an odd frame. */
static size_t convert(struct mic_resampler *resampler, const int16_t *in, size_t count, int16_t *out)
{
    size_t n = 0;
    for (size_t i = 0; i < count; i++) {
        int16_t sample = in[i];
        if (!resampler->have_sample) {
            out[n++] = sample;
            resampler->have_sample = 1;
            resampler->phase = 2;
        } else {
            while (resampler->phase <= 3) {
                out[n++] = (int16_t)(resampler->last_sample +
                                     ((int)sample - resampler->last_sample) * resampler->phase / 3);
                resampler->phase += 2;
            }
            resampler->phase -= 3;
        }
        resampler->last_sample = sample;
    }
    return n;
}

static int send_mic(struct conversation *c, int16_t *pcm, size_t frames)
{
    long long now = now_ms();
    /* Suppress quiet residual echo while the speaker is on, and for its
     * short tail. The server's VAD supplies the actual turn boundaries. */
    if ((c->playing && !c->cutting) || (now < c->echo_until && !c->hearing)) {
        double power = 0;
        for (size_t i = 0; i < frames; i++) power += (double)pcm[i] * pcm[i];
        double db = 10 * log10(1 + power / frames) - 90.309;
        double threshold = -28.0 - 0.33 * c->session->config.sensitivity;
        if (c->session->config.barge_in && db >= threshold) c->loud_until = now + 350;
        /* The call mic already has DSP echo suppression. Send its quiet lead-in
         * as well so the provider's prefix padding can preserve the first word. */
        if (!c->session->config.barge_in || (!c->voip_hfp && now >= c->loud_until))
            memset(pcm, 0, frames * sizeof *pcm);
    }
    int16_t converted[MIC_CHUNK * 3 / 2 + 2];
    size_t count = convert(&c->resampler, pcm, frames, converted);
    static const char prefix[] = "{\"type\":\"input_audio_buffer.append\",\"audio\":\"";
    char text[sizeof prefix + ((sizeof converted + 2) / 3) * 4 + 3];
    memcpy(text, prefix, sizeof prefix - 1);
    size_t len = sizeof prefix - 1 + b64_encode(converted, count * sizeof *converted, text + sizeof prefix - 1, 0, 1);
    memcpy(text + len, "\"}", 3);
    if (!live(c->session)) return 0;
    int rc = send_text(&c->ws, text);
    if (!rc) {
        c->sent_samples += count;
        struct asr_recovery_turn *turn = &c->recovery;
        if (turn->phase == RECOVERY_CAPTURE) {
            if (!c->hfp) turn->hfp_lost = 1;
            if (!turn->hfp_lost && !turn->overflow) {
                /* These queued frames precede the snapshot. They still go
                 * out live but would repeat audio already in the ASR lead. */
                size_t skip = turn->queued_hfp < frames ? turn->queued_hfp : frames;
                turn->queued_hfp -= skip;
                turn->splice_at += skip == frames ? count : skip * REALTIME_RATE / CAP_RATE;
                size_t tail = frames - skip;
                if (tail > RECOVERED_TURN_FRAMES - turn->count) turn->overflow = 1;
                else {
                    memcpy(c->session->asr->turn + turn->count, pcm + skip, tail * sizeof *pcm);
                    turn->count += tail;
                }
            }
        }
    }
    return rc;
}

static int item_event(struct conversation *c, const char *type, const char *id, int content, int heard)
{
    if (!*id) return 0;
    char item[sizeof c->audio[0].item * 6 + 3], msg[sizeof item + 160];
    if (json_esc(item, sizeof item, id) < 0) return -1;
    if (heard >= 0)
        snprintf(msg, sizeof msg, "{\"type\":\"%s\",\"item_id\":%s,\"content_index\":%d,\"audio_end_ms\":%d}",
                 type, item, content, heard);
    else snprintf(msg, sizeof msg, "{\"type\":\"%s\",\"item_id\":%s}", type, item);
    return send_text(&c->ws, msg);
}

static int interrupt(struct conversation *c)
{
    if (c->playing && !c->cutting) {
        c->cutting = 1;
        core_realtime_cut(c->session->run);
    }
    if (c->responding && !c->cancel_sent) {
        c->cancel_sent = 1;
        if (send_text(&c->ws, "{\"type\":\"response.cancel\"}")) return -1;
    }
    return 0;
}

static void drain(struct conversation *c)
{
    if (c->playing && !c->draining && !c->cutting) {
        c->draining = 1;
        core_realtime_drain(c->session->run);
    }
}

static int playback(struct conversation *c)
{
    int heard;
    if (!c->playing || !core_realtime_played(c->session->run, &heard)) return 0;
    if (c->cutting) {
        /* The mixer reports time across all queued items. Truncate each part to its own heard duration. */
        size_t remaining = (size_t)heard * (REALTIME_RATE / 1000);
        for (size_t i = 0; i < c->audio_count; i++) {
            const struct audio_part *part = &c->audio[i];
            if (remaining >= part->frames) {
                remaining -= part->frames;
                continue;
            }
            if (item_event(c, "conversation.item.truncate", part->item, part->index,
                           (int)(remaining / (REALTIME_RATE / 1000)))) return -1;
            remaining = 0;
        }
    }
    c->playing = c->draining = c->cutting = 0;
    c->loud_until = 0;
    c->audio_count = 0;
    c->active_ms = now_ms();
    c->echo_until = c->active_ms + ECHO_TAIL_MS;
    if (c->responding && !c->closed && !c->hearing) core_realtime_turn(c->session->run, 0);
    return 0;
}

static int audio_delta(struct conversation *c, struct json *json)
{
    if (c->cancel_sent || c->cutting || (c->closed && !c->playing)) return 0;
    char item[sizeof c->audio[0].item];
    int index = (int)json_int(json, json_get(json, 0, "content_index"), 0);
    if (!json_str(json, json_get(json, 0, "item_id"), item, sizeof item) || !item[0]) return -1;
    int delta = json_get(json, 0, "delta");
    if (json_type(json, delta) != J_STR) return -1;
    const struct jtok *tok = &json->t[delta];
    size_t len = (size_t)(tok->end - tok->start);
    if (!len || len > 512 * 1024) return -1;
    size_t cap = (len / 4) * 3 + 3;
    uint8_t *pcm = malloc(cap);
    if (!pcm) return -1;
    long bytes = b64_decode(json->s + tok->start, len, pcm, cap);
    struct audio_part *part = c->audio_count ? &c->audio[c->audio_count - 1] : NULL;
    if (!part || part->index != index || strcmp(part->item, item)) {
        if (c->audio_count == MAX_AUDIO_PARTS) { free(pcm); return -1; }
        part = &c->audio[c->audio_count++];
        strcpy(part->item, item);
        part->index = index;
        part->frames = 0;
    }
    if (!c->playing) {
        c->playing = 1;
        c->playback_ms = now_ms();
    }
    int rc = bytes > 0 && !(bytes % 2) ? core_realtime_audio(c->session->run, pcm, (size_t)bytes) : -1;
    if (!rc) part->frames += (size_t)bytes / 2;
    free(pcm);
    return rc;
}

static int output_tool(struct conversation *c, const char *id, const char *result)
{
    if (strlen(result) > REALTIME_TOOL_RESULT_MAX) result = "{\"error\":\"Tool result exceeded the available space\"}";
    char call_id[960];
    size_t cap = strlen(result) * 6 + 3;
    char *output = malloc(cap);
    if (!output) return -1;
    int rc = -1;
    if (json_esc(call_id, sizeof call_id, id) >= 0 && json_esc(output, cap, result) >= 0) {
        size_t msg_cap = strlen(call_id) + strlen(output) + 140;
        char *msg = malloc(msg_cap);
        if (msg) {
            snprintf(msg, msg_cap, "{\"type\":\"conversation.item.create\",\"item\":{"
                     "\"type\":\"function_call_output\",\"call_id\":%s,\"output\":%s}}", call_id, output);
            rc = send_text(&c->ws, msg);
            free(msg);
        }
    }
    free(output);
    return rc;
}

static int output_clock(struct conversation *c, const char *id)
{
    struct tm local;
    char date_time[32], offset[8], result[96];
    if (!clock_local(&local) ||
        !strftime(date_time, sizeof date_time, "%Y-%m-%dT%H:%M:%S", &local) ||
        strftime(offset, sizeof offset, "%z", &local) != 5) {
        fprintf(stderr, "realtime: local clock unavailable\n");
        return output_tool(c, id, "{\"error\":\"Local clock is unavailable\"}");
    }
    snprintf(result, sizeof result, "{\"local_time\":\"%s%.3s:%s\"}", date_time, offset, offset + 3);
    return output_tool(c, id, result);
}

static int tool_calls(struct conversation *c, const struct json *json, int output)
{
    if (json_type(json, output) != J_ARR) return 0;
    JSON_EACH(json, output, i) {
        if (!json_streq(json, json_get(json, i, "type"), "function_call")) continue;
        char name[72], id[160];
        if (!json_str(json, json_get(json, i, "name"), name, sizeof name) ||
            !json_str(json, json_get(json, i, "call_id"), id, sizeof id) || !*id)
            return -1;
        if (!live(c->session)) return 0;
        if (!strcmp(name, "end_conversation")) {
            c->closed = 1;
            c->wants_response = 0;
            return 0;
        }
        if (!strcmp(name, "get_local_time")) {
            if (output_clock(c, id)) return -1;
            c->wants_response = 1;
            continue;
        }
        int arguments = json_get(json, i, "arguments");
        char *result = NULL;
        if (json_type(json, arguments) == J_STR) {
            const struct jtok *tok = &json->t[arguments];
            size_t cap = (size_t)(tok->end - tok->start) + 1;
            if (cap <= 8192) {
                char *args = malloc(cap);
                if (args) {
                    if (json_str(json, arguments, args, cap)) result = realtime_mcp_call(&c->mcp, name, args);
                    free(args);
                }
            }
        }
        int rc = live(c->session) ? output_tool(c, id, result ? result : "{\"error\":\"Home Assistant tool failed or is unavailable\"}") : 0;
        free(result);
        if (rc) return rc;
        c->wants_response = 1;
    }
    return 0;
}

static int same_item(const struct json *j, const char *expected)
{
    char id[160];
    return event_item_id(j, id, sizeof id) && !strcmp(id, expected);
}

static void begin_recovery(struct conversation *c, const struct json *j)
{
    struct realtime_session *session = c->session;
    struct asr_recovery_audio *asr = session->asr;
    struct asr_recovery_turn *turn = &c->recovery;
    char id[sizeof turn->original];
    if (!asr || turn->phase != RECOVERY_OFF || !event_item_id(j, id, sizeof id)) return;

    pthread_mutex_lock(&mic_lock);
    if (active == session && uses_hfp(session) && asr->count >= MIC_CHUNK) {
        size_t start = (asr->next + ASR_LEAD_FRAMES - asr->count) % ASR_LEAD_FRAMES;
        size_t first = ASR_LEAD_FRAMES - start;
        if (first > asr->count) first = asr->count;
        memcpy(asr->turn, asr->history + start, first * sizeof asr->turn[0]);
        memcpy(asr->turn + first, asr->history, (asr->count - first) * sizeof asr->turn[0]);
        unsigned sequence = turn->sequence + 1;
        *turn = (struct asr_recovery_turn){
            .phase = RECOVERY_CAPTURE, .sequence = sequence, .lead = asr->count, .count = asr->count,
            .queued_hfp = session->count, .splice_at = c->sent_samples, .end_ms = -1,
        };
        strcpy(turn->original, id);
    }
    pthread_mutex_unlock(&mic_lock);
}

static void finish_recovery(struct conversation *c, const char *fallback)
{
    if (fallback) fprintf(stderr, "realtime: ASR recovery fallback: %s\n", fallback);
    c->recovery.phase = RECOVERY_OFF;
    c->wants_response = 1;
    c->active_ms = now_ms();
}

static int create_recovered_item(struct conversation *c)
{
    struct asr_recovery_turn *turn = &c->recovery;
    unsigned run = c->session->run;
    snprintf(turn->replacement, sizeof turn->replacement, "item_asr_%u_%u", run, turn->sequence);
    snprintf(turn->event, sizeof turn->event, "asr_create_%u_%u", run, turn->sequence);
    char original[sizeof turn->original * 6 + 3], prefix[sizeof original + 370];
    if (json_esc(original, sizeof original, turn->original) < 0) {
        finish_recovery(c, "invalid_item_id");
        return 0;
    }
    int len = snprintf(prefix, sizeof prefix,
                       "{\"type\":\"conversation.item.create\",\"event_id\":\"%s\",\"previous_item_id\":%s,"
                       "\"item\":{\"id\":\"%s\",\"role\":\"user\",\"type\":\"message\","
                       "\"content\":[{\"type\":\"input_audio\",\"audio\":\"",
                       turn->event, original, turn->replacement);
    if (len < 0 || (size_t)len >= sizeof prefix) {
        finish_recovery(c, "invalid_item_id");
        return 0;
    }
    size_t samples = turn->count * 3 / 2 + 2;
    int16_t *pcm = malloc(samples * sizeof *pcm);
    static const char suffix[] = "\"}]}}";
    char *message = malloc((size_t)len + (samples * sizeof *pcm + 2) / 3 * 4 + sizeof suffix);
    if (!pcm || !message) {
        free(pcm);
        free(message);
        finish_recovery(c, "allocation");
        return 0;
    }
    struct mic_resampler converted = {0};
    samples = convert(&converted, c->session->asr->turn, turn->count, pcm);
    memcpy(message, prefix, (size_t)len);
    size_t encoded = b64_encode(pcm, samples * sizeof *pcm, message + len, 0, 1);
    memcpy(message + len + encoded, suffix, sizeof suffix);
    int rc = 0;
    if (live(c->session)) {
        rc = send_text(&c->ws, message);
    }
    free(pcm);
    free(message);
    if (rc) return -1;
    turn->phase = RECOVERY_CREATE;
    turn->deadline = now_ms() + ASR_ACK_MS;
    return 0;
}

static int commit_recovery(struct conversation *c)
{
    struct asr_recovery_turn *turn = &c->recovery;
    c->accepted = c->hearing = 0;

    /* The server's VAD offset includes the trailing silence. Bound the clip
     * there when available so packets sent while the event was in flight
     * cannot add audio from a second utterance. */
    if (turn->end_ms >= 0 && turn->end_ms <= SESSION_MAX_MS * 2) {
        uint64_t end = (uint64_t)turn->end_ms * (REALTIME_RATE / 1000);
        uint64_t tail = end > turn->splice_at ? (end - turn->splice_at) * CAP_RATE / REALTIME_RATE : 0;
        if (tail < turn->count - turn->lead) turn->count = turn->lead + (size_t)tail;
    }
    if (turn->overflow || turn->hfp_lost || turn->count - turn->lead < MIC_CHUNK) {
        finish_recovery(c, turn->overflow ? "turn_too_long" : "missing_hfp_tail");
        return 0;
    }
    if (turn->has_original) return create_recovered_item(c);
    turn->phase = RECOVERY_ORIGINAL;
    turn->deadline = now_ms() + ASR_ACK_MS;
    return 0;
}

/* A positive result means the event was consumed. Never ask for a response
 * while both copies of the interrupted command could be in the context. */
static int recovery_event(struct conversation *c, struct json *j, int type)
{
    struct asr_recovery_turn *turn = &c->recovery;
    if (turn->phase == RECOVERY_OFF) return 0;
    int added = json_streq(j, type, "conversation.item.added");
    int done = json_streq(j, type, "conversation.item.done") || json_streq(j, type, "conversation.item.created");
    if (added || done) {
        int item = json_get(j, 0, "item"), id = json_get(j, item, "id");
        if (turn->phase <= RECOVERY_ORIGINAL && json_streq(j, id, turn->original)) {
            turn->has_original = 1;
            return turn->phase == RECOVERY_ORIGINAL && create_recovered_item(c) ? -1 : 1;
        }
        if (turn->phase == RECOVERY_CREATE && json_streq(j, id, turn->replacement)) {
            if (!done && !json_streq(j, json_get(j, item, "status"), "completed")) return 1;
            char original[sizeof turn->original * 6 + 3], message[sizeof original + 184];
            if (json_esc(original, sizeof original, turn->original) < 0) return -1;
            snprintf(turn->event, sizeof turn->event, "asr_delete_%u_%u", c->session->run, turn->sequence);
            snprintf(message, sizeof message,
                     "{\"type\":\"conversation.item.delete\",\"event_id\":\"%s\",\"item_id\":%s}",
                     turn->event, original);
            if (send_text(&c->ws, message)) return -1;
            turn->phase = RECOVERY_DELETE;
            turn->deadline = now_ms() + ASR_ACK_MS;
            return 1;
        }
    }
    if (turn->phase == RECOVERY_DELETE && json_streq(j, type, "conversation.item.deleted") &&
        same_item(j, turn->original)) {
        finish_recovery(c, NULL);
        return 1;
    }
    if (json_streq(j, type, "error")) {
        int error = json_get(j, 0, "error");
        if (turn->phase >= RECOVERY_CREATE && json_streq(j, json_get(j, error, "event_id"), turn->event)) {
            if (turn->phase == RECOVERY_CREATE) finish_recovery(c, "create_rejected");
            else if (json_streq(j, json_get(j, error, "code"), "conversation_item_not_found"))
                finish_recovery(c, NULL);
            else return -1;
            return 1;
        }
    }
    if (turn->phase >= RECOVERY_ORIGINAL &&
        (json_streq(j, type, "input_audio_buffer.speech_started") ||
         json_streq(j, type, "input_audio_buffer.speech_stopped"))) return 1;
    if (json_streq(j, type, "input_audio_buffer.committed")) {
        if (turn->phase == RECOVERY_CAPTURE && same_item(j, turn->original)) return commit_recovery(c) ? -1 : 1;
        /* Appends already in flight can create an extra item. Drop it while
         * the selected interrupted turn is being finalized. */
        char id[sizeof turn->original];
        if (event_item_id(j, id, sizeof id) && strcmp(id, turn->original) &&
            item_event(c, "conversation.item.delete", id, 0, -1)) return -1;
        return 1;
    }
    return 0;
}

static int event(struct conversation *c, struct json *json)
{
    int type = json_get(json, 0, "type");
    unsigned run = c->session->run;
    int recovery = recovery_event(c, json, type);
    if (recovery) return recovery < 0 ? -1 : 0;
    if (json_streq(json, type, "input_audio_buffer.speech_started")) {
        long long now = now_ms();
        int over_reply = c->playing || now < c->echo_until;
        c->accepted = !over_reply || (c->session->config.barge_in && (c->voip_hfp || now < c->loud_until));
        if (c->accepted) {
            c->hearing = 1;
            c->wants_response = 0;
            c->active_ms = now;
            if (over_reply && c->hfp) begin_recovery(c, json);
            core_realtime_turn(run, 1);
            return interrupt(c);
        }
    } else if (json_streq(json, type, "input_audio_buffer.speech_stopped")) {
        if (c->recovery.phase == RECOVERY_CAPTURE && same_item(json, c->recovery.original))
            c->recovery.end_ms = json_int(json, json_get(json, 0, "audio_end_ms"), -1);
        if (c->accepted) {
            c->hearing = 0;
            c->active_ms = now_ms();
            core_realtime_turn(run, 0);
        }
    } else if (json_streq(json, type, "input_audio_buffer.committed")) {
        if (c->accepted) {
            c->accepted = c->hearing = 0;
            c->wants_response = 1;
        } else {
            char item[sizeof c->audio[0].item];
            json_str(json, json_get(json, 0, "item_id"), item, sizeof item);
            return item_event(c, "conversation.item.delete", item, 0, -1);
        }
    } else if (json_streq(json, type, "response.output_audio.delta")) {
        return audio_delta(c, json);
    } else if (json_streq(json, type, "response.done")) {
        c->responding = 0;
        /* output_audio.done ends one item. A response can still stream another. */
        drain(c);
        int response = json_get(json, 0, "response");
        int status = json_get(json, response, "status");
        if (json_streq(json, status, "failed")) return -1;
        if (json_streq(json, status, "incomplete")) fprintf(stderr, "realtime: response incomplete\n");
        int cancelled = c->cancel_sent;
        c->cancel_sent = 0;
        c->active_ms = now_ms();
        if (!cancelled && json_streq(json, status, "completed") &&
            tool_calls(c, json, json_get(json, response, "output"))) return -1;
        if (!c->playing && !c->wants_response) core_realtime_turn(run, 1);
    } else if (json_streq(json, type, "error")) {
        int code = json_get(json, json_get(json, 0, "error"), "code");
        /* Generation or playback may finish before a cancellation reaches the provider. */
        if (!json_streq(json, code, "response_cancel_not_active") &&
            !json_streq(json, code, "conversation_item_not_found")) return -1;
    }
    return 0;
}

static enum session_end exchange(struct conversation *c)
{
    struct realtime_session *session = c->session;
    long long begin = now_ms();
    c->active_ms = begin;
    while (live(session)) {
        long long now = now_ms();
        if (now - begin >= SESSION_MAX_MS) return END_SESSION_LIMIT;
        if (!c->closed && c->responding && now - c->response_ms > RESPONSE_TIMEOUT_MS) return END_RESPONSE_TIMEOUT;
        if (c->playing && now - c->playback_ms > PLAYBACK_TIMEOUT_MS) return END_PLAYBACK_TIMEOUT;
        if (c->recovery.phase >= RECOVERY_ORIGINAL && now >= c->recovery.deadline) {
            if (c->recovery.phase != RECOVERY_ORIGINAL) return END_RECOVERY_TIMEOUT;
            finish_recovery(c, "original_unconfirmed");
        }
        if (playback(c)) return END_PLAYBACK_FAILED;
        if (c->closed && !c->playing) return END_CONVERSATION;
        if (!c->playing && !c->responding && !c->hearing && !c->wants_response &&
            c->recovery.phase < RECOVERY_ORIGINAL &&
            now - c->active_ms >= (long long)session->config.idle_seconds * 1000) return END_IDLE;

        if (c->wants_response && !c->responding && !c->playing && !c->hearing && !c->closed &&
            c->recovery.phase == RECOVERY_OFF) {
            if (send_text(&c->ws, "{\"type\":\"response.create\"}")) return END_SEND_FAILED;
            c->wants_response = 0;
            c->responding = 1;
            c->response_ms = now;
            core_realtime_turn(session->run, 0);
        }
        for (int i = 0; i < 5 && live(session) && !c->closed && c->recovery.phase < RECOVERY_ORIGINAL; i++) {
            int16_t pcm[MIC_CHUNK];
            int cut;
            int frames = mic_read(c, pcm, &cut);
            if (frames < 0) return END_MIC_OVERFLOW;
            if (cut && interrupt(c)) return END_SEND_FAILED;
            if (!frames) break;
            if (send_mic(c, pcm, (size_t)frames)) return END_SEND_FAILED;
        }

        struct json json = {0};
        int received = receive_json(&c->ws, 15, &json);
        if (received < 0) return END_RECEIVE_FAILED;
        if (received) {
            int rc = event(c, &json);
            json_free(&json);
            if (rc) return END_EVENT_FAILED;
        }
    }
    return END_LOCAL_STOP;
}

static void *run_session(void *arg)
{
    struct realtime_session *session = arg;
    struct conversation conversation = { .session = session };
    int connected = rt_ws_connect(&conversation.ws, session->config.endpoint, session->config.model,
                                  session->config.api_key, NULL) == 0;
    enum session_end reason = connected ? END_SETUP_FAILED : END_CONNECT_FAILED;
    if (connected) {
        if (live(session) && *session->config.ha_endpoint &&
            realtime_mcp_open(&conversation.mcp, &session->config))
            fprintf(stderr, "realtime: Home Assistant tools unavailable\n");
        if (live(session) && configure(&conversation.ws, &session->config,
                                      realtime_mcp_tools(&conversation.mcp), session) == 0) {
            pthread_mutex_lock(&mic_lock);
            if (active == session) start_hfp(session);
            pthread_mutex_unlock(&mic_lock);
            reason = exchange(&conversation);
        }
    }
    int still_live = live(session);
    if (!still_live) reason = END_LOCAL_STOP;
    int failed = reason >= END_CONNECT_FAILED;
    if (failed) fprintf(stderr, "realtime: session failed: %s\n", session_end_name[reason]);
    realtime_echo_stop(session->run);
    if (still_live) core_realtime_finish(session->run, failed);
    if (connected) ws_close(&conversation.ws);
    realtime_mcp_close(&conversation.mcp);
    realtime_config_clear(&session->config);
    free(session->asr);
    free(session);
    return NULL;
}

int realtime_start(unsigned slot, unsigned run, int allow_hfp)
{
    if (slot >= 2) return 0;
    struct realtime_session *session = calloc(1, sizeof *session);
    if (!session) return 0;
    realtime_config_get(&session->config);
    int call_audio_supported = realtime_voip_supported();
    session->dual_mic = call_audio_supported && allow_hfp;
    if (!call_audio_supported) {
        session->config.playback_stream = REALTIME_PLAYBACK_TTS;
        session->config.microphone_mode = REALTIME_MIC_ASR;
    }
    if (session->dual_mic && session->config.barge_in && session->config.microphone_mode == REALTIME_MIC_HFP_ASR_RECOVERY)
        session->asr = calloc(1, sizeof *session->asr);
    session->run = run;
    pthread_t thread;
    pthread_mutex_lock(&mic_lock);
    int started = !active && session->config.backend[slot] == REALTIME_OPENAI &&
                  *session->config.api_key && !pthread_create(&thread, NULL, run_session, session);
    if (started) {
        int echo_supported = !strcmp(board.codename, "cannoli");
#ifndef __ANDROID__
        if (getenv("HASSMIC_FAKE_ECHO")) echo_supported = 1;
#endif
        realtime_echo_start(run, echo_supported && session->config.barge_in);
        active = session;
    }
    pthread_mutex_unlock(&mic_lock);
    if (started) pthread_detach(thread);
    else {
        realtime_config_clear(&session->config);
        free(session->asr);
        free(session);
    }
    return started;
}
