#include "realtime_config.h"
#include "json.h"
#include "statefile.h"
#include "../third_party/monocypher.h"
#include <arpa/inet.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define FILE_CAP 10000

static pthread_once_t once = PTHREAD_ONCE_INIT;
static pthread_mutex_t config_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t save_lock = PTHREAD_MUTEX_INITIALIZER;
static unsigned revision = 1;
static struct realtime_config current = {
    .backend = { REALTIME_ASSIST, REALTIME_ASSIST },
    .playback_stream = REALTIME_PLAYBACK_TTS,
    .microphone_mode = REALTIME_MIC_ASR,
    .endpoint = "wss://api.openai.com/v1/realtime",
    .model = "gpt-realtime",
    .voice = "marin",
    .turn_detection = "server_vad",
    .semantic_eagerness = "auto",
    .vad_threshold = 50,
    .vad_prefix_padding_ms = 300,
    .vad_silence_duration_ms = 500,
    .idle_seconds = 10,
    .barge_in = 1,
    .sensitivity = 50,
};

static const char *const playback_streams[] = {
    [REALTIME_PLAYBACK_TTS] = "tts",
    [REALTIME_PLAYBACK_VOIP] = "voip",
    NULL,
};

static const char *const microphone_modes[] = {
    [REALTIME_MIC_ASR] = "asr",
    [REALTIME_MIC_HFP] = "hfp",
    [REALTIME_MIC_HFP_ASR_RECOVERY] = "hfp_asr_recovery",
    NULL,
};

static int ascii_alnum(unsigned char c)
{
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9');
}

static int valid_id(const char *s, int empty_ok, const char *marks)
{
    if (!*s) return empty_ok;
    for (; *s; s++) if (!ascii_alnum((unsigned char)*s) && !strchr(marks, *s)) return 0;
    return 1;
}

static int valid_secret(const char *s)
{
    return valid_id(s, 1, "-._~+/=");
}

static int valid_endpoint(const char *s, int ha)
{
    const char *host;
    if (ha && !*s) return 1;
    if (!ha && !strncmp(s, "wss://", 6)) host = s + 6;
    else if (ha && !strncmp(s, "http://", 7)) host = s + 7;
    else if (ha && !strncmp(s, "https://", 8)) host = s + 8;
    else return 0;

    const char *p = host;
    if (*p == '[') {
        char addr[INET6_ADDRSTRLEN]; struct in6_addr bytes;
        const char *end = strchr(++p, ']');
        if (!end || end == p || (size_t)(end - p) >= sizeof addr) return 0;
        memcpy(addr, p, (size_t)(end - p));
        addr[end - p] = 0;
        if (inet_pton(AF_INET6, addr, &bytes) != 1) return 0;
        p = end + 1;
    } else {
        while (ascii_alnum((unsigned char)*p) || *p == '-' || *p == '.') p++;
        if (p == host || !ascii_alnum((unsigned char)*host)) return 0;
    }
    if (*p == ':') {
        unsigned port = 0;
        while (*++p >= '0' && *p <= '9') {
            port = port * 10 + (unsigned)(*p - '0');
            if (port > 65535) return 0;
        }
        if (!port) return 0;
    }
    if (*p++ != '/') return 0;   /* Both endpoints identify a path. The client adds the model query. */
    for (; *p; p++)
        if (!ascii_alnum((unsigned char)*p) && !strchr("/-._~!$&'()*+,;=:@", *p)) return 0;
    return 1;
}

static int digit(char c)
{
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    return -1;
}

static int plain_text_utf8(const unsigned char *s, size_t n)
{
    for (size_t i = 0; i < n;) {
        const unsigned char c = s[i++];
        if (c < 0x80) {
            if ((c < 0x20 && c != '\n' && c != '\r' && c != '\t') || c == 0x7f) return 0;
            continue;
        }
        if (c < 0xc2 || c > 0xf4) return 0;
        const size_t tail = c < 0xe0 ? 1 : c < 0xf0 ? 2 : 3;
        if (tail > n - i) return 0;
        const unsigned char first = s[i];
        if ((c == 0xe0 && first < 0xa0) || (c == 0xed && first >= 0xa0) ||
            (c == 0xf0 && first < 0x90) || (c == 0xf4 && first >= 0x90)) return 0;
        for (size_t j = 0; j < tail; j++) if ((s[i++] & 0xc0) != 0x80) return 0;
    }
    return 1;
}

/* JSON's general string reader normalizes controls and can truncate. These protocol fields are ASCII
 * and may not contain escapes. UTF-8 instructions use hex to preserve line breaks exactly. */
static int read_ascii(const struct json *j, const char *name, char *out, size_t cap)
{
    int i = json_get(j, 0, name);
    if (json_type(j, i) != J_STR) return -1;
    size_t n = (size_t)(j->t[i].end - j->t[i].start);
    if (n >= cap) return -1;
    const char *s = j->s + j->t[i].start;
    for (size_t k = 0; k < n; k++)
        if ((unsigned char)s[k] <= 0x20 || (unsigned char)s[k] >= 0x7f || s[k] == '\\' || s[k] == '"') return -1;
    memcpy(out, s, n);
    out[n] = 0;
    return 0;
}

static int read_instructions(const struct json *j, struct realtime_config *c)
{
    int i = json_get(j, 0, "instructions_hex");
    if (json_type(j, i) != J_STR) return -1;
    size_t n = (size_t)(j->t[i].end - j->t[i].start);
    if (n % 2 || n / 2 >= sizeof c->instructions) return -1;
    const char *s = j->s + j->t[i].start;
    for (size_t k = 0; k < n / 2; k++) {
        int a = digit(s[2 * k]), b = digit(s[2 * k + 1]);
        if (a < 0 || b < 0) return -1;
        c->instructions[k] = (char)(a * 16 + b);
    }
    c->instructions[n / 2] = 0;
    return plain_text_utf8((const unsigned char *)c->instructions, n / 2) ? 0 : -1;
}

static long long read_unsigned(const struct json *j, int t, long long max)
{
    if (json_type(j, t) != J_NUM) return -1;
    long long value = 0;
    for (int i = j->t[t].start; i < j->t[t].end; i++) {
        const char c = j->s[i];
        if (c < '0' || c > '9') return -1;
        value = value * 10 + c - '0';             /* Limits are well below LLONG_MAX / 10. */
        if (value > max) return -1;
    }
    return value;
}

static int read_choice(const struct json *j, const char *name, char *out, size_t cap,
                       const char *saved, const char *const *choices)
{
    if (json_get(j, 0, name) < 0) {
        snprintf(out, cap, "%s", saved);
        return 0;
    }
    if (read_ascii(j, name, out, cap)) return -1;
    for (; *choices; choices++) if (!strcmp(out, *choices)) return 0;
    return -1;
}

static int read_optional_unsigned(const struct json *j, const char *name, unsigned *out,
                                  unsigned saved, unsigned max)
{
    int token = json_get(j, 0, name);
    long long value = token < 0 ? (long long)saved : read_unsigned(j, token, max);
    if (value < 0) return -1;
    *out = (unsigned)value;
    return 0;
}

static int read_optional_bool(const struct json *j, const char *name, int *out, int saved)
{
    int token = json_get(j, 0, name);
    int type = json_type(j, token);
    if (token >= 0 && type != J_TRUE && type != J_FALSE) return -1;
    *out = token < 0 ? saved : type == J_TRUE;
    return 0;
}

static int read_route(const struct json *j, int token, int fallback, const char *const *names)
{
    if (token < 0) return fallback;
    for (int i = 0; names[i]; i++)
        if (json_streq(j, token, names[i])) return i;
    return -1;
}

static const char *read_audio_options(const struct json *j, struct realtime_config *c,
                                      const struct realtime_config *saved)
{
    const int was_voip = saved->playback_stream == REALTIME_PLAYBACK_VOIP;
    const int was_recovery = saved->legacy_voip_asr_recovery;
    int legacy_voip, legacy_recovery;
    if (read_optional_bool(j, "voip_audio", &legacy_voip, was_voip))
        return "Set VoIP audio to on or off";
    if (read_optional_bool(j, "voip_asr_recovery", &legacy_recovery, was_recovery))
        return "Set micAsr interruption recovery to on or off";

    const int playback_token = json_get(j, 0, "playback_stream");
    int playback = read_route(j, playback_token, legacy_voip ? REALTIME_PLAYBACK_VOIP : REALTIME_PLAYBACK_TTS,
                              playback_streams);
    if (playback < 0) return "Choose TTS or VoIP playback";

    /* A legacy VoIP toggle selected both routes. Preserve newer independent selections
     * when an older settings page returns the same legacy values. */
    enum realtime_microphone_mode fallback = saved->microphone_mode;
    if ((playback_token < 0 && legacy_voip != was_voip) || legacy_recovery != was_recovery) {
        fallback = REALTIME_MIC_ASR;
        if (playback == REALTIME_PLAYBACK_VOIP)
            fallback = legacy_recovery ? REALTIME_MIC_HFP_ASR_RECOVERY : REALTIME_MIC_HFP;
    }
    const int microphone_token = json_get(j, 0, "microphone_mode");
    int microphone = read_route(j, microphone_token, fallback, microphone_modes);
    if (microphone < 0) return "Choose a valid microphone mode";

    c->playback_stream = (enum realtime_playback_stream)playback;
    c->microphone_mode = (enum realtime_microphone_mode)microphone;
    c->legacy_voip_asr_recovery = legacy_recovery;
    if (microphone_token >= 0 && json_get(j, 0, "voip_asr_recovery") < 0)
        c->legacy_voip_asr_recovery = c->microphone_mode == REALTIME_MIC_HFP_ASR_RECOVERY;
    return NULL;
}

/* Existing state files and older settings pages may omit the OpenAI controls. */
static const char *read_openai_options(const struct json *j, struct realtime_config *c,
                                       const struct realtime_config *saved)
{
    static const char *const effort[] = { "", "minimal", "low", "medium", "high", "xhigh", NULL };
    static const char *const noise[] = { "", "off", "near_field", "far_field", NULL };
    static const char *const detection[] = { "server_vad", "semantic_vad", NULL };
    static const char *const eagerness[] = { "auto", "low", "medium", "high", NULL };

    const char *audio_error = read_audio_options(j, c, saved);
    if (audio_error) return audio_error;

    if (read_optional_unsigned(j, "max_output_tokens", &c->max_output_tokens, saved->max_output_tokens, 4096))
        return "Set max output tokens to 1–4096, or choose Unlimited";
    if (read_choice(j, "reasoning_effort", c->reasoning_effort, sizeof c->reasoning_effort,
                    saved->reasoning_effort, effort))
        return "Choose a valid OpenAI reasoning effort";
    if (read_choice(j, "noise_reduction", c->noise_reduction, sizeof c->noise_reduction, saved->noise_reduction, noise))
        return "Choose a valid OpenAI noise reduction mode";
    if (read_choice(j, "turn_detection", c->turn_detection, sizeof c->turn_detection, saved->turn_detection, detection))
        return "Choose Normal or Semantic turn detection";
    if (read_choice(j, "semantic_eagerness", c->semantic_eagerness, sizeof c->semantic_eagerness,
                    saved->semantic_eagerness, eagerness))
        return "Choose a valid Semantic turn detection eagerness";
    if (read_optional_unsigned(j, "vad_threshold", &c->vad_threshold, saved->vad_threshold, 100))
        return "Set the turn detection threshold to 0–100 percent";
    if (read_optional_unsigned(j, "vad_prefix_padding_ms", &c->vad_prefix_padding_ms, saved->vad_prefix_padding_ms, 2000))
        return "Set prefix padding to 0–2000 ms";
    if (read_optional_unsigned(j, "vad_silence_duration_ms", &c->vad_silence_duration_ms,
                               saved->vad_silence_duration_ms, 10000) || c->vad_silence_duration_ms < 100)
        return "Set silence duration to 100–10000 ms";
    return NULL;
}

static int read_backend(const struct json *j, int t)
{
    if (json_streq(j, t, "assist")) return REALTIME_ASSIST;
    if (json_streq(j, t, "openai")) return REALTIME_OPENAI;
    return -1;
}

static const char *read_profile(const struct json *j, struct realtime_config *c)
{
    const int list = json_get(j, 0, "backend");
    if (json_type(j, list) != J_ARR || json_len(j, list) != 2) return "Choose a backend for both assistant slots";
    const int first = json_first(j, list);
    const int a = read_backend(j, first), b = read_backend(j, json_next(j, list, first));
    if (a < 0 || b < 0) return "Invalid assistant backend";
    c->backend[0] = (enum realtime_backend)a;
    c->backend[1] = (enum realtime_backend)b;

    if (read_ascii(j, "endpoint", c->endpoint, sizeof c->endpoint) || !valid_endpoint(c->endpoint, 0))
        return "OpenAI endpoint must be a wss:// URL with a path and no query";
    if (read_ascii(j, "model", c->model, sizeof c->model) || !valid_id(c->model, 0, "-_."))
        return "Enter a valid OpenAI model ID";
    if (read_ascii(j, "voice", c->voice, sizeof c->voice) || !valid_id(c->voice, 0, "-_."))
        return "Enter a valid OpenAI voice";
    if (read_ascii(j, "language", c->language, sizeof c->language) || !valid_id(c->language, 1, "-"))
        return "Enter a language code such as pl-PL, or leave it blank";
    if (read_instructions(j, c)) return "Instructions must fit in 2047 UTF-8 bytes";
    if (read_ascii(j, "ha_endpoint", c->ha_endpoint, sizeof c->ha_endpoint) || !valid_endpoint(c->ha_endpoint, 1))
        return "Enter a Home Assistant MCP URL with http:// or https:// and a path";

    long long idle = read_unsigned(j, json_get(j, 0, "idle_seconds"), 120);
    long long sensitivity = read_unsigned(j, json_get(j, 0, "sensitivity"), 100);
    int type = json_type(j, json_get(j, 0, "barge_in"));
    if (idle < 1 || sensitivity < 0 || (type != J_TRUE && type != J_FALSE))
        return "Set silence timeout to 1–120 seconds and sensitivity to 0–100";
    c->idle_seconds = (unsigned)idle;
    c->sensitivity = (int)sensitivity;
    c->barge_in = type == J_TRUE;
    return NULL;
}

static const char *backend_name(enum realtime_backend b)
{
    return b == REALTIME_OPENAI ? "openai" : "assist";
}

static int write_profile(char *out, size_t cap, const struct realtime_config *c)
{
    return snprintf(out, cap, "{\"backend\":[\"%s\",\"%s\"],\"endpoint\":\"%s\","
                    "\"model\":\"%s\",\"voice\":\"%s\",\"language\":\"%s\","
                    "\"max_output_tokens\":%u,\"reasoning_effort\":\"%s\","
                    "\"noise_reduction\":\"%s\",\"turn_detection\":\"%s\",\"semantic_eagerness\":\"%s\","
                    "\"vad_threshold\":%u,\"vad_prefix_padding_ms\":%u,\"vad_silence_duration_ms\":%u,"
                    "\"idle_seconds\":%u,\"barge_in\":%s,\"sensitivity\":%d,"
                    "\"playback_stream\":\"%s\",\"microphone_mode\":\"%s\","
                    "\"voip_audio\":%s,\"voip_asr_recovery\":%s,\"ha_endpoint\":\"%s\"",
                    backend_name(c->backend[0]), backend_name(c->backend[1]), c->endpoint,
                    c->model, c->voice, c->language, c->max_output_tokens,
                    c->reasoning_effort, c->noise_reduction, c->turn_detection, c->semantic_eagerness,
                    c->vad_threshold, c->vad_prefix_padding_ms, c->vad_silence_duration_ms,
                    c->idle_seconds, c->barge_in ? "true" : "false",
                    c->sensitivity,
                    playback_streams[c->playback_stream], microphone_modes[c->microphone_mode],
                    c->playback_stream == REALTIME_PLAYBACK_VOIP ? "true" : "false",
                    c->legacy_voip_asr_recovery ? "true" : "false", c->ha_endpoint);
}

static int state_path(char *out, size_t cap)
{
    const char *dir = getenv("HASSMIC_STATE");
    int n = snprintf(out, cap, "%s/realtime.json", dir ? dir : "/data/local/hassmic/state");
    return n < 0 || (size_t)n >= cap ? -1 : 0;
}

static void load(void)
{
    char path[320], data[FILE_CAP];
    if (state_path(path, sizeof path)) return;
    int fd = open(path, O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK);
    if (fd < 0) return;
    struct stat statbuf;
    if (fstat(fd, &statbuf) || !S_ISREG(statbuf.st_mode) || (statbuf.st_mode & 077) ||
        statbuf.st_size <= 0 || statbuf.st_size >= (off_t)sizeof data) {
        close(fd);
        return;
    }
    FILE *f = fdopen(fd, "r");
    if (!f) { close(fd); return; }
    size_t n = fread(data, 1, sizeof data, f);
    fclose(f);
    struct json j = {0};
    struct realtime_config candidate = {0};
    if (n == (size_t)statbuf.st_size && !memchr(data, 0, n) &&
        !json_parse(&j, data, n) && !read_profile(&j, &candidate) &&
        !read_openai_options(&j, &candidate, &current) &&
        !read_ascii(&j, "api_key", candidate.api_key, sizeof candidate.api_key) &&
        !read_ascii(&j, "ha_token", candidate.ha_token, sizeof candidate.ha_token) &&
        valid_secret(candidate.api_key) && valid_secret(candidate.ha_token)) {
        if (!candidate.api_key[0]) candidate.backend[0] = candidate.backend[1] = REALTIME_ASSIST;
        if (!candidate.ha_endpoint[0]) crypto_wipe(candidate.ha_token, sizeof candidate.ha_token);
        current = candidate;
    }
    json_free(&j);
    realtime_config_clear(&candidate);
    crypto_wipe(data, sizeof data);
}

void realtime_config_init(void)
{
    pthread_once(&once, load);
}

static unsigned snapshot(struct realtime_config *out)
{
    realtime_config_init();
    pthread_mutex_lock(&config_lock);
    *out = current;
    unsigned rev = revision;
    pthread_mutex_unlock(&config_lock);
    return rev;
}

void realtime_config_get(struct realtime_config *out)
{
    (void)snapshot(out);
}

void realtime_config_clear(struct realtime_config *c)
{
    crypto_wipe(c, sizeof *c);
}

static int persist(const struct realtime_config *c)
{
    char path[320], data[FILE_CAP], hex[2 * sizeof c->instructions];
    if (state_path(path, sizeof path)) return -1;
    const char *digits = "0123456789abcdef";
    size_t i = 0;
    for (; c->instructions[i]; i++) {
        const unsigned char b = (unsigned char)c->instructions[i];
        hex[2 * i] = digits[b >> 4];
        hex[2 * i + 1] = digits[b & 15];
    }
    hex[2 * i] = 0;
    const int n = write_profile(data, sizeof data, c);
    int rc = -1;
    if (n >= 0 && (size_t)n < sizeof data) {
        int tail = snprintf(data + n, sizeof data - (size_t)n,
                            ",\"instructions_hex\":\"%s\",\"api_key\":\"%s\",\"ha_token\":\"%s\"}\n",
                            hex, c->api_key, c->ha_token);
        if (tail >= 0 && (size_t)tail < sizeof data - (size_t)n)
            rc = statefile_write(path, 0600, data, (size_t)n + (size_t)tail);
    }
    crypto_wipe(data, sizeof data);
    return rc;
}

/* save_lock is held across the write and publication of a profile revision. */
static int apply_config(const struct realtime_config *candidate)
{
    if (persist(candidate)) return -1;
    pthread_mutex_lock(&config_lock);
    realtime_config_clear(&current);
    current = *candidate;
    revision++;
    pthread_mutex_unlock(&config_lock);
    return 0;
}

enum secret_action { SECRET_INVALID, SECRET_KEEP, SECRET_REPLACE, SECRET_CLEAR };

static enum secret_action secret_action(const struct json *j, const char *name)
{
    int i = json_get(j, 0, name);
    if (json_streq(j, i, "keep")) return SECRET_KEEP;
    if (json_streq(j, i, "replace")) return SECRET_REPLACE;
    if (json_streq(j, i, "clear")) return SECRET_CLEAR;
    return SECRET_INVALID;
}

static int update_secret(enum secret_action action, char *out, size_t cap,
                         const char *replacement, const char *saved, int same_endpoint)
{
    if (action == SECRET_INVALID || !valid_secret(replacement)) return -1;
    if (action == SECRET_REPLACE) {
        size_t n = strlen(replacement);
        if (!n || n >= cap) return -1;
        memcpy(out, replacement, n + 1);
    } else if (*replacement) return -1;
    else if (action == SECRET_KEEP) {
        if (*saved && !same_endpoint) return -2;
        snprintf(out, cap, "%s", saved);
    }
    return 0;
}

enum realtime_config_result realtime_config_save(const struct json *j, const char *key,
                                                 const char *token, const char **error)
{
    struct realtime_config candidate = {0}, previous;
    enum realtime_config_result result = REALTIME_CONFIG_INVALID;
    long long expected = read_unsigned(j, json_get(j, 0, "revision"), 0xffffffffu);
    *error = read_profile(j, &candidate);
    if (*error) { realtime_config_clear(&candidate); return result; }

    pthread_mutex_lock(&save_lock);
    unsigned rev = snapshot(&previous);
    if (expected < 0 || (unsigned)expected != rev) {
        *error = "These settings changed in another session. Reopen this form.";
        result = REALTIME_CONFIG_CONFLICT;
        goto done;
    }
    *error = read_openai_options(j, &candidate, &previous);
    if (*error) goto done;
    enum secret_action api_action = secret_action(j, "api_key_action");
    int rc = update_secret(api_action, candidate.api_key, sizeof candidate.api_key,
                           key, previous.api_key, !strcmp(candidate.endpoint, previous.endpoint));
    if (rc) {
        *error = rc == -2 ? "Changing the OpenAI endpoint requires replacing or clearing its API key" : "Invalid OpenAI API key";
        goto done;
    }
    rc = update_secret(secret_action(j, "ha_token_action"), candidate.ha_token, sizeof candidate.ha_token,
                       token, previous.ha_token, !strcmp(candidate.ha_endpoint, previous.ha_endpoint));
    if (rc) {
        *error = rc == -2 ? "Changing the Home Assistant endpoint requires replacing or clearing its token" : "Invalid Home Assistant token";
        goto done;
    }
    if (!candidate.ha_endpoint[0] && candidate.ha_token[0]) {
        *error = "Enter a Home Assistant MCP endpoint for the token";
        goto done;
    }
    if (api_action == SECRET_CLEAR) candidate.backend[0] = candidate.backend[1] = REALTIME_ASSIST;
    if (!candidate.api_key[0] && (candidate.backend[0] == REALTIME_OPENAI || candidate.backend[1] == REALTIME_OPENAI)) {
        *error = "Enter an API key before selecting OpenAI Realtime";
        goto done;
    }
    if (apply_config(&candidate)) {
        *error = "Unable to save Realtime settings";
        result = REALTIME_CONFIG_IO;
        goto done;
    }
    result = REALTIME_CONFIG_OK;
done:
    pthread_mutex_unlock(&save_lock);
    realtime_config_clear(&candidate);
    realtime_config_clear(&previous);
    return result;
}

size_t realtime_config_status(char *out, size_t cap, const char *const wake[2], int available, int voip_supported)
{
    struct realtime_config c;
    unsigned rev = snapshot(&c);
    char instructions[6 * sizeof c.instructions + 2], first[6 * 64 + 2], second[6 * 64 + 2];
    int n = write_profile(out, cap, &c);
    if (n < 0 || (size_t)n >= cap) { realtime_config_clear(&c); return 0; }
    json_esc(instructions, sizeof instructions, c.instructions);
    json_esc(first, sizeof first, wake[0]);
    json_esc(second, sizeof second, wake[1]);
    int tail = snprintf(out + n, cap - (size_t)n,
                        ",\"revision\":%u,\"instructions\":%s,\"api_key_set\":%s,\"ha_token_set\":%s,"
                        "\"wake_words\":[%s,%s],\"available\":%s,\"voip_audio_supported\":%s,\"hfp_audio_supported\":%s}",
                        rev, instructions, c.api_key[0] ? "true" : "false", c.ha_token[0] ? "true" : "false",
                        first, second, available ? "true" : "false", voip_supported ? "true" : "false",
                        voip_supported ? "true" : "false");
    realtime_config_clear(&c);
    return tail >= 0 && (size_t)tail < cap - (size_t)n ? (size_t)n + (size_t)tail : 0;
}
