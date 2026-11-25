#ifndef REALTIME_CONFIG_H
#define REALTIME_CONFIG_H

#include <stddef.h>

enum realtime_backend { REALTIME_ASSIST, REALTIME_OPENAI };
enum realtime_playback_stream { REALTIME_PLAYBACK_TTS, REALTIME_PLAYBACK_VOIP };
enum realtime_microphone_mode { REALTIME_MIC_ASR, REALTIME_MIC_HFP, REALTIME_MIC_HFP_ASR_RECOVERY };

struct realtime_config {
    enum realtime_backend backend[2];
    enum realtime_playback_stream playback_stream;
    enum realtime_microphone_mode microphone_mode;
    char endpoint[512], api_key[512], model[96], voice[64], language[32], instructions[2048];
    char reasoning_effort[24], noise_reduction[16], turn_detection[16], semantic_eagerness[8];
    unsigned max_output_tokens, vad_threshold, vad_prefix_padding_ms;
    unsigned vad_silence_duration_ms;
    unsigned idle_seconds;
    int barge_in, sensitivity;
    int legacy_voip_asr_recovery;  /* Older settings pages kept this preference when VoIP was off. */
    char ha_endpoint[512], ha_token[2401];
};

void realtime_config_init(void);
void realtime_config_get(struct realtime_config *out);
void realtime_config_clear(struct realtime_config *config);

struct json;
enum realtime_config_result { REALTIME_CONFIG_OK, REALTIME_CONFIG_INVALID, REALTIME_CONFIG_CONFLICT, REALTIME_CONFIG_IO };

/* Secrets supplied separately from the profile JSON. Both inputs are owned by the caller. */
enum realtime_config_result realtime_config_save(const struct json *profile, const char *key,
                                                  const char *token, const char **error);
/* Status never includes credential bytes. Wake names are supplied by the satellite core. */
size_t realtime_config_status(char *out, size_t cap, const char *const wake[2], int available, int voip_supported);

int realtime_test_openai(const struct realtime_config *cfg);
int realtime_test_home_assistant(const struct realtime_config *cfg);

#endif
