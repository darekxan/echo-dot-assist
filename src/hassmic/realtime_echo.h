#ifndef HASSMIC_REALTIME_ECHO_H
#define HASSMIC_REALTIME_ECHO_H

#include <stddef.h>
#include <stdint.h>

/* Positions use the 16 kHz capture sample clock. Only the playback worker publishes the 24 kHz reference. */
void realtime_echo_start(unsigned run, int enabled);
void realtime_echo_stop(unsigned run);
void realtime_echo_render(unsigned run, uint64_t capture_pos, const int16_t *pcm, size_t frames, unsigned queued_frames);
void realtime_echo_render_end(unsigned run, uint64_t capture_pos, int flushed);

/* The capture thread filters a copy before voice gain. Returns 1 when the copy replaces the original. */
int realtime_echo_capture(unsigned run, uint64_t at, const int16_t *pcm, int16_t *out, size_t frames);

#endif
