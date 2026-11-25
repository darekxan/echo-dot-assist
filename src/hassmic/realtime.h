#ifndef HASSMIC_REALTIME_H
#define HASSMIC_REALTIME_H

#include <stddef.h>
#include <stdint.h>

#define REALTIME_RATE 24000

/* The core calls these under core_lock. They never wait for network I/O. */
int realtime_available(unsigned slot);
int realtime_start(unsigned slot, unsigned run, int allow_hfp);
void realtime_audio(unsigned run, const int16_t *pcm, size_t frames);
void realtime_stop(unsigned run);
void realtime_interrupt(unsigned run);

/* The selected mic takes over after the first reply opens. */
int realtime_wants_voip(unsigned run);
void realtime_reply_playback(unsigned run, int voip);
void realtime_stop_hfp(unsigned run);

/* The network worker calls these without taking core_lock itself. */
int core_realtime_audio(unsigned run, const void *pcm, size_t bytes);
void core_realtime_drain(unsigned run);
void core_realtime_cut(unsigned run);
int core_realtime_played(unsigned run, int *heard_ms);
void core_realtime_turn(unsigned run, int listening);
void core_realtime_finish(unsigned run, int failed);

/* Used by the ESPHome connection to ignore delayed events from an old Assist run. */
int core_realtime_active(void);
int core_realtime_supported(void);
int core_realtime_voip_supported(void);

#endif
