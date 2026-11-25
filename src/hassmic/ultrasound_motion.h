#ifndef HASSMIC_ULTRASOUND_MOTION_H
#define HASSMIC_ULTRASOUND_MOTION_H

#include <stdint.h>

int ultrasound_motion_present(void);
int ultrasound_motion_enable(int set); /* -1 queries the preference */
int ultrasound_motion_state(void);    /* -1 unavailable, 0 clear, 1 motion */
int ultrasound_presence_state(void);  /* -1 unavailable, 0 clear, 1 presence */
void ultrasound_motion_start(void (*changed)(void));
void ultrasound_motion_mute(int muted);
void ultrasound_motion_stop(void);

/* Stock LIPC event handling, shared with the detector test. */
struct ultrasound_detector {
    int state, major;
    int64_t expires;
    int presence, movement;
    int64_t presence_expires;
};
int ultrasound_event_parse(const char *line, int *status, int64_t *sequence);
void ultrasound_detector_reset(struct ultrasound_detector *detector);
int ultrasound_detector_event(struct ultrasound_detector *detector, int status, int64_t now);
int ultrasound_detector_tick(struct ultrasound_detector *detector, int64_t now);

#endif
