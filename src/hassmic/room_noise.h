#ifndef ROOM_NOISE_H
#define ROOM_NOISE_H

#include <stddef.h>
#include <stdint.h>

#define ROOM_NOISE_WINDOW 40       /* 4 seconds of 100 ms levels */

struct room_noise {
    float levels[ROOM_NOISE_WINDOW];
    int count, next, samples;
    double sum, power;
    float floor_db;
    long long last_level_ms, last_feed_ms;
};

void room_noise_reset(struct room_noise *meter);
/* Feed 16 kHz mono PCM16 from before speech gain and denoising. Only level summaries are kept.
 * Ineligible audio clears the window. The last complete estimate remains available for up to 30 seconds. */
void room_noise_feed(struct room_noise *meter, const int16_t *pcm, size_t n, long long now_ms, int eligible);
float room_noise_level(const struct room_noise *meter, long long now_ms);  /* relative dBFS or NAN */

#endif
