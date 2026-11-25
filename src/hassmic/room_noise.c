#include "room_noise.h"
#include "audio.h"
#include <math.h>

#define FRAME_SAMPLES (CAP_RATE / 10)
#define MIN_FRAMES 20
#define STALE_MS 30000

static void clear_window(struct room_noise *meter)
{
    meter->count = meter->next = meter->samples = 0;
    meter->sum = meter->power = 0;
}

void room_noise_reset(struct room_noise *meter)
{
    *meter = (struct room_noise){ .last_level_ms = -1, .last_feed_ms = -1 };
}

static void finish_frame(struct room_noise *meter, long long now_ms)
{
    double mean = meter->sum / FRAME_SAMPLES;
    double power = meter->power / FRAME_SAMPLES - mean * mean;   /* remove the DC offset */
    meter->levels[meter->next] = (float)(10 * log10(fmax(power / (32768.0 * 32768.0), 1e-10)));
    meter->next = (meter->next + 1) % ROOM_NOISE_WINDOW;
    meter->sum = meter->power = 0;
    meter->samples = 0;
    if (meter->count < ROOM_NOISE_WINDOW) meter->count++;
    if (meter->count < MIN_FRAMES) return;

    float sorted[ROOM_NOISE_WINDOW];
    for (int i = 0; i < meter->count; i++) {
        float value = meter->levels[i];
        int j = i;
        while (j && sorted[j - 1] > value) { sorted[j] = sorted[j - 1]; j--; }
        sorted[j] = value;
    }
    meter->floor_db = sorted[(meter->count - 1) / 4];       /* lower quartile rejects brief voices and impacts */
    meter->last_level_ms = now_ms;
}

float room_noise_level(const struct room_noise *meter, long long now_ms)
{
    return meter->last_level_ms >= 0 && now_ms >= meter->last_level_ms &&
        now_ms - meter->last_level_ms <= STALE_MS ? meter->floor_db : NAN;
}

void room_noise_feed(struct room_noise *meter, const int16_t *pcm, size_t n, long long now_ms, int eligible)
{
    if (meter->last_level_ms >= 0 && isnan(room_noise_level(meter, now_ms))) room_noise_reset(meter);
    /* A capture gap breaks the window, with an allowance for this block's duration. */
    if (meter->last_feed_ms >= 0 && (now_ms < meter->last_feed_ms ||
        now_ms - meter->last_feed_ms > 1000 + (long long)(n / (CAP_RATE / 1000)))) clear_window(meter);
    meter->last_feed_ms = now_ms;
    if (!eligible) { clear_window(meter); return; }

    while (n) {
        size_t count = (size_t)(FRAME_SAMPLES - meter->samples);
        if (count > n) count = n;
        for (size_t i = 0; i < count; i++) {
            double value = pcm[i];
            meter->sum += value;
            meter->power += value * value;
        }
        pcm += count;
        n -= count;
        meter->samples += (int)count;
        if (meter->samples == FRAME_SAMPLES) finish_frame(meter, now_ms);
    }
}
