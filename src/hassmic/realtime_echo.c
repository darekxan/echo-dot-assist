#include "realtime_echo.h"
#include "audio.h"
#include "realtime.h"
#include <limits.h>
#include <math.h>
#include <pthread.h>
#include <string.h>

/* The mixer reports bytes in its input ring, upstream of the speaker and microphone DSP. Use that to place the
 * reference roughly on the capture clock. The signal comparison still has to find the acoustic delay. */
enum {
    REF_SAMPLES = CAP_RATE * 3,
    RENDER_JITTER = CAP_RATE * 80 / 1000,
    EMPTY_QUEUE_JITTER = CAP_RATE * 20 / 1000,
    MAX_QUEUED = REALTIME_RATE * 2,
    MAX_DELAY = CAP_RATE * 420 / 1000,
    CLOCK_LEAD = CAP_RATE * 32 / 1000,
    CAPTURE_BLOCK = CAP_RATE / 50,
    MATCH_STRIDE = 4,
    EARLY_REFLECTION = CAP_RATE * 24 / 1000,
    REF_WINDOW = MAX_DELAY + CLOCK_LEAD + CAPTURE_BLOCK + MATCH_STRIDE,
    PATHS = 5,
};

static pthread_mutex_t reference_lock = PTHREAD_MUTEX_INITIALIZER;
static struct {
    unsigned run;
    uint64_t first, end;
    int16_t samples[REF_SAMPLES];
    int16_t group[3];
    unsigned count;
    int playing;
} reference;

void realtime_echo_start(unsigned run, int enabled)
{
    pthread_mutex_lock(&reference_lock);
    reference.run = enabled ? run : 0;
    reference.first = reference.end = 0;
    reference.count = 0;
    reference.playing = 0;
    pthread_mutex_unlock(&reference_lock);
}

void realtime_echo_stop(unsigned run)
{
    pthread_mutex_lock(&reference_lock);
    if (run && run == reference.run) reference.run = 0;
    pthread_mutex_unlock(&reference_lock);
}

static void append_reference(int16_t sample)
{
    reference.samples[reference.end % REF_SAMPLES] = sample;
    reference.end++;
    if (reference.end - reference.first > REF_SAMPLES) reference.first = reference.end - REF_SAMPLES;
}

void realtime_echo_render(unsigned run, uint64_t capture_pos, const int16_t *pcm, size_t frames, unsigned queued_frames)
{
    if (!run || !frames) return;
    pthread_mutex_lock(&reference_lock);
    if (run != reference.run) { pthread_mutex_unlock(&reference_lock); return; }

    uint64_t position = capture_pos;
    if (queued_frames <= MAX_QUEUED) position += (uint64_t)queued_frames * CAP_RATE / REALTIME_RATE;
    if (!reference.end || position > reference.end + REF_SAMPLES) {
        reference.first = reference.end = position;
        reference.count = 0;
    } else if (!reference.playing || position > reference.end + RENDER_JITTER ||
               (!queued_frames && position > reference.end + EMPTY_QUEUE_JITTER)) {
        /* Keep the previous reply's tail for matching. Start a new segment when the mixer ran dry. */
        while (reference.end < position) append_reference(0);
        reference.count = 0;
    }
    reference.playing = 1;

    /* Continuous 24 to 16 kHz, with the resampling phase retained across mixer buffers. */
    for (size_t i = 0; i < frames; i++) {
        reference.group[reference.count++] = pcm[i];
        if (reference.count == 3) {
            append_reference(reference.group[0]);
            append_reference((int16_t)(((int)reference.group[1] + reference.group[2]) / 2));
            reference.count = 0;
        }
    }
    pthread_mutex_unlock(&reference_lock);
}

void realtime_echo_render_end(unsigned run, uint64_t capture_pos, int flushed)
{
    pthread_mutex_lock(&reference_lock);
    if (run && run == reference.run) {
        if (flushed && reference.end > capture_pos) {
            reference.end = capture_pos;
            if (reference.first > reference.end) reference.first = reference.end;
        }
        reference.count = 0;
        reference.playing = 0;
    }
    pthread_mutex_unlock(&reference_lock);
}

/* Reference lock is held for the copy only. The capture thread owns the matcher. */
static int capture_reference(unsigned run, uint64_t at, int16_t *samples, size_t count)
{
    int64_t from = (int64_t)at - MAX_DELAY - MATCH_STRIDE;
    int64_t to = from + (int64_t)count;
    pthread_mutex_lock(&reference_lock);
    if (run != reference.run || reference.first == reference.end ||
        (int64_t)reference.end <= from || (int64_t)reference.first >= to) {
        pthread_mutex_unlock(&reference_lock);
        return 0;
    }
    memset(samples, 0, count * sizeof *samples);
    int64_t first = (int64_t)reference.first > from ? (int64_t)reference.first : from;
    int64_t end = (int64_t)reference.end < to ? (int64_t)reference.end : to;
    while (first < end) {
        size_t offset = (uint64_t)first % REF_SAMPLES;
        size_t n = REF_SAMPLES - offset;
        if ((int64_t)n > end - first) n = (size_t)(end - first);
        memcpy(samples + (first - from), reference.samples + offset, n * sizeof *samples);
        first += (int64_t)n;
    }
    pthread_mutex_unlock(&reference_lock);
    return 1;
}

struct match { size_t offset; double score; };

/* Score changes in the signal as well as the waveform. The difference makes
 * independent low-pitched voices less likely to match the same lag. */
static struct match match_reference(const int16_t *ref, const float *mic, size_t n,
                                    size_t first, size_t last, size_t step)
{
    struct match best = { 0, 0 };
    for (size_t offset = first; offset <= last; offset += step) {
        double cross = 0, power = 0;
        for (size_t i = MATCH_STRIDE; i < n; i += MATCH_STRIDE) {
            int v = (int)ref[offset + i] - ref[offset + i - MATCH_STRIDE];
            cross += mic[i] * v;
            power += (float)v * v;
        }
        double score = cross * cross / (power + 1);
        if (score > best.score) best = (struct match){ offset, score };
    }
    return best;
}

static double sample_power(const float *pcm, size_t n)
{
    double power = 0;
    for (size_t i = 0; i < n; i++) power += pcm[i] * pcm[i];
    return power;
}

static int suppress_echo(unsigned run, uint64_t at, const int16_t *pcm, int16_t *out, size_t n)
{
    enum { LAST_OFFSET = MAX_DELAY + CLOCK_LEAD + MATCH_STRIDE };
    int16_t ref[REF_WINDOW];
    if (n < CAP_RATE / 100 || !capture_reference(run, at, ref, LAST_OFFSET + n)) return 0;

    float residual[CAPTURE_BLOCK];
    for (size_t i = 0; i < n; i++) residual[i] = pcm[i];
    double original = sample_power(residual, n);
    if (original < n * 16.0) return 0;

    /* Fit the dominant path, then nearby reflections. Orthogonalize each
     * path against the earlier ones so shared energy is only removed once. */
    float paths[PATHS][CAPTURE_BLOCK];
    double norms[PATHS];
    size_t first = 0, last = LAST_OFFSET;
    for (int path = 0; path < PATHS; path++) {
        float diff[CAPTURE_BLOCK];
        double power = 0;
        for (size_t i = MATCH_STRIDE; i < n; i += MATCH_STRIDE) {
            diff[i] = residual[i] - residual[i - MATCH_STRIDE];
            power += diff[i] * diff[i];
        }
        struct match best = match_reference(ref, diff, n, first, last, MATCH_STRIDE);
        if (best.score < power * (path ? 0.16 : 0.25) || best.score <= 0) break;
        size_t lo = best.offset >= MATCH_STRIDE ? best.offset - MATCH_STRIDE + 1 : 0;
        size_t hi = best.offset + MATCH_STRIDE - 1 < LAST_OFFSET ? best.offset + MATCH_STRIDE - 1 : LAST_OFFSET;
        best = match_reference(ref, diff, n, lo, hi, 1);
        if (!path) {
            first = best.offset > EARLY_REFLECTION ? best.offset - EARLY_REFLECTION : 0;
            last = best.offset + EARLY_REFLECTION < LAST_OFFSET ? best.offset + EARLY_REFLECTION : LAST_OFFSET;
        }

        float *echo = paths[path];
        for (size_t i = 0; i < n; i++) echo[i] = ref[best.offset + i];
        double candidate_power = sample_power(echo, n);
        for (int p = 0; p < path; p++) {
            double projection = 0;
            for (size_t i = 0; i < n; i++) projection += echo[i] * paths[p][i];
            float component = (float)(projection / norms[p]);
            for (size_t i = 0; i < n; i++) echo[i] -= component * paths[p][i];
        }
        double echo_power = sample_power(echo, n);
        if (echo_power < candidate_power * 0.01 || echo_power < 1) break;
        norms[path] = echo_power;
        double cross = 0;
        for (size_t i = 0; i < n; i++) cross += residual[i] * echo[i];
        if (cross * cross < echo_power * original * 0.008) break;
        float gain = (float)(cross / (echo_power + 1));
        for (size_t i = 0; i < n; i++) residual[i] -= gain * echo[i];
    }

    double left = sample_power(residual, n) / original;
    if (left > 0.86) return 0;
    /* A block explained almost entirely by the reference leaves little that
     * should enter the mic gain and wake the remote voice activity detector. */
    float gain = left < 0.40 ? (float)(left / 0.40) : 1;
    gain *= gain;
    for (size_t i = 0; i < n; i++) {
        float sample = residual[i] * gain;
        out[i] = (int16_t)lrintf(fmaxf(-32768, fminf(32767, sample)));
    }
    return 1;
}

int realtime_echo_capture(unsigned run, uint64_t at, const int16_t *pcm, int16_t *out, size_t frames)
{
    unsigned filtered = 0;
    for (size_t i = 0; i < frames; ) {
        size_t n = frames - i;
        if (n > CAPTURE_BLOCK) n = CAPTURE_BLOCK;
        if (suppress_echo(run, at + i, pcm + i, out + i, n)) filtered += (unsigned)n;
        else memcpy(out + i, pcm + i, n * sizeof *out);
        i += n;
    }
    return filtered != 0;
}
