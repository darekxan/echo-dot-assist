#define _XOPEN_SOURCE 700
#define _DARWIN_C_SOURCE
#include "board.h"
#include "ultrasound_motion.h"
#include <assert.h>
#include <limits.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

const struct board board = { .codename = "cannoli" };
static const char *dir;
static atomic_int changes, became_unavailable;

static void pause_ms(long ms)
{
    struct timespec delay = { .tv_sec = ms / 1000, .tv_nsec = ms % 1000 * 1000000 };
    nanosleep(&delay, NULL);
}

static void path(char file[PATH_MAX], const char *name)
{
    int size = snprintf(file, PATH_MAX, "%s/%s", dir, name);
    assert(size > 0 && size < PATH_MAX);
}

static void write_text(const char *name, const char *text)
{
    char file[PATH_MAX], temp[PATH_MAX];
    path(file, name);
    int size = snprintf(temp, sizeof temp, "%s.tmp.%ld", file, (long)getpid());
    assert(size > 0 && size < (int)sizeof temp);
    FILE *out = fopen(temp, "w");
    assert(out);
    assert(fputs(text, out) >= 0);
    assert(!fclose(out));
    assert(!rename(temp, file));
}

static int read_text(const char *name, char *text, int cap)
{
    char file[PATH_MAX];
    path(file, name);
    FILE *in = fopen(file, "r");
    if (!in) return 0;
    int ok = fgets(text, cap, in) != NULL;
    fclose(in);
    return ok;
}

static int mode(void)
{
    char out[8];
    return read_text("mode", out, sizeof out) ? atoi(out) : -1;
}

static void changed(void)
{
    atomic_fetch_add(&changes, 1);
    if (ultrasound_motion_state() < 0) atomic_fetch_add(&became_unavailable, 1);
}

static int change_count(void)
{
    return atomic_load(&changes);
}

static void wait_for(int (*check)(void), int expected)
{
    for (int attempt = 0; attempt < 160; ++attempt) {
        if (check() == expected) return;
        pause_ms(30);
    }
    fprintf(stderr, "expected %d, last value was %d\n", expected, check());
    abort();
}

static int simulate_lipc(const char *name, int argc, char **argv)
{
    dir = getenv("HASSMIC_FAKE_ULTRASOUND");
    assert(dir);
    if (!strcmp(name, "lipc-get-prop")) {
        assert(argc == 4 && !strcmp(argv[1], "-i"));
        assert(!strcmp(argv[2], "com.doppler.lasp") && !strcmp(argv[3], "LASP_CMD_SET_ULTRASOUND"));
        printf("%d\n", mode());
    } else if (!strcmp(name, "lipc-set-prop")) {
        assert(argc == 5 && !strcmp(argv[1], "-i"));
        assert(!strcmp(argv[2], "com.doppler.lasp") && !strcmp(argv[3], "LASP_CMD_SET_ULTRASOUND"));
        assert(!strcmp(argv[4], "0") || !strcmp(argv[4], "1"));
        write_text("mode", argv[4]);
    } else {
        assert(argc == 5 && !strcmp(argv[1], "-m") && !strcmp(argv[2], "-t"));
        assert(!strcmp(argv[3], "com.doppler.lasp") && !strcmp(argv[4], "notify_ultrasound_event"));
        int previous = -1;
        char last[64] = "";
        for (;;) {
            int current = mode();
            if (current == 1 && previous != 1) {
                puts("[11-25 18:43:30.404530] notify_ultrasound_event 0 0");
                fflush(stdout);
            }
            previous = current;
            char event[64];
            char disconnect[8];
            if (read_text("disconnect", disconnect, sizeof disconnect)) {
                char file[PATH_MAX];
                path(file, "disconnect");
                unlink(file);
                return 0;
            }
            if (current == 1 && read_text("event", event, sizeof event) && strcmp(event, last)) {
                printf("notify_ultrasound_event %s\n", event);
                fflush(stdout);
                strcpy(last, event);
            }
            pause_ms(20);
        }
    }
    return 0;
}

static void test_detector(void)
{
    int status = -1;
    int64_t sequence = -1;
    assert(ultrasound_event_parse("[11-25 18:43:30.404530] notify_ultrasound_event 0 0\n", &status, &sequence));
    assert(status == 0 && sequence == 0);
    assert(ultrasound_event_parse("notify_ultrasound_event 5 4294967295", &status, &sequence));
    assert(status == 5 && sequence == 4294967295LL);
    assert(ultrasound_event_parse("notify_ultrasound_event 6 -2147483648", &status, &sequence));
    assert(status == 6 && sequence == -2147483648LL);
    const char *bad[] = {
        "notify_ultrasound_event 3 1", "notify_ultrasound_event 7 3",
        "notify_ultrasound_event 8 1", "notify_ultrasound_event -1 0",
        "notify_ultrasound_event 0", "notify_ultrasound_event 2 1 5",
        "notify_ultrasound_event 0 1x", "not_notify_ultrasound_event 0 1",
        "notify_ultrasound_event 0 999999999999999999999", "[ notify_ultrasound_event 0 0"
    };
    for (size_t i = 0; i < sizeof bad / sizeof *bad; ++i)
        assert(!ultrasound_event_parse(bad[i], &status, &sequence));
    struct ultrasound_detector detector;
    ultrasound_detector_reset(&detector);
    assert(ultrasound_detector_tick(&detector, 1000) == -1);
    assert(ultrasound_detector_event(&detector, 7, 1000) == -1);
    assert(ultrasound_detector_event(&detector, 4, 1000) == 0);
    assert(ultrasound_detector_event(&detector, 2, 1010) == 0);
    assert(ultrasound_detector_event(&detector, 5, 1020) == 1);
    assert(ultrasound_detector_tick(&detector, 10000) == 1);
    assert(ultrasound_detector_event(&detector, 6, 10000) == 0);
    assert(ultrasound_detector_event(&detector, 1, 20000) == 1);
    assert(ultrasound_detector_event(&detector, 0, 22000) == 1);
    assert(ultrasound_detector_tick(&detector, 24999) == 1);
    assert(ultrasound_detector_tick(&detector, 25000) == 0);
    ultrasound_detector_reset(&detector);
    assert(ultrasound_detector_tick(&detector, 30000) == -1);
    puts("ultrasound event parsing and detector state passed");
}

static void test_presence(void)
{
    struct ultrasound_detector detector;
    ultrasound_detector_reset(&detector);
    assert(ultrasound_detector_tick(&detector, 0) == -1 && detector.presence == -1);
    assert(ultrasound_detector_event(&detector, 7, 50) == -1 && detector.presence == -1);
    assert(ultrasound_detector_event(&detector, 4, 100) == 0 && detector.presence == 0);
    assert(ultrasound_detector_event(&detector, 6, 200) == 0 && detector.presence == 1);
    assert(ultrasound_detector_tick(&detector, 60000) == 0 && detector.presence == 1);
    assert(ultrasound_detector_event(&detector, 0, 60100) == 0 && detector.presence == 1);
    assert(ultrasound_detector_event(&detector, 4, 80000) == 0 && detector.presence == 1);
    assert(ultrasound_detector_tick(&detector, 90099) == 0 && detector.presence == 1);
    assert(ultrasound_detector_tick(&detector, 90100) == 0 && detector.presence == 0);

    assert(ultrasound_detector_event(&detector, 5, 100000) == 1 && detector.presence == 1);
    assert(ultrasound_detector_event(&detector, 0, 101000) == 1 && detector.presence == 1);
    assert(ultrasound_detector_tick(&detector, 105000) == 0 && detector.presence == 1);
    assert(ultrasound_detector_event(&detector, 2, 130999) == 0 && detector.presence == 1);
    assert(ultrasound_detector_event(&detector, 0, 132000) == 0 && detector.presence == 1);
    assert(ultrasound_detector_tick(&detector, 161999) == 0 && detector.presence == 1);
    assert(ultrasound_detector_tick(&detector, 162000) == 0 && detector.presence == 0);
    ultrasound_detector_reset(&detector);
    assert(ultrasound_detector_tick(&detector, 200000) == -1 && detector.presence == -1);
    puts("ultrasound presence state and clear delay passed");
}

int main(int argc, char **argv)
{
    const char *name = strrchr(argv[0], '/');
    name = name ? name + 1 : argv[0];
    if (!strncmp(name, "lipc-", 5)) return simulate_lipc(name, argc, argv);
    test_detector();
    test_presence();
    char tmp[] = "/tmp/hassmic-ultrasound-XXXXXX", binary[PATH_MAX], file[PATH_MAX];
    dir = mkdtemp(tmp);
    assert(dir && realpath(argv[0], binary));
    const char *tools[] = { "lipc-get-prop", "lipc-set-prop", "lipc-wait-event" };
    for (size_t i = 0; i < sizeof tools / sizeof *tools; ++i) {
        path(file, tools[i]);
        assert(!symlink(binary, file));
    }
    assert(!setenv("HASSMIC_FAKE_ULTRASOUND", dir, 1));
    assert(!setenv("HASSMIC_STATE", dir, 1));
    assert(!setenv("HASSMIC_BOOT_ID", "/nonexistent/test-boot-id", 1));
    /* LASP persists mode independently of HassMic. A disabled restart
     * cleans up our claim, while an externally enabled mode is shared. */
    write_text("mode", "1");
    write_text("ultrasound-owner", "hassmic-ultrasound-owned 1\n");
    assert(ultrasound_motion_present());
#ifdef __linux__
    signal(SIGCHLD, SIG_IGN); /* match main.c */
#endif
    ultrasound_motion_start(NULL);
    wait_for(mode, 0);
    assert(ultrasound_motion_state() == -1);
    assert(ultrasound_presence_state() == -1);
    ultrasound_motion_start(changed);
    ultrasound_motion_enable(1);
    wait_for(mode, 1);
    wait_for(ultrasound_motion_state, 0);
    wait_for(ultrasound_presence_state, 0);
    wait_for(change_count, 1);
    write_text("event", "2 22");
    wait_for(ultrasound_presence_state, 1);
    wait_for(change_count, 2);
    assert(ultrasound_motion_state() == 0);
    write_text("event", "5 23");
    wait_for(ultrasound_motion_state, 1);
    write_text("disconnect", "1");
    wait_for(ultrasound_motion_state, -1);
    wait_for(ultrasound_presence_state, -1);
    wait_for(ultrasound_motion_state, 1);
    wait_for(ultrasound_presence_state, 1);
    assert(atomic_load(&became_unavailable) > 0);
    ultrasound_motion_mute(1);
    assert(ultrasound_motion_state() == -1);
    assert(ultrasound_presence_state() == -1);
    wait_for(mode, 0);
    ultrasound_motion_stop();

    write_text("event", "0 24");
    write_text("mode", "1");
    ultrasound_motion_mute(0);
    ultrasound_motion_start(changed);
    wait_for(ultrasound_motion_state, 0);
    wait_for(ultrasound_presence_state, 0);
    ultrasound_motion_stop();
    assert(mode() == 1);
    assert(ultrasound_motion_state() == -1);
    assert(ultrasound_presence_state() == -1);
    puts("ultrasound subscription, mute, reconnect and ownership passed");

    const char *files[] = { "mode", "event", "disconnect", "ultrasound-owner",
                           "lipc-get-prop", "lipc-set-prop", "lipc-wait-event" };
    for (size_t i = 0; i < sizeof files / sizeof *files; ++i) {
        path(file, files[i]);
        unlink(file);
    }
    assert(!rmdir(dir));
    return 0;
}
