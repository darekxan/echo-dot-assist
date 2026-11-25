/* Cannoli's stock LASP detector emits movement events without PuffinApp or a
 * raw capture client. The stock property has no owner or compare-and-set API.
 * Claim only a mode read as off and journal the claim before changing it. */
#define _GNU_SOURCE
#include "ultrasound_motion.h"
#include "board.h"
#include "statefile.h"
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#ifdef __linux__
#include <dirent.h>
#include <sys/prctl.h>
#include <sys/syscall.h>
#endif

#define LIPC_SOURCE "com.doppler.lasp"
#define MODE_PROPERTY "LASP_CMD_SET_ULTRASOUND"
#define EVENT_NAME "notify_ultrasound_event"
#define HOLD_MS 5000
#define PRESENCE_CLEAR_MS 30000
#define CHECK_MS 3000
#define CLI_MS 850
#define RETRY_MS 1600
#define POLL_MS 100
#define SUBSCRIBE_MS 250
#define INITIAL_EVENT_MS 10000

typedef void (*changed_fn)(void);
static const char ownership_record[] = "hassmic-ultrasound-owned 1\n";
static atomic_int enabled, muted, stopping, motion_state = -1, presence_state = -1;
static _Atomic(changed_fn) changed_cb;
static pthread_mutex_t worker_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_t worker;
static int started;

struct tools { char get[320], set[320], subscribe[320]; };
struct child {
    pid_t pid;
    int fd;
#ifdef __linux__
    int pidfd;
#endif
};
struct owner { char path[320], dir[288]; int claimed; };
struct event_stream {
    struct ultrasound_detector detector;
    char line[256];
    size_t used;
    int last_status;
};

static int64_t now_ms(void)
{
    struct timespec time;
    clock_gettime(CLOCK_MONOTONIC, &time);
    return (int64_t)time.tv_sec * 1000 + time.tv_nsec / 1000000;
}

static void sleep_ms(int ms)
{
    struct timespec dur = { .tv_sec = ms / 1000, .tv_nsec = (ms % 1000) * 1000000L };
    while (nanosleep(&dur, &dur) && errno == EINTR) {}
}

static int valid_status(int64_t status)
{
    return status >= 0 && status <= 6 && status != 3;
}

int ultrasound_event_parse(const char *line, int *status, int64_t *sequence)
{
    while (isspace((unsigned char)*line)) line++;
    if (*line == '[') {
        line = strchr(line, ']');
        if (!line) return 0;
        do { line++; } while (isspace((unsigned char)*line));
    }
    size_t len = sizeof EVENT_NAME - 1;
    if (strncmp(line, EVENT_NAME, len) || !isspace((unsigned char)line[len])) return 0;
    line += len;
    char *end;
    errno = 0;
    long long first = strtoll(line, &end, 10);
    if (errno || end == line || !isspace((unsigned char)*end) || !valid_status(first)) return 0;
    line = end;
    long long second = strtoll(line, &end, 10);
    if (errno || end == line || second < INT32_MIN || second > UINT32_MAX) return 0;
    while (isspace((unsigned char)*end)) end++;
    if (*end) return 0;
    *status = (int)first;
    *sequence = second;
    return 1;
}

void ultrasound_detector_reset(struct ultrasound_detector *detector)
{
    *detector = (struct ultrasound_detector){ .state = -1, .presence = -1 };
}

int ultrasound_detector_tick(struct ultrasound_detector *detector, int64_t now)
{
    if (detector->state > 0 && !detector->major && now >= detector->expires) detector->state = 0;
    if (detector->presence > 0 && !detector->movement && now >= detector->presence_expires) detector->presence = 0;
    return detector->state;
}

int ultrasound_detector_event(struct ultrasound_detector *detector, int status, int64_t now)
{
    if (!valid_status(status)) return detector->state;
    int major = status & 1;
    if (major) {
        detector->state = 1;
        if (!detector->major) detector->expires = now + HOLD_MS;
    } else if (detector->state < 0) detector->state = 0;
    detector->major = major;

    int movement = (status & 3) != 0; /* major or minor movement */
    if (movement) detector->presence = 1;
    else if (detector->movement) detector->presence_expires = now + PRESENCE_CLEAR_MS;
    else if (detector->presence < 0) detector->presence = 0;
    detector->movement = movement;
    return ultrasound_detector_tick(detector, now);
}

static int tool_path(char *out, size_t cap, const char *dir, const char *name)
{
    int n = snprintf(out, cap, "%s/%s", dir, name);
    return n > 0 && (size_t)n < cap && !access(out, X_OK);
}

static int find_tools(struct tools *tools)
{
    if (strcmp(board.codename, "cannoli")) return 0;
    const char *dir = "/system/bin";
#ifndef __ANDROID__
    dir = getenv("HASSMIC_FAKE_ULTRASOUND");
    if (!dir || !*dir) return 0;
#endif
    return tool_path(tools->get, sizeof tools->get, dir, "lipc-get-prop") &&
           tool_path(tools->set, sizeof tools->set, dir, "lipc-set-prop") &&
           tool_path(tools->subscribe, sizeof tools->subscribe, dir, "lipc-wait-event");
}

int ultrasound_motion_present(void)
{
    struct tools tools;
    return find_tools(&tools);
}

static int open_child(struct child *child, char *const argv[])
{
    int pipes[2];
#ifdef __linux__
    if (pipe2(pipes, O_CLOEXEC)) return -1;
    int gate[2];
    if (pipe2(gate, O_CLOEXEC)) { close(pipes[0]); close(pipes[1]); return -1; }
#else
    if (pipe(pipes)) return -1;
    fcntl(pipes[0], F_SETFD, FD_CLOEXEC);
    fcntl(pipes[1], F_SETFD, FD_CLOEXEC);
#endif
    pid_t parent = getpid();
    pid_t pid = fork();
    if (!pid) {
#ifdef __linux__
        /* Keep this PID alive until the parent owns a pidfd. SIGCHLD is
         * ignored by HassMic, so a PID alone cannot be used safely. */
        close(gate[1]);
        char go;
        ssize_t count;
        do { count = read(gate[0], &go, 1); } while (count < 0 && errno == EINTR);
        close(gate[0]);
        if (count != 1) _exit(126);
#endif
        gid_t group = getegid();
        if (setregid(group, group)) _exit(126); /* stock AIPC group, as in core_run */
#ifdef __linux__
        if (prctl(PR_SET_PDEATHSIG, SIGKILL, 0, 0, 0) || getppid() != parent) _exit(126);
#else
        (void)parent;
#endif
        close(pipes[0]);
        if (pipes[1] == STDOUT_FILENO) fcntl(STDOUT_FILENO, F_SETFD, 0);
        else if (dup2(pipes[1], STDOUT_FILENO) < 0) _exit(126);
        int null = open("/dev/null", O_WRONLY | O_CLOEXEC);
        if (null >= 0 && null != STDERR_FILENO) {
            dup2(null, STDERR_FILENO);
            close(null);
        }
        execv(argv[0], argv);
        _exit(127);
    }
    close(pipes[1]);
#ifdef __linux__
    close(gate[0]);
    int pidfd = pid > 0 ? (int)syscall(SYS_pidfd_open, pid, 0) : -1;
    ssize_t go = pidfd < 0 ? -1 : write(gate[1], "1", 1);
    close(gate[1]);
    if (go != 1) {
        if (pidfd >= 0) close(pidfd);
        if (pid > 0) while (waitpid(pid, NULL, 0) < 0 && errno == EINTR) {}
        close(pipes[0]);
        return -1;
    }
#else
    if (pid < 0) { close(pipes[0]); return -1; }
#endif
    *child = (struct child){ .pid = pid, .fd = pipes[0],
#ifdef __linux__
                             .pidfd = pidfd,
#endif
    };
    return 0;
}

static void close_child(struct child *child)
{
    if (child->fd < 0) return;
    close(child->fd);
#ifdef __linux__
    syscall(SYS_pidfd_send_signal, child->pidfd, SIGKILL, NULL, 0);
    close(child->pidfd);
#else
    /* Host tests on this path retain dead children until waitpid. */
    if (waitpid(child->pid, NULL, WNOHANG) != 0) { child->fd = -1; return; }
    kill(child->pid, SIGKILL);
#endif
    while (waitpid(child->pid, NULL, 0) < 0 && errno == EINTR) {}
    child->fd = -1;
}

static int run_cli(char *const argv[], char *out, size_t cap)
{
    struct child child = { .fd = -1 };
    if (open_child(&child, argv)) return 0;
    int64_t deadline = now_ms() + CLI_MS;
    int complete = 0;
    size_t used = 0;
    while (used + 1 < cap) {
        int64_t left = deadline - now_ms();
        if (left <= 0) break;
        struct pollfd ready = { child.fd, POLLIN, 0 };
        int rc = poll(&ready, 1, (int)left);
        if (rc < 0 && errno == EINTR) continue;
        if (rc <= 0) break;
        ssize_t count = read(child.fd, out + used, cap - 1 - used);
        if (count < 0 && errno == EINTR) continue;
        if (count <= 0) { complete = count == 0; break; }
        used += (size_t)count;
    }
    out[used] = 0;
    close_child(&child);
    return complete;
}

static int read_mode(struct tools *tools)
{
    char out[80], *end;
    char *argv[] = { tools->get, "-i", LIPC_SOURCE, MODE_PROPERTY, NULL };
    if (!run_cli(argv, out, sizeof out)) return -1;
    errno = 0;
    long mode = strtol(out, &end, 10);
    if (errno || end == out || mode < 0 || mode > INT_MAX) return -1;
    while (isspace((unsigned char)*end)) end++;
    return *end ? -1 : (int)mode;
}

static int write_mode(struct tools *tools, int on)
{
    char out[80];
    char *argv[] = { tools->set, "-i", LIPC_SOURCE, MODE_PROPERTY, on ? "1" : "0", NULL };
    run_cli(argv, out, sizeof out);
    return read_mode(tools);
}

/* A mixer restart invalidates the event subscription. The mixer may be hidden
 * by the stock proc policy, in which case LIPC transport and mode checks still
 * detect a lost subscription. Only rescan when the known mixer disappears. */
static uint64_t mixer_instance(void)
{
#ifdef __ANDROID__
    static long pid;
    if (pid < 0) return 0;
    if (pid) {
        char path[48];
        struct stat info;
        snprintf(path, sizeof path, "/proc/%ld", pid);
        if (!stat(path, &info)) return (uint64_t)info.st_ino;
    }
    DIR *proc = opendir("/proc");
    if (!proc) return 0;
    uint64_t instance = 0;
    struct dirent *entry;
    while ((entry = readdir(proc))) {
        if (!isdigit((unsigned char)entry->d_name[0])) continue;
        char path[64];
        if (snprintf(path, sizeof path, "/proc/%.16s/comm", entry->d_name) >= (int)sizeof path) continue;
        FILE *file = fopen(path, "re");
        if (!file) continue;
        char name[16];
        int mixer = fgets(name, sizeof name, file) && !strcmp(name, "mixer\n");
        fclose(file);
        if (!mixer) continue;
        snprintf(path, sizeof path, "/proc/%.16s", entry->d_name);
        struct stat info;
        if (!stat(path, &info)) {
            instance = (uint64_t)info.st_ino;
            pid = strtol(entry->d_name, NULL, 10);
        }
        break;
    }
    closedir(proc);
    if (!instance) pid = -1;
    return instance;
#else
    return 0;
#endif
}

static void forget_owner(struct owner *owner)
{
    owner->claimed = 0;
    if (owner->path[0] && unlink(owner->path) == 0) statefile_sync(owner->dir);
}

static void load_owner(struct owner *owner)
{
    const char *dir = getenv("HASSMIC_STATE");
    if (!dir) dir = "/data/local/hassmic/state";
    if (strlen(dir) >= sizeof owner->dir) return;
    strcpy(owner->dir, dir);
    snprintf(owner->path, sizeof owner->path, "%s/ultrasound-owner", owner->dir);
    int fd = open(owner->path, O_RDONLY | O_NOFOLLOW | O_CLOEXEC | O_NONBLOCK);
    if (fd < 0) return;
    char recorded[sizeof ownership_record];
    ssize_t count = read(fd, recorded, sizeof recorded);
    close(fd);
    owner->claimed = count == (ssize_t)(sizeof recorded - 1) &&
                     !memcmp(recorded, ownership_record, sizeof recorded - 1);
}

static int claim_mode(struct owner *owner)
{
    if (!owner->path[0] || statefile_write(owner->path, 0600, ownership_record, sizeof ownership_record - 1)) return 0;
    owner->claimed = 1;
    return 1;
}

static void release_mode(struct tools *tools, struct owner *owner)
{
    if (!owner->claimed) return;
    int mode = read_mode(tools);
    if (mode == 1) mode = write_mode(tools, 0);
    if (mode >= 0 && mode != 1) forget_owner(owner);
}

static int wanted(void)
{
    return atomic_load(&enabled) && !atomic_load(&muted) && !atomic_load(&stopping);
}

static void publish(int motion, int presence, const char *reason)
{
    int motion_changed = atomic_exchange(&motion_state, motion) != motion;
    int presence_changed = atomic_exchange(&presence_state, presence) != presence;
    if (!motion_changed && !presence_changed) return;
    if (motion_changed)
        fprintf(stderr, "ultrasound motion: %s (%s)\n",
                motion < 0 ? "unavailable" : motion ? "detected" : "clear", reason);
    if (presence_changed)
        fprintf(stderr, "ultrasound presence: %s (%s)\n",
                presence < 0 ? "unavailable" : presence ? "detected" : "clear", reason);
    changed_fn changed = atomic_load(&changed_cb);
    if (changed) changed();
}

static void event_bytes(struct event_stream *stream, const char *chunk, size_t count)
{
    for (size_t i = 0; i < count && wanted(); ++i) {
        if (chunk[i] == '\n') {
            if (stream->used < sizeof stream->line) {
                int status;
                int64_t sequence;
                stream->line[stream->used] = 0;
                if (ultrasound_event_parse(stream->line, &status, &sequence)) {
                    if (status != stream->last_status) {
                        fprintf(stderr, "ultrasound motion: stock status=%d sequence=%lld\n",
                                status, (long long)sequence);
                        stream->last_status = status;
                    }
                    int motion = ultrasound_detector_event(&stream->detector, status, now_ms());
                    publish(motion, stream->detector.presence, "stock event");
                }
            }
            stream->used = 0;
        } else if (stream->used < sizeof stream->line) stream->line[stream->used++] = chunk[i];
    }
}

static void subscribe(struct tools *tools, struct owner *owner, int mode)
{
    char *argv[] = { tools->subscribe, "-m", "-t", LIPC_SOURCE, EVENT_NAME, NULL };
    struct child child = { .fd = -1 };
    if (open_child(&child, argv)) return;
    struct pollfd ready = { child.fd, POLLIN, 0 };
    poll(&ready, 1, SUBSCRIBE_MS); /* allow the subscriber to register before enabling LASP */
    if (!wanted() || (ready.revents & (POLLERR | POLLHUP | POLLNVAL))) goto done;
    if (mode == 0) {
        if (!claim_mode(owner)) {
            fprintf(stderr, "ultrasound motion: could not record ownership\n");
            goto done;
        }
        mode = read_mode(tools);
        if (mode != 0 || !wanted()) {
            /* A different client may have enabled it while the journal was written. */
            forget_owner(owner);
            if (mode != 1 || !wanted()) goto done;
        } else if (write_mode(tools, 1) != 1) goto done;
    }
    fprintf(stderr, "ultrasound motion: subscribed (%s mode)\n", owner->claimed ? "owned" : "existing");
    struct event_stream stream = { .last_status = -1 };
    ultrasound_detector_reset(&stream.detector);
    int64_t check = now_ms() + CHECK_MS, first_event = now_ms() + INITIAL_EVENT_MS;
    uint64_t mixer = mixer_instance();
    char chunk[256];
    while (wanted()) {
        int rc = poll(&ready, 1, POLL_MS);
        if (rc < 0 && errno != EINTR) break;
        if (rc > 0 && (ready.revents & POLLIN)) {
            ssize_t bytes = read(child.fd, chunk, sizeof chunk);
            if (bytes == 0 || (bytes < 0 && errno != EINTR)) break;
            if (bytes > 0) event_bytes(&stream, chunk, (size_t)bytes);
        } else if (rc > 0 && (ready.revents & (POLLERR | POLLHUP | POLLNVAL))) break;
        if (!wanted()) break;
        int64_t now = now_ms();
        if (now >= check) {
            uint64_t current = mixer_instance();
            if ((mixer && current != mixer) || read_mode(tools) != 1) break;
            mixer = current;
            check = now_ms() + CHECK_MS;
        }
        if (owner->claimed && stream.detector.state < 0 && now >= first_event) break;
        int motion = ultrasound_detector_tick(&stream.detector, now);
        publish(motion, stream.detector.presence, "hold elapsed");
    }
done:
    close_child(&child);
    if (owner->claimed) release_mode(tools, owner);
    publish(-1, -1, wanted() ? "subscription ended" : "disabled or muted");
}

static void *detect(void *unused)
{
    (void)unused;
    struct tools tools;
    if (!find_tools(&tools)) return NULL;
    struct owner owner = {0};
    load_owner(&owner);
    int64_t retry = 0;
    while (!atomic_load(&stopping)) {
        if (now_ms() >= retry && (wanted() || owner.claimed)) {
            if (owner.claimed) release_mode(&tools, &owner);
            if (wanted()) {
                int mode = read_mode(&tools);
                if ((mode == 0 || mode == 1) && wanted()) subscribe(&tools, &owner, mode);
            }
            retry = now_ms() + RETRY_MS;
        }
        sleep_ms(POLL_MS);
    }
    release_mode(&tools, &owner);
    publish(-1, -1, "stopped");
    return NULL;
}

void ultrasound_motion_start(void (*changed)(void))
{
    if (changed) atomic_store(&changed_cb, changed);
    if (!ultrasound_motion_present()) return;
    pthread_mutex_lock(&worker_lock);
    if (!started) {
        atomic_store(&stopping, 0);
        if (pthread_create(&worker, NULL, detect, NULL) == 0) started = 1;
        else fprintf(stderr, "ultrasound motion: sensor worker could not start\n");
    }
    pthread_mutex_unlock(&worker_lock);
}

void ultrasound_motion_stop(void)
{
    pthread_mutex_lock(&worker_lock);
    if (started) {
        atomic_store(&stopping, 1);
        pthread_join(worker, NULL);
        started = 0;
    }
    atomic_store(&changed_cb, NULL);
    pthread_mutex_unlock(&worker_lock);
}

int ultrasound_motion_enable(int set)
{
    if (set >= 0) atomic_store(&enabled, set != 0);
    return atomic_load(&enabled);
}

void ultrasound_motion_mute(int value)
{
    atomic_store(&muted, value != 0);
}

int ultrasound_motion_state(void)
{
    return wanted() ? atomic_load(&motion_state) : -1;
}

int ultrasound_presence_state(void)
{
    return wanted() ? atomic_load(&presence_state) : -1;
}
