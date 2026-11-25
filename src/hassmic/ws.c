/* Minimal RFC 6455 WebSocket endpoint, both directions, blocking sockets.  No extensions, no subprotocols. */
#include "ws.h"
#include <fcntl.h>
#include <netdb.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
#include "hash.h"
#include "net.h"
#include "netio.h"

#define WS_GUID "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
#define WS_MAX_MESSAGE (4u << 20)

void ws_random(void *out, size_t len)
{
    int f = open("/dev/urandom", O_RDONLY);
    if (f < 0 || read(f, out, len) != (ssize_t)len) abort();        /* no randomness: nothing sane to do */
    close(f);
}

static int64_t ws_now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * INT64_C(1000) + ts.tv_nsec / 1000000;
}

static int read_ready(int fd, int64_t deadline)
{
    struct pollfd p = { .fd = fd, .events = POLLIN };
    for (;;) {
        int64_t remaining = deadline - ws_now_ms();
        int rc = poll(&p, 1, remaining > 0 ? (int)remaining : 0);
        if (rc >= 0 || errno != EINTR) return rc;
    }
}

static int read_exact(int fd, void *out, size_t n, int64_t deadline)
{
    if (deadline < 0) return read_full(fd, out, n) == (ssize_t)n ? 0 : -1;
    uint8_t *p = out;
    while (n) {
        if (ws_now_ms() >= deadline || read_ready(fd, deadline) != 1) return -1;
        ssize_t got = recv(fd, p, n, MSG_DONTWAIT);
        if (got > 0) {
            p += got;
            n -= (size_t)got;
        } else if (!got || (errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK)) {
            return -1;
        }
    }
    return 0;
}

static int read_headers(int fd, char *buf, size_t cap, int64_t deadline)
{
    size_t n = 0;
    while (n < cap - 1) {
        if (read_exact(fd, buf + n, 1, deadline)) return -1;
        n++;
        if (n >= 4 && !memcmp(buf + n - 4, "\r\n\r\n", 4)) { buf[n] = 0; return 0; }
    }
    return -1;
}

static const char *header(const char *hdrs, const char *name, char *out, size_t outsz)
{
    size_t nl = strlen(name);
    for (const char *p = hdrs; (p = strstr(p, "\r\n")); ) {
        p += 2;
        if (!strncasecmp(p, name, nl) && p[nl] == ':') {
            const char *v = p + nl + 1; size_t i = 0;
            while (*v == ' ') v++;
            while (*v && *v != '\r' && i < outsz - 1) out[i++] = *v++;
            out[i] = 0;
            return out;
        }
    }
    return NULL;
}

static int has_token(const char *headers, const char *name, const char *token)
{
    char value[128];
    if (!header(headers, name, value, sizeof value)) return 0;
    const char *p = value;
    while (*p) {
        p += strspn(p, " \t,");
        size_t n = strcspn(p, ",");
        while (n && (p[n - 1] == ' ' || p[n - 1] == '\t')) n--;
        if (n == strlen(token) && !strncasecmp(p, token, n)) return 1;
        p += strcspn(p, ",");
    }
    return 0;
}

static void accept_key(const char *key, char *out)
{
    char cat[128]; uint8_t d[20];
    snprintf(cat, sizeof cat, "%s" WS_GUID, key);
    sha1(cat, strlen(cat), d);
    b64_encode(d, 20, out, 0, 1);
}

static void ws_init(struct ws *w, int fd, int client)
{
    memset(w, 0, sizeof *w);
    w->fd = fd; w->client = client;
    pthread_mutex_init(&w->wlock, NULL);
}

int ws_accept(struct ws *w, int fd, char *path, size_t pathsz)
{
    char h[4096], key[128], acc[64], resp[256], origin[8];
    if (read_headers(fd, h, sizeof h, -1) || strncmp(h, "GET ", 4) || !header(h, "Sec-WebSocket-Key", key, sizeof key)) return -1;
    /* Only a browser sends Origin (Sendspin servers never do; the spec has clients refuse it): a page open anywhere on
     * the LAN would otherwise reach the endpoint */
    if (header(h, "Origin", origin, sizeof origin)) {
        static const char no[] = "HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
        write_all(fd, no, sizeof no - 1);
        return -1;
    }
    if (path) { size_t i = 0; for (const char *p = h + 4; *p && *p != ' ' && i < pathsz - 1; p++) path[i++] = *p; path[i] = 0; }
    accept_key(key, acc);
    int n = snprintf(resp, sizeof resp, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: %s\r\n\r\n", acc);
    if (write_all(fd, resp, n) < 0) return -1;
    ws_init(w, fd, 0);
    return 0;
}

int ws_connect(struct ws *w, const char *host, int port, const char *path)
{
    char ports[8], req[512], h[4096], key[32], want[64], got[64]; uint8_t nonce[16];
    snprintf(ports, sizeof ports, "%d", port);
    int fd = net_connect(host, ports, 5);
    if (fd < 0) return -1;
    ws_random(nonce, sizeof nonce); b64_encode(nonce, 16, key, 0, 1);
    int v6 = strchr(host, ':') != NULL;                         /* Host: [v6]:port */
    int n = snprintf(req, sizeof req, "GET %s HTTP/1.1\r\nHost: %s%s%s:%d\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                     "Sec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n\r\n", path, v6 ? "[" : "", host, v6 ? "]" : "", port, key);
    accept_key(key, want);
    if (write_all(fd, req, n) < 0 || read_headers(fd, h, sizeof h, -1) || !strstr(h, " 101") ||
        !header(h, "Sec-WebSocket-Accept", got, sizeof got) || strcmp(got, want)) { close(fd); return -1; }
    struct timeval none = { 0, 0 };                     /* an idle connection is fine once it is up */
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &none, sizeof none);
    ws_init(w, fd, 1);
    return 0;
}

int ws_upgrade_client(struct ws *w, int fd, const char *authority, const char *path,
                      const char *headers, int timeout_ms)
{
    char request[4096], response[4096], key[32], want[64], got[64];
    uint8_t nonce[16];
    ws_random(nonce, sizeof nonce);
    b64_encode(nonce, sizeof nonce, key, 0, 1);
    accept_key(key, want);
    int n = snprintf(request, sizeof request,
                     "GET %s HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                     "Sec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n%s\r\n",
                     path, authority, key, headers);
    const int64_t deadline = ws_now_ms() + timeout_ms;
    if (n < 0 || (size_t)n >= sizeof request || write_all(fd, request, (size_t)n) < 0 ||
        read_headers(fd, response, sizeof response, deadline) ||
        strncmp(response, "HTTP/1.1 101", 12) || (response[12] != ' ' && response[12] != '\r') ||
        !has_token(response, "Upgrade", "websocket") || !has_token(response, "Connection", "upgrade") ||
        !header(response, "Sec-WebSocket-Accept", got, sizeof got) || strcmp(got, want)) {
        close(fd);
        return -1;
    }
    ws_init(w, fd, 1);
    return 0;
}

static int send_frame(struct ws *w, int opcode, const void *data, size_t len)
{
    uint8_t h[14], mask[4]; size_t hl = 2; int rc = 0;
    h[0] = 0x80 | opcode;
    if (len < 126) h[1] = len; else if (len < 65536) { h[1] = 126; h[2] = len >> 8; h[3] = len; hl = 4; }
    else { h[1] = 127; for (int i = 0; i < 8; i++) h[2 + i] = (uint64_t)len >> (56 - 8 * i); hl = 10; }
    pthread_mutex_lock(&w->wlock);
    if (w->client) {                                    /* client frames are masked */
        uint8_t *m = malloc(len ? len : 1);
        if (!m) { pthread_mutex_unlock(&w->wlock); return -1; }
        ws_random(mask, 4); h[1] |= 0x80; memcpy(h + hl, mask, 4); hl += 4;
        for (size_t i = 0; i < len; i++) m[i] = ((const uint8_t *)data)[i] ^ mask[i & 3];
        rc = write_all(w->fd, h, hl) < 0 || (len && write_all(w->fd, m, len) < 0) ? -1 : 0;
        free(m);
    } else rc = write_all(w->fd, h, hl) < 0 || (len && write_all(w->fd, data, len) < 0) ? -1 : 0;
    pthread_mutex_unlock(&w->wlock);
    return rc;
}

int ws_send(struct ws *w, int opcode, const void *data, size_t len) { return send_frame(w, opcode, data, len); }

#define WS_HEAD 256                                     /* what is kept of a message too big: enough for its id */

/* n bytes of the socket read and dropped: 0, or -1 */
static int skip(int fd, uint64_t n)
{
    uint8_t b[4096];
    while (n) { size_t k = n < sizeof b ? (size_t)n : sizeof b; if (read_full(fd, b, k) != (ssize_t)k) return -1; n -= k; }
    return 0;
}

static int recv_message(struct ws *w, int timeout_ms, int frame_timeout_ms,
                        int *opcode, uint8_t **data, size_t *len)
{
    size_t total = 0; int first_op = 0, over = 0;
    int64_t idle = timeout_ms >= 0 ? ws_now_ms() + timeout_ms : -1, deadline = -1;
    for (;;) {
        uint8_t h[2], ext[8], mask[4]; uint64_t n;
        if (idle >= 0 && deadline < 0) {
            int ready = read_ready(w->fd, idle);
            if (ready <= 0) return ready == 0 ? 3 : -1;
            deadline = ws_now_ms() + frame_timeout_ms;
        }
        if (read_exact(w->fd, h, 2, deadline)) return 0;
        int fin = h[0] & 0x80, op = h[0] & 0x0f, masked = h[1] & 0x80;
        n = h[1] & 0x7f;
        if (idle >= 0 && ((h[0] & 0x70) || masked ||
            (op < 8 ? (first_op ? op != 0 : op != WS_TEXT && op != WS_BINARY)
                    : (!fin || op > WS_PONG)))) return -1;
        if (n == 126) { if (read_exact(w->fd, ext, 2, deadline)) return 0; n = ext[0] << 8 | ext[1]; }
        else if (n == 127) { if (read_exact(w->fd, ext, 8, deadline)) return 0; n = 0; for (int i = 0; i < 8; i++) n = n << 8 | ext[i]; }
        if (masked && read_exact(w->fd, mask, 4, deadline)) return 0;
        if (op >= 8 && n > 125) return -1;                /* a control frame that long is no frame */
        if (op < 8 && (over || n > WS_MAX_MESSAGE || total + n > WS_MAX_MESSAGE)) {   /* too big: read to its end, its head kept */
            if (idle >= 0) return -1;
            if (n > 16 * (uint64_t)WS_MAX_MESSAGE || total + n > 16 * (uint64_t)WS_MAX_MESSAGE) return -1;   /* (no frame is that; the message may be any length) */
            if (total > WS_HEAD) total = WS_HEAD;       /* earlier fragments may have brought up to 4 MB: only the head stays */
            size_t keep = total < WS_HEAD ? WS_HEAD - total : 0; if (keep > n) keep = (size_t)n;
            if (total + keep + 1 > w->cap) { uint8_t *nb = realloc(w->buf, total + keep + 1); if (!nb) return -1; w->buf = nb; w->cap = total + keep + 1; }
            if (keep && read_full(w->fd, w->buf + total, keep) != (ssize_t)keep) return 0;
            if (masked) for (size_t i = 0; i < keep; i++) w->buf[total + i] ^= mask[i & 3];
            if (skip(w->fd, n - keep)) return 0;
            total += keep; over = 1;
            if (op) first_op = op;
            if (fin) { w->buf[total] = 0; *opcode = first_op; *data = w->buf; *len = total; return 2; }
            continue;
        }
        if (total + n + 1 > w->cap) {
            size_t cap = total + n + 1 + 4096; uint8_t *nb = realloc(w->buf, cap);
            if (!nb) return -1;
            w->buf = nb; w->cap = cap;
        }
        uint8_t *dst = w->buf + total;
        if (n && read_exact(w->fd, dst, n, deadline)) return 0;
        if (masked) for (uint64_t i = 0; i < n; i++) dst[i] ^= mask[i & 3];

        if (op >= 8) {                                  /* control frames may interleave with a fragmented message */
            if (op == WS_PING && send_frame(w, WS_PONG, dst, n) < 0 && idle >= 0) return -1;
            else if (op == WS_CLOSE) { send_frame(w, WS_CLOSE, dst, n < 2 ? 0 : 2); return 0; }
            if (!first_op) {
                if (idle >= 0 && ws_now_ms() >= idle) return 3;
                deadline = -1;
            }
            continue;
        }
        if (op) first_op = op;
        total += n;
        if (fin) { w->buf[total] = 0; *opcode = first_op; *data = w->buf; *len = total; return 1; }
    }
}

int ws_recv(struct ws *w, int *opcode, uint8_t **data, size_t *len)
{
    return recv_message(w, -1, 0, opcode, data, len);
}

int ws_recv_timed(struct ws *w, int timeout_ms, int frame_timeout_ms,
                  int *opcode, uint8_t **data, size_t *len)
{
    if (timeout_ms < 0 || frame_timeout_ms <= 0) return -1;
    int rc = recv_message(w, timeout_ms, frame_timeout_ms, opcode, data, len);
    return rc == 1 ? 1 : rc == 3 ? 0 : -1;
}

void ws_close(struct ws *w)
{
    if (w->fd >= 0) { send_frame(w, WS_CLOSE, "\x03\xe8", 2); shutdown(w->fd, SHUT_RDWR); close(w->fd); w->fd = -1; }
    free(w->buf); w->buf = NULL; w->cap = 0;
    pthread_mutex_destroy(&w->wlock);
}
