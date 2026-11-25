/*
 * Media over HTTPS (issue #30).  Home Assistant builds the URLs of TTS replies, announcements and play_media with
 * get_url(): when it serves HTTPS itself (http: ssl_certificate), that prefers the external URL and never hands out a
 * plain http:// one.  proto_esphome.c's own HTTP/1.0 client stays for http://; https:// goes through the firmware's
 * libcurl 7.50.1 (dlopen'ed, as in update.c and davs.c: OpenSSL and the system CA store, /system/etc/security/cacerts).
 * A thread runs the transfer and writes the body into one end of a socketpair; the other end goes to the decoders, so
 * they read it as they read a plain fetch, and the media player's cancel, a shutdown() of that socket, ends the
 * transfer too (the next write fails, and a write callback that returns short stops curl).
 *
 * The certificate is checked by default (system CAs, and the name).  With HTTPS on, Home Assistant hands out no URL by
 * IP address (get_url: allow_ip false), so a public certificate passes as long as the Echo reaches that name; a
 * self-signed one does not: setting "https_verify" off accepts any certificate (the page says what that costs).
 */
#include "https.h"
#include "net.h"
#include <dlfcn.h>
#include <errno.h>
#include <poll.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>
#include <arpa/inet.h>

static atomic_int verify = 1;
int https_verify(int set) { if (set >= 0) atomic_store(&verify, !!set); return atomic_load(&verify); }

/* ---------------------------------------------------------------- libcurl, from the firmware */

enum { OPT_WRITEDATA = 10001, OPT_URL = 10002, OPT_PROXY = 10004, OPT_POSTFIELDS = 10015,
       OPT_USERAGENT = 10018, OPT_HTTPHEADER = 10023, OPT_LOW_SPEED_LIMIT = 19, OPT_LOW_SPEED_TIME = 20,
       OPT_HEADERDATA = 10029, OPT_FAILONERROR = 45, OPT_FOLLOWLOCATION = 52, OPT_SSL_VERIFYPEER = 64,
       OPT_POSTFIELDSIZE = 60, OPT_MAXREDIRS = 68, OPT_CONNECTTIMEOUT = 78, OPT_SSL_VERIFYHOST = 81,
       OPT_HTTP_VERSION = 84, OPT_NOSIGNAL = 99, OPT_CONNECT_ONLY = 141, OPT_CAINFO = 10065,
       OPT_TIMEOUT_MS = 155, OPT_CONNECTTIMEOUT_MS = 156,
       OPT_PROTOCOLS = 181, OPT_REDIR_PROTOCOLS = 182, OPT_RESOLVE = 10203, OPT_WRITEFUNCTION = 20011,
       OPT_HEADERFUNCTION = 20079, OPT_OPENSOCKETFUNCTION = 20163, OPT_OPENSOCKETDATA = 10164,
       INFO_RESPONSE_CODE = 0x200002, INFO_ACTIVESOCKET = 0x500000 + 44,
       PROTO_HTTP = 1, PROTO_HTTPS = 2, PROTO_HTTP_S = PROTO_HTTP | PROTO_HTTPS,
       CURL_HTTP_1_1 = 2, CURLE_WRITE_ERROR = 23, CURLE_PEER_FAILED_VERIFICATION = 51,
       CURLE_SSL_CACERT = 60, CURLE_AGAIN = 81 };
struct curl_sockaddr { int family, socktype, protocol; unsigned addrlen; struct sockaddr_storage addr; };
struct curl_slist;

static struct {
    void *(*init)(void);
    int (*setopt)(void *, int, ...);
    int (*perform)(void *);
    void (*cleanup)(void *);
    int (*getinfo)(void *, int, ...);
    const char *(*strerror)(int);
    struct curl_slist *(*slist_append)(struct curl_slist *, const char *);
    void (*slist_free_all)(struct curl_slist *);
    int (*send)(void *, const void *, size_t, size_t *);
    int (*recv)(void *, void *, size_t, size_t *);
} curl;
static int curl_ok;
static pthread_once_t curl_once = PTHREAD_ONCE_INIT;

static void curl_load(void)
{
    void *h;
    if (!(h = dlopen("libcurl.so", RTLD_NOW)) && !(h = dlopen("libcurl.so.4", RTLD_NOW)) &&
        !(h = dlopen("libcurl.4.dylib", RTLD_NOW))) {    /* Echo, Linux and macOS */
        fprintf(stderr, "media: no libcurl for https (%s)\n", dlerror()); return;
    }
    curl.init = (void *(*)(void))dlsym(h, "curl_easy_init");
    curl.setopt = (int (*)(void *, int, ...))dlsym(h, "curl_easy_setopt");
    curl.perform = (int (*)(void *))dlsym(h, "curl_easy_perform");
    curl.cleanup = (void (*)(void *))dlsym(h, "curl_easy_cleanup");
    curl.getinfo = (int (*)(void *, int, ...))dlsym(h, "curl_easy_getinfo");
    curl.strerror = (const char *(*)(int))dlsym(h, "curl_easy_strerror");
    curl.slist_append = (struct curl_slist *(*)(struct curl_slist *, const char *))dlsym(h, "curl_slist_append");
    curl.slist_free_all = (void (*)(struct curl_slist *))dlsym(h, "curl_slist_free_all");
    curl.send = (int (*)(void *, const void *, size_t, size_t *))dlsym(h, "curl_easy_send");
    curl.recv = (int (*)(void *, void *, size_t, size_t *))dlsym(h, "curl_easy_recv");
    curl_ok = curl.init && curl.setopt && curl.perform && curl.cleanup && curl.getinfo && curl.strerror
              && curl.slist_append && curl.slist_free_all;
}

static int private4(struct in_addr a)
{
    uint32_t ip = ntohl(a.s_addr);
    return ip >> 24 == 10 || ip >> 24 == 127 || ip >> 20 == 0xac1 ||
           ip >> 16 == 0xc0a8 || ip >> 16 == 0xa9fe || ip >> 22 == 0x191;
}

static int private_address(const struct curl_sockaddr *a)
{
    if (a->family == AF_INET && a->addrlen >= sizeof(struct sockaddr_in)) {
        struct sockaddr_in v4;
        memcpy(&v4, &a->addr, sizeof v4);
        return private4(v4.sin_addr);
    }
    if (a->family == AF_INET6 && a->addrlen >= sizeof(struct sockaddr_in6)) {
        struct sockaddr_in6 v6;
        memcpy(&v6, &a->addr, sizeof v6);
        if (IN6_IS_ADDR_V4MAPPED(&v6.sin6_addr)) {
            struct in_addr mapped;
            memcpy(&mapped, v6.sin6_addr.s6_addr + 12, sizeof mapped);
            return private4(mapped);
        }
        return IN6_IS_ADDR_LOOPBACK(&v6.sin6_addr) || IN6_IS_ADDR_LINKLOCAL(&v6.sin6_addr) ||
               (v6.sin6_addr.s6_addr[0] & 0xfe) == 0xfc;
    }
    return 0;
}

static int on_socket(void *arg, int purpose, struct curl_sockaddr *a)
{
    (void)purpose;
    if (arg && !private_address(a)) return -1;
    return net_socket(a->family, a->socktype, a->protocol);   /* the egress lock lets this group out: an external URL */
}

/* ---------------------------------------------------------------- one transfer */

struct xfer {
    pthread_mutex_t m; pthread_cond_t c;
    int state;                          /* 0 connecting, 1 the body comes (https_open returns), -1 failed */
    int refs;                           /* https_open and the thread: the last one frees it */
    int wfd;                            /* our end of the socketpair */
    void *h; struct curl_slist *resolve;
    char url[1024];
};

static void set_state(struct xfer *x, int s)
{
    pthread_mutex_lock(&x->m);
    if (!x->state) { x->state = s; pthread_cond_signal(&x->c); }
    pthread_mutex_unlock(&x->m);
}

static void put(struct xfer *x)
{
    pthread_mutex_lock(&x->m); int last = !--x->refs; pthread_mutex_unlock(&x->m);
    if (!last) return;
    pthread_mutex_destroy(&x->m); pthread_cond_destroy(&x->c);
    free(x);
}

/* The end of a response's headers: a 2xx one is the body's (redirects have their own; FAILONERROR fails 4xx and up),
 * and https_open may return.  The caller learns of a failure before it has a socket, as with a plain fetch. */
static size_t on_header(char *d, size_t size, size_t n, void *arg)
{
    struct xfer *x = arg; long code = 0;
    if (size * n <= 2 && (d[0] == '\r' || d[0] == '\n')) {
        curl.getinfo(x->h, INFO_RESPONSE_CODE, &code);
        if (code >= 200 && code < 300) set_state(x, 1);
    }
    return size * n;
}

static size_t on_data(char *d, size_t size, size_t n, void *arg)
{
    struct xfer *x = arg; size_t len = size * n, off = 0;
    set_state(x, 1);
    while (off < len) {                 /* blocks while the decoders hold back: the TTS queue is full */
        ssize_t w = send(x->wfd, d + off, len - off, MSG_NOSIGNAL);
        if (w <= 0) return 0;           /* the reader hung up (cancel, or it played what it wanted): stop */
        off += (size_t)w;
    }
    return len;
}

static void *run(void *arg)
{
    struct xfer *x = arg;
    int rc = curl.perform(x->h);
    pthread_mutex_lock(&x->m); int began = x->state == 1; pthread_mutex_unlock(&x->m);
    if (rc && !(rc == CURLE_WRITE_ERROR && began))     /* a write error once the body came: the reader stopped it */
        fprintf(stderr, "media: %s: %s%s\n", x->url, curl.strerror(rc),
                rc == CURLE_PEER_FAILED_VERIFICATION || rc == CURLE_SSL_CACERT
                    ? " (the settings page's \"Check HTTPS certificates\" off accepts it)" : "");
    set_state(x, -1);
    close(x->wfd);                      /* the reader's EOF */
    curl.cleanup(x->h);
    if (x->resolve) curl.slist_free_all(x->resolve);
    put(x);
    return NULL;
}

/* A ".local" host (homeassistant.local) by our own mDNS lookup, as net_connect does for http://: neither the system
 * resolver nor this libcurl asks mDNS.  The certificate is still checked against the name. */
static struct curl_slist *resolve_local(const char *url)
{
    const char *scheme = strstr(url, "://");
    if (!scheme) return NULL;
    char host[256], entry[300], ip[INET_ADDRSTRLEN];
    const char *p = scheme + 3, *end = p + strcspn(p, "/?#"), *colon;
    const char *at = memchr(p, '@', (size_t)(end - p));
    if (at) p = at + 1;
    if (*p == '[') return NULL;                         /* an IPv6 address */
    colon = memchr(p, ':', (size_t)(end - p));
    size_t hl = (size_t)((colon ? colon : end) - p);
    if (hl < 7 || hl >= sizeof host || strncasecmp(p + hl - 6, ".local", 6)) return NULL;
    memcpy(host, p, hl); host[hl] = 0;
    unsigned a = mdns_resolve4(host);
    if (!a || !inet_ntop(AF_INET, &a, ip, sizeof ip)) return NULL;     /* curl tries DNS: some servers serve .local */
    const char *port = colon ? colon + 1 : (scheme - url == 4 ? "80" : "443");
    snprintf(entry, sizeof entry, "%s:%.*s:%s", host, colon ? (int)(end - port) : (int)strlen(port), port, ip);
    return curl.slist_append(NULL, entry);
}

int https_open(const char *url)
{
    pthread_once(&curl_once, curl_load);
    if (!curl_ok) { fprintf(stderr, "media: https is not available: %s\n", url); return -1; }
    int sv[2]; struct xfer *x = calloc(1, sizeof *x); pthread_t t; pthread_attr_t at;
    if (!x) return -1;
    if (socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, sv)) { free(x); return -1; }
    if (!(x->h = curl.init())) { close(sv[0]); close(sv[1]); free(x); return -1; }
    pthread_mutex_init(&x->m, NULL); pthread_cond_init(&x->c, NULL);
    x->refs = 2; x->wfd = sv[1];
    snprintf(x->url, sizeof x->url, "%s", url);
    void *h = x->h; int v = https_verify(-1);
    curl.setopt(h, OPT_URL, x->url);
    curl.setopt(h, OPT_USERAGENT, "hassmic/" VERSION);
    curl.setopt(h, OPT_PROTOCOLS, (long)PROTO_HTTP_S); curl.setopt(h, OPT_REDIR_PROTOCOLS, (long)PROTO_HTTP_S);
    curl.setopt(h, OPT_FOLLOWLOCATION, 1L); curl.setopt(h, OPT_MAXREDIRS, 5L);
    curl.setopt(h, OPT_FAILONERROR, 1L); curl.setopt(h, OPT_NOSIGNAL, 1L);
    /* as the plain fetch: 4 s per connect there (TLS adds its handshake here), 15 s without data (TTS may still be
     * rendering); no limit on the whole: play_media may be a long file */
    curl.setopt(h, OPT_CONNECTTIMEOUT, 10L);
    curl.setopt(h, OPT_LOW_SPEED_LIMIT, 1L); curl.setopt(h, OPT_LOW_SPEED_TIME, 15L);
    curl.setopt(h, OPT_SSL_VERIFYPEER, v ? 1L : 0L); curl.setopt(h, OPT_SSL_VERIFYHOST, v ? 2L : 0L);
    if (getenv("HASSMIC_CA_FILE")) curl.setopt(h, OPT_CAINFO, getenv("HASSMIC_CA_FILE"));     /* tests: their own CA */
    if ((x->resolve = resolve_local(url))) curl.setopt(h, OPT_RESOLVE, x->resolve);
    curl.setopt(h, OPT_HEADERFUNCTION, on_header); curl.setopt(h, OPT_HEADERDATA, x);
    curl.setopt(h, OPT_WRITEFUNCTION, on_data); curl.setopt(h, OPT_WRITEDATA, x);
    curl.setopt(h, OPT_OPENSOCKETFUNCTION, on_socket); curl.setopt(h, OPT_OPENSOCKETDATA, NULL);
    pthread_attr_init(&at); pthread_attr_setdetachstate(&at, PTHREAD_CREATE_DETACHED);
    if (pthread_create(&t, &at, run, x)) {
        pthread_attr_destroy(&at); close(sv[0]); close(sv[1]); curl.cleanup(h);
        if (x->resolve) curl.slist_free_all(x->resolve);
        pthread_mutex_destroy(&x->m); pthread_cond_destroy(&x->c); free(x);
        return -1;
    }
    pthread_attr_destroy(&at);
    pthread_mutex_lock(&x->m);
    while (!x->state) pthread_cond_wait(&x->c, &x->m);
    int ok = x->state == 1;
    pthread_mutex_unlock(&x->m);
    put(x);
    if (!ok) { close(sv[0]); return -1; }
    return sv[0];
}

/* Only these clients carry credentials. Neither a redirect nor a configured proxy may receive them.
 * The media player's certificate setting does not apply. */
static int authenticated_options(void *h, const char *url, unsigned timeout_ms, int *private_only)
{
    const char *ca = getenv("HASSMIC_CA_FILE");
    return curl.setopt(h, OPT_URL, url) ||
           curl.setopt(h, OPT_PROXY, "") ||
           curl.setopt(h, OPT_PROTOCOLS, (long)(private_only ? PROTO_HTTP : PROTO_HTTPS)) ||
           curl.setopt(h, OPT_FOLLOWLOCATION, 0L) ||
           curl.setopt(h, OPT_NOSIGNAL, 1L) ||
           curl.setopt(h, OPT_TIMEOUT_MS, (long)timeout_ms) ||
           curl.setopt(h, OPT_CONNECTTIMEOUT_MS, (long)timeout_ms) ||
           curl.setopt(h, OPT_SSL_VERIFYPEER, 1L) ||
           curl.setopt(h, OPT_SSL_VERIFYHOST, 2L) ||
           (ca && curl.setopt(h, OPT_CAINFO, ca)) ||
           curl.setopt(h, OPT_OPENSOCKETFUNCTION, on_socket) ||
           curl.setopt(h, OPT_OPENSOCKETDATA, private_only);
}

/* Connect-only works on the Fire OS libcurl before its WebSocket API. Keep that curl handle on
 * one thread and expose its decrypted, full-duplex stream to the existing WebSocket framing. */
struct tls_tunnel {
    void *curl_handle;
    int local, remote;
};

struct pipe_bytes {
    uint8_t data[8192];
    size_t offset, len;
};

static int transient_socket_error(void)
{
    return errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK;
}

static void *tunnel_run(void *arg)
{
    struct tls_tunnel *t = arg;
    struct pipe_bytes input = { 0 }, output = { 0 };
    for (;;) {
        int moved = 0;
        if (output.len) {
            ssize_t n = send(t->local, output.data + output.offset, output.len, MSG_DONTWAIT | MSG_NOSIGNAL);
            if (n < 0 && !transient_socket_error()) break;
            if (n > 0) { output.offset += (size_t)n; output.len -= (size_t)n; moved = 1; }
        }
        if (input.len) {
            size_t n = 0;
            int rc = curl.send(t->curl_handle, input.data + input.offset, input.len, &n);
            if (rc != 0 && rc != CURLE_AGAIN) break;
            if (n) { input.offset += n; input.len -= n; moved = 1; }
        }
        if (!input.len) {
            ssize_t n = recv(t->local, input.data, sizeof input.data, MSG_DONTWAIT);
            if (!n || (n < 0 && !transient_socket_error())) break;
            if (n > 0) { input.offset = 0; input.len = (size_t)n; moved = 1; }
        }
        if (!output.len) {
            size_t n = 0;
            int rc = curl.recv(t->curl_handle, output.data, sizeof output.data, &n);
            if (rc != CURLE_AGAIN && (rc != 0 || !n)) break;
            if (n) { output.offset = 0; output.len = n; moved = 1; }
        }
        if (moved) continue;
        struct pollfd fds[] = {
            { .fd = t->local, .events = (input.len ? 0 : POLLIN) | (output.len ? POLLOUT : 0) },
            { .fd = output.len && !input.len ? -1 : t->remote,
              .events = (output.len ? 0 : POLLIN) | (input.len ? POLLOUT : 0) }
        };
        int rc = poll(fds, 2, -1);
        if (rc < 0 && errno != EINTR) break;
        if (rc > 0 && (fds[0].revents & (POLLERR | POLLHUP | POLLNVAL))) break;
        if (rc > 0 && (fds[1].revents & (POLLERR | POLLNVAL))) break;
        if (rc > 0 && (fds[1].revents & POLLHUP) && !(fds[1].revents & POLLIN)) break;
    }
    close(t->local);
    curl.cleanup(t->curl_handle);
    free(t);
    return NULL;
}

int https_tunnel_open(const char *url, unsigned timeout_ms)
{
    if (!url || strncmp(url, "https://", 8) || !timeout_ms) return -1;
    pthread_once(&curl_once, curl_load);
    if (!curl_ok || !curl.send || !curl.recv) return -1;
    void *h = curl.init();
    if (!h) return -1;
    struct tls_tunnel *t = NULL;
    int sv[2] = { -1, -1 }, remote = -1;
    if (authenticated_options(h, url, timeout_ms, NULL) ||
        curl.setopt(h, OPT_HTTP_VERSION, (long)CURL_HTTP_1_1) ||
        curl.setopt(h, OPT_CONNECT_ONLY, 1L) || curl.perform(h) ||
        curl.getinfo(h, INFO_ACTIVESOCKET, &remote) || remote < 0 ||
        socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, sv)) goto fail;
    struct timeval send_timeout = { .tv_sec = 1 };
    if (setsockopt(sv[0], SOL_SOCKET, SO_SNDTIMEO, &send_timeout, sizeof send_timeout)) goto fail;
    t = malloc(sizeof *t);
    if (!t) goto fail;
    *t = (struct tls_tunnel){ .curl_handle = h, .local = sv[1], .remote = remote };
    pthread_t thread;
    if (pthread_create(&thread, NULL, tunnel_run, t)) goto fail;
    pthread_detach(thread);
    return sv[0];

fail:
    if (sv[0] >= 0) close(sv[0]);
    if (sv[1] >= 0) close(sv[1]);
    free(t);
    curl.cleanup(h);
    return -1;
}

#define MCP_MAX_BODY (512u * 1024)

struct post_response {
    struct rt_http_response data;
    size_t cap;
};

static size_t post_data(char *data, size_t size, size_t n, void *arg)
{
    struct post_response *result = arg;
    if (size && n > (MCP_MAX_BODY - result->data.len) / size) return 0;
    size_t len = size * n, need = result->data.len + len + 1;
    if (need > result->cap) {
        size_t cap = need < 4096 ? 4096 : need;
        if (cap <= (MCP_MAX_BODY + 1) / 2) cap *= 2;
        char *buf = realloc(result->data.body, cap);
        if (!buf) return 0;
        result->data.body = buf;
        result->cap = cap;
    }
    memcpy(result->data.body + result->data.len, data, len);
    result->data.len += len;
    result->data.body[result->data.len] = '\0';
    return len;
}

static int post_header_value(const char *p, size_t n, const char *name, char *out, size_t cap)
{
    size_t prefix = strlen(name);
    if (n < prefix || strncasecmp(p, name, prefix)) return 0;
    p += prefix;
    n -= prefix;
    while (n && (*p == ' ' || *p == '\t')) { p++; n--; }
    while (n && strchr(" \r\n\t", p[n - 1])) n--;
    if (n >= cap || memchr(p, '\0', n)) return -1;
    memcpy(out, p, n);
    out[n] = '\0';
    return 0;
}

static size_t post_header(char *data, size_t size, size_t n, void *arg)
{
    struct post_response *r = arg;
    size_t len = size * n;
    if (len >= 5 && !memcmp(data, "HTTP/", 5)) {
        r->data.content_type[0] = '\0';
        r->data.session_id[0] = '\0';
    }
    if (post_header_value(data, len, "Content-Type:", r->data.content_type, sizeof r->data.content_type) ||
        post_header_value(data, len, "Mcp-Session-Id:", r->data.session_id, sizeof r->data.session_id))
        return 0;
    return len;
}

static int add_header(struct curl_slist **list, const char *name, const char *value)
{
    char line[4096];
    for (const unsigned char *p = (const unsigned char *)value; *p; p++)
        if (*p < 0x20 || *p >= 0x7f) return -1;
    int n = snprintf(line, sizeof line, "%s%s", name, value);
    if (n < 0 || (size_t)n >= sizeof line) return -1;
    struct curl_slist *next = curl.slist_append(*list, line);
    if (!next) return -1;
    *list = next;
    return 0;
}

int rt_http_post(const char *url, const char *bearer, const char *session_id,
                 const char *protocol_version, const char *json, unsigned timeout_ms,
                 struct rt_http_response *out)
{
    *out = (struct rt_http_response){ 0 };
    if (!url || !bearer || !*bearer || !json || !timeout_ms) return -1;
    int cleartext = !strncmp(url, "http://", 7);
    if ((!cleartext && strncmp(url, "https://", 8)) || strcspn(url, "\r\n") != strlen(url) ||
        strcspn(url + (cleartext ? 7 : 8), "/?#") == 0 ||
        memchr(url + (cleartext ? 7 : 8), '@', strcspn(url + (cleartext ? 7 : 8), "/?#")))
        return -1;
    pthread_once(&curl_once, curl_load);
    if (!curl_ok) return -1;
    void *h = curl.init();
    if (!h) return -1;
    struct curl_slist *headers = NULL, *resolve = NULL;
    struct post_response response = { 0 };
    long status = 0;
    if (add_header(&headers, "Authorization: Bearer ", bearer) ||
        add_header(&headers, "Content-Type: ", "application/json") ||
        add_header(&headers, "Accept: ", "application/json, text/event-stream") ||
        (session_id && *session_id && add_header(&headers, "Mcp-Session-Id: ", session_id)) ||
        (protocol_version && *protocol_version && add_header(&headers, "MCP-Protocol-Version: ", protocol_version)) ||
        authenticated_options(h, url, timeout_ms, cleartext ? &cleartext : NULL)) goto done;
    resolve = resolve_local(url);
    if ((resolve && curl.setopt(h, OPT_RESOLVE, resolve)) ||
        curl.setopt(h, OPT_HTTPHEADER, headers) ||
        curl.setopt(h, OPT_POSTFIELDS, json) ||
        curl.setopt(h, OPT_POSTFIELDSIZE, (long)strlen(json)) ||
        curl.setopt(h, OPT_HEADERFUNCTION, post_header) ||
        curl.setopt(h, OPT_HEADERDATA, &response) ||
        curl.setopt(h, OPT_WRITEFUNCTION, post_data) ||
        curl.setopt(h, OPT_WRITEDATA, &response) ||
        curl.perform(h) || curl.getinfo(h, INFO_RESPONSE_CODE, &status) || status < 100 || status > 599)
        goto done;
    if (!response.data.body) {
        response.data.body = calloc(1, 1);
        if (!response.data.body) goto done;
    }
    *out = response.data;
    out->status = (int)status;
    response.data.body = NULL;

done:
    curl.cleanup(h);
    if (headers) curl.slist_free_all(headers);
    if (resolve) curl.slist_free_all(resolve);
    free(response.data.body);
    return out->status ? 0 : -1;
}

void rt_http_response_free(struct rt_http_response *out)
{
    free(out->body);
    *out = (struct rt_http_response){ 0 };
}
