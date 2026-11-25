#include "realtime_net.h"

#include <stdio.h>
#include <string.h>

static int printable(const char *s, const char *forbidden)
{
    if (!s || !*s) return 0;
    for (const unsigned char *p = (const unsigned char *)s; *p; p++)
        if (*p <= ' ' || *p >= 0x7f || strchr(forbidden, *p)) return 0;
    return 1;
}

static int model_name(const char *s)
{
    const char *chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.";
    return s && *s && strspn(s, chars) == strlen(s);
}

int rt_ws_connect(struct ws *w, const char *endpoint, const char *model,
                  const char *api_key, const char *safety_id)
{
    if (!endpoint || !*endpoint) endpoint = RT_OPENAI_ENDPOINT;
    if (strncmp(endpoint, "wss://", 6) || !printable(endpoint, "#\\") || !model_name(model) ||
        !printable(api_key, "") || (safety_id && !printable(safety_id, ""))) return -1;

    const char *authority = endpoint + 6;
    size_t authority_len = strcspn(authority, "/?");
    char host[256], url[sizeof host + 10], path[1536], headers[2048];
    if (!authority_len || authority_len >= sizeof host || memchr(authority, '@', authority_len)) return -1;
    memcpy(host, authority, authority_len);
    host[authority_len] = '\0';
    snprintf(url, sizeof url, "https://%s/", host);

    const char *resource = authority + authority_len;
    int n = snprintf(path, sizeof path, "%s%s%cmodel=%s", *resource == '/' ? "" : "/", resource,
                     strchr(resource, '?') ? '&' : '?', model);
    if (n < 0 || (size_t)n >= sizeof path) return -1;
    n = safety_id
        ? snprintf(headers, sizeof headers, "Authorization: Bearer %s\r\nOpenAI-Safety-Identifier: %s\r\n", api_key, safety_id)
        : snprintf(headers, sizeof headers, "Authorization: Bearer %s\r\n", api_key);
    if (n < 0 || (size_t)n >= sizeof headers) return -1;

    int fd = https_tunnel_open(url, 5000);
    if (fd < 0) return -1;
    return ws_upgrade_client(w, fd, host, path, headers, 3000);
}

int rt_ws_recv(struct ws *w, int timeout_ms, int *opcode, uint8_t **data, size_t *len)
{
    return ws_recv_timed(w, timeout_ms, 1500, opcode, data, len);
}
