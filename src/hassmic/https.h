/* Media over HTTPS (issue #30): what Home Assistant hands us for TTS, announcements and play_media when it serves
 * HTTPS itself, fetched through the firmware's libcurl and handed on as a socket like a plain http:// fetch. */
#ifndef HTTPS_H
#define HTTPS_H

#include <stddef.h>

/* GET url (https://, redirects followed); a socket that reads the body, or -1 (logged).  Blocks until the response's
 * headers are in.  shutdown() or close() on the socket stops the transfer. */
int https_open(const char *url);

/* Check the server's certificate against the system's CAs and its name (setting "https_verify", on by default); 0
 * accepts any certificate (self-signed, or not naming the address Home Assistant gives out).  -1 reads */
int https_verify(int set);

/* Bidirectional verified TLS stream to an https:// origin. The caller owns the socket it returns. */
int https_tunnel_open(const char *url, unsigned timeout_ms);

struct rt_http_response {
    int status;
    char content_type[128];
    char session_id[256];
    char *body;
    size_t len;
};

/* MCP clients use HTTPS or explicitly configured private-LAN HTTP. A successful transfer returns
 * its HTTP status regardless of the status class. Only out->body is allocated. */
int rt_http_post(const char *url, const char *bearer, const char *session_id,
                 const char *protocol_version, const char *json, unsigned timeout_ms,
                 struct rt_http_response *out);
void rt_http_response_free(struct rt_http_response *out);
#endif
