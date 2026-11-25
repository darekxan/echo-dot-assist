#ifndef HASSMIC_REALTIME_NET_H
#define HASSMIC_REALTIME_NET_H

#include "https.h"
#include "ws.h"

#define RT_OPENAI_ENDPOINT "wss://api.openai.com/v1/realtime"

/* Open an authenticated Realtime WebSocket. Use ws_send() and ws_close() after success. */
int rt_ws_connect(struct ws *w, const char *endpoint, const char *model,
                  const char *api_key, const char *safety_id);

/* 1 = complete message, 0 = idle timeout, -1 = disconnected or failed. */
int rt_ws_recv(struct ws *w, int timeout_ms, int *opcode, uint8_t **data, size_t *len);

#endif
