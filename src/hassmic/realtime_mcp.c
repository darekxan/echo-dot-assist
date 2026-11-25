#include "realtime_mcp.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <time.h>
#include "https.h"
#include "json.h"
#include "../third_party/monocypher.h"
#include "realtime_config.h"

#define MCP_VERSION "2025-06-18"
#define MCP_DISCOVERY_MS 8000
#define MCP_CALL_MS 10000 /* Leave room in the 12-second buffered microphone while a tool runs. */
#define MCP_MAX_PAGES 6
#define MCP_MAX_TOOLS 64
#define MCP_MAX_SCHEMAS (256 * 1024)
#define MCP_MAX_RESULT_ENVELOPE (16 * 1024 - 1)
#define MCP_MAX_ARGUMENTS (16 * 1024)
#define MCP_MAX_CURSOR 1024

struct tool_name {
    char model[65];
    char server[129];
};

struct realtime_mcp_client {
    char url[512], token[2401], session_id[256], version[16];
    unsigned next_id, tool_count;
    struct tool_name names[MCP_MAX_TOOLS];
    char *tools;
    size_t tools_len;
};

struct rpc_reply {
    struct rt_http_response http;
    struct json json;
    char *event_data;
    int result;
};

static long long monotonic_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000LL + ts.tv_nsec / 1000000;
}

static void reply_free(struct rpc_reply *reply)
{
    json_free(&reply->json);
    free(reply->event_data);
    rt_http_response_free(&reply->http);
}

static int media_type(const char *actual, const char *expected)
{
    size_t n = strlen(expected);
    return !strncasecmp(actual, expected, n) &&
           (actual[n] == 0 || actual[n] == ';' || actual[n] == ' ' || actual[n] == '\t');
}

static int matching_reply(struct rpc_reply *reply, const char *body, size_t len, unsigned id)
{
    struct json *j = &reply->json;
    if (!len || memchr(body, 0, len) || json_parse(j, body, len) ||
        json_type(j, 0) != J_OBJ || !json_streq(j, json_get(j, 0, "jsonrpc"), "2.0") ||
        json_int(j, json_get(j, 0, "id"), -1) != id)
        return 0;
    reply->result = json_get(j, 0, "result");
    return json_type(j, reply->result) == J_OBJ ? 1 : -1;
}

/* A short SSE response can have comments, notifications and split data lines. Pick only the response to our id. */
static int event_reply(struct rpc_reply *reply, unsigned id)
{
    const char *line = reply->http.body, *end = line + reply->http.len;
    reply->event_data = malloc(reply->http.len + 1);
    if (!reply->event_data) return -1;
    size_t used = 0;
    while (line < end) {
        const char *next = line;
        while (next < end && *next != '\n' && *next != '\r') next++;
        size_t len = (size_t)(next - line);
        if (!len && used) {
            int found = matching_reply(reply, reply->event_data, used, id);
            if (found) return found;
            used = 0;
        } else if (len >= 5 && !memcmp(line, "data:", 5)) {
            size_t start = 5 + (len > 5 && line[5] == ' ');
            if (used) reply->event_data[used++] = '\n';
            memcpy(reply->event_data + used, line + start, len - start);
            used += len - start;
        }
        if (next < end && *next == '\r') next++;
        if (next < end && *next == '\n') next++;
        line = next;
    }
    return used ? matching_reply(reply, reply->event_data, used, id) : 0;
}

static int rpc(struct realtime_mcp_client *c, const char *method, const char *params,
               long long deadline, struct rpc_reply *reply)
{
    long long left = deadline - monotonic_ms();
    if (left <= 0) return -1;
    unsigned id = reply ? ++c->next_id : 0;
    size_t cap = strlen(method) + strlen(params) + 100;
    char *request = malloc(cap);
    if (!request) return -1;
    if (reply)
        snprintf(request, cap, "{\"jsonrpc\":\"2.0\",\"id\":%u,\"method\":\"%s\",\"params\":%s}",
                 id, method, params);
    else
        snprintf(request, cap, "{\"jsonrpc\":\"2.0\",\"method\":\"%s\"}", method);

    struct rt_http_response notification = {0};
    struct rt_http_response *http = reply ? &reply->http : &notification;
    int rc = rt_http_post(c->url, c->token, c->session_id, c->version,
                          request, (unsigned)left, http);
    free(request);
    int ok = !rc && http->status >= 200 && http->status < 300;
    if (!reply) {
        rt_http_response_free(http);
        return ok ? 0 : -1;
    }
    if (!ok || !http->body || !http->len) return -1;
    if (http->session_id[0])
        snprintf(c->session_id, sizeof c->session_id, "%s", http->session_id);
    if (media_type(http->content_type, "application/json"))
        return matching_reply(reply, http->body, http->len, id) == 1 ? 0 : -1;
    if (media_type(http->content_type, "text/event-stream"))
        return event_reply(reply, id) == 1 ? 0 : -1;
    return -1;
}

static int rpc_init(struct realtime_mcp_client *c, long long deadline)
{
    struct rpc_reply reply = {0};
    int rc = rpc(c, "initialize",
                 "{\"protocolVersion\":\"" MCP_VERSION "\",\"capabilities\":{},"
                 "\"clientInfo\":{\"name\":\"hassmic\",\"version\":\"1.0\"}}",
                 deadline, &reply);
    if (!rc) {
        const struct json *j = &reply.json;
        int version = json_get(j, reply.result, "protocolVersion");
        if (!json_streq(j, version, "2025-11-25") && !json_streq(j, version, MCP_VERSION) &&
            !json_streq(j, version, "2025-03-26")) rc = -1;
        else json_str(j, version, c->version, sizeof c->version);
    }
    reply_free(&reply);
    return rc ? -1 : rpc(c, "notifications/initialized", "", deadline, NULL);
}

static const char *tool_server_name(const struct realtime_mcp_client *c, const char *model)
{
    for (unsigned i = 0; i < c->tool_count; i++)
        if (!strcmp(c->names[i].model, model)) return c->names[i].server;
    return NULL;
}

static int tool_name_char(unsigned char ch)
{
    return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') ||
           (ch >= '0' && ch <= '9') || ch == '_' || ch == '-';
}

static int tool_add(struct realtime_mcp_client *c, const struct json *j, int tool)
{
    int name = json_get(j, tool, "name"), schema = json_get(j, tool, "inputSchema");
    if (json_type(j, name) != J_STR || json_type(j, schema) != J_OBJ ||
        !json_streq(j, json_get(j, schema, "type"), "object"))
        return 0;
    if (j->t[name].end - j->t[name].start > 128) return 0;
    struct tool_name *slot = &c->names[c->tool_count];
    json_str(j, name, slot->server, sizeof slot->server);
    if (!slot->server[0]) return 0;

    char base[65] = "";
    for (size_t i = 0; slot->server[i]; i++) {
        unsigned char ch = (unsigned char)slot->server[i];
        if (ch < 0x21 || ch > 0x7e) return 0;
        if (i < sizeof base - 1) base[i] = tool_name_char(ch) ? (char)ch : '_';
    }
    for (unsigned i = 0; i < c->tool_count; i++)
        if (!strcmp(c->names[i].server, slot->server)) return 0;

    snprintf(slot->model, sizeof slot->model, "%s", base);
    for (unsigned suffix = 2; !strcmp(slot->model, "end_conversation") || !strcmp(slot->model, "get_local_time") ||
         tool_server_name(c, slot->model); suffix++)
        snprintf(slot->model, sizeof slot->model, "%.60s_%u", base, suffix);

    const char *description = "\"\"";
    int description_len = 2;
    int desc = json_get(j, tool, "description");
    if (json_type(j, desc) == J_STR && j->t[desc].end - j->t[desc].start <= 2048) {
        description = j->s + j->t[desc].start - 1;
        description_len = j->t[desc].end - j->t[desc].start + 2;
    }
    int schema_len = j->t[schema].end - j->t[schema].start;
    if (schema_len > 8192) return 0;
    size_t remaining = MCP_MAX_SCHEMAS - c->tools_len;
    int n = snprintf(c->tools + c->tools_len, remaining,
                     "%s{\"type\":\"function\",\"name\":\"%s\",\"description\":%.*s,\"parameters\":%.*s}",
                     c->tool_count ? "," : "", slot->model, description_len, description,
                     schema_len, j->s + j->t[schema].start);
    if (n < 0 || (size_t)n >= remaining) {
        c->tools[c->tools_len] = 0;
        return 0;
    }
    c->tools_len += (size_t)n;
    c->tool_count++;
    return 1;
}

static int tool_list(struct realtime_mcp_client *c, long long deadline)
{
    char params[MCP_MAX_CURSOR + 20] = "{}";
    for (unsigned page = 0; page < MCP_MAX_PAGES; page++) {
        struct rpc_reply reply = {0};
        int rc = rpc(c, "tools/list", params, deadline, &reply);
        if (rc) { reply_free(&reply); return -1; }
        const struct json *j = &reply.json;
        int tools = json_get(j, reply.result, "tools");
        if (json_type(j, tools) != J_ARR) { reply_free(&reply); return -1; }
        JSON_EACH(j, tools, tool) {
            if (c->tool_count == MCP_MAX_TOOLS) break;
            tool_add(c, j, tool);
        }
        int cursor = json_get(j, reply.result, "nextCursor");
        if (c->tool_count == MCP_MAX_TOOLS || json_type(j, cursor) != J_STR ||
            j->t[cursor].end == j->t[cursor].start) {
            reply_free(&reply);
            return 0;
        }
        int len = j->t[cursor].end - j->t[cursor].start;
        if (len > MCP_MAX_CURSOR) { reply_free(&reply); return -1; }
        char next[sizeof params];
        snprintf(next, sizeof next, "{\"cursor\":\"%.*s\"}", len, j->s + j->t[cursor].start);
        reply_free(&reply);
        if (!strcmp(next, params)) return -1;
        snprintf(params, sizeof params, "%s", next);
    }
    return 0;
}

int realtime_mcp_open(struct realtime_mcp *mcp, const struct realtime_config *config)
{
    if (mcp->client || !config->ha_endpoint[0]) return -1;
    struct realtime_mcp_client *c = calloc(1, sizeof *c);
    if (!c) return -1;
    mcp->client = c;
    c->tools = calloc(1, MCP_MAX_SCHEMAS);
    if (c->tools) {
        snprintf(c->url, sizeof c->url, "%s", config->ha_endpoint);
        snprintf(c->token, sizeof c->token, "%s", config->ha_token);
        long long deadline = monotonic_ms() + MCP_DISCOVERY_MS;
        if (!rpc_init(c, deadline) && !tool_list(c, deadline)) return 0;
    }
    realtime_mcp_close(mcp);
    return -1;
}

const char *realtime_mcp_tools(const struct realtime_mcp *mcp)
{
    return mcp->client ? mcp->client->tools : "";
}

char *realtime_mcp_call(struct realtime_mcp *mcp, const char *name, const char *args)
{
    struct realtime_mcp_client *c = mcp->client;
    const char *tool = c ? tool_server_name(c, name) : NULL;
    if (!tool || !args) {
        fprintf(stderr, "realtime: Home Assistant tool call rejected: unavailable tool or arguments\n");
        return NULL;
    }
    size_t args_len = strnlen(args, MCP_MAX_ARGUMENTS + 1);
    if (args_len > MCP_MAX_ARGUMENTS) return NULL;
    struct json parsed = {0};
    int valid = !json_parse(&parsed, args, args_len) && json_type(&parsed, 0) == J_OBJ;
    json_free(&parsed);
    if (!valid) {
        fprintf(stderr, "realtime: Home Assistant tool %s: invalid arguments\n", name);
        return NULL;
    }

    char quoted[2 * sizeof c->names[0].server + 2];
    if (json_esc(quoted, sizeof quoted, tool) < 0) return NULL;
    size_t cap = strlen(quoted) + args_len + 32;
    char *params = malloc(cap);
    if (!params) return NULL;
    snprintf(params, cap, "{\"name\":%s,\"arguments\":%s}", quoted, args);
    struct rpc_reply reply = {0};
    int rc = rpc(c, "tools/call", params, monotonic_ms() + MCP_CALL_MS, &reply);
    free(params);
    char *output = NULL;
    const struct json *j = &reply.json;
    const char *outcome = NULL;
    int rpc_code = 0;
    if (!rc) {
        const struct jtok *result = &j->t[reply.result];
        int len = result->end - result->start;
        if (json_bool(j, json_get(j, reply.result, "isError"), 0)) {
            outcome = "tool_error";
            output = strdup("{\"error\":\"Home Assistant tool failed\"}");
        } else {
            int structured = json_get(j, reply.result, "structuredContent");
            if (len > MCP_MAX_RESULT_ENVELOPE) {
                /* Use the structured copy when an MCP envelope repeats it as escaped text.
                 * HA-MCP settings links can contain a private connection secret. */
                result = NULL;
                if (json_type(j, structured) == J_OBJ &&
                    j->t[structured].end - j->t[structured].start <= REALTIME_TOOL_RESULT_MAX &&
                    json_get(j, structured, "settings_url") < 0 &&
                    json_get(j, structured, "settings_url_hint") < 0)
                    result = &j->t[structured];
            }
            if (result) {
                int n = result->end - result->start;
                output = strndup(j->s + result->start, (size_t)n);
                if (!output) outcome = "allocation_failed";
                else if (!json_bool(j, json_get(j, structured, "success"), 1)) outcome = "application_error";
            } else {
                outcome = "result_too_large";
                output = strdup("{\"status\":\"result_too_large\",\"message\":\"Home Assistant's reply was too large. "
                                "The action outcome is unknown. Do not repeat a command solely because its result is unavailable.\"}");
            }
        }
    } else if (!reply.http.status) {
        outcome = "transport_error";
        output = strdup("{\"status\":\"unconfirmed\",\"message\":\"Home Assistant returned no result. "
                        "If this was a command, verify the current state before repeating it.\"}");
    } else if (reply.http.status < 200 || reply.http.status >= 300) {
        outcome = "http_error";
    } else {
        int code = json_get(j, json_get(j, 0, "error"), "code");
        if (json_type(j, code) == J_NUM) {
            rpc_code = (int)json_int(j, code, 0);
            outcome = "rpc_error";
        } else outcome = "invalid_response";
    }
    if (outcome)
        fprintf(stderr, "realtime: Home Assistant tool %s: %s (HTTP %d, RPC %d)\n",
                name, outcome, reply.http.status, rpc_code);
    reply_free(&reply);
    return output;
}

void realtime_mcp_close(struct realtime_mcp *mcp)
{
    struct realtime_mcp_client *c = mcp->client;
    if (!c) return;
    free(c->tools);
    crypto_wipe(c->token, sizeof c->token);
    free(c);
    mcp->client = NULL;
}

int realtime_test_home_assistant(const struct realtime_config *config)
{
    struct realtime_mcp mcp = {0};
    int rc = realtime_mcp_open(&mcp, config);
    realtime_mcp_close(&mcp);
    return rc;
}
