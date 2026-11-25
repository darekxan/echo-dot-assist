#ifndef HASSMIC_REALTIME_MCP_H
#define HASSMIC_REALTIME_MCP_H

struct realtime_config;
struct realtime_mcp_client;

#define REALTIME_TOOL_RESULT_MAX (32 * 1024)

/* Each voice session owns its MCP client. Calls on one client are serialized by that session.
 * Start with a zero-initialized struct and close it when the voice session ends. */
struct realtime_mcp {
    struct realtime_mcp_client *client;
};

int realtime_mcp_open(struct realtime_mcp *mcp, const struct realtime_config *config);
/* Zero or more comma-separated OpenAI function definitions, without enclosing brackets. */
const char *realtime_mcp_tools(const struct realtime_mcp *mcp);
/* JSON result owned by the caller, or NULL on failure. Only discovered names are invoked. */
char *realtime_mcp_call(struct realtime_mcp *mcp, const char *name, const char *args);
void realtime_mcp_close(struct realtime_mcp *mcp);
int realtime_test_home_assistant(const struct realtime_config *config);

#endif
