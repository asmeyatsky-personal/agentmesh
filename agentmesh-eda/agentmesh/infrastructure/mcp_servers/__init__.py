"""
MCP Server Infrastructure

Architectural Intent:
- Expose bounded contexts as MCP servers
- Tools = write operations (commands)
- Resources = read operations (queries)
- Prompts = reusable interaction patterns

MCP Integration:
- Each bounded context should have at most one MCP server
- MCP servers live in infrastructure layer, consuming application use cases
- Tool schemas mirror port method signatures

Parallelization Notes:
- MCP server handlers are async by default
- Multiple tool calls can be processed concurrently via the MCP protocol
"""

try:
    from agentmesh.infrastructure.mcp_servers.agent_server import AgentMCPServer
    __all__ = ["AgentMCPServer"]
except ImportError:
    AgentMCPServer = None
    __all__ = []
