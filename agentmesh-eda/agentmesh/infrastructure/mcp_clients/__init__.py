"""
MCP Client Infrastructure

Architectural Intent:
- Consume external bounded contexts via MCP protocol
- Wrap MCP calls behind port interfaces
- Enable testability via mockable clients

MCP Integration:
- MCP clients live in infrastructure layer
- Implement ports defined in domain/application layer
- Handle connection lifecycle and error handling
"""

from agentmesh.infrastructure.mcp_clients.base import MCPClientBase
from agentmesh.infrastructure.mcp_clients.notification_client import (
    NotificationMCPClient,
)

__all__ = ["MCPClientBase", "NotificationMCPClient"]
