"""
MCP Client Base

Architectural Intent:
- Abstract base for all MCP client implementations
- Handles connection lifecycle, tool calls, and error handling
- Provides fallback behavior when MCP is not available

Key Design Decisions:
1. Async by default for all operations
2. Typed tool call interfaces
3. Error handling with structured responses
4. Connection pooling for remote servers
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, Optional, List
import asyncio
import logging

try:
    from mcp import ClientSession

    MCP_CLIENT_AVAILABLE = True
except ImportError:
    MCP_CLIENT_AVAILABLE = False
    ClientSession = Any


logger = logging.getLogger(__name__)


@dataclass
class MCPToolResult:
    """Structured result from MCP tool call"""

    success: bool
    data: Any = None
    error: str = ""

    @classmethod
    def ok(cls, data: Any) -> "MCPToolResult":
        return cls(success=True, data=data)

    @classmethod
    def err(cls, error: str) -> "MCPToolResult":
        return cls(success=False, error=error)


class MCPClientBase(ABC):
    """
    Base class for MCP client implementations.

    Provides:
    - Connection lifecycle management
    - Tool call abstraction
    - Error handling and logging
    - Fallback behavior
    """

    def __init__(
        self,
        server_name: str,
        session: Optional[ClientSession] = None,
        timeout: float = 30.0,
    ):
        self._server_name = server_name
        self._session = session
        self._timeout = timeout
        self._connected = False

    @property
    def server_name(self) -> str:
        """Get the server name this client connects to"""
        return self._server_name

    @property
    def is_connected(self) -> bool:
        """Check if client is connected"""
        return self._connected

    async def connect(self) -> None:
        """Establish connection to MCP server"""
        if not MCP_CLIENT_AVAILABLE:
            logger.warning(f"MCP client library not available for {self._server_name}")
            return

        if self._session is not None:
            self._connected = True
            logger.info(f"Connected to MCP server: {self._server_name}")

    async def disconnect(self) -> None:
        """Close connection to MCP server"""
        if self._session is not None:
            await self._session.close()
            self._connected = False
            logger.info(f"Disconnected from MCP server: {self._server_name}")

    async def call_tool(
        self,
        tool_name: str,
        arguments: Dict[str, Any],
    ) -> MCPToolResult:
        """
        Call an MCP tool on the server.

        Args:
            tool_name: Name of the tool to call
            arguments: Tool arguments as dict

        Returns:
            MCPToolResult with success/error status and data
        """
        if not MCP_CLIENT_AVAILABLE:
            return MCPToolResult.err("MCP client library not available")

        if not self._connected or self._session is None:
            return MCPToolResult.err(f"Not connected to {self._server_name}")

        try:
            result = await asyncio.wait_for(
                self._session.call_tool(tool_name, arguments),
                timeout=self._timeout,
            )

            if hasattr(result, "content"):
                content = result.content
                if isinstance(content, list) and len(content) > 0:
                    return MCPToolResult.ok(
                        content[0].text if hasattr(content[0], "text") else content[0]
                    )

            return MCPToolResult.ok(result)

        except asyncio.TimeoutError:
            logger.error(f"Tool {tool_name} timed out on {self._server_name}")
            return MCPToolResult.err(f"Tool call timed out: {tool_name}")
        except Exception as e:
            logger.error(f"Tool {tool_name} failed on {self._server_name}: {e}")
            return MCPToolResult.err(str(e))

    async def list_tools(self) -> List[Dict[str, Any]]:
        """List available tools on the server"""
        if not MCP_CLIENT_AVAILABLE or self._session is None:
            return []

        try:
            tools = await self._session.list_tools()
            return [
                {
                    "name": t.name,
                    "description": t.description,
                    "input_schema": t.inputSchema,
                }
                for t in tools.tools
            ]
        except Exception as e:
            logger.error(f"Failed to list tools on {self._server_name}: {e}")
            return []

    async def read_resource(self, uri: str) -> Optional[str]:
        """Read a resource from the server"""
        if not MCP_CLIENT_AVAILABLE or self._session is None:
            return None

        try:
            result = await self._session.read_resource(uri)
            if hasattr(result, "contents") and result.contents:
                content = result.contents[0]
                return content.text if hasattr(content, "text") else str(content)
        except Exception as e:
            logger.error(f"Failed to read resource {uri}: {e}")

        return None

    @abstractmethod
    async def health_check(self) -> bool:
        """Check if the MCP server is healthy"""
        pass


class MCPSessionManager:
    """
    Manages MCP client sessions.

    Provides:
    - Session creation and pooling
    - Connection lifecycle
    - Server discovery
    """

    def __init__(self):
        self._sessions: Dict[str, ClientSession] = {}
        self._servers: Dict[str, str] = {}

    def register_server(self, name: str, connection_info: str) -> None:
        """Register an MCP server for connection"""
        self._servers[name] = connection_info

    async def get_session(self, server_name: str) -> Optional[ClientSession]:
        """Get or create a session for a server"""
        if not MCP_CLIENT_AVAILABLE:
            return None

        if server_name in self._sessions:
            return self._sessions[server_name]

        return None

    async def close_all(self) -> None:
        """Close all managed sessions"""
        for session in self._sessions.values():
            await session.close()
        self._sessions.clear()
