"""
MCP Server Registry

Architectural Intent:
- Centralized configuration for MCP servers
- Manages server lifecycle and connections
- Provides dependency injection for MCP clients

MCP Integration:
- Defines server configurations for the agentmesh system
- Supports both local and remote MCP servers
- Handles authentication and environment variables
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
import os
import json


@dataclass
class MCPServerConfig:
    """Configuration for a single MCP server"""

    name: str
    command: str
    args: List[str] = field(default_factory=list)
    env: Dict[str, str] = field(default_factory=dict)
    url: Optional[str] = None
    transport: str = "stdio"

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MCPServerConfig":
        """Create config from dictionary"""
        return cls(
            name=data["name"],
            command=data.get("command", ""),
            args=data.get("args", []),
            env=data.get("env", {}),
            url=data.get("url"),
            transport=data.get("transport", "stdio"),
        )


class MCPServerRegistry:
    """
    Registry for managing MCP server configurations.

    Supports:
    - Loading config from JSON file
    - Environment variable substitution
    - Server lifecycle management
    """

    def __init__(self):
        self._servers: Dict[str, MCPServerConfig] = {}
        self._running_servers: Dict[str, Any] = {}

    def register(self, config: MCPServerConfig) -> None:
        """Register an MCP server configuration"""
        self._servers[config.name] = config

    def get(self, name: str) -> Optional[MCPServerConfig]:
        """Get server configuration by name"""
        return self._servers.get(name)

    def get_all(self) -> Dict[str, MCPServerConfig]:
        """Get all registered server configurations"""
        return self._servers.copy()

    def load_from_file(self, path: str) -> None:
        """Load server configurations from JSON file"""
        with open(path, "r") as f:
            data = json.load(f)

        servers = data.get("mcpServers", {})
        for name, config_data in servers.items():
            config_data["name"] = name
            config = MCPServerConfig.from_dict(config_data)
            self._resolve_env_vars(config)
            self.register(config)

    def _resolve_env_vars(self, config: MCPServerConfig) -> None:
        """Replace ${VAR} patterns with environment variables"""

        def resolve(value: str) -> str:
            if (
                isinstance(value, str)
                and value.startswith("${")
                and value.endswith("}")
            ):
                var_name = value[2:-1]
                return os.environ.get(var_name, value)
            return value

        config.env = {k: resolve(v) for k, v in config.env.items()}
        config.args = [resolve(arg) for arg in config.args]

    def to_json(self) -> str:
        """Export configuration as JSON"""
        servers = {}
        for name, config in self._servers.items():
            servers[name] = {
                "command": config.command,
                "args": config.args,
                "env": config.env,
                "url": config.url,
                "transport": config.transport,
            }
        return json.dumps({"mcpServers": servers}, indent=2)


_registry = MCPServerRegistry()


def get_registry() -> MCPServerRegistry:
    """Get the global MCP server registry"""
    return _registry


def register_server(config: MCPServerConfig) -> None:
    """Register an MCP server in the global registry"""
    _registry.register(config)


def get_server_config(name: str) -> Optional[MCPServerConfig]:
    """Get server configuration from global registry"""
    return _registry.get(name)
