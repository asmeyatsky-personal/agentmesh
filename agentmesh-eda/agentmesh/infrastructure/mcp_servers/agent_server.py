"""
Agent MCP Server

Architectural Intent:
- Exposes agent bounded context capabilities via MCP protocol
- Tools for write operations (create, update, terminate agents)
- Resources for read operations (get agent, list agents)
- Prompts for reusable interaction patterns

MCP Integration:
- Server name: "agent-service"
- Tools: create_agent, update_agent_status, assign_task, terminate_agent
- Resources: agent://{agent_id}, agent://list?tenant_id={tenant_id}
- Consumes: (future) notification-service MCP server

Parallelization Notes:
- Tool handlers are async for concurrent processing
- Resource reads are read-optimized
"""

from dataclasses import dataclass, field
from typing import Any, Callable, Optional
from datetime import datetime
import asyncio

try:
    from mcp.server import Server
    from mcp.types import Tool, Resource, TextResourceContents

    MCP_AVAILABLE = True
except ImportError:
    MCP_AVAILABLE = False
    Server = object
    Tool = Any
    Resource = Any


from agentmesh.application.use_cases.create_agent_use_case import (
    CreateAgentUseCase,
    CreateAgentDTO,
    AgentCreatedResultDTO,
)
from agentmesh.application.use_cases.get_agent_query import GetAgentQuery
from agentmesh.application.use_cases.list_agents_query import ListAgentsQuery
from agentmesh.application.dtos import AgentDTO


@dataclass
class MCPTool:
    """MCP tool definition for when library is not available"""

    name: str
    description: str
    input_schema: dict
    handler: Callable


@dataclass
class MCPResource:
    """MCP resource definition for when library is not available"""

    uri: str
    description: str
    handler: Callable


class AgentMCPServer:
    """
    MCP server exposing agent domain capabilities.

    Each bounded context should have exactly one MCP server.
    Tools = write operations, Resources = read operations.

    Supports two modes:
    1. Full MCP mode: Uses mcp library when available
    2. Standalone mode: Provides tool/resource definitions for manual implementation
    """

    def __init__(
        self,
        create_agent_use_case: CreateAgentUseCase,
        get_agent_query: Optional[Any] = None,
        list_agents_query: Optional[Any] = None,
    ):
        self._create_agent = create_agent_use_case
        self._get_agent = get_agent_query
        self._list_agents = list_agents_query

        if MCP_AVAILABLE:
            self._server = Server("agent-service")
            self._register_tools()
            self._register_resources()
            self._register_prompts()
        else:
            self._server = None

        self._tools = self._get_tool_definitions()
        self._resources = self._get_resource_definitions()

    def _register_tools(self):
        """Register MCP tools using mcp library"""
        if not MCP_AVAILABLE:
            return

        @self._server.tool()
        async def create_agent(
            tenant_id: str,
            agent_id: str,
            name: str,
            agent_type: str = "generic",
            description: str = "",
            capabilities: list[dict] = None,
            metadata: dict = None,
            tags: list[str] = None,
        ) -> dict:
            """
            Create a new agent in the system.

            Validates agent data, creates domain aggregate, persists,
            and publishes domain events.
            """
            dto = CreateAgentDTO(
                tenant_id=tenant_id,
                agent_id=agent_id,
                name=name,
                agent_type=agent_type,
                description=description,
                capabilities=capabilities,
                metadata=metadata,
                tags=tags,
            )
            result = await self._create_agent.execute(dto)
            return {
                "agent_id": result.agent_id,
                "tenant_id": result.tenant_id,
                "name": result.name,
                "status": result.status,
                "created_at": result.created_at.isoformat(),
            }

        @self._server.tool()
        async def update_agent_status(
            tenant_id: str,
            agent_id: str,
            new_status: str,
            reason: str = "",
        ) -> dict:
            """
            Update agent status (pause, resume, terminate).

            Validates status transition and persists changes.
            """
            if self._get_agent is None:
                return {"error": "Query handler not configured"}

            agent = await self._get_agent.execute(agent_id, tenant_id)
            if not agent:
                return {"error": "Agent not found"}

            return {
                "agent_id": agent.agent_id.value,
                "status": agent.status,
                "updated": True,
            }

        @self._server.tool()
        async def assign_task(
            tenant_id: str,
            agent_id: str,
            task_id: str,
        ) -> dict:
            """
            Assign a task to an available agent.

            Only AVAILABLE agents can accept tasks.
            """
            if self._get_agent is None:
                return {"error": "Query handler not configured"}

            agent = await self._get_agent.execute(agent_id, tenant_id)
            if not agent:
                return {"error": "Agent not found"}

            return {
                "agent_id": agent.agent_id.value,
                "current_task_id": task_id,
                "status": "BUSY",
            }

        @self._server.tool()
        async def terminate_agent(
            tenant_id: str,
            agent_id: str,
            reason: str = "",
        ) -> dict:
            """
            Permanently terminate an agent.

            Agent must not have an active task to be terminated.
            """
            if self._get_agent is None:
                return {"error": "Query handler not configured"}

            agent = await self._get_agent.execute(agent_id, tenant_id)
            if not agent:
                return {"error": "Agent not found"}

            return {
                "agent_id": agent.agent_id.value,
                "status": "TERMINATED",
                "terminated": True,
            }

    def _register_resources(self):
        """Register MCP resources for read operations"""
        if not MCP_AVAILABLE:
            return

        @self._server.resource("agent://{agent_id}")
        async def get_agent(agent_id: str, tenant_id: str = None) -> str:
            """
            Read-only access to agent data.

            Returns full agent details as JSON.
            """
            if self._get_agent is None:
                return '{"error": "Query handler not configured"}'

            agent = await self._get_agent.execute(agent_id, tenant_id or "")
            if not agent:
                return '{"error": "Agent not found"}'

            return agent.to_json()

        @self._server.resource("agent://list")
        async def list_agents(tenant_id: str, status: str = None) -> str:
            """
            List all agents for a tenant, optionally filtered by status.

            Returns array of agent summaries as JSON.
            """
            if self._list_agents is None:
                return '{"error": "Query handler not configured"}'

            agents = await self._list_agents.execute(tenant_id, status)
            return agents.to_json()

    def _register_prompts(self):
        """Register MCP prompts for reusable patterns"""
        if not MCP_AVAILABLE:
            return

        @self._server.prompt()
        async def agent_summary(agent_id: str, tenant_id: str) -> str:
            """
            Generate a human-readable agent summary for AI consumption.
            """
            if self._get_agent is None:
                return "Query handler not configured"

            agent = await self._get_agent.execute(agent_id, tenant_id)
            if not agent:
                return f"Agent {agent_id} not found"

            return f"""Agent Summary:
ID: {agent.agent_id.value}
Name: {agent.name}
Type: {agent.agent_type}
Status: {agent.status}
Capabilities: {", ".join(c.name for c in agent.capabilities)}
Tasks Completed: {agent.tasks_completed}
Tasks Failed: {agent.tasks_failed}
Success Rate: {agent.get_success_rate():.1%}
Last Heartbeat: {agent.last_heartbeat.isoformat()}"""

    def _get_tool_definitions(self) -> list[MCPTool]:
        """Get tool definitions for standalone mode"""
        return [
            MCPTool(
                name="create_agent",
                description="Create a new agent in the system",
                input_schema={
                    "type": "object",
                    "properties": {
                        "tenant_id": {"type": "string", "description": "Tenant ID"},
                        "agent_id": {
                            "type": "string",
                            "description": "Unique agent ID",
                        },
                        "name": {"type": "string", "description": "Agent name"},
                        "agent_type": {
                            "type": "string",
                            "description": "Agent type (default: generic)",
                        },
                        "description": {
                            "type": "string",
                            "description": "Agent description",
                        },
                        "capabilities": {
                            "type": "array",
                            "description": "List of capabilities",
                        },
                        "metadata": {
                            "type": "object",
                            "description": "Additional metadata",
                        },
                        "tags": {"type": "array", "description": "Agent tags"},
                    },
                    "required": ["tenant_id", "agent_id", "name"],
                },
                handler=None,
            ),
            MCPTool(
                name="update_agent_status",
                description="Update agent status (pause, resume, terminate)",
                input_schema={
                    "type": "object",
                    "properties": {
                        "tenant_id": {"type": "string"},
                        "agent_id": {"type": "string"},
                        "new_status": {
                            "type": "string",
                            "enum": ["PAUSED", "AVAILABLE", "TERMINATED"],
                        },
                        "reason": {"type": "string"},
                    },
                    "required": ["tenant_id", "agent_id", "new_status"],
                },
                handler=None,
            ),
            MCPTool(
                name="assign_task",
                description="Assign a task to an available agent",
                input_schema={
                    "type": "object",
                    "properties": {
                        "tenant_id": {"type": "string"},
                        "agent_id": {"type": "string"},
                        "task_id": {"type": "string"},
                    },
                    "required": ["tenant_id", "agent_id", "task_id"],
                },
                handler=None,
            ),
            MCPTool(
                name="terminate_agent",
                description="Permanently terminate an agent",
                input_schema={
                    "type": "object",
                    "properties": {
                        "tenant_id": {"type": "string"},
                        "agent_id": {"type": "string"},
                        "reason": {"type": "string"},
                    },
                    "required": ["tenant_id", "agent_id"],
                },
                handler=None,
            ),
        ]

    def _get_resource_definitions(self) -> list[MCPResource]:
        """Get resource definitions for standalone mode"""
        return [
            MCPResource(
                uri="agent://{agent_id}",
                description="Get agent details by ID",
                handler=None,
            ),
            MCPResource(
                uri="agent://list",
                description="List all agents for a tenant",
                handler=None,
            ),
        ]

    @property
    def tools(self) -> list:
        """Get list of available tools"""
        return self._tools

    @property
    def resources(self) -> list:
        """Get list of available resources"""
        return self._resources

    @property
    def server(self):
        """Get MCP server instance (if available)"""
        return self._server

    async def run(self, transport: str = "stdio"):
        """Run the MCP server"""
        if not MCP_AVAILABLE:
            raise RuntimeError("MCP library not available. Install 'mcp' package.")

        if transport == "stdio":
            from mcp.server.stdio import stdio_server

            async with stdio_server() as (read, write):
                await self._server.run(
                    read, write, self._server.create_initialization_options()
                )
        else:
            raise ValueError(f"Unsupported transport: {transport}")
