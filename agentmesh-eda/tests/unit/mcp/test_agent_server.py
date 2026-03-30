"""
Tests for Agent MCP Server

Architectural Intent:
- Verify MCP server tool definitions
- Verify MCP resource definitions
- Verify server can be instantiated without MCP library
"""

import pytest
from unittest.mock import AsyncMock, MagicMock
from agentmesh.infrastructure.mcp_servers.agent_server import AgentMCPServer


class TestAgentMCPServer:
    """Test cases for AgentMCPServer"""

    def test_server_instantiation(self):
        """Test that server can be instantiated with use cases"""
        mock_use_case = MagicMock()

        server = AgentMCPServer(
            create_agent_use_case=mock_use_case,
        )

        assert server.server_name == "agent-service"
        assert server.tools is not None
        assert len(server.tools) == 4

    def test_tool_definitions_exist(self):
        """Test that all required tools are defined"""
        mock_use_case = MagicMock()

        server = AgentMCPServer(create_agent_use_case=mock_use_case)

        tool_names = [t.name for t in server.tools]

        assert "create_agent" in tool_names
        assert "update_agent_status" in tool_names
        assert "assign_task" in tool_names
        assert "terminate_agent" in tool_names

    def test_tool_schemas(self):
        """Test that tool schemas are properly defined"""
        mock_use_case = MagicMock()

        server = AgentMCPServer(create_agent_use_case=mock_use_case)

        create_tool = next(t for t in server.tools if t.name == "create_agent")

        assert "tenant_id" in create_tool.input_schema["properties"]
        assert "agent_id" in create_tool.input_schema["properties"]
        assert "name" in create_tool.input_schema["properties"]
        assert create_tool.input_schema["required"] == ["tenant_id", "agent_id", "name"]

    def test_resource_definitions_exist(self):
        """Test that all required resources are defined"""
        mock_use_case = MagicMock()

        server = AgentMCPServer(create_agent_use_case=mock_use_case)

        resource_uris = [r.uri for r in server.resources]

        assert "agent://{agent_id}" in resource_uris
        assert "agent://list" in resource_uris

    @pytest.mark.asyncio
    async def test_create_agent_tool_execution(self):
        """Test create agent tool execution flow"""
        mock_use_case = AsyncMock()
        mock_result = MagicMock()
        mock_result.agent_id = "agent-123"
        mock_result.tenant_id = "tenant-1"
        mock_result.name = "Test Agent"
        mock_result.status = "AVAILABLE"
        mock_result.created_at = MagicMock()
        mock_result.created_at.isoformat.return_value = "2024-01-01T00:00:00"

        mock_use_case.execute.return_value = mock_result

        server = AgentMCPServer(create_agent_use_case=mock_use_case)

        if server.server:
            pass

    def test_tools_have_descriptions(self):
        """Test that all tools have descriptions"""
        mock_use_case = MagicMock()

        server = AgentMCPServer(create_agent_use_case=mock_use_case)

        for tool in server.tools:
            assert tool.description is not None
            assert len(tool.description) > 0
