"""
Tests for MCP Client Adapters

Architectural Intent:
- Verify MCP client can be instantiated
- Verify tool call handling
- Verify fallback behavior when MCP not available
"""

import pytest
from unittest.mock import AsyncMock, MagicMock
from agentmesh.infrastructure.mcp_clients.base import MCPClientBase, MCPToolResult
from agentmesh.infrastructure.mcp_clients.notification_client import (
    NotificationMCPClient,
    Notification,
)


class TestMCPToolResult:
    """Test MCPToolResult dataclass"""

    def test_success_result(self):
        """Test creating successful result"""
        result = MCPToolResult.ok({"data": "value"})

        assert result.success is True
        assert result.data == {"data": "value"}
        assert result.error == ""

    def test_error_result(self):
        """Test creating error result"""
        result = MCPToolResult.err("Something went wrong")

        assert result.success is False
        assert result.data is None
        assert result.error == "Something went wrong"


class TestMCPClientBase:
    """Test MCPClientBase"""

    def test_client_instantiation(self):
        """Test client can be instantiated"""
        client = MCPClientBase(server_name="test-service")

        assert client.server_name == "test-service"
        assert client.is_connected is False

    @pytest.mark.asyncio
    async def test_health_check_not_connected(self):
        """Test health check when not connected"""
        client = MCPClientBase(server_name="test-service")

        result = await client.call_tool("test_tool", {})

        assert result.success is False
        assert "Not connected" in result.error


class TestNotificationMCPClient:
    """Test NotificationMCPClient"""

    def test_notification_client_instantiation(self):
        """Test notification client can be instantiated"""
        client = NotificationMCPClient()

        assert client.server_name == "notification-service"

    @pytest.mark.asyncio
    async def test_send_notification_not_connected(self):
        """Test sending notification when not connected"""
        client = NotificationMCPClient()

        result = await client.send_notification(
            recipient="user-123",
            message="Test message",
        )

        assert result is False

    def test_notification_dataclass(self):
        """Test Notification dataclass"""
        notification = Notification(
            recipient="user-123",
            message="Test message",
            notification_type="info",
        )

        assert notification.recipient == "user-123"
        assert notification.message == "Test message"
        assert notification.notification_type == "info"

    def test_rate_limit_configuration(self):
        """Test rate limit can be configured"""
        client = NotificationMCPClient(rate_limit=5)

        assert client._rate_limit == 5
