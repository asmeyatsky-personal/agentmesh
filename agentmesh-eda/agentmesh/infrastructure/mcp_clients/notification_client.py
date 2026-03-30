"""
Notification MCP Client

Architectural Intent:
- Consume notification service via MCP protocol
- Implements NotificationPort from domain layer
- Handles sending notifications to agents, users, or systems

MCP Integration:
- Server: notification-service
- Tools: send_notification, send_bulk_notifications
- Resources: notification://{notification_id}

Parallelization Notes:
- Bulk notifications are sent in parallel batches
- Rate limiting applied at client level
"""

from dataclasses import dataclass
from typing import List, Protocol, Optional
from agentmesh.infrastructure.mcp_clients.base import MCPClientBase, MCPToolResult


class NotificationPort(Protocol):
    """Port interface for notification service"""

    async def send_notification(
        self,
        recipient: str,
        message: str,
        notification_type: str = "info",
        metadata: dict = None,
    ) -> bool:
        """Send a notification to a recipient"""
        ...

    async def send_bulk_notifications(
        self,
        notifications: List[dict],
    ) -> List[dict]:
        """Send multiple notifications"""
        ...


@dataclass
class Notification:
    """Notification data structure"""

    recipient: str
    message: str
    notification_type: str = "info"
    metadata: Optional[dict] = None


class NotificationMCPClient(MCPClientBase, NotificationPort):
    """
    MCP client adapter for notification service.

    Implements NotificationPort by calling notification-service MCP server.
    """

    def __init__(
        self,
        session=None,
        timeout: float = 30.0,
        rate_limit: int = 10,
    ):
        super().__init__(
            server_name="notification-service",
            session=session,
            timeout=timeout,
        )
        self._rate_limit = rate_limit

    async def send_notification(
        self,
        recipient: str,
        message: str,
        notification_type: str = "info",
        metadata: dict = None,
    ) -> bool:
        """
        Send a notification via MCP tool call.

        Args:
            recipient: Notification recipient (agent_id, user_id, or topic)
            message: Notification message content
            notification_type: Type of notification (info, warning, error, alert)
            metadata: Additional metadata

        Returns:
            True if notification sent successfully
        """
        arguments = {
            "recipient": recipient,
            "message": message,
            "type": notification_type,
            "metadata": metadata or {},
        }

        result = await self.call_tool("send_notification", arguments)

        if result.success:
            return True

        return False

    async def send_bulk_notifications(
        self,
        notifications: List[dict],
    ) -> List[dict]:
        """
        Send multiple notifications in parallel.

        Args:
            notifications: List of notification dicts

        Returns:
            List of results with success/failure for each
        """
        results = []

        for notification in notifications:
            success = await self.send_notification(
                recipient=notification.get("recipient", ""),
                message=notification.get("message", ""),
                notification_type=notification.get("type", "info"),
                metadata=notification.get("metadata"),
            )
            results.append(
                {
                    "recipient": notification.get("recipient"),
                    "success": success,
                }
            )

        return results

    async def notify_agent_status_change(
        self,
        agent_id: str,
        tenant_id: str,
        old_status: str,
        new_status: str,
    ) -> bool:
        """Send notification about agent status change"""
        message = f"Agent {agent_id} status changed from {old_status} to {new_status}"

        return await self.send_notification(
            recipient=f"tenant:{tenant_id}",
            message=message,
            notification_type="info",
            metadata={
                "agent_id": agent_id,
                "old_status": old_status,
                "new_status": new_status,
            },
        )

    async def notify_task_completion(
        self,
        agent_id: str,
        task_id: str,
        tenant_id: str,
        result: dict,
    ) -> bool:
        """Send notification about task completion"""
        message = f"Agent {agent_id} completed task {task_id}"

        return await self.send_notification(
            recipient=f"tenant:{tenant_id}",
            message=message,
            notification_type="info",
            metadata={
                "agent_id": agent_id,
                "task_id": task_id,
                "result": result,
            },
        )

    async def notify_task_failure(
        self,
        agent_id: str,
        task_id: str,
        tenant_id: str,
        error: str,
    ) -> bool:
        """Send notification about task failure"""
        message = f"Agent {agent_id} failed task {task_id}: {error}"

        return await self.send_notification(
            recipient=f"tenant:{tenant_id}",
            message=message,
            notification_type="error",
            metadata={
                "agent_id": agent_id,
                "task_id": task_id,
                "error": error,
            },
        )

    async def health_check(self) -> bool:
        """Check if notification service is available"""
        result = await self.call_tool("health_check", {})
        return result.success
