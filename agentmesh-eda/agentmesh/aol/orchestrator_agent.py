from loguru import logger
from agentmesh.aol.simple_agent import SimpleAgent
from agentmesh.mal.message import UniversalMessage
from agentmesh.mal.router import MessageRouter
from agentmesh.cqrs.event import TaskAssigned
from agentmesh.mal.adapters.base import MessagePlatformAdapter
import uuid
from datetime import UTC, datetime


class OrchestratorAgent(SimpleAgent):
    def __init__(
        self, id: str, capabilities: list[str], adapter: MessagePlatformAdapter, router: MessageRouter
    ):
        super().__init__(id, capabilities, adapter)
        self.router = router

    async def assign_task(self, tenant_id: str, agent_id: str, task_details: dict):
        logger.info(f"OrchestratorAgent {self.id} assigning task to agent {agent_id} for tenant {tenant_id}")
        task_id = str(uuid.uuid4())
        task_assigned_event = TaskAssigned(
            event_id=str(uuid.uuid4()),
            created_at=datetime.now(UTC),
            task_id=task_id,
            agent_id=agent_id,
            task_details=task_details,
        )
        message = UniversalMessage(
            tenant_id=tenant_id,
            payload=task_assigned_event.__dict__, # Convert dataclass to dict for payload
            metadata={
                "type": "TaskAssigned",
                "source": self.id,
                "correlation_id": task_id,
            },
            routing={
                "targets": [f"nats:agent.task_executor.{agent_id}.commands"]
            },
        )
        await self.router.route_message(message)
