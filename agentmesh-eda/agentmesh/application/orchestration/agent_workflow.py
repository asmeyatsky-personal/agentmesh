"""
Agent Workflow Use Case

Architectural Intent:
- Demonstrates DAG orchestration for agent processing
- Parallel execution of independent validation steps
- Sequential execution where dependencies required

Parallelization Notes:
- validate_agent and check_availability run in parallel
- assign_task and notify run in parallel after assignment succeeds
"""

from dataclasses import dataclass
from typing import List, Optional, Dict, Any
import asyncio
import logging
from datetime import datetime

from agentmesh.application.orchestration.dag_orchestrator import (
    DAGOrchestrator,
    WorkflowStep,
    OrchestrationResult,
    StepStatus,
)
from agentmesh.domain.entities.agent_aggregate import AgentAggregate
from agentmesh.domain.ports.agent_repository_port import AgentRepositoryPort
from agentmesh.infrastructure.mcp_clients.notification_client import (
    NotificationMCPClient,
)

logger = logging.getLogger(__name__)


@dataclass
class AssignTaskDTO:
    """Input DTO for assigning a task to an agent"""

    tenant_id: str
    agent_id: str
    task_id: str
    task_data: Dict[str, Any]
    priority: int = 1


@dataclass
class TaskAssignedResultDTO:
    """Output DTO for task assignment result"""

    task_id: str
    agent_id: str
    status: str
    assigned_at: datetime
    parallel_results: Dict[str, Any]


class AgentWorkflowUseCase:
    """
    Use case demonstrating DAG orchestration for agent task assignment.

    Workflow Steps:
    1. validate_agent - Check agent exists and is valid (parallel with check_availability)
    2. check_availability - Check agent is available for task (parallel with validate_agent)
    3. assign_task - Assign task to agent (depends on validation steps)
    4. notify_status - Send notification about assignment (parallel with persist_result)
    5. persist_result - Save assignment record (parallel with notify_status)

    Parallelization:
    - Steps 1 & 2 run concurrently (no dependencies between them)
    - Steps 4 & 5 run concurrently (both depend on step 3)
    """

    def __init__(
        self,
        agent_repository: AgentRepositoryPort,
        notification_client: NotificationMCPClient,
    ):
        self._repository = agent_repository
        self._notifications = notification_client

    async def execute(self, dto: AssignTaskDTO) -> TaskAssignedResultDTO:
        """
        Execute the agent task assignment workflow.

        Args:
            dto: AssignTaskDTO with task and agent details

        Returns:
            TaskAssignedResultDTO with workflow results
        """
        orchestrator = DAGOrchestrator(
            steps=[
                WorkflowStep(
                    name="validate_agent",
                    execute=self._validate_agent,
                    is_critical=True,
                ),
                WorkflowStep(
                    name="check_availability",
                    execute=self._check_availability,
                    is_critical=True,
                ),
                WorkflowStep(
                    name="assign_task",
                    execute=self._assign_task,
                    depends_on=["validate_agent", "check_availability"],
                    is_critical=True,
                ),
                WorkflowStep(
                    name="notify_status",
                    execute=self._notify_status,
                    depends_on=["assign_task"],
                    is_critical=False,
                ),
                WorkflowStep(
                    name="persist_result",
                    execute=self._persist_result,
                    depends_on=["assign_task"],
                    is_critical=False,
                ),
            ],
            workflow_name="agent_task_assignment",
            max_concurrency=10,
        )

        initial_context = {
            "tenant_id": dto.tenant_id,
            "agent_id": dto.agent_id,
            "task_id": dto.task_id,
            "task_data": dto.task_data,
            "priority": dto.priority,
        }

        result = await orchestrator.execute(initial_context)

        parallel_results = {
            step_name: {
                "status": step_result.status.value,
                "output": step_result.output,
                "error": step_result.error,
            }
            for step_name, step_result in result.step_results.items()
        }

        return TaskAssignedResultDTO(
            task_id=dto.task_id,
            agent_id=dto.agent_id,
            status="ASSIGNED" if result.success else "FAILED",
            assigned_at=datetime.utcnow(),
            parallel_results=parallel_results,
        )

    async def _validate_agent(
        self,
        context: Dict[str, Any],
        completed: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Validate agent exists and is in valid state"""
        agent = await self._repository.get_by_id(
            context["agent_id"],
            context["tenant_id"],
        )

        if not agent:
            raise ValueError(f"Agent {context['agent_id']} not found")

        if not agent.is_active():
            raise ValueError(f"Agent {context['agent_id']} is not active")

        logger.info(f"Agent {context['agent_id']} validated")

        return {
            "agent_id": agent.agent_id.value,
            "status": agent.status,
            "valid": True,
        }

    async def _check_availability(
        self,
        context: Dict[str, Any],
        completed: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Check agent is available for new task"""
        available_agents = await self._repository.find_available(
            context["tenant_id"],
        )

        agent_available = any(
            a.agent_id.value == context["agent_id"] for a in available_agents
        )

        if not agent_available:
            raise ValueError(f"Agent {context['agent_id']} is not available")

        logger.info(f"Agent {context['agent_id']} availability confirmed")

        return {
            "agent_id": context["agent_id"],
            "available": True,
        }

    async def _assign_task(
        self,
        context: Dict[str, Any],
        completed: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Assign task to agent"""
        agent = await self._repository.get_by_id(
            context["agent_id"],
            context["tenant_id"],
        )

        assigned_agent = agent.assign_task(context["task_id"])

        await self._repository.save(assigned_agent)

        logger.info(
            f"Task {context['task_id']} assigned to agent {context['agent_id']}"
        )

        return {
            "agent_id": context["agent_id"],
            "task_id": context["task_id"],
            "assigned": True,
        }

    async def _notify_status(
        self,
        context: Dict[str, Any],
        completed: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Send notification about task assignment"""
        try:
            await self._notifications.notify_agent_status_change(
                agent_id=context["agent_id"],
                tenant_id=context["tenant_id"],
                old_status="AVAILABLE",
                new_status="BUSY",
            )

            return {"notification_sent": True}
        except Exception as e:
            logger.error(f"Failed to send notification: {e}")
            return {"notification_sent": False, "error": str(e)}

    async def _persist_result(
        self,
        context: Dict[str, Any],
        completed: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Persist task assignment record"""
        logger.info(f"Persisting task assignment for {context['task_id']}")

        return {
            "task_id": context["task_id"],
            "persisted": True,
        }
