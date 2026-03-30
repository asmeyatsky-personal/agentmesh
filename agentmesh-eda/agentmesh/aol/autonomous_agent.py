"""
Autonomous Agent Implementation

Architectural Intent:
- Agent that makes independent decisions without central orchestrator
- Uses AgentAutonomyService for decision-making
- Manages own task queue and prioritization
- Enables decentralized, scalable multi-agent systems
- Publishes events for integration with other systems

Key Design Decisions:
1. Agent is independent - no central control needed
2. Decisions transparent and auditable
3. Self-organizing based on capabilities and load
4. Event-driven communication with other agents
5. Graceful degradation if dependencies fail
"""

from dataclasses import dataclass
from typing import List, Optional, Dict
from datetime import datetime
from loguru import logger
import asyncio
from enum import Enum

from agentmesh.domain.entities.agent_aggregate import AgentAggregate
from agentmesh.domain.services.agent_autonomy_service import (
    AgentAutonomyService,
    TaskOffering,
    DecisionResult
)
from agentmesh.domain.services.agent_load_balancer_service import AgentLoadBalancerService
from agentmesh.domain.ports.agent_repository_port import AgentRepositoryPort
from agentmesh.mal.message import UniversalMessage
from agentmesh.cqrs.bus import EventBus


class AgentExecutionState(Enum):
    """Agent execution states"""
    IDLE = "IDLE"
    PROCESSING_TASK = "PROCESSING_TASK"
    WAITING_FOR_TASKS = "WAITING_FOR_TASKS"
    PAUSED = "PAUSED"
    FAILED = "FAILED"


@dataclass
class TaskExecutionResult:
    """Result of task execution"""
    task_id: str
    status: str  # SUCCESS, FAILED, TIMEOUT
    result: Dict = None
    error_message: str = ""
    execution_time_ms: float = 0.0


class AutonomousAgent:
    """
    Autonomous Agent: Makes decisions independently.

    Capabilities:
    - Decide whether to accept tasks
    - Prioritize task queue
    - Self-manage workload
    - Collaborate with other agents
    - Publish events for integration
    - Monitor own health
    - Request help when needed

    Design:
    - Uses AgentAutonomyService for decisions
    - No central orchestrator required
    - Event-driven communication
    - Transparent decision-making
    """

    def __init__(self,
                 agent_aggregate: AgentAggregate,
                 autonomy_service: AgentAutonomyService,
                 load_balancer: AgentLoadBalancerService,
                 agent_repository: AgentRepositoryPort,
                 event_bus: EventBus):
        """
        Initialize autonomous agent.

        Args:
            agent_aggregate: Domain model for this agent
            autonomy_service: Decision-making service
            load_balancer: For helping other agents
            agent_repository: For persistence
            event_bus: For publishing events
        """
        self.aggregate = agent_aggregate
        self.autonomy = autonomy_service
        self.load_balancer = load_balancer
        self.repository = agent_repository
        self.event_bus = event_bus

        # Runtime state (protected by _lock for concurrent access)
        self._lock = asyncio.Lock()
        self.state = AgentExecutionState.IDLE
        self.task_queue: List[TaskOffering] = []
        self.current_task: Optional[TaskOffering] = None
        self.execution_history: List[TaskExecutionResult] = []

        logger.info(f"AutonomousAgent initialized: {agent_aggregate.agent_id.value}")

    # ==================== Task Management ====================

    async def process_task_offerings(self,
                                    task_offerings: List[TaskOffering]) -> List[str]:
        """
        Autonomously process offered tasks.

        Algorithm:
        1. Filter tasks agent can do
        2. Prioritize by attractiveness
        3. Accept tasks up to capacity
        4. Store in task queue
        5. Start processing

        Returns:
            List of accepted task IDs
        """
        if not task_offerings:
            logger.debug(f"{self.aggregate.agent_id.value}: No task offerings")
            return []

        logger.info(f"{self.aggregate.agent_id.value}: Processing {len(task_offerings)} task offerings")

        async with self._lock:
            # Step 1: Evaluate and accept tasks
            accepted_tasks = []
            for task in task_offerings:
                decision = self.autonomy.should_accept_task(self.aggregate, task)

                if decision.should_accept:
                    accepted_tasks.append(task)
                    logger.info(
                        f"{self.aggregate.agent_id.value}: Accepted task {task.task_id} "
                        f"(score: {decision.confidence:.2f}, reason: {decision.reason})"
                    )
                else:
                    logger.debug(
                        f"{self.aggregate.agent_id.value}: Rejected task {task.task_id} "
                        f"(reason: {decision.reason})"
                    )

            if not accepted_tasks:
                return []

            # Step 2: Prioritize accepted tasks
            prioritized = self.autonomy.prioritize_tasks(self.aggregate, accepted_tasks)
            logger.info(f"{self.aggregate.agent_id.value}: Prioritized {len(prioritized)} tasks")

            # Step 3: Add to task queue
            self.task_queue.extend(prioritized)

            # Step 4: Update aggregate
            for task in prioritized:
                try:
                    self.aggregate = self.aggregate.assign_task(task.task_id)
                except ValueError:
                    logger.warning(
                        f"{self.aggregate.agent_id.value}: Could not assign task {task.task_id}"
                    )

            # Step 5: Persist updated agent state
            await self.repository.save(self.aggregate)

            return [task.task_id for task in prioritized]

    async def execute_tasks(self) -> List[TaskExecutionResult]:
        """
        Execute tasks in queue sequentially.

        Processes tasks in priority order.
        Updates aggregate state with completion/failure.
        Publishes events for integration.

        Returns:
            List of execution results
        """
        results = []

        while True:
            async with self._lock:
                if not self.task_queue:
                    break
                if self.state == AgentExecutionState.PAUSED:
                    logger.info(f"{self.aggregate.agent_id.value}: Paused, stopping task execution")
                    break

                # Get next task
                current_task = self.task_queue.pop(0)
                self.state = AgentExecutionState.PROCESSING_TASK

                # Ensure aggregate tracks this task (may already be assigned for first task)
                if self.aggregate.current_task_id != current_task.task_id:
                    try:
                        self.aggregate = self.aggregate.assign_task(current_task.task_id)
                    except ValueError:
                        logger.warning(
                            f"{self.aggregate.agent_id.value}: Could not assign task "
                            f"{current_task.task_id} to aggregate, skipping"
                        )
                        continue

            try:
                logger.info(f"{self.aggregate.agent_id.value}: Starting task {current_task.task_id}")

                # Execute task (simulated) — runs outside lock to avoid blocking
                result = await self._execute_task(current_task)
                results.append(result)

                async with self._lock:
                    # Update aggregate based on result
                    if result.status == "SUCCESS":
                        self.aggregate = self.aggregate.complete_task()
                        logger.info(
                            f"{self.aggregate.agent_id.value}: Completed task {current_task.task_id} "
                            f"({result.execution_time_ms}ms)"
                        )
                    else:
                        self.aggregate = self.aggregate.fail_task(result.error_message)
                        logger.error(
                            f"{self.aggregate.agent_id.value}: Failed task {current_task.task_id}: "
                            f"{result.error_message}"
                        )

                    # Persist state
                    await self.repository.save(self.aggregate)

            except Exception as e:
                logger.error(f"{self.aggregate.agent_id.value}: Task execution error: {e}")
                async with self._lock:
                    if self.aggregate.current_task_id is not None:
                        self.aggregate = self.aggregate.fail_task(str(e))
                    await self.repository.save(self.aggregate)

        async with self._lock:
            self.state = AgentExecutionState.IDLE
        return results

    # ==================== Health Monitoring ====================

    async def check_health(self) -> bool:
        """
        Check agent's own health.

        Returns True if healthy, False otherwise.
        Automatically updates aggregate state.

        STUB: Uses random metrics for demo/testing. Replace with real
        monitoring service integration for production use.
        """
        logger.debug(f"{self.aggregate.agent_id.value}: Running health check")

        from agentmesh.domain.value_objects.agent_value_objects import HealthMetrics
        import random

        metrics = HealthMetrics(
            success_rate=self.aggregate.get_success_rate(),
            response_time_ms=random.uniform(50, 500),
            error_rate=1.0 - self.aggregate.get_success_rate(),
            cpu_usage_percent=random.uniform(10, 80),
            memory_usage_percent=random.uniform(20, 70),
            tasks_completed=self.aggregate.tasks_completed,
            tasks_failed=self.aggregate.tasks_failed
        )

        healthy_thresholds = {
            "min_success_rate": 0.5,
            "max_error_rate": 0.3,
            "max_response_time_ms": 5000,
            "max_cpu_percent": 90,
            "max_memory_percent": 90,
        }

        is_healthy = metrics.is_healthy(healthy_thresholds)

        if is_healthy:
            self.aggregate = self.aggregate.mark_healthy(metrics)
            logger.info(f"{self.aggregate.agent_id.value}: Health check passed")
        else:
            self.aggregate = self.aggregate.mark_unhealthy("Health check failed")
            logger.warning(f"{self.aggregate.agent_id.value}: Health check failed")

        await self.repository.save(self.aggregate)
        return is_healthy

    # ==================== Collaboration ====================

    async def request_help(self,
                          task_id: str,
                          required_capabilities: List[str],
                          available_agents: List[AgentAggregate]) -> Optional[str]:
        """
        Request help from another agent.

        Finds best agent with required capabilities.
        Delegates portion of task.

        Args:
            task_id: Task needing help
            required_capabilities: What capabilities needed
            available_agents: Agents that can help

        Returns:
            ID of agent that will help, or None if none available
        """
        logger.info(
            f"{self.aggregate.agent_id.value}: Requesting help for task {task_id} "
            f"(capabilities: {required_capabilities})"
        )

        helper = self.load_balancer.select_best_agent(
            required_capabilities,
            available_agents
        )

        if helper:
            logger.info(
                f"{self.aggregate.agent_id.value}: Found helper agent {helper.agent_id.value}"
            )
            return helper.agent_id.value
        else:
            logger.warning(f"{self.aggregate.agent_id.value}: No suitable helper found")
            return None

    async def pause(self) -> None:
        """Pause agent operations"""
        self.state = AgentExecutionState.PAUSED
        self.aggregate = self.aggregate.pause()
        await self.repository.save(self.aggregate)
        logger.info(f"{self.aggregate.agent_id.value}: Paused")

    async def resume(self) -> None:
        """Resume agent operations"""
        self.state = AgentExecutionState.IDLE
        self.aggregate = self.aggregate.resume()
        await self.repository.save(self.aggregate)
        logger.info(f"{self.aggregate.agent_id.value}: Resumed")

    async def terminate(self) -> None:
        """Terminate agent"""
        self.aggregate = self.aggregate.terminate()
        await self.repository.save(self.aggregate)
        logger.info(f"{self.aggregate.agent_id.value}: Terminated")

    # ==================== Private Methods ====================

    async def _execute_task(self, task: TaskOffering) -> TaskExecutionResult:
        """
        Execute single task.

        STUB: Simulates execution with sleep and random success/failure.
        Override this method or inject a real task executor for production use.
        """
        import time
        import random

        start_time = time.time()

        try:
            # Simulate task execution
            duration = task.estimated_duration_seconds or 5
            await asyncio.sleep(min(duration / 100, 1))  # Scale down for demo

            # Simulate random success/failure based on agent performance
            success_threshold = self.aggregate.get_success_rate()
            if random.random() < success_threshold:
                execution_time_ms = (time.time() - start_time) * 1000
                return TaskExecutionResult(
                    task_id=task.task_id,
                    status="SUCCESS",
                    result={"output": f"Task {task.task_id} completed successfully"},
                    execution_time_ms=execution_time_ms
                )
            else:
                raise Exception("Task execution failed (simulated)")

        except Exception as e:
            execution_time_ms = (time.time() - start_time) * 1000
            return TaskExecutionResult(
                task_id=task.task_id,
                status="FAILED",
                error_message=str(e),
                execution_time_ms=execution_time_ms
            )

    # ==================== State Queries ====================

    def get_workload(self) -> float:
        """Get current workload (0.0-1.0)"""
        return self.autonomy.get_agent_workload(self.aggregate)

    def get_task_queue_size(self) -> int:
        """Get number of pending tasks"""
        return len(self.task_queue)

    def should_accept_more_tasks(self) -> bool:
        """Check if agent can accept more tasks"""
        workload = self.get_workload()
        return workload < 0.8  # 80% threshold

    def get_status_summary(self) -> Dict:
        """Get agent status summary"""
        return {
            "agent_id": self.aggregate.agent_id.value,
            "status": self.aggregate.status,
            "execution_state": self.state.value,
            "workload": self.get_workload(),
            "task_queue_size": self.get_task_queue_size(),
            "success_rate": self.aggregate.get_success_rate(),
            "tasks_completed": self.aggregate.tasks_completed,
            "tasks_failed": self.aggregate.tasks_failed,
            "is_healthy": self.aggregate.is_healthy(),
        }
