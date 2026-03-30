"""
Agent Aggregate Root

Architectural Intent:
- Central domain model for agent lifecycle management
- Enforces all business rules and invariants
- Immutable state via frozen dataclass
- State transitions via methods that return new instances
- Encapsulates agent-specific business logic

Key Design Decisions:
1. Frozen dataclass for immutability and thread safety
2. All state changes return new instances (don't mutate)
3. Validation of invariants in __post_init__
4. Methods represent business operations, not just getters/setters
5. Rich methods that check business rules before state changes

Domain Language (Ubiquitous Language):
- Agent: An autonomous system that processes tasks
- Capability: Skill the agent can perform
- Status: Current operational state
- Heartbeat: Periodic health signal
- Task: Unit of work assigned to agent
"""

from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, UTC
from typing import List, Dict, Optional, Set
from agentmesh.domain.value_objects.agent_value_objects import (
    AgentId,
    AgentCapability,
    AgentStatus,
    ResourceRequirement,
    HealthMetrics,
    AgentHeartbeat
)


@dataclass(frozen=True)
class AgentAggregate:
    """
    Agent Aggregate Root

    Represents an autonomous agent in the system.

    Responsibilities:
    - Manage agent lifecycle (creation, activation, termination)
    - Track capabilities and performance
    - Monitor health and availability
    - Enforce business rules for task assignment

    Invariants:
    - Agent must have unique ID within tenant
    - Agent must have at least one capability
    - Status must be valid (AVAILABLE, BUSY, PAUSED, UNHEALTHY, TERMINATED)
    - Health check timeout must be positive
    - Resource requirements must be non-negative
    - Agent cannot transition to TERMINATED state once set
    - Cannot assign tasks to PAUSED or TERMINATED agents
    """

    # Identity
    agent_id: AgentId
    tenant_id: str

    # Basic Info
    name: str
    description: str = ""
    agent_type: str = "generic"  # e.g., "data_processor", "analyzer", "orchestrator"

    # Capabilities
    capabilities: List[AgentCapability] = field(default_factory=list)

    # Current State
    status: str = AgentStatus.AVAILABLE
    current_task_id: Optional[str] = None
    tasks_assigned: int = 0
    tasks_completed: int = 0
    tasks_failed: int = 0

    # Health Monitoring
    last_heartbeat: datetime = field(default_factory=lambda: datetime.now(UTC))
    health_check_timeout_seconds: int = 30
    current_metrics: Optional[HealthMetrics] = None

    # Resources
    resource_requirements: ResourceRequirement = field(default_factory=lambda: ResourceRequirement())

    # Lifecycle
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    activated_at: Optional[datetime] = None
    terminated_at: Optional[datetime] = None

    # Metadata
    metadata: Dict[str, str] = field(default_factory=dict)
    tags: Set[str] = field(default_factory=set)

    def __post_init__(self):
        """Validate all invariants on creation"""
        # Validate identity
        if not self.tenant_id or not self.tenant_id.strip():
            raise ValueError("tenant_id cannot be empty")

        # Validate name
        if not self.name or not self.name.strip():
            raise ValueError("name cannot be empty")

        # Validate capabilities
        if not self.capabilities:
            raise ValueError("Agent must have at least one capability")

        # Validate status
        if not AgentStatus.validate(self.status):
            raise ValueError(f"Invalid status: {self.status}")

        # Validate health check timeout
        if self.health_check_timeout_seconds <= 0:
            raise ValueError("Health check timeout must be positive")

        # Validate heartbeat is not in future
        if self.last_heartbeat > datetime.now(UTC):
            raise ValueError("Last heartbeat cannot be in future")

        # Validate consistency
        if self.status == AgentStatus.TERMINATED and self.terminated_at is None:
            raise ValueError("Terminated agent must have terminated_at timestamp")

        if self.status != AgentStatus.TERMINATED and self.terminated_at is not None:
            raise ValueError("Non-terminated agent cannot have terminated_at")

    # ==================== Query Methods ====================

    def has_capability(self, capability_name: str) -> bool:
        """Check if agent has specific capability"""
        return any(c.name == capability_name for c in self.capabilities)

    def has_all_capabilities(self, required_capabilities: List[str]) -> bool:
        """Check if agent has all required capabilities"""
        return all(self.has_capability(cap) for cap in required_capabilities)

    def get_capability(self, capability_name: str) -> Optional[AgentCapability]:
        """Get capability by name"""
        return next((c for c in self.capabilities if c.name == capability_name), None)

    def is_available(self) -> bool:
        """Check if agent is available for new tasks"""
        return self.status == AgentStatus.AVAILABLE

    def is_healthy(self, thresholds: Dict[str, float] = None) -> bool:
        """
        Check if agent is healthy based on:
        1. Heartbeat recency (not exceeded timeout)
        2. Current metrics (if available)
        """
        # Check heartbeat
        age_seconds = (datetime.now(UTC) - self.last_heartbeat).total_seconds()
        if age_seconds > self.health_check_timeout_seconds:
            return False

        # Check metrics if available
        if self.current_metrics and thresholds:
            return self.current_metrics.is_healthy(thresholds)

        return True

    def is_active(self) -> bool:
        """Check if agent is not terminated"""
        return self.status != AgentStatus.TERMINATED

    def get_success_rate(self) -> float:
        """Calculate success rate from task history"""
        total = self.tasks_completed + self.tasks_failed
        if total == 0:
            return 1.0  # New agents start with perfect record
        return self.tasks_completed / total

    def get_availability_score(self) -> float:
        """
        Calculate availability score (0-1) based on status and health.

        Score = 1.0 for AVAILABLE
        Score = 0.5 for BUSY
        Score = 0.2 for PAUSED
        Score = 0.0 for UNHEALTHY or TERMINATED
        """
        status_scores = {
            AgentStatus.AVAILABLE: 1.0,
            AgentStatus.BUSY: 0.5,
            AgentStatus.PAUSED: 0.2,
            AgentStatus.UNHEALTHY: 0.0,
            AgentStatus.TERMINATED: 0.0,
        }
        return status_scores.get(self.status, 0.0)

    def get_load(self) -> float:
        """
        Estimate current load (0-1) based on status.

        Returns 0.0 if AVAILABLE
        Returns 0.8 if BUSY
        Returns 1.0 if PAUSED/UNHEALTHY/TERMINATED
        """
        load_map = {
            AgentStatus.AVAILABLE: 0.0,
            AgentStatus.BUSY: 0.8,
            AgentStatus.PAUSED: 1.0,
            AgentStatus.UNHEALTHY: 1.0,
            AgentStatus.TERMINATED: 1.0,
        }
        return load_map.get(self.status, 1.0)

    # ==================== State Transition Methods ====================
    # All return new instances (don't mutate)

    def mark_healthy(self, metrics: Optional[HealthMetrics] = None) -> 'AgentAggregate':
        """
        Record successful heartbeat and update metrics.

        Invariants:
        - If previously UNHEALTHY, can transition back to AVAILABLE
        - Updates last_heartbeat timestamp
        """
        # If agent was unhealthy and metrics are good, mark as available
        new_status = self.status
        if self.status == AgentStatus.UNHEALTHY and metrics and metrics.is_healthy({}):
            new_status = AgentStatus.AVAILABLE

        return replace(
            self,
            last_heartbeat=datetime.now(UTC),
            current_metrics=metrics,
            status=new_status
        )

    def mark_unhealthy(self, reason: str = "") -> 'AgentAggregate':
        """
        Mark agent as unhealthy due to missed heartbeat.

        Reason string is added to metadata for debugging.
        """
        new_metadata = dict(self.metadata)
        new_metadata["unhealthy_reason"] = reason
        new_metadata["unhealthy_at"] = datetime.now(UTC).isoformat()

        return replace(
            self,
            status=AgentStatus.UNHEALTHY,
            metadata=new_metadata
        )

    def assign_task(self, task_id: str) -> 'AgentAggregate':
        """
        Assign task to agent.

        Invariants:
        - Agent must be AVAILABLE
        - Cannot have current task
        - Task ID must be non-empty
        """
        if self.status != AgentStatus.AVAILABLE:
            raise ValueError(
                f"Cannot assign task to {self.status} agent. "
                f"Only AVAILABLE agents can accept tasks."
            )

        if self.current_task_id is not None:
            raise ValueError(
                f"Agent already has task {self.current_task_id}. "
                f"Complete it before assigning new task."
            )

        if not task_id or not task_id.strip():
            raise ValueError("task_id cannot be empty")

        return replace(
            self,
            status=AgentStatus.BUSY,
            current_task_id=task_id,
            tasks_assigned=self.tasks_assigned + 1
        )

    def complete_task(self) -> 'AgentAggregate':
        """
        Mark current task as completed.

        Invariants:
        - Agent must have a current task
        - Agent transitions back to AVAILABLE
        """
        if self.current_task_id is None:
            raise ValueError("Agent has no current task to complete")

        return replace(
            self,
            status=AgentStatus.AVAILABLE,
            current_task_id=None,
            tasks_completed=self.tasks_completed + 1
        )

    def fail_task(self, error_message: str = "") -> 'AgentAggregate':
        """
        Mark current task as failed.

        Invariants:
        - Agent must have a current task
        - Agent transitions back to AVAILABLE
        - Error message stored in metadata for debugging
        """
        if self.current_task_id is None:
            raise ValueError("Agent has no current task to fail")

        new_metadata = dict(self.metadata)
        new_metadata["last_error"] = error_message
        new_metadata["last_error_at"] = datetime.now(UTC).isoformat()

        return replace(
            self,
            status=AgentStatus.AVAILABLE,
            current_task_id=None,
            tasks_failed=self.tasks_failed + 1,
            metadata=new_metadata
        )

    def pause(self) -> 'AgentAggregate':
        """
        Pause agent operations.

        Invariants:
        - Agent must not be TERMINATED
        - If BUSY, must complete/fail current task first
        """
        if self.status == AgentStatus.TERMINATED:
            raise ValueError("Cannot pause terminated agent")

        if self.status == AgentStatus.BUSY:
            raise ValueError("Cannot pause agent with active task. Complete task first.")

        return replace(self, status=AgentStatus.PAUSED)

    def resume(self) -> 'AgentAggregate':
        """
        Resume agent operations.

        Invariants:
        - Agent must be PAUSED
        """
        if self.status != AgentStatus.PAUSED:
            raise ValueError(f"Can only resume PAUSED agents, agent is {self.status}")

        return replace(self, status=AgentStatus.AVAILABLE)

    def activate(self) -> 'AgentAggregate':
        """
        Activate agent for operation.

        Sets activated_at timestamp.
        """
        return replace(
            self,
            activated_at=datetime.now(UTC),
            status=AgentStatus.AVAILABLE
        )

    def terminate(self) -> 'AgentAggregate':
        """
        Permanently terminate agent.

        Invariants:
        - Agent must not already be TERMINATED
        - If BUSY, must complete/fail current task first
        """
        if self.status == AgentStatus.TERMINATED:
            raise ValueError("Agent is already terminated")

        if self.status == AgentStatus.BUSY:
            raise ValueError("Cannot terminate agent with active task. Complete task first.")

        return replace(
            self,
            status=AgentStatus.TERMINATED,
            terminated_at=datetime.now(UTC)
        )

    def add_capability(self, capability: AgentCapability) -> 'AgentAggregate':
        """
        Add new capability to agent.

        Invariants:
        - Cannot add duplicate capability (by name)
        """
        if any(c.name == capability.name for c in self.capabilities):
            raise ValueError(f"Agent already has capability: {capability.name}")

        new_capabilities = list(self.capabilities) + [capability]
        return replace(self, capabilities=new_capabilities)

    def upgrade_capability(self, capability_name: str, new_level: int) -> 'AgentAggregate':
        """
        Upgrade capability proficiency level.

        Invariants:
        - Capability must exist
        - New level must be valid (1-5)
        - Can only increase level, not decrease
        """
        capability = self.get_capability(capability_name)
        if not capability:
            raise ValueError(f"Agent does not have capability: {capability_name}")

        if new_level <= capability.proficiency_level:
            raise ValueError(
                f"Can only upgrade capability. Current: {capability.proficiency_level}, "
                f"Requested: {new_level}"
            )

        new_capabilities = [
            AgentCapability(c.name, new_level) if c.name == capability_name else c
            for c in self.capabilities
        ]
        return replace(self, capabilities=new_capabilities)

    def add_tag(self, tag: str) -> 'AgentAggregate':
        """Add tag to agent for organization/filtering"""
        if not tag or not tag.strip():
            raise ValueError("tag cannot be empty")

        new_tags = set(self.tags) | {tag}
        return replace(self, tags=new_tags)

    def remove_tag(self, tag: str) -> 'AgentAggregate':
        """Remove tag from agent"""
        new_tags = set(self.tags) - {tag}
        return replace(self, tags=new_tags)

    def update_metadata(self, key: str, value: str) -> 'AgentAggregate':
        """Update metadata key-value pair"""
        new_metadata = dict(self.metadata)
        new_metadata[key] = value
        return replace(self, metadata=new_metadata)
