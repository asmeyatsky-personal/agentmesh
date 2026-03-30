"""
Agent Domain Events

Architectural Intent:
- Represent important things that happen in the agent domain
- Enable event sourcing and event-driven architecture
- Allow other bounded contexts to react to agent changes
- Provide audit trail of agent lifecycle

Key Design Decisions:
1. Events are immutable (frozen dataclasses)
2. Base DomainEvent provides common attributes
3. Event versioning for backward compatibility
4. Timestamps in UTC for consistency
5. Minimal data in events (references, not full objects)

Domain Events:
- AgentCreatedEvent: New agent registered
- AgentActivatedEvent: Agent ready for operation
- AgentCapabilityAddedEvent: Agent gained new skill
- AgentTaskAssignedEvent: Task assigned to agent
- AgentTaskCompletedEvent: Agent completed task
- AgentTaskFailedEvent: Agent failed task
- AgentStatusChangedEvent: Agent status changed
- AgentHealthCheckPassedEvent: Agent passed health check
- AgentHealthCheckFailedEvent: Agent failed health check
- AgentTerminatedEvent: Agent removed from system
"""

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Dict, Optional, List
import uuid


@dataclass(frozen=True)
class AgentCreatedEvent:
    """
    Event: Agent was created and registered in system.

    Triggered when: CreateAgentUseCase executes successfully
    Listened by: Agent registry, observability system
    """

    aggregate_id: str  # agent_id
    tenant_id: str
    name: str
    agent_type: str
    capabilities: List[str]
    metadata: Dict[str, str] = field(default_factory=dict)
    occurred_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    version: str = "1.0"

    @property
    def agent_id(self) -> str:
        """Get agent ID from aggregate_id"""
        return self.aggregate_id

    def __str__(self) -> str:
        return f"AgentCreated(id={self.agent_id}, name={self.name}, caps={len(self.capabilities)})"


@dataclass(frozen=True)
class AgentActivatedEvent:
    """
    Event: Agent activated and ready for operation.

    Triggered when: Agent.activate() is called
    Listened by: Agent registry, scheduling system
    """

    aggregate_id: str  # agent_id
    tenant_id: str
    activated_at: datetime
    occurred_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    version: str = "1.0"

    @property
    def agent_id(self) -> str:
        """Get agent ID from aggregate_id"""
        return self.aggregate_id

    def __str__(self) -> str:
        return f"AgentActivated(id={self.agent_id})"


@dataclass(frozen=True)
class AgentCapabilityAddedEvent:
    """
    Event: Agent gained new capability.

    Triggered when: Agent.add_capability() is called
    Listened by: Agent registry, capability matcher
    """

    aggregate_id: str  # agent_id
    tenant_id: str
    capability_name: str
    proficiency_level: int
    occurred_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    version: str = "1.0"

    @property
    def agent_id(self) -> str:
        """Get agent ID from aggregate_id"""
        return self.aggregate_id

    def __str__(self) -> str:
        return f"AgentCapabilityAdded(id={self.agent_id}, cap={self.capability_name}, level={self.proficiency_level})"


@dataclass(frozen=True)
class AgentCapabilityUpgradedEvent:
    """
    Event: Agent improved capability level.

    Triggered when: Agent.upgrade_capability() is called
    Listened by: Agent registry, performance analyzer
    """

    aggregate_id: str  # agent_id
    tenant_id: str
    capability_name: str
    from_level: int
    to_level: int
    occurred_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    version: str = "1.0"

    @property
    def agent_id(self) -> str:
        """Get agent ID from aggregate_id"""
        return self.aggregate_id

    def __str__(self) -> str:
        return f"AgentCapabilityUpgraded(id={self.agent_id}, cap={self.capability_name}, level={self.from_level}->{self.to_level})"


@dataclass(frozen=True)
class AgentTaskAssignedEvent:
    """
    Event: Task was assigned to agent.

    Triggered when: Agent.assign_task() is called
    Listened by: Task tracking, performance metrics
    """

    aggregate_id: str  # agent_id
    tenant_id: str
    task_id: str
    assigned_at: datetime
    occurred_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    version: str = "1.0"

    @property
    def agent_id(self) -> str:
        """Get agent ID from aggregate_id"""
        return self.aggregate_id

    def __str__(self) -> str:
        return f"AgentTaskAssigned(agent={self.agent_id}, task={self.task_id})"


@dataclass(frozen=True)
class AgentTaskCompletedEvent:
    """
    Event: Agent completed assigned task.

    Triggered when: Agent.complete_task() is called
    Listened by: Task tracking, metrics, next-agent notification
    """

    aggregate_id: str  # agent_id
    tenant_id: str
    task_id: str
    completed_at: datetime
    result: Dict = field(default_factory=dict)  # Task result/output
    execution_time_ms: int = 0  # How long task took
    occurred_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    version: str = "1.0"

    @property
    def agent_id(self) -> str:
        """Get agent ID from aggregate_id"""
        return self.aggregate_id

    def __str__(self) -> str:
        return f"AgentTaskCompleted(agent={self.agent_id}, task={self.task_id}, time={self.execution_time_ms}ms)"


@dataclass(frozen=True)
class AgentTaskFailedEvent:
    """
    Event: Agent failed to complete assigned task.

    Triggered when: Agent.fail_task() is called
    Listened by: Task tracking, error handling, retry mechanism
    """

    aggregate_id: str  # agent_id
    tenant_id: str
    task_id: str
    failed_at: datetime
    error_message: str
    error_code: str = "UNKNOWN"
    occurred_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    version: str = "1.0"

    @property
    def agent_id(self) -> str:
        """Get agent ID from aggregate_id"""
        return self.aggregate_id

    def __str__(self) -> str:
        return f"AgentTaskFailed(agent={self.agent_id}, task={self.task_id}, error={self.error_code})"


@dataclass(frozen=True)
class AgentStatusChangedEvent:
    """
    Event: Agent status changed (AVAILABLE -> BUSY -> AVAILABLE, etc).

    Triggered when: Any state transition occurs
    Listened by: Monitoring system, load balancer, UI
    """

    aggregate_id: str  # agent_id
    tenant_id: str
    from_status: str
    to_status: str
    changed_at: datetime
    reason: str = ""
    occurred_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    version: str = "1.0"

    @property
    def agent_id(self) -> str:
        """Get agent ID from aggregate_id"""
        return self.aggregate_id

    def __str__(self) -> str:
        return f"AgentStatusChanged(id={self.agent_id}, {self.from_status}->{self.to_status})"


@dataclass(frozen=True)
class AgentHealthCheckPassedEvent:
    """
    Event: Agent passed health check (heartbeat received).

    Triggered when: Agent.mark_healthy() is called
    Listened by: Health monitoring, alerting system
    """

    aggregate_id: str  # agent_id
    tenant_id: str
    checked_at: datetime
    success_rate: float
    response_time_ms: float
    cpu_usage_percent: float
    memory_usage_percent: float
    occurred_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    version: str = "1.0"

    @property
    def agent_id(self) -> str:
        """Get agent ID from aggregate_id"""
        return self.aggregate_id

    def __str__(self) -> str:
        return f"AgentHealthCheckPassed(id={self.agent_id}, cpu={self.cpu_usage_percent}%, mem={self.memory_usage_percent}%)"


@dataclass(frozen=True)
class AgentHealthCheckFailedEvent:
    """
    Event: Agent failed health check (heartbeat timeout).

    Triggered when: Agent.mark_unhealthy() is called
    Listened by: Alerting system, failover mechanism
    """

    aggregate_id: str  # agent_id
    tenant_id: str
    checked_at: datetime
    last_heartbeat: datetime
    timeout_seconds: int
    reason: str = "Heartbeat timeout"
    occurred_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    version: str = "1.0"

    @property
    def agent_id(self) -> str:
        """Get agent ID from aggregate_id"""
        return self.aggregate_id

    def __str__(self) -> str:
        return f"AgentHealthCheckFailed(id={self.agent_id}, reason={self.reason})"


@dataclass(frozen=True)
class AgentPausedEvent:
    """
    Event: Agent was paused.

    Triggered when: Agent.pause() is called
    Listened by: Task scheduler, monitoring
    """

    aggregate_id: str  # agent_id
    tenant_id: str
    paused_at: datetime
    reason: str = ""
    occurred_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    version: str = "1.0"

    @property
    def agent_id(self) -> str:
        """Get agent ID from aggregate_id"""
        return self.aggregate_id

    def __str__(self) -> str:
        return f"AgentPaused(id={self.agent_id})"


@dataclass(frozen=True)
class AgentResumedEvent:
    """
    Event: Agent was resumed.

    Triggered when: Agent.resume() is called
    Listened by: Task scheduler, monitoring
    """

    aggregate_id: str  # agent_id
    tenant_id: str
    resumed_at: datetime
    occurred_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    version: str = "1.0"

    @property
    def agent_id(self) -> str:
        """Get agent ID from aggregate_id"""
        return self.aggregate_id

    def __str__(self) -> str:
        return f"AgentResumed(id={self.agent_id})"


@dataclass(frozen=True)
class AgentTerminatedEvent:
    """
    Event: Agent was terminated (removed from service).

    Triggered when: Agent.terminate() is called
    Listened by: Resource cleanup, monitoring, accounting
    """

    aggregate_id: str  # agent_id
    tenant_id: str
    terminated_at: datetime
    reason: str = ""
    tasks_completed: int = 0
    tasks_failed: int = 0
    occurred_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    version: str = "1.0"

    @property
    def agent_id(self) -> str:
        """Get agent ID from aggregate_id"""
        return self.aggregate_id

    def __str__(self) -> str:
        return f"AgentTerminated(id={self.agent_id}, completed={self.tasks_completed}, failed={self.tasks_failed})"
