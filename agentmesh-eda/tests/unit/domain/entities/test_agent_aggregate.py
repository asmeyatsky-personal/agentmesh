"""
Unit Tests for Agent Aggregate

Architectural Intent:
- Verify aggregate enforces all invariants
- Test all state transitions
- Ensure immutability
- Validate business logic

Test Strategy:
- Unit tests for domain logic (no dependencies)
- One test per business rule
- Clear arrange-act-assert pattern
- Descriptive test names
"""

import pytest
from datetime import datetime, timedelta, UTC

from agentmesh.domain.entities.agent_aggregate import AgentAggregate
from agentmesh.domain.value_objects.agent_value_objects import (
    AgentId,
    AgentCapability,
    AgentStatus,
    ResourceRequirement,
    HealthMetrics
)


class TestAgentCreation:
    """Test agent aggregate creation and invariants"""

    def test_create_agent_with_valid_data(self):
        """Happy path: Create agent with all required data"""
        agent = AgentAggregate(
            agent_id=AgentId("agent-001"),
            tenant_id="tenant-1",
            name="Data Processor",
            capabilities=[AgentCapability("data_processing", 4)],
            agent_type="processor"
        )

        assert agent.agent_id.value == "agent-001"
        assert agent.tenant_id == "tenant-1"
        assert agent.name == "Data Processor"
        assert agent.status == AgentStatus.AVAILABLE
        assert len(agent.capabilities) == 1

    def test_create_agent_requires_capabilities(self):
        """Invariant: Agent must have at least one capability"""
        with pytest.raises(ValueError, match="at least one capability"):
            AgentAggregate(
                agent_id=AgentId("agent-001"),
                tenant_id="tenant-1",
                name="Empty Agent",
                capabilities=[]  # VIOLATION
            )

    def test_create_agent_requires_name(self):
        """Invariant: Agent must have non-empty name"""
        with pytest.raises(ValueError, match="name cannot be empty"):
            AgentAggregate(
                agent_id=AgentId("agent-001"),
                tenant_id="tenant-1",
                name="",  # VIOLATION
                capabilities=[AgentCapability("test", 1)]
            )

    def test_create_agent_requires_tenant_id(self):
        """Invariant: Agent must have tenant ID"""
        with pytest.raises(ValueError, match="tenant_id"):
            AgentAggregate(
                agent_id=AgentId("agent-001"),
                tenant_id="",  # VIOLATION
                name="Test Agent",
                capabilities=[AgentCapability("test", 1)]
            )

    def test_create_agent_requires_valid_status(self):
        """Invariant: Status must be valid enum value"""
        with pytest.raises(ValueError, match="Invalid status"):
            AgentAggregate(
                agent_id=AgentId("agent-001"),
                tenant_id="tenant-1",
                name="Test Agent",
                capabilities=[AgentCapability("test", 1)],
                status="INVALID_STATUS"  # VIOLATION
            )

    def test_agent_defaults_to_available_status(self):
        """Default: New agent starts as AVAILABLE"""
        agent = AgentAggregate(
            agent_id=AgentId("agent-001"),
            tenant_id="tenant-1",
            name="Test Agent",
            capabilities=[AgentCapability("test", 1)]
        )
        assert agent.status == AgentStatus.AVAILABLE


class TestAgentImmutability:
    """Test that agent aggregates are immutable"""

    def test_agent_is_frozen(self):
        """Invariant: Agent is immutable (frozen dataclass)"""
        agent = AgentAggregate(
            agent_id=AgentId("agent-001"),
            tenant_id="tenant-1",
            name="Test Agent",
            capabilities=[AgentCapability("test", 1)]
        )

        # Should not be able to modify
        with pytest.raises(Exception):  # FrozenInstanceError
            agent.name = "Modified"

    def test_state_transition_returns_new_instance(self):
        """State change must return new instance, not modify original"""
        original_agent = AgentAggregate(
            agent_id=AgentId("agent-001"),
            tenant_id="tenant-1",
            name="Test Agent",
            capabilities=[AgentCapability("test", 1)]
        )

        # Assign task returns new instance
        busy_agent = original_agent.assign_task("task-123")

        # Original unchanged
        assert original_agent.status == AgentStatus.AVAILABLE
        assert original_agent.current_task_id is None
        assert original_agent.tasks_assigned == 0

        # New instance changed
        assert busy_agent.status == AgentStatus.BUSY
        assert busy_agent.current_task_id == "task-123"
        assert busy_agent.tasks_assigned == 1

        # Different objects
        assert busy_agent is not original_agent


class TestAgentTaskManagement:
    """Test task assignment and completion"""

    def test_assign_task_to_available_agent(self):
        """Happy path: Assign task to available agent"""
        agent = AgentAggregate(
            agent_id=AgentId("agent-001"),
            tenant_id="tenant-1",
            name="Test Agent",
            capabilities=[AgentCapability("test", 1)]
        )

        busy_agent = agent.assign_task("task-123")

        assert busy_agent.status == AgentStatus.BUSY
        assert busy_agent.current_task_id == "task-123"
        assert busy_agent.tasks_assigned == 1

    def test_cannot_assign_task_to_busy_agent(self):
        """Invariant: Cannot assign task to BUSY agent"""
        agent = AgentAggregate(
            agent_id=AgentId("agent-001"),
            tenant_id="tenant-1",
            name="Test Agent",
            capabilities=[AgentCapability("test", 1)],
            status=AgentStatus.BUSY,
            current_task_id="task-1"
        )

        with pytest.raises(ValueError, match="Cannot assign task"):
            agent.assign_task("task-2")

    def test_cannot_assign_task_to_paused_agent(self):
        """Invariant: Cannot assign task to PAUSED agent"""
        agent = AgentAggregate(
            agent_id=AgentId("agent-001"),
            tenant_id="tenant-1",
            name="Test Agent",
            capabilities=[AgentCapability("test", 1)],
            status=AgentStatus.PAUSED
        )

        with pytest.raises(ValueError, match="Cannot assign task"):
            agent.assign_task("task-1")

    def test_complete_task(self):
        """Happy path: Complete assigned task"""
        agent = AgentAggregate(
            agent_id=AgentId("agent-001"),
            tenant_id="tenant-1",
            name="Test Agent",
            capabilities=[AgentCapability("test", 1)],
            status=AgentStatus.BUSY,
            current_task_id="task-123",
            tasks_assigned=1
        )

        completed_agent = agent.complete_task()

        assert completed_agent.status == AgentStatus.AVAILABLE
        assert completed_agent.current_task_id is None
        assert completed_agent.tasks_completed == 1

    def test_fail_task(self):
        """Task failure: Mark task as failed"""
        agent = AgentAggregate(
            agent_id=AgentId("agent-001"),
            tenant_id="tenant-1",
            name="Test Agent",
            capabilities=[AgentCapability("test", 1)],
            status=AgentStatus.BUSY,
            current_task_id="task-123",
            tasks_assigned=1
        )

        failed_agent = agent.fail_task("Database connection timeout")

        assert failed_agent.status == AgentStatus.AVAILABLE
        assert failed_agent.current_task_id is None
        assert failed_agent.tasks_failed == 1

    def test_cannot_complete_task_without_current_task(self):
        """Invariant: Cannot complete task if no current task"""
        agent = AgentAggregate(
            agent_id=AgentId("agent-001"),
            tenant_id="tenant-1",
            name="Test Agent",
            capabilities=[AgentCapability("test", 1)],
            status=AgentStatus.AVAILABLE,
            current_task_id=None
        )

        with pytest.raises(ValueError, match="no current task"):
            agent.complete_task()


class TestAgentStatusTransitions:
    """Test state machine transitions"""

    def test_pause_available_agent(self):
        """State transition: AVAILABLE -> PAUSED"""
        agent = AgentAggregate(
            agent_id=AgentId("agent-001"),
            tenant_id="tenant-1",
            name="Test Agent",
            capabilities=[AgentCapability("test", 1)]
        )

        paused = agent.pause()
        assert paused.status == AgentStatus.PAUSED

    def test_resume_paused_agent(self):
        """State transition: PAUSED -> AVAILABLE"""
        agent = AgentAggregate(
            agent_id=AgentId("agent-001"),
            tenant_id="tenant-1",
            name="Test Agent",
            capabilities=[AgentCapability("test", 1)],
            status=AgentStatus.PAUSED
        )

        resumed = agent.resume()
        assert resumed.status == AgentStatus.AVAILABLE

    def test_cannot_pause_busy_agent(self):
        """Invariant: Cannot pause agent with active task"""
        agent = AgentAggregate(
            agent_id=AgentId("agent-001"),
            tenant_id="tenant-1",
            name="Test Agent",
            capabilities=[AgentCapability("test", 1)],
            status=AgentStatus.BUSY,
            current_task_id="task-1"
        )

        with pytest.raises(ValueError, match="active task"):
            agent.pause()

    def test_terminate_agent(self):
        """State transition: Any -> TERMINATED"""
        agent = AgentAggregate(
            agent_id=AgentId("agent-001"),
            tenant_id="tenant-1",
            name="Test Agent",
            capabilities=[AgentCapability("test", 1)]
        )

        terminated = agent.terminate()
        assert terminated.status == AgentStatus.TERMINATED
        assert terminated.terminated_at is not None

    def test_cannot_terminate_busy_agent(self):
        """Invariant: Cannot terminate agent with active task"""
        agent = AgentAggregate(
            agent_id=AgentId("agent-001"),
            tenant_id="tenant-1",
            name="Test Agent",
            capabilities=[AgentCapability("test", 1)],
            status=AgentStatus.BUSY,
            current_task_id="task-1"
        )

        with pytest.raises(ValueError, match="active task"):
            agent.terminate()


class TestAgentCapabilities:
    """Test capability management"""

    def test_check_has_capability(self):
        """Query: Check if agent has capability"""
        agent = AgentAggregate(
            agent_id=AgentId("agent-001"),
            tenant_id="tenant-1",
            name="Test Agent",
            capabilities=[
                AgentCapability("data_processing", 4),
                AgentCapability("analysis", 3)
            ]
        )

        assert agent.has_capability("data_processing")
        assert agent.has_capability("analysis")
        assert not agent.has_capability("reporting")

    def test_check_has_all_capabilities(self):
        """Query: Check if agent has all required capabilities"""
        agent = AgentAggregate(
            agent_id=AgentId("agent-001"),
            tenant_id="tenant-1",
            name="Test Agent",
            capabilities=[
                AgentCapability("data_processing", 4),
                AgentCapability("analysis", 3),
                AgentCapability("visualization", 2)
            ]
        )

        assert agent.has_all_capabilities(["data_processing", "analysis"])
        assert not agent.has_all_capabilities(["data_processing", "reporting"])

    def test_add_capability(self):
        """Add new capability to agent"""
        agent = AgentAggregate(
            agent_id=AgentId("agent-001"),
            tenant_id="tenant-1",
            name="Test Agent",
            capabilities=[AgentCapability("test", 1)]
        )

        upgraded = agent.add_capability(AgentCapability("analysis", 3))

        assert len(upgraded.capabilities) == 2
        assert upgraded.has_capability("analysis")

    def test_cannot_add_duplicate_capability(self):
        """Invariant: Cannot add capability that already exists"""
        agent = AgentAggregate(
            agent_id=AgentId("agent-001"),
            tenant_id="tenant-1",
            name="Test Agent",
            capabilities=[AgentCapability("analysis", 3)]
        )

        with pytest.raises(ValueError, match="already has capability"):
            agent.add_capability(AgentCapability("analysis", 4))

    def test_upgrade_capability(self):
        """Upgrade existing capability proficiency"""
        agent = AgentAggregate(
            agent_id=AgentId("agent-001"),
            tenant_id="tenant-1",
            name="Test Agent",
            capabilities=[AgentCapability("analysis", 3)]
        )

        upgraded = agent.upgrade_capability("analysis", 5)

        assert upgraded.get_capability("analysis").proficiency_level == 5


class TestAgentHealth:
    """Test health monitoring"""

    def test_agent_starts_healthy(self):
        """Default: New agent is healthy"""
        agent = AgentAggregate(
            agent_id=AgentId("agent-001"),
            tenant_id="tenant-1",
            name="Test Agent",
            capabilities=[AgentCapability("test", 1)]
        )

        assert agent.is_healthy()

    def test_agent_becomes_unhealthy_after_timeout(self):
        """Agent is unhealthy if heartbeat timed out"""
        old_heartbeat = datetime.now(UTC) - timedelta(seconds=60)
        agent = AgentAggregate(
            agent_id=AgentId("agent-001"),
            tenant_id="tenant-1",
            name="Test Agent",
            capabilities=[AgentCapability("test", 1)],
            last_heartbeat=old_heartbeat,
            health_check_timeout_seconds=30
        )

        assert not agent.is_healthy()

    def test_mark_healthy_updates_heartbeat(self):
        """Mark healthy: Update heartbeat timestamp"""
        old_agent = AgentAggregate(
            agent_id=AgentId("agent-001"),
            tenant_id="tenant-1",
            name="Test Agent",
            capabilities=[AgentCapability("test", 1)],
            last_heartbeat=datetime.now(UTC) - timedelta(seconds=60)
        )

        healthy_agent = old_agent.mark_healthy()

        assert healthy_agent.is_healthy()
        assert healthy_agent.last_heartbeat > old_agent.last_heartbeat


class TestAgentPerformance:
    """Test performance tracking"""

    def test_calculate_success_rate(self):
        """Query: Calculate success rate from task history"""
        agent = AgentAggregate(
            agent_id=AgentId("agent-001"),
            tenant_id="tenant-1",
            name="Test Agent",
            capabilities=[AgentCapability("test", 1)],
            tasks_completed=8,
            tasks_failed=2
        )

        assert agent.get_success_rate() == 0.8  # 8/(8+2)

    def test_new_agent_has_perfect_success_rate(self):
        """Default: New agent has perfect success rate"""
        agent = AgentAggregate(
            agent_id=AgentId("agent-001"),
            tenant_id="tenant-1",
            name="Test Agent",
            capabilities=[AgentCapability("test", 1)],
            tasks_completed=0,
            tasks_failed=0
        )

        assert agent.get_success_rate() == 1.0
