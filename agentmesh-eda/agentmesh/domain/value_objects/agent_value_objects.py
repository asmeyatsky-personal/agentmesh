"""
Agent Domain Value Objects

Architectural Intent:
- Encapsulate agent-related concepts without identity
- Provide type safety and domain-specific semantics
- Validate invariants at construction time
- Enable rich domain language in code

Key Design Decisions:
1. Immutable dataclasses (frozen=True) for thread safety
2. Validation in __post_init__ for fail-fast behavior
3. Comparison by value (built into dataclasses)
4. Small focused objects (single responsibility)
"""

from dataclasses import dataclass
from typing import List, Dict, Optional
from datetime import datetime, timedelta, UTC
from enum import Enum


@dataclass(frozen=True)
class AgentId:
    """
    Value Object: Agent Identity

    Invariants:
    - Must be non-empty string
    - Should follow naming conventions (alphanumeric with hyphens)
    """
    value: str

    def __post_init__(self):
        if not self.value or not self.value.strip():
            raise ValueError("AgentId cannot be empty")
        if len(self.value) < 3:
            raise ValueError("AgentId must be at least 3 characters")
        if len(self.value) > 256:
            raise ValueError("AgentId cannot exceed 256 characters")
        # Validate format: alphanumeric, hyphens, underscores
        if not all(c.isalnum() or c in '-_' for c in self.value):
            raise ValueError("AgentId must contain only alphanumeric characters, hyphens, and underscores")

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True)
class AgentCapability:
    """
    Value Object: Agent Capability with Proficiency Level

    Represents a skill the agent can perform.

    Invariants:
    - Capability name must be non-empty
    - Proficiency level must be 1-5 (novice to expert)
    """
    name: str
    proficiency_level: int  # 1=novice, 2=basic, 3=intermediate, 4=advanced, 5=expert

    def __post_init__(self):
        if not self.name or not self.name.strip():
            raise ValueError("Capability name cannot be empty")
        if not 1 <= self.proficiency_level <= 5:
            raise ValueError(f"Proficiency level must be 1-5, got {self.proficiency_level}")

    def is_expert(self) -> bool:
        """Check if agent is expert level (5)"""
        return self.proficiency_level == 5

    def is_advanced(self) -> bool:
        """Check if agent is at least advanced (4+)"""
        return self.proficiency_level >= 4

    def __str__(self) -> str:
        levels = {1: "novice", 2: "basic", 3: "intermediate", 4: "advanced", 5: "expert"}
        return f"{self.name}({levels[self.proficiency_level]})"


class AgentStatus(str, Enum):
    """
    Value Object: Agent Status Enumeration

    Represents current state of agent in system.
    Inherits from str so comparisons like status == "AVAILABLE" still work.
    """
    AVAILABLE = "AVAILABLE"
    BUSY = "BUSY"
    PAUSED = "PAUSED"
    UNHEALTHY = "UNHEALTHY"
    TERMINATED = "TERMINATED"

    @staticmethod
    def validate(status: str) -> bool:
        """Validate status string"""
        try:
            AgentStatus(status)
            return True
        except ValueError:
            return False


@dataclass(frozen=True)
class ResourceRequirement:
    """
    Value Object: Resource Requirements for Agent

    Specifies hardware/software requirements.

    Invariants:
    - All values must be non-negative
    - Cpu and memory are in standard units
    """
    cpu_cores: float = 1.0  # Number of CPU cores
    memory_gb: float = 1.0  # Memory in gigabytes
    disk_gb: float = 10.0  # Disk space in gigabytes
    gpu_count: int = 0  # Number of GPUs
    custom: Dict[str, float] = None  # Custom requirements

    def __post_init__(self):
        if self.cpu_cores <= 0:
            raise ValueError("CPU cores must be positive")
        if self.memory_gb <= 0:
            raise ValueError("Memory must be positive")
        if self.disk_gb <= 0:
            raise ValueError("Disk must be positive")
        if self.gpu_count < 0:
            raise ValueError("GPU count must be non-negative")

    def total_cost_estimate(self, hourly_rates: Dict[str, float]) -> float:
        """Estimate hourly cost based on rates"""
        cost = 0.0
        cost += self.cpu_cores * hourly_rates.get("cpu_per_core", 0.1)
        cost += self.memory_gb * hourly_rates.get("memory_per_gb", 0.05)
        cost += self.disk_gb * hourly_rates.get("disk_per_gb", 0.01)
        cost += self.gpu_count * hourly_rates.get("gpu", 1.0)
        return cost


@dataclass(frozen=True)
class HealthMetrics:
    """
    Value Object: Agent Health Metrics

    Snapshot of agent's health at a point in time.

    Invariants:
    - Success rate between 0.0 and 1.0
    - Response time is positive
    - All metrics are recent (measured within timeout period)
    """
    success_rate: float  # 0.0-1.0
    response_time_ms: float  # Milliseconds
    error_rate: float  # 0.0-1.0
    cpu_usage_percent: float  # 0-100
    memory_usage_percent: float  # 0-100
    tasks_completed: int
    tasks_failed: int

    def __post_init__(self):
        if not 0.0 <= self.success_rate <= 1.0:
            raise ValueError("Success rate must be 0.0-1.0")
        if not 0.0 <= self.error_rate <= 1.0:
            raise ValueError("Error rate must be 0.0-1.0")
        if self.response_time_ms < 0:
            raise ValueError("Response time must be positive")
        if not 0 <= self.cpu_usage_percent <= 100:
            raise ValueError("CPU usage must be 0-100%")
        if not 0 <= self.memory_usage_percent <= 100:
            raise ValueError("Memory usage must be 0-100%")
        if self.tasks_completed < 0 or self.tasks_failed < 0:
            raise ValueError("Task counts must be non-negative")

    def is_healthy(self, thresholds: Dict[str, float]) -> bool:
        """
        Check if agent is healthy based on thresholds.

        Thresholds dict should have:
        - min_success_rate: Minimum success rate (e.g., 0.8 = 80%)
        - max_error_rate: Maximum error rate (e.g., 0.1 = 10%)
        - max_response_time_ms: Maximum response time
        - max_cpu_percent: Maximum CPU usage
        - max_memory_percent: Maximum memory usage
        """
        if self.success_rate < thresholds.get("min_success_rate", 0.8):
            return False
        if self.error_rate > thresholds.get("max_error_rate", 0.1):
            return False
        if self.response_time_ms > thresholds.get("max_response_time_ms", 5000):
            return False
        if self.cpu_usage_percent > thresholds.get("max_cpu_percent", 90):
            return False
        if self.memory_usage_percent > thresholds.get("max_memory_percent", 90):
            return False
        return True


@dataclass(frozen=True)
class AgentHeartbeat:
    """
    Value Object: Agent Heartbeat Signal

    Represents periodic health check from agent.

    Invariants:
    - Timestamp must be recent (not in future or too old)
    - Metrics must be valid
    """
    agent_id: str
    timestamp: datetime
    metrics: HealthMetrics
    version: str = "1.0"

    def __post_init__(self):
        now = datetime.now(UTC)
        if self.timestamp > now:
            raise ValueError("Heartbeat timestamp cannot be in future")
        age_seconds = (now - self.timestamp).total_seconds()
        if age_seconds > 3600:  # 1 hour old is too old
            raise ValueError(f"Heartbeat is too old ({age_seconds}s)")

    def is_recent(self, timeout_seconds: int = 30) -> bool:
        """Check if heartbeat is recent enough"""
        age = (datetime.now(UTC) - self.timestamp).total_seconds()
        return age < timeout_seconds
