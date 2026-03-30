"""
Application DTOs

Architectural Intent:
- Data transfer objects for application layer
- Separate internal domain models from external representations
- Used by use cases and MCP servers
"""

from dataclasses import dataclass
from typing import List, Optional, Dict, Any
from datetime import datetime


@dataclass
class AgentDTO:
    """Agent data transfer object"""

    agent_id: str
    tenant_id: str
    name: str
    agent_type: str
    status: str
    capabilities: List[str]
    created_at: datetime
    tasks_completed: int = 0
    tasks_failed: int = 0
    metadata: Dict[str, str] = None

    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}

    def to_dict(self) -> Dict[str, Any]:
        return {
            "agent_id": self.agent_id,
            "tenant_id": self.tenant_id,
            "name": self.name,
            "agent_type": self.agent_type,
            "status": self.status,
            "capabilities": self.capabilities,
            "created_at": self.created_at.isoformat(),
            "tasks_completed": self.tasks_completed,
            "tasks_failed": self.tasks_failed,
            "metadata": self.metadata,
        }

    def to_json(self) -> str:
        import json

        return json.dumps(self.to_dict())


@dataclass
class TaskDTO:
    """Task data transfer object"""

    task_id: str
    tenant_id: str
    agent_id: Optional[str]
    status: str
    priority: int
    created_at: datetime
    data: Dict[str, Any] = None

    def __post_init__(self):
        if self.data is None:
            self.data = {}


__all__ = ["AgentDTO", "TaskDTO"]
