"""
Universal Message Format

Architectural Intent:
- Single message format used across all messaging platforms
- Provides type-safe structure for routing, payload, and security
- Auto-generates message ID and timestamp on creation
- Serializable to/from JSON bytes for transport

Key Design Decisions:
1. Not frozen because __post_init__ must populate metadata defaults
2. All fields are typed Dict[str, Any] for flexibility across platforms
3. tenant_id is a required routing concept for multi-tenancy isolation
4. serialize/deserialize provide platform-agnostic transport encoding

Note: This dataclass is intentionally mutable to allow __post_init__
to populate metadata defaults (id, timestamp). Consumers should treat
instances as effectively immutable after construction.
"""

from dataclasses import dataclass, field
from typing import Dict, Any
import uuid
from datetime import UTC, datetime


@dataclass
class UniversalMessage:
    """
    Platform-agnostic message format for the Message Abstraction Layer.

    Fields:
        metadata: Message identity and lifecycle (id, timestamp, source, etc.)
        routing: Routing directives (targets, required_capabilities, etc.)
        payload: Business data (intent, content, etc.)
        context: Execution context (domain, correlation_id, etc.)
        security: Security context (auth tokens, encryption flags, etc.)
        tenant_id: Tenant scope for multi-tenancy isolation
    """
    metadata: Dict[str, Any] = field(default_factory=dict)
    routing: Dict[str, Any] = field(default_factory=dict)
    payload: Dict[str, Any] = field(default_factory=dict)
    context: Dict[str, Any] = field(default_factory=dict)
    security: Dict[str, Any] = field(default_factory=dict)
    tenant_id: str = "default_tenant"

    def __post_init__(self):
        if not self.tenant_id or not self.tenant_id.strip():
            raise ValueError("tenant_id cannot be empty")
        if not self.metadata.get("id"):
            self.metadata["id"] = str(uuid.uuid4())
        if not self.metadata.get("timestamp"):
            self.metadata["timestamp"] = datetime.now(UTC).isoformat()

    def serialize(self) -> bytes:
        import json

        return json.dumps(self.__dict__).encode("utf-8")

    @classmethod
    def deserialize(cls, data: bytes) -> "UniversalMessage":
        import json

        return cls(**json.loads(data))
