"""
Comprehensive API Input Validation Models

Pydantic-based models for robust API request validation.

Architectural Intent:
- Comprehensive input validation for all API endpoints
- Automatic error message generation
- Type safety and data sanitization
- Support for complex nested structures
"""

from pydantic import BaseModel, Field, validator, constr, confloat, conint
from typing import List, Optional, Dict, Any, Literal, Union
from datetime import UTC, datetime
from enum import Enum


# Base models
class BaseRequest(BaseModel):
    """Base model with common functionality"""

    class Config:
        extra = "forbid"  # Forbid extra fields
        validate_assignment = True
        use_enum_values = True


class BaseResponse(BaseModel):
    """Base response model with common fields"""

    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(UTC), description="Response timestamp"
    )
    api_version: str = Field(default="1.0.0", description="API version")


# Tenant validation
class TenantId(str):
    """Custom tenant ID validation"""

    @classmethod
    def __get_validators__(cls):
        yield cls.validate_format
        yield cls.validate_length

    @validator("pre")
    def validate_format(cls, v):
        if not v or not v.strip():
            raise ValueError("Tenant ID cannot be empty")
        if not v.replace("-", "").replace("_", "").isalnum():
            raise ValueError(
                "Tenant ID must contain only alphanumeric characters, hyphens, and underscores"
            )
        return v

    @validator("pre")
    def validate_length(cls, v):
        if len(v) < 3:
            raise ValueError("Tenant ID must be at least 3 characters")
        if len(v) > 64:
            raise ValueError("Tenant ID cannot exceed 64 characters")
        return v


class AgentId(str):
    """Custom agent ID validation"""

    @classmethod
    def __get_validators__(cls):
        yield cls.validate_format
        yield cls.validate_length

    @validator("pre")
    def validate_format(cls, v):
        if not v or not v.strip():
            raise ValueError("Agent ID cannot be empty")
        if not v.replace("-", "").replace("_", "").isalnum():
            raise ValueError(
                "Agent ID must contain only alphanumeric characters, hyphens, and underscores"
            )
        return v

    @validator("pre")
    def validate_length(cls, v):
        if len(v) < 3:
            raise ValueError("Agent ID must be at least 3 characters")
        if len(v) > 256:
            raise ValueError("Agent ID cannot exceed 256 characters")
        return v


# Agent capability validation
class AgentCapability(BaseRequest):
    """Agent capability validation"""

    name: constr(min_length=1, max_length=100, description="Capability name")
    level: confloat(ge=1.0, le=5.0, description="Proficiency level (1.0-5.0)")

    @validator("name")
    def validate_name(cls, v):
        if not v or not v.strip():
            raise ValueError("Capability name cannot be empty")
        # Check for valid characters
        if not v.replace("-", "").replace("_", "").isalnum():
            raise ValueError(
                "Capability name must contain only alphanumeric characters, hyphens, and underscores"
            )
        return v


# Agent status enums
class AgentStatusEnum(str, Enum):
    """Valid agent status values"""

    AVAILABLE = "AVAILABLE"
    BUSY = "BUSY"
    PAUSED = "PAUSED"
    UNHEALTHY = "UNHEALTHY"
    TERMINATED = "TERMINATED"


class AgentTypeEnum(str, Enum):
    """Valid agent type values"""

    GENERIC = "generic"
    PROCESSOR = "processor"
    ANALYZER = "analyzer"
    COORDINATOR = "coordinator"
    WORKER = "worker"
    SPECIALIZED = "specialized"


# Message routing enums
class RoutingStrategyEnum(str, Enum):
    """Valid routing strategies"""

    ROUND_ROBIN = "round_robin"
    LEAST_CONNECTIONS = "least_connections"
    CAPABILITY_BASED = "capability_based"
    LOAD_BALANCED = "load_balanced"
    PRIORITY_QUEUE = "priority_queue"


# Request models
class TenantCreateRequest(BaseRequest):
    """Tenant creation request"""

    tenant_id: TenantId = Field(..., description="Unique tenant identifier")
    name: constr(min_length=1, max_length=256) = Field(
        ..., description="Tenant display name"
    )
    description: Optional[str] = Field(default=None, description="Tenant description")
    settings: Optional[Dict[str, Any]] = Field(
        default=None, description="Tenant configuration settings"
    )
    metadata: Optional[Dict[str, str]] = Field(
        default=None, description="Tenant metadata"
    )


class AgentCreateRequest(BaseRequest):
    """Enhanced agent creation request with comprehensive validation"""

    tenant_id: TenantId = Field(..., description="Tenant ID for agent")
    agent_id: AgentId = Field(..., description="Unique agent identifier")
    name: constr(min_length=3, max_length=256) = Field(..., description="Agent name")
    agent_type: AgentTypeEnum = Field(
        default=AgentTypeEnum.GENERIC, description="Agent type"
    )
    description: Optional[constr(max_length=1000)] = Field(
        default="", description="Agent description"
    )
    capabilities: List[AgentCapability] = Field(
        default=[], description="List of agent capabilities with proficiency levels"
    )
    resource_requirements: Optional[Dict[str, Any]] = Field(
        default=None, description="Resource requirements (CPU, memory, storage, etc.)"
    )
    metadata: Optional[Dict[str, str]] = Field(
        default=None, description="Agent metadata as key-value pairs"
    )
    tags: Optional[List[constr(max_length=50)]] = Field(
        default=None, description="List of agent tags for categorization"
    )

    @validator("capabilities")
    def validate_capabilities(cls, v):
        if not v:
            raise ValueError("Agent must have at least one capability")

        # Check for duplicate capabilities
        capability_names = [cap.name for cap in v]
        if len(capability_names) != len(set(capability_names)):
            raise ValueError("Duplicate capability names not allowed")

        return v

    @validator("resource_requirements")
    def validate_resource_requirements(cls, v):
        if v is None:
            return v

        # Validate common resource fields
        if "cpu" in v and not isinstance(v["cpu"], (int, float)) or v["cpu"] <= 0:
            raise ValueError("CPU must be a positive number")
        if (
            "memory" in v
            and not isinstance(v["memory"], (int, float))
            or v["memory"] <= 0
        ):
            raise ValueError("Memory must be a positive number")
        if (
            "storage" in v
            and not isinstance(v["storage"], (int, float))
            or v["storage"] <= 0
        ):
            raise ValueError("Storage must be a positive number")

        return v


class AgentUpdateRequest(BaseRequest):
    """Agent update request"""

    name: Optional[constr(min_length=3, max_length=256)] = Field(
        default=None, description="New agent name"
    )
    agent_type: Optional[AgentTypeEnum] = Field(
        default=None, description="New agent type"
    )
    description: Optional[constr(max_length=1000)] = Field(
        default=None, description="Updated agent description"
    )
    capabilities: Optional[List[AgentCapability]] = Field(
        default=None, description="Updated capabilities list"
    )
    resource_requirements: Optional[Dict[str, Any]] = Field(
        default=None, description="Updated resource requirements"
    )
    metadata: Optional[Dict[str, str]] = Field(
        default=None, description="Updated metadata"
    )
    tags: Optional[List[constr(max_length=50)]] = Field(
        default=None, description="Updated tags"
    )


class TaskAssignmentRequest(BaseRequest):
    """Task assignment request"""

    tenant_id: TenantId = Field(..., description="Tenant ID")
    agent_id: AgentId = Field(..., description="Target agent ID")
    task_id: constr(min_length=1, max_length=256) = Field(
        ..., description="Task identifier"
    )
    task_type: constr(min_length=1, max_length=100) = Field(
        ..., description="Task type"
    )
    priority: Literal["low", "medium", "high", "urgent"] = Field(
        default="medium", description="Task priority"
    )
    payload: Optional[Dict[str, Any]] = Field(
        default=None, description="Task payload data"
    )
    routing_strategy: RoutingStrategyEnum = Field(
        default=RoutingStrategyEnum.CAPABILITY_BASED,
        description="Routing strategy for task assignment",
    )
    timeout_seconds: Optional[conint(ge=1, le=3600)] = Field(
        default=300, description="Task timeout in seconds"
    )


class MessageRoutingRequest(BaseRequest):
    """Message routing request"""

    tenant_id: TenantId = Field(..., description="Tenant ID")
    message_type: constr(min_length=1, max_length=100) = Field(
        ..., description="Message type"
    )
    payload: Dict[str, Any] = Field(..., description="Message payload")
    routing_targets: List[str] = Field(
        ..., description="List of routing targets (agents, queues, etc.)"
    )
    routing_strategy: RoutingStrategyEnum = Field(
        default=RoutingStrategyEnum.LOAD_BALANCED, description="Routing strategy to use"
    )
    priority: Literal["low", "medium", "high"] = Field(
        default="medium", description="Message priority"
    )
    retry_count: conint(ge=0, le=5) = Field(
        default=3, description="Number of retry attempts on failure"
    )
    timeout_ms: conint(ge=100, le=30000) = Field(
        default=5000, description="Timeout in milliseconds"
    )


# Query models
class AgentListQuery(BaseRequest):
    """Agent listing query parameters"""

    tenant_id: TenantId = Field(..., description="Tenant ID")
    status: Optional[AgentStatusEnum] = Field(
        default=None, description="Filter by agent status"
    )
    agent_type: Optional[AgentTypeEnum] = Field(
        default=None, description="Filter by agent type"
    )
    capabilities: Optional[List[str]] = Field(
        default=None, description="Filter by required capabilities"
    )
    limit: conint(ge=1, le=1000) = Field(
        default=50, description="Maximum number of results to return"
    )
    offset: conint(ge=0, le=10000) = Field(
        default=0, description="Number of results to skip (for pagination)"
    )
    sort_by: Literal["name", "created_at", "status"] = Field(
        default="created_at", description="Sort field"
    )
    sort_order: Literal["asc", "desc"] = Field(default="desc", description="Sort order")


class MetricsQuery(BaseRequest):
    """Metrics query parameters"""

    tenant_id: Optional[TenantId] = Field(
        default=None, description="Filter by tenant ID"
    )
    start_time: Optional[datetime] = Field(
        default=None, description="Start time for metrics window"
    )
    end_time: Optional[datetime] = Field(
        default=None, description="End time for metrics window"
    )
    metrics: Optional[List[str]] = Field(
        default=None, description="Specific metrics to retrieve"
    )
    granularity: Literal["minute", "hour", "day"] = Field(
        default="hour", description="Metrics granularity"
    )


# Response models
class PaginationInfo(BaseModel):
    """Pagination information"""

    total: int = Field(..., description="Total number of items")
    limit: int = Field(..., description="Items per page")
    offset: int = Field(..., description="Items skipped")
    has_more: bool = Field(..., description="Whether more items exist")
    next_offset: Optional[int] = Field(default=None, description="Offset for next page")


class AgentInfoResponse(BaseResponse):
    """Comprehensive agent information response"""

    agent_id: str = Field(..., description="Agent ID")
    tenant_id: str = Field(..., description="Tenant ID")
    name: str = Field(..., description="Agent name")
    agent_type: AgentTypeEnum = Field(..., description="Agent type")
    status: AgentStatusEnum = Field(..., description="Current status")
    capabilities: List[Dict[str, Any]] = Field(..., description="Agent capabilities")
    resource_requirements: Optional[Dict[str, Any]] = Field(
        default=None, description="Resource requirements"
    )
    metadata: Dict[str, str] = Field(default_factory=dict, description="Agent metadata")
    tags: List[str] = Field(default_factory=list, description="Agent tags")
    created_at: datetime = Field(..., description="Creation timestamp")
    updated_at: Optional[datetime] = Field(
        default=None, description="Last update timestamp"
    )
    last_heartbeat: Optional[datetime] = Field(
        default=None, description="Last heartbeat timestamp"
    )
    performance_metrics: Optional[Dict[str, float]] = Field(
        default=None, description="Performance metrics"
    )


class AgentListResponse(BaseResponse):
    """Paginated agent list response"""

    agents: List[AgentInfoResponse] = Field(..., description="Agent list")
    pagination: PaginationInfo = Field(..., description="Pagination information")
    filters_applied: Optional[Dict[str, Any]] = Field(
        default=None, description="Filters that were applied to this query"
    )


class TaskAssignmentResponse(BaseResponse):
    """Task assignment response"""

    task_id: str = Field(..., description="Task ID")
    agent_id: str = Field(..., description="Assigned agent ID")
    status: Literal["assigned", "accepted", "started", "completed", "failed"] = Field(
        default="assigned", description="Assignment status"
    )
    assigned_at: datetime = Field(..., description="Assignment timestamp")
    started_at: Optional[datetime] = Field(
        default=None, description="Task start timestamp"
    )
    completed_at: Optional[datetime] = Field(
        default=None, description="Task completion timestamp"
    )
    result: Optional[Dict[str, Any]] = Field(default=None, description="Task result")
    error: Optional[str] = Field(default=None, description="Error message if failed")


class MessageRoutingResponse(BaseResponse):
    """Message routing response"""

    message_id: str = Field(..., description="Message ID")
    status: Literal["queued", "routed", "delivered", "failed"] = Field(
        default="queued", description="Message status"
    )
    routed_targets: List[str] = Field(..., description="Targets message was routed to")
    routing_strategy: RoutingStrategyEnum = Field(
        ..., description="Routing strategy used"
    )
    attempt_count: int = Field(..., description="Number of routing attempts")
    timestamp: datetime = Field(..., description="Routing timestamp")
    delivered_at: Optional[datetime] = Field(
        default=None, description="Delivery timestamp"
    )


class SystemMetricsResponse(BaseResponse):
    """System metrics response"""

    tenant_id: Optional[str] = Field(default=None, description="Tenant ID if filtered")
    time_window: Dict[str, datetime] = Field(..., description="Time window for metrics")
    metrics: Dict[str, Any] = Field(..., description="System metrics")
    uptime: float = Field(..., description="System uptime in seconds")
    agent_count: int = Field(..., description="Total agent count")
    message_throughput: float = Field(..., description="Messages per second")
    error_rate: float = Field(..., description="Error rate percentage")
    performance_summary: Dict[str, str] = Field(..., description="Performance summary")


# Health check models
class HealthCheckResponse(BaseResponse):
    """Comprehensive health check response"""

    status: Literal["healthy", "degraded", "unhealthy"] = Field(
        ..., description="Overall health status"
    )
    timestamp: datetime = Field(..., description="Health check timestamp")
    version: str = Field(..., description="System version")
    uptime: float = Field(..., description="System uptime in seconds")
    components: Dict[str, Dict[str, Any]] = Field(
        ..., description="Component health status"
    )
    system_info: Dict[str, Any] = Field(..., description="System information")
    dependencies: Dict[str, Dict[str, Any]] = Field(
        ..., description="Dependency health status"
    )
    recommendations: Optional[List[str]] = Field(
        default=None, description="System recommendations"
    )


# Error response models
class ValidationErrorResponse(BaseResponse):
    """Validation error response"""

    field: str = Field(..., description="Field that failed validation")
    message: str = Field(..., description="Validation error message")
    value: Any = Field(..., description="Invalid value")
    allowed_values: Optional[List[Any]] = Field(
        default=None, description="Allowed values for this field"
    )


class ErrorDetail(BaseModel):
    """Detailed error information"""

    code: str = Field(..., description="Error code")
    message: str = Field(..., description="Error message")
    field: Optional[str] = Field(
        default=None, description="Field associated with error"
    )
    details: Optional[Dict[str, Any]] = Field(
        default=None, description="Additional error details"
    )
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(UTC), description="Error timestamp"
    )


class ErrorResponse(BaseResponse):
    """Standard error response"""

    error: str = Field(..., description="Error type")
    message: str = Field(..., description="Error message")
    code: str = Field(..., description="Error code")
    details: List[ErrorDetail] = Field(..., description="Detailed error information")
    request_id: Optional[str] = Field(
        default=None, description="Request ID for tracking"
    )
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(UTC), description="Error timestamp"
    )
    path: Optional[str] = Field(
        default=None, description="Request path that caused the error"
    )


# Custom exception for validation
class ValidationError(Exception):
    """Custom validation exception"""

    def __init__(self, field: str, message: str, value: Any = None):
        self.field = field
        self.message = message
        self.value = value
        super().__init__(f"Validation failed for field '{field}': {message}")
