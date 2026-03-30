"""
FastAPI-based REST API for AgentMesh

Provides comprehensive REST API with OpenAPI/Swagger documentation.

Architectural Intent:
- Replace Flask with FastAPI for better performance and documentation
- Automatic OpenAPI/Swagger documentation generation
- Built-in request validation and serialization
- Support for API versioning
- Integration with existing AgentMesh architecture
"""

import time
from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.openapi.utils import get_openapi
from pydantic import BaseModel, Field, validator
from typing import List, Optional, Dict, Any
from datetime import UTC, datetime
import asyncio
from contextlib import asynccontextmanager
from loguru import logger
from agentmesh.middleware.security import (
    SecurityMiddleware,
    SecurityConfig,
    SECURITY_CONFIGS,
    SecurityLevel,
    APIKeyValidator,
    generate_secure_token,
)
from agentmesh.security.auth import verify_access_token

from agentmesh.application.use_cases.create_agent_use_case import (
    CreateAgentUseCase,
    CreateAgentDTO,
)
from agentmesh.domain.value_objects.agent_value_objects import AgentCapability
from agentmesh.infrastructure.observability.metrics import AgentMeshMetrics
from agentmesh.api.validation_models import (
    AgentCreateRequest,
    AgentUpdateRequest,
    TaskAssignmentRequest,
    MessageRoutingRequest,
    AgentListQuery,
    MetricsQuery,
    AgentInfoResponse,
    AgentListResponse,
    TaskAssignmentResponse,
    MessageRoutingResponse,
    SystemMetricsResponse,
    HealthCheckResponse,
    ErrorResponse,
    ValidationErrorResponse,
    PaginationInfo,
    ValidationError,
    AgentStatusEnum,
    AgentTypeEnum,
    RoutingStrategyEnum,
)


class HealthResponse(BaseModel):
    """Health check response"""

    status: str = Field(..., description="System health status")
    timestamp: datetime = Field(..., description="Check timestamp")
    version: str = Field(..., description="API version")
    uptime: float = Field(..., description="System uptime in seconds")


class MetricsResponse(BaseModel):
    """System metrics response"""

    agent_count: int = Field(..., description="Total number of agents")
    messages_processed: int = Field(..., description="Messages processed")
    uptime: float = Field(..., description="System uptime")
    performance_metrics: Dict[str, float] = Field(
        ..., description="Performance metrics"
    )


# Security
security = HTTPBearer(auto_error=False)

# Track app startup time for real uptime
_app_start_time = time.time()

# Track runtime metrics
_runtime_metrics = {
    "agents_created": 0,
    "messages_processed": 0,
    "requests_total": 0,
}


async def get_current_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
) -> Dict[str, Any]:
    """Authenticate request via Bearer token. Returns decoded JWT payload."""
    if credentials is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"error": "Missing authentication token", "code": "AUTH_REQUIRED"},
            headers={"WWW-Authenticate": "Bearer"},
        )
    try:

        class _CredentialsException(Exception):
            pass

        payload = verify_access_token(credentials.credentials, _CredentialsException)
        return payload
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"error": "Invalid or expired token", "code": "AUTH_INVALID"},
            headers={"WWW-Authenticate": "Bearer"},
        )


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global _app_start_time
    _app_start_time = time.time()
    logger.info("AgentMesh API starting up...")

    yield

    logger.info("AgentMesh API shutting down...")


# Create FastAPI application
app = FastAPI(
    title="AgentMesh EDA API",
    description="Event-Driven Architecture for Agentic AI Systems",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    lifespan=lifespan,
)

# Add security middleware
security_config = SECURITY_CONFIGS[SecurityLevel.PRODUCTION]
app.add_middleware(SecurityMiddleware, security_config)

# Add compression middleware
app.add_middleware(GZipMiddleware, minimum_size=1000)


# Dependency injection
async def get_create_agent_use_case() -> CreateAgentUseCase:
    """Dependency injection for CreateAgentUseCase"""
    from agentmesh.infrastructure.adapters.in_memory_agent_repository import (
        InMemoryAgentRepository,
    )
    from agentmesh.cqrs.bus import EventBus
    from agentmesh.mal.router import MessageRouter

    repo = InMemoryAgentRepository()
    router = MessageRouter()
    event_bus = EventBus(router)
    return CreateAgentUseCase(repo, event_bus)


# API Routes


@app.get(
    "/health",
    response_model=HealthResponse,
    summary="Health Check",
    description="Check API health status",
    tags=["Health"],
    responses={200: {"model": HealthResponse, "description": "System healthy"}},
)
async def health_check():
    """Health check endpoint"""
    return HealthResponse(
        status="healthy",
        timestamp=datetime.now(UTC),
        version="1.0.0",
        uptime=time.time() - _app_start_time,
    )


@app.get(
    "/v1/tenants/{tenant_id}/agents",
    response_model=AgentListResponse,
    summary="List Agents",
    description="Get all agents for a tenant",
    tags=["Agents"],
    responses={
        200: {"model": AgentListResponse, "description": "List of agents"},
        404: {"model": ErrorResponse, "description": "Tenant not found"},
        500: {"model": ErrorResponse, "description": "Internal server error"},
    },
)
async def list_agents(
    tenant_id: str,
    use_case: CreateAgentUseCase = Depends(get_create_agent_use_case),
    user: Dict[str, Any] = Depends(get_current_user),
):
    """List all agents for a tenant"""
    try:
        # This would use actual repository in real implementation
        # For demo, return empty list
        return AgentListResponse(agents=[], total=0, tenant_id=tenant_id)
    except Exception as e:
        logger.error(f"Error listing agents: {e}")
        raise HTTPException(
            status_code=500, detail={"error": str(e), "code": "LIST_AGENTS_ERROR"}
        )


@app.post(
    "/v1/tenants/{tenant_id}/agents",
    response_model=AgentInfoResponse,
    summary="Create Agent",
    description="Create a new agent",
    tags=["Agents"],
    responses={
        200: {"model": AgentInfoResponse, "description": "Agent created successfully"},
        400: {"model": ErrorResponse, "description": "Invalid request"},
        409: {"model": ErrorResponse, "description": "Agent already exists"},
        500: {"model": ErrorResponse, "description": "Internal server error"},
    },
)
async def create_agent(
    tenant_id: str,
    agent_data: AgentCreateRequest,
    use_case: CreateAgentUseCase = Depends(get_create_agent_use_case),
    user: Dict[str, Any] = Depends(get_current_user),
):
    """Create a new agent"""
    try:
        # Convert request DTO to use case DTO
        create_dto = CreateAgentDTO(
            tenant_id=tenant_id,
            agent_id=agent_data.agent_id,
            name=agent_data.name,
            agent_type=agent_data.agent_type,
            description=agent_data.description,
            capabilities=[
                {"name": cap.name, "level": cap.level}
                for cap in agent_data.capabilities
            ],
            metadata=agent_data.metadata,
            tags=agent_data.tags,
        )

        result = await use_case.execute(create_dto)

        # Record metrics
        _runtime_metrics["agents_created"] += 1
        AgentMeshMetrics.record_agent_created(tenant_id)

        return AgentInfoResponse(
            agent_id=result.agent_id,
            tenant_id=result.tenant_id,
            name=result.name,
            agent_type=AgentTypeEnum(result.agent_type)
            if result.agent_type
            else AgentTypeEnum.GENERIC,
            status=AgentStatusEnum(result.status)
            if result.status
            else AgentStatusEnum.AVAILABLE,
            capabilities=[
                {"name": cap.name, "level": cap.level} for cap in result.capabilities
            ],
            resource_requirements=result.resource_requirements,
            metadata=result.metadata or {},
            tags=result.tags or [],
            created_at=result.created_at,
            updated_at=None,
            last_heartbeat=None,
            performance_metrics=None,
        )

    except Exception as e:
        logger.error(f"Error creating agent: {e}")
        raise HTTPException(
            status_code=500, detail={"error": str(e), "code": "CREATE_AGENT_ERROR"}
        )


@app.get(
    "/v1/tenants/{tenant_id}/agents/{agent_id}",
    response_model=AgentResponse,
    summary="Get Agent",
    description="Get specific agent by ID",
    tags=["Agents"],
    responses={
        200: {"model": AgentResponse, "description": "Agent found"},
        404: {"model": ErrorResponse, "description": "Agent not found"},
        500: {"model": ErrorResponse, "description": "Internal server error"},
    },
)
async def get_agent(
    tenant_id: str,
    agent_id: str,
    user: Dict[str, Any] = Depends(get_current_user),
):
    """Get specific agent by ID"""
    try:
        # In real implementation, would fetch from repository
        raise HTTPException(
            status_code=404,
            detail={"error": f"Agent {agent_id} not found", "code": "AGENT_NOT_FOUND"},
        )
    except Exception as e:
        logger.error(f"Error getting agent: {e}")
        raise HTTPException(
            status_code=500, detail={"error": str(e), "code": "GET_AGENT_ERROR"}
        )


@app.get(
    "/v1/metrics",
    response_model=MetricsResponse,
    summary="Get Metrics",
    description="Get system performance metrics",
    tags=["Metrics"],
    responses={
        200: {"model": MetricsResponse, "description": "Metrics retrieved"},
        500: {"model": ErrorResponse, "description": "Internal server error"},
    },
)
async def get_metrics():
    """Get system metrics"""
    try:
        uptime = time.time() - _app_start_time
        throughput = _runtime_metrics["requests_total"] / uptime if uptime > 0 else 0.0
        return MetricsResponse(
            agent_count=_runtime_metrics["agents_created"],
            messages_processed=_runtime_metrics["messages_processed"],
            uptime=uptime,
            performance_metrics={
                "requests_total": float(_runtime_metrics["requests_total"]),
                "throughput": round(throughput, 2),
            },
        )
    except Exception as e:
        logger.error(f"Error getting metrics: {e}")
        raise HTTPException(
            status_code=500, detail={"error": str(e), "code": "METRICS_ERROR"}
        )


# Exception handlers
@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc: HTTPException):
    """Handle HTTP exceptions"""
    return JSONResponse(
        status_code=exc.status_code,
        content=exc.detail if isinstance(exc.detail, dict) else {"error": exc.detail},
    )


@app.exception_handler(Exception)
async def general_exception_handler(request, exc: Exception):
    """Handle general exceptions"""
    logger.error(f"Unhandled exception: {exc}")
    return JSONResponse(
        status_code=500,
        content={"error": "Internal server error", "code": "INTERNAL_ERROR"},
    )


# Custom OpenAPI schema
def custom_openapi():
    """Generate custom OpenAPI schema"""
    if app.openapi_schema:
        return app.openapi_schema

    openapi_schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=app.routes,
    )

    # Add custom extensions
    openapi_schema["x-api-version"] = "1.0.0"
    openapi_schema["x-contact"] = {
        "name": "AgentMesh Team",
        "email": "support@agentmesh.ai",
    }
    openapi_schema["x-license"] = {
        "name": "MIT",
        "url": "https://opensource.org/licenses/MIT",
    }

    return openapi_schema


app.openapi = custom_openapi


if __name__ == "__main__":
    import uvicorn

    # Run the FastAPI application
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info", access_log=True)
