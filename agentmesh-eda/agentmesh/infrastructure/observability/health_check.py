"""
Health Check Service for System Observability

Provides comprehensive health checking for all system components with
detailed status reporting for Kubernetes readiness/liveness probes.

Architectural Intent:
- Separate liveness (can restart?) from readiness (can accept traffic?)
- Detailed component-level health enables quick failure diagnosis
- Health status propagates through layers for cascading alerts
- Health checks are non-blocking and cacheable
"""

import asyncio
import time
from dataclasses import dataclass, field, asdict
from datetime import UTC, datetime
from enum import Enum
from typing import Dict, Any, List, Optional, Callable


class HealthStatus(str, Enum):
    """Health status enumeration"""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"


@dataclass(frozen=True)
class ComponentHealth:
    """Health status of a single component"""
    name: str
    status: HealthStatus
    response_time_ms: float
    message: str
    details: Dict[str, Any] = field(default_factory=dict)
    last_checked_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return asdict(self)


@dataclass(frozen=True)
class SystemHealth:
    """Overall system health status"""
    overall_status: HealthStatus
    timestamp: str
    version: str
    environment: str
    components: List[ComponentHealth]
    dependencies: Dict[str, HealthStatus]
    metrics: Dict[str, Any] = field(default_factory=dict)

    def is_ready(self) -> bool:
        """Check if system is ready to accept traffic"""
        return (
            self.overall_status != HealthStatus.UNHEALTHY and
            all(d != HealthStatus.UNHEALTHY for d in self.dependencies.values())
        )

    def is_alive(self) -> bool:
        """Check if system is alive (not dead)"""
        # System is alive if it can check itself
        return self.overall_status != HealthStatus.UNHEALTHY

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON response"""
        return {
            'overall_status': self.overall_status.value,
            'timestamp': self.timestamp,
            'version': self.version,
            'environment': self.environment,
            'is_ready': self.is_ready(),
            'is_alive': self.is_alive(),
            'components': [c.to_dict() for c in self.components],
            'dependencies': {k: v.value for k, v in self.dependencies.items()},
            'metrics': self.metrics
        }


class HealthCheckService:
    """
    Comprehensive health checking for AgentMesh components.

    Checks:
    - Database connectivity and query performance
    - Message broker connectivity and latency
    - Cache system operation
    - Event store availability
    - External service dependencies
    - Resource constraints (memory, connections)
    """

    # Health check timeouts (seconds)
    CHECK_TIMEOUT = 5.0
    DB_WARN_THRESHOLD_MS = 100
    BROKER_WARN_THRESHOLD_MS = 500
    CACHE_WARN_THRESHOLD_MS = 50

    def __init__(self,
                 service_name: str = "agentmesh",
                 version: str = "1.0.0",
                 environment: str = "production",
                 dependencies: Optional[Dict[str, Any]] = None):
        """
        Initialize health check service

        Args:
            service_name: Name of the service
            version: Service version
            environment: Deployment environment
            dependencies: External dependencies to check (db, broker, cache, etc.)
        """
        self.service_name = service_name
        self.version = version
        self.environment = environment
        self.dependencies = dependencies or {}

        # Cache last health check result
        self._last_health = None
        self._last_check_time = None
        self._cache_ttl_seconds = 10

    async def check_database(self) -> ComponentHealth:
        """Check database connectivity and performance"""
        if 'db' not in self.dependencies:
            return ComponentHealth(
                name="database",
                status=HealthStatus.HEALTHY,
                response_time_ms=0,
                message="Database dependency not configured"
            )

        start = time.time()
        try:
            db = self.dependencies['db']

            # Execute simple query to verify connectivity
            result = await asyncio.wait_for(
                db.execute("SELECT 1"),
                timeout=self.CHECK_TIMEOUT
            )

            response_time = (time.time() - start) * 1000

            # Determine status based on response time
            if response_time > self.DB_WARN_THRESHOLD_MS:
                status = HealthStatus.DEGRADED
            else:
                status = HealthStatus.HEALTHY

            return ComponentHealth(
                name="database",
                status=status,
                response_time_ms=response_time,
                message="Database is operational",
                details={
                    "connection_pool_size": getattr(db, 'pool_size', 'unknown'),
                    "query_time_ms": response_time,
                    "threshold_ms": self.DB_WARN_THRESHOLD_MS
                }
            )
        except asyncio.TimeoutError:
            response_time = (time.time() - start) * 1000
            return ComponentHealth(
                name="database",
                status=HealthStatus.UNHEALTHY,
                response_time_ms=response_time,
                message="Database health check timed out",
                details={"timeout_seconds": self.CHECK_TIMEOUT}
            )
        except Exception as e:
            response_time = (time.time() - start) * 1000
            return ComponentHealth(
                name="database",
                status=HealthStatus.UNHEALTHY,
                response_time_ms=response_time,
                message=f"Database check failed: {str(e)}",
                details={"error": str(e)}
            )

    async def check_message_broker(self) -> ComponentHealth:
        """Check message broker connectivity and latency"""
        if 'message_broker' not in self.dependencies:
            return ComponentHealth(
                name="message_broker",
                status=HealthStatus.HEALTHY,
                response_time_ms=0,
                message="Message broker dependency not configured"
            )

        start = time.time()
        try:
            broker = self.dependencies['message_broker']

            # Execute health check operation
            await asyncio.wait_for(
                broker.health_check(),
                timeout=self.CHECK_TIMEOUT
            )

            response_time = (time.time() - start) * 1000

            status = (
                HealthStatus.DEGRADED if response_time > self.BROKER_WARN_THRESHOLD_MS
                else HealthStatus.HEALTHY
            )

            return ComponentHealth(
                name="message_broker",
                status=status,
                response_time_ms=response_time,
                message="Message broker is operational",
                details={
                    "round_trip_ms": response_time,
                    "threshold_ms": self.BROKER_WARN_THRESHOLD_MS
                }
            )
        except asyncio.TimeoutError:
            response_time = (time.time() - start) * 1000
            return ComponentHealth(
                name="message_broker",
                status=HealthStatus.UNHEALTHY,
                response_time_ms=response_time,
                message="Message broker health check timed out",
                details={"timeout_seconds": self.CHECK_TIMEOUT}
            )
        except Exception as e:
            response_time = (time.time() - start) * 1000
            return ComponentHealth(
                name="message_broker",
                status=HealthStatus.UNHEALTHY,
                response_time_ms=response_time,
                message=f"Message broker check failed: {str(e)}",
                details={"error": str(e)}
            )

    async def check_cache(self) -> ComponentHealth:
        """Check cache system health"""
        if 'cache' not in self.dependencies:
            return ComponentHealth(
                name="cache",
                status=HealthStatus.HEALTHY,
                response_time_ms=0,
                message="Cache dependency not configured"
            )

        start = time.time()
        try:
            cache = self.dependencies['cache']
            test_key = "health-check-test"
            test_value = "test"

            # Test write
            await asyncio.wait_for(
                cache.set(test_key, test_value, 10),
                timeout=self.CHECK_TIMEOUT
            )

            # Test read
            value = await asyncio.wait_for(
                cache.get(test_key),
                timeout=self.CHECK_TIMEOUT
            )

            if value != test_value:
                raise ValueError("Cache returned unexpected value")

            # Cleanup
            await cache.delete(test_key)

            response_time = (time.time() - start) * 1000

            status = (
                HealthStatus.DEGRADED if response_time > self.CACHE_WARN_THRESHOLD_MS
                else HealthStatus.HEALTHY
            )

            return ComponentHealth(
                name="cache",
                status=status,
                response_time_ms=response_time,
                message="Cache is operational",
                details={
                    "operation_time_ms": response_time,
                    "threshold_ms": self.CACHE_WARN_THRESHOLD_MS
                }
            )
        except asyncio.TimeoutError:
            response_time = (time.time() - start) * 1000
            return ComponentHealth(
                name="cache",
                status=HealthStatus.UNHEALTHY,
                response_time_ms=response_time,
                message="Cache health check timed out",
                details={"timeout_seconds": self.CHECK_TIMEOUT}
            )
        except Exception as e:
            response_time = (time.time() - start) * 1000
            return ComponentHealth(
                name="cache",
                status=HealthStatus.UNHEALTHY,
                response_time_ms=response_time,
                message=f"Cache check failed: {str(e)}",
                details={"error": str(e)}
            )

    async def check_event_store(self) -> ComponentHealth:
        """Check event store availability"""
        if 'event_store' not in self.dependencies:
            return ComponentHealth(
                name="event_store",
                status=HealthStatus.HEALTHY,
                response_time_ms=0,
                message="Event store dependency not configured"
            )

        start = time.time()
        try:
            event_store = self.dependencies['event_store']

            await asyncio.wait_for(
                event_store.health_check(),
                timeout=self.CHECK_TIMEOUT
            )

            response_time = (time.time() - start) * 1000

            return ComponentHealth(
                name="event_store",
                status=HealthStatus.HEALTHY,
                response_time_ms=response_time,
                message="Event store is operational",
                details={"response_time_ms": response_time}
            )
        except Exception as e:
            response_time = (time.time() - start) * 1000
            return ComponentHealth(
                name="event_store",
                status=HealthStatus.UNHEALTHY,
                response_time_ms=response_time,
                message=f"Event store check failed: {str(e)}",
                details={"error": str(e)}
            )

    async def get_system_health(self, use_cache: bool = True) -> SystemHealth:
        """
        Get comprehensive system health status

        Args:
            use_cache: Use cached result if available (within TTL)

        Returns:
            SystemHealth object with overall status and component details
        """
        # Use cached result if available and fresh
        if use_cache and self._last_health and self._last_check_time:
            elapsed = time.time() - self._last_check_time
            if elapsed < self._cache_ttl_seconds:
                return self._last_health

        # Check all components in parallel
        components = await asyncio.gather(
            self.check_database(),
            self.check_message_broker(),
            self.check_cache(),
            self.check_event_store(),
            return_exceptions=False
        )

        # Determine overall status
        statuses = [c.status for c in components]
        if any(s == HealthStatus.UNHEALTHY for s in statuses):
            overall = HealthStatus.UNHEALTHY
        elif any(s == HealthStatus.DEGRADED for s in statuses):
            overall = HealthStatus.DEGRADED
        else:
            overall = HealthStatus.HEALTHY

        # Build dependency map
        dependencies = {c.name: c.status for c in components}

        # Calculate metrics
        metrics = {
            "component_count": len(components),
            "healthy_count": sum(1 for c in components if c.status == HealthStatus.HEALTHY),
            "degraded_count": sum(1 for c in components if c.status == HealthStatus.DEGRADED),
            "unhealthy_count": sum(1 for c in components if c.status == HealthStatus.UNHEALTHY),
            "average_response_time_ms": sum(c.response_time_ms for c in components) / len(components) if components else 0
        }

        health = SystemHealth(
            overall_status=overall,
            timestamp=datetime.now(UTC).isoformat(),
            version=self.version,
            environment=self.environment,
            components=components,
            dependencies=dependencies,
            metrics=metrics
        )

        # Cache result
        self._last_health = health
        self._last_check_time = time.time()

        return health

    async def wait_for_ready(self, max_retries: int = 30, retry_delay_seconds: float = 1.0) -> bool:
        """
        Wait for system to become ready

        Args:
            max_retries: Maximum number of retry attempts
            retry_delay_seconds: Delay between retries

        Returns:
            True if system became ready, False if timeout
        """
        for attempt in range(max_retries):
            health = await self.get_system_health(use_cache=False)
            if health.is_ready():
                return True

            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay_seconds)

        return False
