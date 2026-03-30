"""
Structured Logging with OpenTelemetry Integration

Implements JSON-formatted structured logging with automatic trace context propagation,
enabling full observability across distributed systems.

Architectural Intent:
- All logs include trace context (trace_id, span_id) for correlation
- JSON format enables log aggregation and analysis
- Automatic instrumentation of common libraries
- Integration with Jaeger for distributed tracing
"""

import logging
import json
import sys
from datetime import UTC, datetime
from typing import Optional, Dict, Any
from dataclasses import dataclass, asdict

try:
    from opentelemetry import trace, metrics
    from opentelemetry.exporter.jaeger.thrift import JaegerExporter
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.exporter.prometheus import PrometheusMetricReader
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
    from opentelemetry.instrumentation.redis import RedisInstrumentor
    from opentelemetry.instrumentation.requests import RequestsInstrumentor
    _OTEL_AVAILABLE = True
except ImportError:
    _OTEL_AVAILABLE = False
    trace = None


@dataclass(frozen=True)
class LogContext:
    """Immutable log context with trace information"""
    timestamp: str
    level: str
    logger: str
    message: str
    trace_id: str
    span_id: str
    service_name: str
    tenant_id: Optional[str] = None
    user_id: Optional[str] = None
    request_id: Optional[str] = None
    duration_ms: Optional[float] = None
    error: Optional[str] = None
    stack_trace: Optional[str] = None
    custom_fields: Optional[Dict[str, Any]] = None

    def to_json(self) -> str:
        """Serialize to JSON string"""
        data = asdict(self)
        data['custom_fields'] = self.custom_fields or {}
        return json.dumps(data, default=str)


class JsonFormatter(logging.Formatter):
    """JSON formatter for structured logging"""

    def __init__(self, service_name: str):
        super().__init__()
        self.service_name = service_name

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON"""
        trace_id = "0" * 32
        span_id = "0" * 16

        if _OTEL_AVAILABLE and trace is not None:
            span = trace.get_current_span()
            span_context = span.get_span_context()
            trace_id = format(span_context.trace_id, '032x')
            span_id = format(span_context.span_id, '016x')

        context = LogContext(
            timestamp=datetime.now(UTC).isoformat(),
            level=record.levelname,
            logger=record.name,
            message=record.getMessage(),
            trace_id=trace_id,
            span_id=span_id,
            service_name=self.service_name,
            duration_ms=record.msecs if hasattr(record, 'msecs') else None,
            error=str(record.exc_info) if record.exc_info else None,
            stack_trace=self.formatException(record) if record.exc_info else None
        )

        return context.to_json()


class StructuredLogger:
    """
    Structured logging with OpenTelemetry integration.

    Provides:
    - JSON-formatted structured logs
    - Automatic trace context propagation
    - Instrumentation of common libraries
    - Integration with Jaeger, ELK, Datadog
    - Custom context management
    """

    def __init__(self,
                 service_name: str,
                 jaeger_agent_host: str = "localhost",
                 jaeger_agent_port: int = 6831,
                 log_level: str = "INFO"):
        """
        Initialize structured logger

        Args:
            service_name: Name of the service (e.g., 'agentmesh-router')
            jaeger_agent_host: Jaeger agent hostname
            jaeger_agent_port: Jaeger agent port
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        """
        self.service_name = service_name
        self.logger = logging.getLogger(service_name)
        self.logger.setLevel(getattr(logging, log_level.upper()))

        # Setup OpenTelemetry tracing (if available)
        if _OTEL_AVAILABLE:
            self._setup_tracing(jaeger_agent_host, jaeger_agent_port)
        else:
            logging.getLogger(__name__).info(
                "OpenTelemetry not available — structured logging will work without tracing"
            )

        # Setup JSON formatter
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter(service_name))
        self.logger.addHandler(handler)

    def _setup_tracing(self, jaeger_host: str, jaeger_port: int):
        """Configure OpenTelemetry with Jaeger exporter"""
        jaeger_exporter = JaegerExporter(
            agent_host_name=jaeger_host,
            agent_port=jaeger_port,
        )

        tracer_provider = TracerProvider()
        tracer_provider.add_span_processor(BatchSpanProcessor(jaeger_exporter))
        trace.set_tracer_provider(tracer_provider)

        # Auto-instrument common libraries
        FastAPIInstrumentor().instrument()
        SQLAlchemyInstrumentor().instrument()
        RedisInstrumentor().instrument()
        RequestsInstrumentor().instrument()

    def get_logger(self) -> logging.Logger:
        """Get the configured logger"""
        return self.logger

    def info(self, message: str, **context):
        """Log info level message with context"""
        self._log_with_context(logging.INFO, message, context)

    def warning(self, message: str, **context):
        """Log warning level message with context"""
        self._log_with_context(logging.WARNING, message, context)

    def error(self, message: str, exception: Optional[Exception] = None, **context):
        """Log error level message with exception"""
        context['error'] = str(exception) if exception else None
        self._log_with_context(logging.ERROR, message, context, exc_info=exception)

    def debug(self, message: str, **context):
        """Log debug level message with context"""
        self._log_with_context(logging.DEBUG, message, context)

    def _log_with_context(self,
                         level: int,
                         message: str,
                         context: Dict[str, Any],
                         exc_info: Optional[Exception] = None):
        """Internal method to log with context"""
        if context:
            # Add context as JSON string to avoid formatting issues
            context_str = json.dumps(context, default=str)
            message = f"{message} | context: {context_str}"

        self.logger.log(level, message, exc_info=exc_info)

    def log_operation(self,
                     operation_name: str,
                     duration_ms: float,
                     success: bool,
                     **context):
        """Log completed operation with metrics"""
        level = logging.INFO if success else logging.WARNING
        message = f"Operation completed: {operation_name} ({duration_ms:.2f}ms)"

        context['operation'] = operation_name
        context['duration_ms'] = duration_ms
        context['success'] = success

        self._log_with_context(level, message, context)


# Global logger instance
_logger_instance: Optional[StructuredLogger] = None


def get_logger(service_name: str = "agentmesh") -> StructuredLogger:
    """Get or create global logger instance"""
    global _logger_instance
    if _logger_instance is None:
        _logger_instance = StructuredLogger(service_name)
    return _logger_instance


def configure_logger(service_name: str,
                    jaeger_agent_host: str = "localhost",
                    jaeger_agent_port: int = 6831,
                    log_level: str = "INFO"):
    """Configure global logger"""
    global _logger_instance
    _logger_instance = StructuredLogger(
        service_name,
        jaeger_agent_host,
        jaeger_agent_port,
        log_level
    )
    return _logger_instance
