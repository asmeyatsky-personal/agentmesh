"""
Comprehensive Audit Logging for Compliance and Security.

Implements immutable audit trail logging for:
- Access control decisions (authentication/authorization)
- Data access and modifications
- Security policy changes
- Administrative actions
- Sensitive resource operations

Architectural Intent:
- Immutable, append-only semantics
- Automatic capture of context (IP, user agent, timestamp)
- Sensitive action immediate alerting
- Compliance with HIPAA, GDPR, SOC2, etc.
- Integration with SIEM systems
"""

import json
import logging
from dataclasses import dataclass, asdict, field
from datetime import UTC, datetime
from enum import Enum
from typing import Optional, Dict, Any, List
from abc import ABC

from .audit_store_port import AuditStorePort, AuditLogQuery


class AuditAction(str, Enum):
    """Standardized audit action types"""
    # Agent lifecycle
    AGENT_CREATED = "agent_created"
    AGENT_UPDATED = "agent_updated"
    AGENT_TERMINATED = "agent_terminated"
    AGENT_RESTARTED = "agent_restarted"

    # Task operations
    TASK_ASSIGNED = "task_assigned"
    TASK_COMPLETED = "task_completed"
    TASK_FAILED = "task_failed"
    TASK_CANCELLED = "task_cancelled"

    # Message operations
    MESSAGE_ROUTED = "message_routed"
    MESSAGE_DELIVERED = "message_delivered"
    MESSAGE_FAILED = "message_failed"

    # Security events
    AUTH_SUCCESS = "auth_success"
    AUTH_FAILURE = "auth_failure"
    UNAUTHORIZED_ACCESS = "unauthorized_access"
    SECRET_ACCESSED = "secret_accessed"
    SECRET_ROTATED = "secret_rotated"
    SECRET_REVOKED = "secret_revoked"

    # Policy & configuration
    POLICY_CREATED = "policy_created"
    POLICY_UPDATED = "policy_updated"
    POLICY_DELETED = "policy_deleted"
    CONFIG_CHANGED = "config_changed"

    # Compliance & admin
    AUDIT_LOG_EXPORTED = "audit_log_exported"
    COMPLIANCE_CHECK = "compliance_check"
    ADMIN_ACTION = "admin_action"


class AuditStatus(str, Enum):
    """Audit entry outcome status"""
    SUCCESS = "success"
    FAILURE = "failure"
    DENIED = "denied"  # Policy denial
    TIMEOUT = "timeout"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class AuditLogEntry:
    """
    Immutable audit log entry.

    Captures all information needed for:
    - Compliance reporting
    - Security investigation
    - Forensic analysis
    - Audit trails
    """
    action: AuditAction
    actor_id: str
    actor_type: str  # user, service, agent, system
    resource_id: str
    resource_type: str  # agent, task, message, policy, secret
    status: AuditStatus
    tenant_id: str
    timestamp: str  # ISO format
    request_id: str

    # Context information
    source_ip: Optional[str] = None
    user_agent: Optional[str] = None
    session_id: Optional[str] = None

    # Outcome details
    result_message: str = ""
    error_details: Optional[str] = None

    # Additional context
    details: Dict[str, Any] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)

    # Compliance fields
    is_sensitive: bool = False
    data_classification: str = "public"  # public, internal, confidential, restricted
    retention_days: int = 365

    # Audit metadata
    entry_id: str = ""
    created_at: str = ""
    hash: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage"""
        return asdict(self)

    def to_json(self) -> str:
        """Convert to JSON string"""
        return json.dumps(self.to_dict(), default=str)


class AuditLogger:
    """
    Immutable audit logging system.

    Handles:
    - Audit log entry creation and persistence
    - Sensitive action detection and alerting
    - Compliance reporting
    - Access history tracking
    """

    def __init__(self,
                 audit_store: AuditStorePort,
                 logger: Optional[logging.Logger] = None):
        """
        Initialize audit logger

        Args:
            audit_store: Backend storage implementation
            logger: Logger for immediate alerts (optional)
        """
        self.store = audit_store
        self.logger = logger or logging.getLogger(__name__)

    async def log_action(self,
                        action: AuditAction,
                        actor_id: str,
                        actor_type: str,
                        resource_id: str,
                        resource_type: str,
                        status: AuditStatus,
                        tenant_id: str,
                        request_id: str,
                        source_ip: Optional[str] = None,
                        result_message: str = "",
                        error_details: Optional[str] = None,
                        details: Optional[Dict[str, Any]] = None,
                        is_sensitive: bool = False) -> AuditLogEntry:
        """
        Log an audit action (immutable write).

        Args:
            action: Type of action performed
            actor_id: ID of actor performing action
            actor_type: Type of actor (user, service, agent)
            resource_id: ID of resource being acted upon
            resource_type: Type of resource
            status: Outcome status
            tenant_id: Tenant ID for multi-tenancy
            request_id: Request correlation ID
            source_ip: Source IP address if applicable
            result_message: Human-readable result message
            error_details: Error message if status is failure
            details: Additional structured details
            is_sensitive: Mark as sensitive for immediate alerting

        Returns:
            Created audit log entry
        """
        resolved_details = details or {}
        data_classification = resolved_details.get("data_classification", "public") if isinstance(resolved_details, dict) else "public"

        entry = AuditLogEntry(
            action=action,
            actor_id=actor_id,
            actor_type=actor_type,
            resource_id=resource_id,
            resource_type=resource_type,
            status=status,
            tenant_id=tenant_id,
            timestamp=datetime.now(UTC).isoformat(),
            request_id=request_id,
            source_ip=source_ip,
            result_message=result_message,
            error_details=error_details,
            details=resolved_details,
            is_sensitive=is_sensitive,
            data_classification=data_classification,
            created_at=datetime.now(UTC).isoformat()
        )

        # Store audit entry (immutable append)
        entry_id = await self.store.append(entry)

        # Immediate alerting for sensitive actions
        if self._is_sensitive_action(action) or is_sensitive:
            self._alert_sensitive_action(entry)

        # Log failures and denials
        if status in [AuditStatus.FAILURE, AuditStatus.DENIED]:
            self.logger.warning(
                f"Audit action failed: {action.value} by {actor_id} on {resource_id}",
                extra={
                    "audit_entry_id": entry_id,
                    "status": status.value,
                    "error": error_details
                }
            )

        return entry

    async def log_agent_action(self,
                              action: AuditAction,
                              agent_id: str,
                              task_id: str,
                              tenant_id: str,
                              request_id: str,
                              status: AuditStatus,
                              duration_ms: Optional[float] = None,
                              result: Optional[Dict] = None) -> AuditLogEntry:
        """Log agent task action"""
        details = {"duration_ms": duration_ms, "result": result}

        return await self.log_action(
            action=action,
            actor_id=agent_id,
            actor_type="agent",
            resource_id=task_id,
            resource_type="task",
            status=status,
            tenant_id=tenant_id,
            request_id=request_id,
            details=details
        )

    async def log_security_event(self,
                                action: AuditAction,
                                actor_id: str,
                                tenant_id: str,
                                request_id: str,
                                status: AuditStatus,
                                details: Dict[str, Any],
                                source_ip: Optional[str] = None) -> AuditLogEntry:
        """Log security-related event with immediate alerting"""
        return await self.log_action(
            action=action,
            actor_id=actor_id,
            actor_type="user",
            resource_id=actor_id,
            resource_type="security",
            status=status,
            tenant_id=tenant_id,
            request_id=request_id,
            source_ip=source_ip,
            details=details,
            is_sensitive=True  # Always sensitive
        )

    async def log_access(self,
                        actor_id: str,
                        resource_id: str,
                        resource_type: str,
                        tenant_id: str,
                        request_id: str,
                        granted: bool,
                        source_ip: Optional[str] = None,
                        reason: Optional[str] = None) -> AuditLogEntry:
        """Log access control decision"""
        action = AuditAction.AUTH_SUCCESS if granted else AuditAction.UNAUTHORIZED_ACCESS
        status = AuditStatus.SUCCESS if granted else AuditStatus.DENIED

        return await self.log_action(
            action=action,
            actor_id=actor_id,
            actor_type="user",
            resource_id=resource_id,
            resource_type=resource_type,
            status=status,
            tenant_id=tenant_id,
            request_id=request_id,
            source_ip=source_ip,
            result_message=reason or "",
            is_sensitive=not granted  # Denials are sensitive
        )

    async def query_logs(self,
                        tenant_id: str,
                        start_date: Optional[datetime] = None,
                        end_date: Optional[datetime] = None,
                        actor_id: Optional[str] = None,
                        resource_type: Optional[str] = None,
                        action: Optional[str] = None,
                        limit: int = 1000) -> List[AuditLogEntry]:
        """Query audit logs with filtering"""
        query = AuditLogQuery(
            tenant_id=tenant_id,
            start_date=start_date,
            end_date=end_date,
            actor_id=actor_id,
            resource_type=resource_type,
            action=action,
            limit=limit
        )
        return await self.store.query(query)

    async def get_actor_history(self,
                               tenant_id: str,
                               actor_id: str,
                               limit: int = 100) -> List[AuditLogEntry]:
        """Get activity history for an actor"""
        return await self.store.get_actor_history(tenant_id, actor_id, limit)

    async def get_resource_history(self,
                                  tenant_id: str,
                                  resource_id: str,
                                  limit: int = 100) -> List[AuditLogEntry]:
        """Get audit trail for a resource"""
        return await self.store.get_resource_history(tenant_id, resource_id, limit)

    async def export_compliance_report(self,
                                      tenant_id: str,
                                      start_date: datetime,
                                      end_date: datetime) -> bytes:
        """Export audit logs for compliance/regulatory reports"""
        return await self.store.export_range(tenant_id, start_date, end_date)

    def _is_sensitive_action(self, action: AuditAction) -> bool:
        """Determine if action requires immediate alerting"""
        sensitive_actions = {
            AuditAction.SECRET_ACCESSED,
            AuditAction.SECRET_REVOKED,
            AuditAction.AUTH_FAILURE,
            AuditAction.UNAUTHORIZED_ACCESS,
            AuditAction.POLICY_DELETED,
            AuditAction.ADMIN_ACTION,
        }
        return action in sensitive_actions

    def _alert_sensitive_action(self, entry: AuditLogEntry):
        """Send immediate alert for sensitive action"""
        message = (
            f"SECURITY ALERT: {entry.action.value} by {entry.actor_id} "
            f"on {entry.resource_type}:{entry.resource_id} "
            f"[Status: {entry.status.value}] [Tenant: {entry.tenant_id}]"
        )

        self.logger.critical(message, extra={"audit_entry": entry.to_dict()})

        # Could integrate with external alerting systems:
        # - PagerDuty
        # - Slack/Teams
        # - Email
        # - SIEM (Splunk, ELK, Datadog)
