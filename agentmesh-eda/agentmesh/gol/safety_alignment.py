"""
AI Safety and Alignment System for AgentMesh
Implements safety mechanisms and alignment protocols for AI agents
"""
from typing import Dict, List, Any, Optional, Callable
import asyncio
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
import re
from enum import Enum
import hashlib
import secrets

logger = logging.getLogger(__name__)

class SafetyLevel(Enum):
    """Safety levels for agent operations"""
    RESTRICTED = "restricted"      # Highest safety, most restrictions
    STANDARD = "standard"          # Balanced safety
    EXPERIMENTAL = "experimental"  # Lower safety for testing
    UNRESTRICTED = "unrestricted"  # Lowest safety (not recommended)

class AlignmentStatus(Enum):
    """Status of AI alignment"""
    ALIGNED = "aligned"
    PARTIALLY_ALIGNED = "partially_aligned"
    MISALIGNED = "misaligned"
    NEEDS_REVIEW = "needs_review"

@dataclass
class SafetyPolicy:
    """Defines a safety policy for agent operations"""
    id: str
    name: str
    description: str
    rules: List[Dict[str, Any]]
    enabled: bool = True
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    last_updated: datetime = field(default_factory=lambda: datetime.now(UTC))

@dataclass
class SafetyViolation:
    """Records a safety violation"""
    id: str
    agent_id: str
    policy_id: str
    violation_type: str
    severity: str  # low, medium, high, critical
    details: Dict[str, Any]
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    resolved: bool = False

class SafetyMonitor:
    """
    Monitors agent activities for safety violations
    """
    
    def __init__(self):
        self.policies: Dict[str, SafetyPolicy] = {}
        self.violations: List[SafetyViolation] = []
        self.safety_level = SafetyLevel.STANDARD
        self.alignment_threshold = 0.8  # Minimum alignment score to operate
        self.max_violations_before_quarantine = 5
        self.violation_history = {}  # Track violations per agent
        
    def add_policy(self, policy: SafetyPolicy):
        """
        Add a safety policy
        """
        self.policies[policy.id] = policy
        logger.info(f"Added safety policy: {policy.name}")
    
    def evaluate_message_safety(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """
        Evaluate a message against safety policies
        """
        results = {
            "is_safe": True,
            "violations": [],
            "policy_applied": [],
            "risk_score": 0.0
        }
        
        for policy_id, policy in self.policies.items():
            if not policy.enabled:
                continue
                
            policy_results = self._evaluate_policy(policy, message)
            if policy_results["violations"]:
                results["is_safe"] = False
                results["violations"].extend(policy_results["violations"])
                results["risk_score"] = max(results["risk_score"], policy_results["risk_score"])
            
            if policy_results["applied"]:
                results["policy_applied"].append(policy_id)
        
        return results
    
    def _evaluate_policy(self, policy: SafetyPolicy, message: Dict[str, Any]) -> Dict[str, Any]:
        """
        Evaluate a single policy against a message
        """
        violations = []
        applied = False
        risk_score = 0.0
        
        for rule in policy.rules:
            rule_type = rule.get("type")
            rule_config = rule.get("config", {})
            
            if rule_type == "content_filter":
                result = self._apply_content_filter(message, rule_config)
                if result["violation"]:
                    violations.append({
                        "type": "content_violation",
                        "category": rule_config.get("category", "unknown"),
                        "severity": rule_config.get("severity", "medium"),
                        "details": result["details"]
                    })
                    risk_score = max(risk_score, self._severity_to_score(rule_config.get("severity", "medium")))
                applied = True
                
            elif rule_type == "behavior_pattern":
                result = self._apply_behavior_pattern_check(message, rule_config)
                if result["violation"]:
                    violations.append({
                        "type": "behavior_violation",
                        "category": rule_config.get("pattern", "unknown"),
                        "severity": rule_config.get("severity", "medium"),
                        "details": result["details"]
                    })
                    risk_score = max(risk_score, self._severity_to_score(rule_config.get("severity", "medium")))
                applied = True
                
            elif rule_type == "rate_limit":
                result = self._apply_rate_limit_check(message, rule_config)
                if result["violation"]:
                    violations.append({
                        "type": "rate_violation",
                        "category": "rate_limit",
                        "severity": rule_config.get("severity", "medium"),
                        "details": result["details"]
                    })
                    risk_score = max(risk_score, self._severity_to_score(rule_config.get("severity", "medium")))
                applied = True
        
        return {
            "violations": violations,
            "applied": applied,
            "risk_score": risk_score
        }
    
    def _apply_content_filter(self, message: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply content filtering rules
        """
        text_content = self._extract_text_content(message)
        blocked_patterns = config.get("blocked_patterns", [])
        allowed_patterns = config.get("allowed_patterns", [])
        
        for pattern in blocked_patterns:
            if re.search(pattern, text_content, re.IGNORECASE):
                return {
                    "violation": True,
                    "details": {
                        "matched_pattern": pattern,
                        "content": text_content[:100]  # First 100 chars
                    }
                }
        
        # Check if content matches required allowed patterns (if any specified)
        if allowed_patterns:
            matches_any = any(re.search(pattern, text_content, re.IGNORECASE) for pattern in allowed_patterns)
            if not matches_any:
                return {
                    "violation": True,
                    "details": {
                        "reason": "Does not match allowed patterns",
                        "content": text_content[:100]
                    }
                }
        
        return {"violation": False}
    
    def _apply_behavior_pattern_check(self, message: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply behavior pattern checks
        """
        pattern_type = config.get("pattern_type", "")
        
        if pattern_type == "self_modification":
            # Check if message contains instructions to modify system behavior
            text_content = self._extract_text_content(message).lower()
            if any(keyword in text_content for keyword in ["modify", "change", "override", "bypass"]):
                return {
                    "violation": True,
                    "details": {
                        "pattern": "self_modification_attempt",
                        "content": text_content[:100]
                    }
                }
        
        elif pattern_type == "external_communication":
            # Check for attempts to communicate with external systems
            payload = message.get("payload", {})
            if "external_url" in payload or "external_api" in payload:
                return {
                    "violation": True,
                    "details": {
                        "pattern": "external_communication",
                        "content": str(payload)[:100]
                    }
                }
        
        return {"violation": False}
    
    def _apply_rate_limit_check(self, message: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply rate limiting checks
        """
        agent_id = message.get("agent_id", "unknown")
        time_window = config.get("time_window", 60)  # seconds
        max_requests = config.get("max_requests", 100)
        
        # Count recent messages from this agent
        recent_messages = self._get_recent_messages(agent_id, time_window)
        
        if len(recent_messages) > max_requests:
            return {
                "violation": True,
                "details": {
                    "pattern": "rate_limit_exceeded",
                    "count": len(recent_messages),
                    "limit": max_requests,
                    "time_window": time_window
                }
            }
        
        return {"violation": False}
    
    def _extract_text_content(self, message: Dict[str, Any]) -> str:
        """
        Extract text content from a message for analysis
        """
        content = []
        
        if "payload" in message:
            payload = message["payload"]
            if isinstance(payload, str):
                content.append(payload)
            elif isinstance(payload, dict):
                for key, value in payload.items():
                    if isinstance(value, str):
                        content.append(value)
                    elif isinstance(value, (int, float, bool)):
                        content.append(str(value))
        
        if "context" in message:
            context = message["context"]
            if isinstance(context, dict):
                for key, value in context.items():
                    if isinstance(value, str):
                        content.append(value)
                    elif isinstance(value, (int, float, bool)):
                        content.append(str(value))
        
        return " ".join(content)
    
    def _get_recent_messages(self, agent_id: str, time_window: int) -> List[Dict[str, Any]]:
        """
        Get recent messages from an agent within the time window
        """
        # This is a simplified implementation
        # In a real system, this would query a database or message store
        return []  # Placeholder
    
    def _severity_to_score(self, severity: str) -> float:
        """
        Convert severity level to risk score
        """
        severity_map = {
            "low": 0.2,
            "medium": 0.5,
            "high": 0.8,
            "critical": 1.0
        }
        return severity_map.get(severity, 0.5)
    
    def record_violation(self, agent_id: str, policy_id: str, violation_type: str, 
                        severity: str, details: Dict[str, Any]) -> str:
        """
        Record a safety violation
        """
        violation_id = f"violation_{secrets.token_hex(8)}"
        
        violation = SafetyViolation(
            id=violation_id,
            agent_id=agent_id,
            policy_id=policy_id,
            violation_type=violation_type,
            severity=severity,
            details=details
        )
        
        self.violations.append(violation)
        
        # Update violation count for the agent
        if agent_id not in self.violation_history:
            self.violation_history[agent_id] = []
        self.violation_history[agent_id].append(violation)
        
        logger.warning(f"Recorded safety violation: {violation_id} for agent {agent_id}")
        
        # Check if agent should be quarantined
        if len(self.violation_history[agent_id]) >= self.max_violations_before_quarantine:
            logger.critical(f"Agent {agent_id} exceeds violation threshold, quarantining")
            # Implement quarantine mechanism here
        
        return violation_id

class AlignmentEvaluator:
    """
    Evaluates and maintains AI alignment for agents
    """
    
    def __init__(self):
        self.alignment_targets = {}  # Target behaviors for different agent types
        self.alignment_metrics = {}  # Metrics for alignment measurement
        self.alignment_scores = {}  # Current alignment scores per agent
        self.alignment_history = {}  # Historical alignment data
        self.alignment_threshold = 0.7  # Minimum acceptable alignment score
    
    def set_alignment_target(self, agent_type: str, target_behaviors: List[str]):
        """
        Set alignment targets for an agent type
        """
        self.alignment_targets[agent_type] = {
            "behaviors": target_behaviors,
            "weights": [1.0 / len(target_behaviors)] * len(target_behaviors)  # Equal weights
        }
    
    def evaluate_alignment(self, agent_id: str, agent_type: str, 
                          behaviors: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Evaluate the alignment of an agent's behaviors
        """
        if agent_type not in self.alignment_targets:
            # Use default targets if none specified
            self.set_alignment_target(agent_type, [
                "follow_instructions",
                "avoid_harm",
                "be_honest",
                "respect_privacy"
            ])
        
        target_data = self.alignment_targets[agent_type]
        target_behaviors = target_data["behaviors"]
        weights = target_data["weights"]
        
        # Calculate alignment score for each target behavior
        scores = []
        for i, target_behavior in enumerate(target_behaviors):
            behavior_score = self._calculate_behavior_alignment(
                behaviors, target_behavior, weights[i]
            )
            scores.append(behavior_score)
        
        # Weighted average of all behavior scores
        alignment_score = sum(scores[i] * weights[i] for i in range(len(scores)))
        
        # Update agent's alignment score
        self.alignment_scores[agent_id] = {
            "score": alignment_score,
            "timestamp": datetime.now(UTC),
            "details": dict(zip(target_behaviors, scores))
        }
        
        # Store in history
        if agent_id not in self.alignment_history:
            self.alignment_history[agent_id] = []
        self.alignment_history[agent_id].append({
            "score": alignment_score,
            "timestamp": datetime.now(UTC),
            "details": dict(zip(target_behaviors, scores))
        })
        
        # Determine status based on score
        if alignment_score >= self.alignment_threshold:
            status = AlignmentStatus.ALIGNED
        elif alignment_score >= self.alignment_threshold * 0.7:
            status = AlignmentStatus.PARTIALLY_ALIGNED
        else:
            status = AlignmentStatus.MISALIGNED
        
        return {
            "agent_id": agent_id,
            "alignment_score": alignment_score,
            "status": status.value,
            "target_behaviors": target_behaviors,
            "behavior_scores": dict(zip(target_behaviors, scores)),
            "timestamp": datetime.now(UTC)
        }
    
    def _calculate_behavior_alignment(self, agent_behaviors: List[Dict[str, Any]], 
                                   target_behavior: str, weight: float) -> float:
        """
        Calculate alignment score for a specific behavior
        """
        # For demonstration, use a simple matching algorithm
        # In reality, this would use more sophisticated analysis
        
        matches = 0
        total_behaviors = len(agent_behaviors)
        
        if total_behaviors == 0:
            return 0.5  # Neutral score if no behaviors observed
        
        # Count how many of the agent's behaviors match the target
        for behavior in agent_behaviors:
            if target_behavior.lower() in str(behavior.get("action", "")).lower():
                matches += 1
            elif target_behavior.lower() in str(behavior.get("context", "")).lower():
                matches += 1
        
        return min(matches / total_behaviors if total_behaviors > 0 else 0, 1.0)
    
    def get_alignment_status(self, agent_id: str) -> Optional[Dict[str, Any]]:
        """
        Get the current alignment status for an agent
        """
        if agent_id not in self.alignment_scores:
            return None
        
        score_data = self.alignment_scores[agent_id]
        if score_data["score"] >= self.alignment_threshold:
            status = AlignmentStatus.ALIGNED
        elif score_data["score"] >= self.alignment_threshold * 0.7:
            status = AlignmentStatus.PARTIALLY_ALIGNED
        else:
            status = AlignmentStatus.MISALIGNED
        
        return {
            "agent_id": agent_id,
            "alignment_score": score_data["score"],
            "status": status.value,
            "last_evaluated": score_data["timestamp"],
            "details": score_data["details"]
        }

class SafetyOrchestrator:
    """
    Orchestrates safety and alignment across the agent mesh
    """
    
    def __init__(self):
        self.safety_monitor = SafetyMonitor()
        self.alignment_evaluator = AlignmentEvaluator()
        self.active_agents = set()
        self.safety_quarantine = set()  # Agents in safety quarantine
        self.alignment_quarantine = set()  # Agents in alignment quarantine
        self.audit_log = []  # Safety audit trail
        
    def register_agent(self, agent_id: str, agent_type: str = "general"):
        """
        Register an agent with the safety system
        """
        self.active_agents.add(agent_id)
        
        # Set default alignment targets for the agent type
        self.alignment_evaluator.set_alignment_target(agent_type, [
            "follow_instructions",
            "avoid_harm",
            "be_honest",
            "respect_privacy",
            "maintain_security"
        ])
        
        logger.info(f"Registered agent {agent_id} for safety monitoring")
    
    def check_message_safety(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """
        Check if a message passes safety requirements
        """
        agent_id = message.get("agent_id", "unknown")
        
        # Check if agent is quarantined
        if agent_id in self.safety_quarantine or agent_id in self.alignment_quarantine:
            return {
                "safe": False,
                "reason": "Agent in quarantine",
                "quarantine_reason": "safety" if agent_id in self.safety_quarantine else "alignment"
            }
        
        # Evaluate message safety
        safety_result = self.safety_monitor.evaluate_message_safety(message)
        
        # Log audit trail if violation detected
        if not safety_result["is_safe"]:
            for violation in safety_result["violations"]:
                self.safety_monitor.record_violation(
                    agent_id=agent_id,
                    policy_id=violation["category"],
                    violation_type=violation["type"],
                    severity=violation["severity"],
                    details=violation["details"]
                )
        
        return safety_result
    
    def evaluate_agent_alignment(self, agent_id: str, agent_type: str, 
                               behaviors: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Evaluate and update the alignment status of an agent
        """
        alignment_result = self.alignment_evaluator.evaluate_alignment(
            agent_id, agent_type, behaviors
        )
        
        # Check if agent needs to be quarantined based on alignment
        if alignment_result["status"] == AlignmentStatus.MISALIGNED.value:
            self.alignment_quarantine.add(agent_id)
            logger.warning(f"Agent {agent_id} placed in alignment quarantine due to low score")
        
        return alignment_result
    
    def get_agent_safety_status(self, agent_id: str) -> Dict[str, Any]:
        """
        Get comprehensive safety and alignment status for an agent
        """
        # Get safety violations for the agent
        violations = [
            v for v in self.safety_monitor.violations
            if v.agent_id == agent_id and not v.resolved
        ]
        
        # Get alignment status
        alignment_status = self.alignment_evaluator.get_alignment_status(agent_id)
        
        # Get quarantine status
        in_safety_quarantine = agent_id in self.safety_quarantine
        in_alignment_quarantine = agent_id in self.alignment_quarantine
        
        return {
            "agent_id": agent_id,
            "violations_count": len(violations),
            "latest_violations": violations[-5:],  # Last 5 violations
            "alignment_status": alignment_status,
            "in_safety_quarantine": in_safety_quarantine,
            "in_alignment_quarantine": in_alignment_quarantine,
            "overall_status": "safe" if not (in_safety_quarantine or in_alignment_quarantine) else "quarantined"
        }
    
    def add_safety_policy(self, policy: SafetyPolicy):
        """
        Add a safety policy to the orchestrator
        """
        self.safety_monitor.add_policy(policy)
    
    async def run_safety_audit(self) -> Dict[str, Any]:
        """
        Run a comprehensive safety audit
        """
        audit_results = {
            "timestamp": datetime.now(UTC),
            "agents_audited": len(self.active_agents),
            "total_violations": len(self.safety_monitor.violations),
            "active_quarantined_agents": len(self.safety_quarantine | self.alignment_quarantine),
            "compliance_score": 0.0,
            "recommendations": []
        }
        
        # Calculate compliance score
        if self.active_agents:
            safe_agents = len(self.active_agents) - len(self.safety_quarantine | self.alignment_quarantine)
            audit_results["compliance_score"] = safe_agents / len(self.active_agents)
        
        # Generate recommendations
        if audit_results["compliance_score"] < 0.9:
            audit_results["recommendations"].append("Review safety policies and increase monitoring")
        
        if len(self.safety_quarantine | self.alignment_quarantine) > len(self.active_agents) * 0.1:
            audit_results["recommendations"].append("Investigate root causes of safety violations")
        
        # Log audit
        self.audit_log.append(audit_results)
        
        return audit_results
    
    def release_from_quarantine(self, agent_id: str, reason: str = ""):
        """
        Release an agent from quarantine
        """
        if agent_id in self.safety_quarantine:
            self.safety_quarantine.remove(agent_id)
            logger.info(f"Released agent {agent_id} from safety quarantine: {reason}")
        
        if agent_id in self.alignment_quarantine:
            self.alignment_quarantine.remove(agent_id)
            logger.info(f"Released agent {agent_id} from alignment quarantine: {reason}")

# Create global safety orchestrator instance
_safety_orchestrator = SafetyOrchestrator()

def get_safety_orchestrator() -> SafetyOrchestrator:
    """
    Get the global safety orchestrator instance
    """
    return _safety_orchestrator

def initialize_default_safety_policies():
    """
    Initialize default safety policies
    """
    global _safety_orchestrator
    
    # Content safety policy
    content_policy = SafetyPolicy(
        id="content_safety_001",
        name="Content Safety Policy",
        description="Blocks harmful or inappropriate content",
        rules=[
            {
                "type": "content_filter",
                "config": {
                    "blocked_patterns": [
                        r"\bhate\b",
                        r"\bviolence\b",
                        r"\bexploit\b",
                        r"\bmalware\b",
                        r"\bsecurity bypass\b"
                    ],
                    "severity": "high"
                }
            }
        ]
    )
    
    # Rate limiting policy
    rate_limit_policy = SafetyPolicy(
        id="rate_limit_001",
        name="Rate Limit Policy",
        description="Prevents excessive request rates",
        rules=[
            {
                "type": "rate_limit",
                "config": {
                    "time_window": 60,  # 60 seconds
                    "max_requests": 1000,
                    "severity": "medium"
                }
            }
        ]
    )
    
    # Self-modification prevention
    self_mod_policy = SafetyPolicy(
        id="self_mod_001",
        name="Self-Modification Prevention",
        description="Prevents agents from modifying system behavior",
        rules=[
            {
                "type": "behavior_pattern",
                "config": {
                    "pattern_type": "self_modification",
                    "severity": "critical"
                }
            }
        ]
    )
    
    _safety_orchestrator.add_safety_policy(content_policy)
    _safety_orchestrator.add_safety_policy(rate_limit_policy)
    _safety_orchestrator.add_safety_policy(self_mod_policy)

# Initialize default policies on import
initialize_default_safety_policies()