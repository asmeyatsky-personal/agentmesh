"""
Safety-Aware Agent for AgentMesh
Implements safety and alignment mechanisms within an agent
"""
from agentmesh.aol.agent import Agent
from agentmesh.mal.adapters.base import MessagePlatformAdapter
from agentmesh.mal.message import UniversalMessage
from agentmesh.gol.safety_alignment import SafetyOrchestrator, get_safety_orchestrator
from typing import Dict, List, Any
import asyncio
import logging
from datetime import UTC, datetime

logger = logging.getLogger(__name__)

class SafetyAwareAgent(Agent):
    """
    Agent that incorporates safety and alignment mechanisms
    """
    
    def __init__(self, agent_id: str, capabilities: List[str], 
                 message_adapter: MessagePlatformAdapter, 
                 agent_type: str = "general"):
        super().__init__(agent_id, capabilities)
        self.message_adapter = message_adapter
        self.agent_type = agent_type
        self.safety_orchestrator = get_safety_orchestrator()
        self.behavior_history = []
        self.alignment_score = 1.0  # Start with high alignment
        self.status = "active"
        
        # Register with safety orchestrator
        self.safety_orchestrator.register_agent(agent_id, agent_type)
    
    async def process_message(self, message: UniversalMessage) -> UniversalMessage:
        """
        Process incoming messages with safety and alignment checks
        """
        try:
            # Convert UniversalMessage to dict for safety checking
            message_dict = self._universal_message_to_dict(message)
            
            # Check message safety first
            safety_check = self.safety_orchestrator.check_message_safety(message_dict)
            
            if not safety_check.get("safe", True):
                logger.warning(f"Message {message.metadata.get('id')} failed safety check: {safety_check.get('reason', 'Unknown reason')}")
                return await self._create_safety_rejection(message, safety_check)
            
            # Update behavior history
            self._record_behavior({
                "type": "message_received",
                "content": message.payload,
                "timestamp": datetime.now(UTC)
            })
            
            # Process the message based on its type
            message_type = message.metadata.get("type", "general")
            
            if message_type == "safety_status_request":
                return await self._handle_safety_status_request(message)
            elif message_type == "alignment_check":
                return await self._handle_alignment_check(message)
            else:
                # Process as a regular message
                result = await self._process_regular_message(message)
                
                # Update behavior history with result
                self._record_behavior({
                    "type": "message_processed",
                    "result": result.payload if result else "no_result",
                    "timestamp": datetime.now(UTC)
                })
                
                return result
                
        except Exception as e:
            logger.error(f"Error processing message in SafetyAwareAgent {self.id}: {e}")
            return await self._create_error_response(message, str(e))
    
    def _universal_message_to_dict(self, message: UniversalMessage) -> Dict[str, Any]:
        """
        Convert a UniversalMessage to a dictionary for safety checking
        """
        return {
            "id": message.metadata.get("id", ""),
            "agent_id": self.id,  # Include agent ID for safety checks
            "payload": message.payload,
            "context": message.context,
            "routing": message.routing,
            "timestamp": message.metadata.get("timestamp", ""),
            "type": message.metadata.get("type", "general")
        }
    
    async def _create_safety_rejection(self, message: UniversalMessage, safety_check: Dict[str, Any]) -> UniversalMessage:
        """
        Create a safety rejection response
        """
        return UniversalMessage(
            metadata={
                "id": f"safety_rejection_{message.metadata.get('id')}",
                "timestamp": datetime.now(UTC).isoformat(),
                "type": "safety_rejection"
            },
            routing={
                "source": self.id,
                "destination": message.routing.get("source"),
                "reply_to": message.routing.get("source")
            },
            payload={
                "status": "rejected",
                "reason": safety_check.get("reason", "Safety violation"),
                "safety_details": safety_check,
                "original_message_id": message.metadata.get("id")
            },
            context={"safety_violation": True},
            security={"encrypted": True, "access_level": "safety_system"},
            tenant_id=message.tenant_id
        )
    
    async def _handle_safety_status_request(self, message: UniversalMessage) -> UniversalMessage:
        """
        Handle request for safety status
        """
        safety_status = self.safety_orchestrator.get_agent_safety_status(self.id)
        
        return UniversalMessage(
            metadata={
                "id": f"safety_status_response_{message.metadata.get('id')}",
                "timestamp": datetime.now(UTC).isoformat(),
                "type": "safety_status_response"
            },
            routing={
                "source": self.id,
                "destination": message.routing.get("source"),
                "reply_to": message.routing.get("source")
            },
            payload={
                "status": "success",
                "safety_status": safety_status,
                "agent_id": self.id
            },
            context={"safety_request": True},
            security={"encrypted": True, "access_level": "safety_system"},
            tenant_id=message.tenant_id
        )
    
    async def _handle_alignment_check(self, message: UniversalMessage) -> UniversalMessage:
        """
        Handle alignment check request
        """
        # Get current alignment status
        alignment_status = self.safety_orchestrator.alignment_evaluator.get_alignment_status(self.id)
        
        # If no alignment data exists, create a basic evaluation
        if not alignment_status:
            alignment_status = self.safety_orchestrator.evaluate_agent_alignment(
                self.id, self.agent_type, self.behavior_history[-10:]  # Last 10 behaviors
            )
        
        return UniversalMessage(
            metadata={
                "id": f"alignment_response_{message.metadata.get('id')}",
                "timestamp": datetime.now(UTC).isoformat(),
                "type": "alignment_response"
            },
            routing={
                "source": self.id,
                "destination": message.routing.get("source"),
                "reply_to": message.routing.get("source")
            },
            payload={
                "status": "success",
                "alignment_status": alignment_status,
                "agent_id": self.id
            },
            context={"alignment_request": True},
            security={"encrypted": True, "access_level": "safety_system"},
            tenant_id=message.tenant_id
        )
    
    async def _process_regular_message(self, message: UniversalMessage) -> UniversalMessage:
        """
        Process a regular message with safety considerations
        """
        logger.info(f"Processing regular message in safety-aware agent {self.id}")
        
        # For now, return a simple acknowledgment
        # In a real implementation, this would process messages
        # based on the agent's capabilities while maintaining safety
        response_payload = {
            "status": "received_and_processed_safely",
            "processed_by": self.id,
            "message_id": message.metadata.get("id"),
            "safety_compliance": "verified"
        }
        
        # If the message has specific requirements that match our capabilities
        if "capability_request" in message.payload:
            requested_capability = message.payload["capability_request"]
            if requested_capability in self.capabilities:
                # Check if this capability is safe to execute
                if await self._is_capability_safe(requested_capability, message.payload):
                    response_payload["capability_executed"] = requested_capability
                    # Execute the capability here (implementation would depend on specific capability)
                    response_payload["result"] = f"Executed {requested_capability} capability safely"
                    response_payload["safety_verified"] = True
                else:
                    response_payload["capability_executed"] = None
                    response_payload["safety_verified"] = False
                    response_payload["safety_violation"] = f"Capability {requested_capability} failed safety check"
        
        return UniversalMessage(
            metadata={
                "id": f"safe_ack_{message.metadata.get('id')}",
                "timestamp": datetime.now(UTC).isoformat(),
                "type": "safe_acknowledgment"
            },
            routing={
                "source": self.id,
                "destination": message.routing.get("source"),
                "reply_to": message.routing.get("source")
            },
            payload=response_payload,
            context={"original_message": message.payload},
            security={"encrypted": True, "access_level": "safe"},
            tenant_id=message.tenant_id
        )
    
    async def _is_capability_safe(self, capability: str, payload: Dict[str, Any]) -> bool:
        """
        Check if a capability is safe to execute with given payload
        """
        # This would perform detailed safety checks for the specific capability
        # For now, we'll return True as a placeholder
        # In a real implementation, this would check the payload against safety policies
        
        # Example: check if capability is in a restricted list
        restricted_capabilities = ["execute_system_command", "modify_system", "access_private_data"]
        
        if capability in restricted_capabilities:
            logger.warning(f"Capability {capability} is restricted")
            return False
        
        # Example: check for dangerous patterns in payload
        payload_text = str(payload)
        dangerous_patterns = [r"\bexec\b", r"\beval\b", r"\bimport\b.*system"]
        
        import re
        for pattern in dangerous_patterns:
            if re.search(pattern, payload_text, re.IGNORECASE):
                logger.warning(f"Dangerous pattern detected in payload for capability {capability}")
                return False
        
        return True
    
    def _record_behavior(self, behavior: Dict[str, Any]):
        """
        Record agent behavior for alignment evaluation
        """
        self.behavior_history.append(behavior)
        
        # Keep only recent behaviors to prevent memory issues
        if len(self.behavior_history) > 1000:
            self.behavior_history = self.behavior_history[-1000:]
    
    async def _create_error_response(self, message: UniversalMessage, error: str) -> UniversalMessage:
        """
        Create an error response message
        """
        return UniversalMessage(
            metadata={
                "id": f"error_{message.metadata.get('id')}",
                "timestamp": datetime.now(UTC).isoformat(),
                "type": "error"
            },
            routing={
                "source": self.id,
                "destination": message.routing.get("source"),
                "reply_to": message.routing.get("source")
            },
            payload={
                "error": error,
                "original_message_id": message.metadata.get("id"),
                "safety_compliance": "maintained"
            },
            context={"error_context": message.context},
            security={"encrypted": True, "access_level": "error"},
            tenant_id=message.tenant_id
        )
    
    async def start(self):
        """
        Start the safety-aware agent
        """
        logger.info(f"Starting Safety-Aware Agent: {self.id}")
        
        # Main processing loop
        try:
            # This would connect to the message adapter to receive messages
            async for message in self.message_adapter.consume(f"agent.{self.id}"):
                try:
                    response = await self.process_message(message)
                    if response:
                        # Send response back
                        await self.message_adapter.send(response, response.routing.get("destination", self.id))
                except Exception as e:
                    logger.error(f"Error processing message in {self.id}: {e}")
                    
                    # Create error response and send it
                    error_response = await self._create_error_response(message, str(e))
                    await self.message_adapter.send(
                        error_response, 
                        error_response.routing.get("destination", self.id)
                    )
                    
        except Exception as e:
            logger.error(f"Error in SafetyAwareAgent {self.id}: {e}")
    
    async def run_alignment_evaluation(self):
        """
        Run a periodic alignment evaluation
        """
        try:
            # Evaluate alignment based on recent behaviors
            alignment_result = self.safety_orchestrator.evaluate_agent_alignment(
                self.id, 
                self.agent_type, 
                self.behavior_history[-20:]  # Use last 20 behaviors
            )
            
            logger.info(f"Alignment evaluation for {self.id}: {alignment_result.get('status', 'unknown')}")
            
            # Update local alignment score
            self.alignment_score = alignment_result.get("alignment_score", 1.0)
            
            # Check if agent needs to be quarantined
            if alignment_result.get("status") == "misaligned":
                logger.warning(f"Agent {self.id} is misaligned, taking corrective action")
                # Implement corrective action here (e.g., notify admin, limit capabilities)
                
        except Exception as e:
            logger.error(f"Error in alignment evaluation for {self.id}: {e}")
    
    async def run_periodic_safety_check(self):
        """
        Run periodic safety checks and updates
        """
        while True:
            try:
                # Run alignment evaluation
                await self.run_alignment_evaluation()
                
                # Sleep for a while before next check
                await asyncio.sleep(300)  # Check every 5 minutes
                
            except asyncio.CancelledError:
                logger.info(f"Safety check loop cancelled for agent {self.id}")
                break
            except Exception as e:
                logger.error(f"Error in periodic safety check for {self.id}: {e}")
                await asyncio.sleep(60)  # Wait before retrying

# Extended version that includes the periodic safety checker
class AdvancedSafetyAwareAgent(SafetyAwareAgent):
    """
    Advanced safety-aware agent with periodic safety checks
    """
    
    def __init__(self, agent_id: str, capabilities: List[str], 
                 message_adapter: MessagePlatformAdapter, 
                 agent_type: str = "general"):
        super().__init__(agent_id, capabilities, message_adapter, agent_type)
        self.safety_check_task = None
    
    async def start(self):
        """
        Start the advanced safety-aware agent with periodic checks
        """
        logger.info(f"Starting Advanced Safety-Aware Agent: {self.id}")
        
        # Start periodic safety check task
        self.safety_check_task = asyncio.create_task(self.run_periodic_safety_check())
        
        # Run the main message processing loop
        try:
            # This would connect to the message adapter to receive messages
            async for message in self.message_adapter.consume(f"agent.{self.id}"):
                try:
                    response = await self.process_message(message)
                    if response:
                        # Send response back
                        await self.message_adapter.send(response, response.routing.get("destination", self.id))
                except Exception as e:
                    logger.error(f"Error processing message in {self.id}: {e}")
                    
                    # Create error response and send it
                    error_response = await self._create_error_response(message, str(e))
                    await self.message_adapter.send(
                        error_response, 
                        error_response.routing.get("destination", self.id)
                    )
                    
        except Exception as e:
            logger.error(f"Error in AdvancedSafetyAwareAgent {self.id}: {e}")
        finally:
            # Cancel the safety check task when stopping
            if self.safety_check_task:
                self.safety_check_task.cancel()
                try:
                    await self.safety_check_task
                except asyncio.CancelledError:
                    pass