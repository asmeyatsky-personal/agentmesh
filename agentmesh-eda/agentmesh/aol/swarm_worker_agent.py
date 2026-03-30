"""
Swarm Intelligence Worker Agent for AgentMesh
Implements specialized behavior for participating in swarm intelligence operations
"""
from agentmesh.aol.agent import Agent
from agentmesh.mal.adapters.base import MessagePlatformAdapter
from agentmesh.mal.message import UniversalMessage
from typing import Dict, List, Any
import asyncio
import logging
from datetime import UTC, datetime
import json

logger = logging.getLogger(__name__)

class SwarmWorkerAgent(Agent):
    """
    Specialized agent designed to work within a swarm intelligence framework
    """
    
    def __init__(self, agent_id: str, capabilities: List[str], 
                 message_adapter: MessagePlatformAdapter, 
                 swarm_orchestrator_id: str = "swarm_orchestrator"):
        super().__init__(agent_id, capabilities)
        self.message_adapter = message_adapter
        self.swarm_orchestrator_id = swarm_orchestrator_id
        self.current_tasks = {}
        self.task_results = {}
        self.swarm_membership = True
        self.status = "idle"
        self.performance_metrics = {
            "tasks_completed": 0,
            "tasks_failed": 0,
            "average_completion_time": 0,
            "reliability_score": 0.9
        }
        
    async def process_message(self, message: UniversalMessage) -> UniversalMessage:
        """
        Process incoming messages with special handling for swarm operations
        """
        try:
            message_type = message.metadata.get("type", "general")
            
            if message_type == "task_assignment":
                return await self._handle_task_assignment(message)
            elif message_type == "task_status_request":
                return await self._handle_status_request(message)
            elif message_type == "swarm_coordination":
                return await self._handle_coordination_message(message)
            elif message_type == "result_aggregation":
                return await self._handle_result_aggregation(message)
            else:
                # Handle as a regular message
                return await self._process_regular_message(message)
                
        except Exception as e:
            logger.error(f"Error processing message in SwarmWorkerAgent {self.id}: {e}")
            return await self._create_error_response(message, str(e))
    
    async def _handle_task_assignment(self, message: UniversalMessage) -> UniversalMessage:
        """
        Handle assignment of a task from the swarm orchestrator
        """
        try:
            task_description = message.payload.get("task_description", "")
            task_requirements = message.payload.get("task_requirements", [])
            task_id = message.metadata.get("task_id")
            
            logger.info(f"Agent {self.id} received task assignment: {task_id}")
            
            # Verify that this agent has required capabilities
            if not all(req in self.capabilities for req in task_requirements):
                logger.warning(f"Agent {self.id} lacks required capabilities: {task_requirements}")
                return await self._create_task_rejection(message)
            
            # Update agent status
            self.status = "processing_task"
            self.current_tasks[task_id] = {
                "description": task_description,
                "requirements": task_requirements,
                "assigned_at": message.metadata.get("timestamp"),
                "status": "in_progress"
            }
            
            # Process the task (this is where the actual work happens)
            task_result = await self._execute_task(task_id, task_description, task_requirements)
            
            # Prepare result message
            result_message = UniversalMessage(
                metadata={
                    "id": f"task_result_{task_id}",
                    "timestamp": datetime.now(UTC).isoformat(),
                    "type": "task_result",
                    "task_id": task_id,
                    "original_message_id": message.metadata.get("id")
                },
                routing={
                    "source": self.id,
                    "destination": self.swarm_orchestrator_id,
                    "reply_to": message.routing.get("source", self.swarm_orchestrator_id)
                },
                payload={
                    "status": "completed",
                    "result": task_result,
                    "task_id": task_id,
                    "agent_id": self.id
                },
                context={
                    "swarm_task": True,
                    "task_id": task_id
                },
                security={
                    "encrypted": True,
                    "access_level": "swarm_member"
                },
                tenant_id=message.tenant_id
            )
            
            # Update performance metrics
            self.performance_metrics["tasks_completed"] += 1
            
            logger.info(f"Agent {self.id} completed task {task_id}")
            
            return result_message
            
        except Exception as e:
            logger.error(f"Error handling task assignment: {e}")
            return await self._create_task_error_response(message, str(e))
    
    async def _execute_task(self, task_id: str, description: str, requirements: List[str]) -> Dict[str, Any]:
        """
        Execute the assigned task
        """
        logger.info(f"Executing task {task_id} for agent {self.id}")
        
        # Update status
        self.current_tasks[task_id]["status"] = "executing"
        
        try:
            # Simulate work - in a real implementation, this would do the actual task
            # based on the agent's capabilities and the task requirements
            await asyncio.sleep(2)  # Simulate work time
            
            # Generate result based on task requirements
            result = {
                "agent_id": self.id,
                "task_id": task_id,
                "completed_at": datetime.now(UTC).isoformat(),
                "status": "success",
                "output": f"Task completed by agent {self.id}",
                "metadata": {
                    "execution_time": 2.0,  # seconds
                    "capabilities_used": requirements
                }
            }
            
            # Store result for potential aggregation
            self.task_results[task_id] = result
            
            return result
        except Exception as e:
            logger.error(f"Error executing task {task_id}: {e}")
            self.performance_metrics["tasks_failed"] += 1
            return {
                "agent_id": self.id,
                "task_id": task_id,
                "completed_at": datetime.now(UTC).isoformat(),
                "status": "failed",
                "error": str(e)
            }
    
    async def _handle_status_request(self, message: UniversalMessage) -> UniversalMessage:
        """
        Handle a status request from the swarm orchestrator
        """
        status_info = {
            "agent_id": self.id,
            "status": self.status,
            "current_tasks": len(self.current_tasks),
            "capabilities": self.capabilities,
            "performance_metrics": self.performance_metrics,
            "last_heartbeat": datetime.now(UTC).isoformat()
        }
        
        return UniversalMessage(
            metadata={
                "id": f"status_response_{message.metadata.get('id')}",
                "timestamp": datetime.now(UTC).isoformat(),
                "type": "status_response"
            },
            routing={
                "source": self.id,
                "destination": message.routing.get("source"),
                "reply_to": message.routing.get("reply_to")
            },
            payload=status_info,
            context={"request_id": message.metadata.get("id")},
            security={"encrypted": True, "access_level": "swarm_member"},
            tenant_id=message.tenant_id
        )
    
    async def _handle_coordination_message(self, message: UniversalMessage) -> UniversalMessage:
        """
        Handle coordination messages from other swarm members
        """
        coordination_type = message.payload.get("coordination_type")
        
        if coordination_type == "request_help":
            return await self._handle_help_request(message)
        elif coordination_type == "share_knowledge":
            return await self._handle_knowledge_sharing(message)
        elif coordination_type == "resource_request":
            return await self._handle_resource_request(message)
        else:
            return await self._create_error_response(message, f"Unknown coordination type: {coordination_type}")
    
    async def _handle_help_request(self, message: UniversalMessage) -> UniversalMessage:
        """
        Handle a request for help from another agent
        """
        requesting_agent = message.routing.get("source")
        help_needed_for = message.payload.get("help_needed_for", [])
        
        # Check if this agent can help
        can_help = any(cap in self.capabilities for cap in help_needed_for)
        
        if can_help:
            logger.info(f"Agent {self.id} can help {requesting_agent}")
            return UniversalMessage(
                metadata={
                    "id": f"help_response_{message.metadata.get('id')}",
                    "timestamp": datetime.now(UTC).isoformat(),
                    "type": "help_response"
                },
                routing={
                    "source": self.id,
                    "destination": requesting_agent,
                    "reply_to": message.routing.get("source")
                },
                payload={
                    "can_help": True,
                    "capabilities": [cap for cap in self.capabilities if cap in help_needed_for],
                    "availability": "immediate"
                },
                context={"original_request": message.payload},
                security={"encrypted": True, "access_level": "swarm_member"},
                tenant_id=message.tenant_id
            )
        else:
            return UniversalMessage(
                metadata={
                    "id": f"help_response_{message.metadata.get('id')}",
                    "timestamp": datetime.now(UTC).isoformat(),
                    "type": "help_response"
                },
                routing={
                    "source": self.id,
                    "destination": requesting_agent,
                    "reply_to": message.routing.get("source")
                },
                payload={
                    "can_help": False,
                    "capabilities": self.capabilities,
                    "reason": "No matching capabilities"
                },
                context={"original_request": message.payload},
                security={"encrypted": True, "access_level": "swarm_member"},
                tenant_id=message.tenant_id
            )
    
    async def _handle_knowledge_sharing(self, message: UniversalMessage) -> UniversalMessage:
        """
        Handle knowledge sharing from other agents
        """
        knowledge_data = message.payload.get("knowledge", {})
        
        # In a real implementation, this would update the agent's knowledge base
        logger.info(f"Agent {self.id} received knowledge from {message.routing.get('source')}")
        
        # Store or process the shared knowledge
        self._store_shared_knowledge(knowledge_data)
        
        return UniversalMessage(
            metadata={
                "id": f"knowledge_ack_{message.metadata.get('id')}",
                "timestamp": datetime.now(UTC).isoformat(),
                "type": "knowledge_ack"
            },
            routing={
                "source": self.id,
                "destination": message.routing.get("source"),
                "reply_to": message.routing.get("source")
            },
            payload={"status": "received", "knowledge_stored": True},
            context={"original_knowledge": knowledge_data},
            security={"encrypted": True, "access_level": "swarm_member"},
            tenant_id=message.tenant_id
        )
    
    def _store_shared_knowledge(self, knowledge_data: Dict[str, Any]):
        """
        Store knowledge shared by other agents
        """
        # Implementation would store knowledge in agent's knowledge base
        # This could be in memory, a local database, or a knowledge graph
        pass
    
    async def _handle_resource_request(self, message: UniversalMessage) -> UniversalMessage:
        """
        Handle resource requests from other agents
        """
        resource_request = message.payload.get("resource_request", {})
        resource_type = resource_request.get("type")
        
        # Check if agent has the requested resource
        has_resource = self._check_resource_availability(resource_type)
        
        if has_resource:
            # In a real implementation, this would allocate the resource
            logger.info(f"Agent {self.id} granting resource request for {resource_type}")
            
            return UniversalMessage(
                metadata={
                    "id": f"resource_response_{message.metadata.get('id')}",
                    "timestamp": datetime.now(UTC).isoformat(),
                    "type": "resource_response"
                },
                routing={
                    "source": self.id,
                    "destination": message.routing.get("source"),
                    "reply_to": message.routing.get("source")
                },
                payload={
                    "resource_type": resource_type,
                    "status": "granted",
                    "access_token": f"resource_token_{self.id}_{resource_type}"
                },
                context={"original_request": resource_request},
                security={"encrypted": True, "access_level": "swarm_member"},
                tenant_id=message.tenant_id
            )
        else:
            return UniversalMessage(
                metadata={
                    "id": f"resource_response_{message.metadata.get('id')}",
                    "timestamp": datetime.now(UTC).isoformat(),
                    "type": "resource_response"
                },
                routing={
                    "source": self.id,
                    "destination": message.routing.get("source"),
                    "reply_to": message.routing.get("source")
                },
                payload={
                    "resource_type": resource_type,
                    "status": "denied",
                    "reason": "Resource not available"
                },
                context={"original_request": resource_request},
                security={"encrypted": True, "access_level": "swarm_member"},
                tenant_id=message.tenant_id
            )
    
    def _check_resource_availability(self, resource_type: str) -> bool:
        """
        Check if the agent has the requested resource available
        """
        # Implementation would check resource availability
        # For now, return True for demonstration
        return True
    
    async def _handle_result_aggregation(self, message: UniversalMessage) -> UniversalMessage:
        """
        Handle request to contribute to result aggregation
        """
        aggregation_data = message.payload.get("aggregation_data", {})
        
        # Contribute to the aggregation process
        contribution = self._generate_aggregation_contribution(aggregation_data)
        
        return UniversalMessage(
            metadata={
                "id": f"aggregation_response_{message.metadata.get('id')}",
                "timestamp": datetime.now(UTC).isoformat(),
                "type": "aggregation_response"
            },
            routing={
                "source": self.id,
                "destination": message.routing.get("source"),
                "reply_to": message.routing.get("source")
            },
            payload={
                "contribution": contribution,
                "agent_id": self.id
            },
            context={"aggregation_request": aggregation_data},
            security={"encrypted": True, "access_level": "swarm_member"},
            tenant_id=message.tenant_id
        )
    
    def _generate_aggregation_contribution(self, aggregation_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate a contribution to the result aggregation
        """
        # Implementation would generate a meaningful contribution
        # based on the agent's local knowledge and task results
        return {
            "agent_id": self.id,
            "timestamp": datetime.now(UTC).isoformat(),
            "contribution": "Local data and insights from agent"
        }
    
    async def _process_regular_message(self, message: UniversalMessage) -> UniversalMessage:
        """
        Process a regular (non-swarm) message
        """
        logger.info(f"Processing regular message in agent {self.id}")
        
        # For now, return a simple acknowledgment
        return UniversalMessage(
            metadata={
                "id": f"ack_{message.metadata.get('id')}",
                "timestamp": datetime.now(UTC).isoformat(),
                "type": "acknowledgment"
            },
            routing={
                "source": self.id,
                "destination": message.routing.get("source"),
                "reply_to": message.routing.get("source")
            },
            payload={
                "status": "received",
                "processed_by": self.id,
                "message_id": message.metadata.get("id")
            },
            context={"original_message": message.payload},
            security={"encrypted": True, "access_level": "standard"},
            tenant_id=message.tenant_id
        )
    
    async def _create_task_rejection(self, message: UniversalMessage) -> UniversalMessage:
        """
        Create a message to reject a task assignment
        """
        return UniversalMessage(
            metadata={
                "id": f"task_rejection_{message.metadata.get('id')}",
                "timestamp": datetime.now(UTC).isoformat(),
                "type": "task_rejection"
            },
            routing={
                "source": self.id,
                "destination": message.routing.get("source"),
                "reply_to": message.routing.get("source")
            },
            payload={
                "status": "rejected",
                "reason": "Agent lacks required capabilities",
                "required_capabilities": message.payload.get("task_requirements", []),
                "agent_capabilities": self.capabilities
            },
            context={"rejected_task": message.payload},
            security={"encrypted": True, "access_level": "swarm_member"},
            tenant_id=message.tenant_id
        )
    
    async def _create_task_error_response(self, message: UniversalMessage, error: str) -> UniversalMessage:
        """
        Create an error response for a task
        """
        return UniversalMessage(
            metadata={
                "id": f"task_error_{message.metadata.get('id')}",
                "timestamp": datetime.now(UTC).isoformat(),
                "type": "task_error"
            },
            routing={
                "source": self.id,
                "destination": message.routing.get("source"),
                "reply_to": message.routing.get("source")
            },
            payload={
                "status": "failed",
                "error": error,
                "task_id": message.metadata.get("task_id")
            },
            context={"failed_task": message.payload},
            security={"encrypted": True, "access_level": "swarm_member"},
            tenant_id=message.tenant_id
        )
    
    async def _create_error_response(self, message: UniversalMessage, error: str) -> UniversalMessage:
        """
        Create a general error response
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
                "original_message_id": message.metadata.get("id")
            },
            context={"error_context": message.context},
            security={"encrypted": True, "access_level": "error"},
            tenant_id=message.tenant_id
        )
    
    async def start(self):
        """
        Start the swarm worker agent
        """
        logger.info(f"Starting Swarm Worker Agent: {self.id}")
        
        # Register with the swarm orchestrator
        await self._register_with_swarm()
        
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
                    
        except Exception as e:
            logger.error(f"Error in SwarmWorkerAgent {self.id}: {e}")
        finally:
            await self._deregister_from_swarm()
    
    async def _register_with_swarm(self):
        """
        Register this agent with the swarm orchestrator
        """
        logger.info(f"Agent {self.id} registering with swarm")
        
        registration_message = UniversalMessage(
            metadata={
                "id": f"registration_{self.id}",
                "timestamp": datetime.now(UTC).isoformat(),
                "type": "agent_registration"
            },
            routing={
                "source": self.id,
                "destination": self.swarm_orchestrator_id,
                "reply_to": self.id
            },
            payload={
                "agent_id": self.id,
                "capabilities": self.capabilities,
                "role": "worker",
                "status": "available",
                "swarm_membership": True
            },
            context={"registration": True},
            security={"encrypted": True, "access_level": "swarm_member"},
            tenant_id="swarm_tenant"
        )
        
        try:
            await self.message_adapter.send(registration_message, self.swarm_orchestrator_id)
            logger.info(f"Agent {self.id} registered with swarm")
        except Exception as e:
            logger.error(f"Failed to register agent {self.id} with swarm: {e}")
    
    async def _deregister_from_swarm(self):
        """
        Deregister this agent from the swarm orchestrator
        """
        logger.info(f"Agent {self.id} deregistering from swarm")
        
        deregistration_message = UniversalMessage(
            metadata={
                "id": f"deregistration_{self.id}",
                "timestamp": datetime.now(UTC).isoformat(),
                "type": "agent_deregistration"
            },
            routing={
                "source": self.id,
                "destination": self.swarm_orchestrator_id,
                "reply_to": self.id
            },
            payload={
                "agent_id": self.id,
                "reason": "shutdown",
                "tasks_in_progress": list(self.current_tasks.keys())
            },
            context={"deregistration": True},
            security={"encrypted": True, "access_level": "swarm_member"},
            tenant_id="swarm_tenant"
        )
        
        try:
            await self.message_adapter.send(deregistration_message, self.swarm_orchestrator_id)
            logger.info(f"Agent {self.id} deregistered from swarm")
        except Exception as e:
            logger.error(f"Failed to deregister agent {self.id} from swarm: {e}")