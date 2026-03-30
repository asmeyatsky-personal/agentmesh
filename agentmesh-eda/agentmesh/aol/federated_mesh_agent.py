"""
Federated Learning Agent for AgentMesh
Implements an agent that can participate in federated learning while maintaining other capabilities
"""
from agentmesh.aol.agent import Agent
from agentmesh.aol.federated_learning import FederatedLearningAgent as FLAgent, FederatedLearningCoordinator
from agentmesh.mal.adapters.base import MessagePlatformAdapter
from agentmesh.mal.message import UniversalMessage
from typing import Dict, List, Any
import asyncio
import logging
from datetime import UTC, datetime

logger = logging.getLogger(__name__)

class FederatedLearningMeshAgent(Agent):
    """
    Agent that combines general capabilities with federated learning functionality
    """
    
    def __init__(self, agent_id: str, capabilities: List[str], 
                 message_adapter: MessagePlatformAdapter):
        super().__init__(agent_id, capabilities)
        self.message_adapter = message_adapter
        self.federated_agent = FLAgent(agent_id, capabilities)
        self.coordinator = None
        self.registered_models = set()
        self.status = "idle"
        
    def set_coordinator(self, coordinator: FederatedLearningCoordinator):
        """
        Set the federated learning coordinator
        """
        self.federated_agent.set_coordinator(coordinator)
        self.coordinator = coordinator
    
    async def process_message(self, message: UniversalMessage) -> UniversalMessage:
        """
        Process incoming messages with special handling for federated learning operations
        """
        try:
            message_type = message.metadata.get("type", "general")
            
            if message_type == "federated_model_registration":
                return await self._handle_model_registration(message)
            elif message_type == "federated_training_request":
                return await self._handle_training_request(message)
            elif message_type == "federated_model_sync":
                return await self._handle_model_sync(message)
            elif message_type == "federated_metrics_request":
                return await self._handle_metrics_request(message)
            else:
                # Process as a regular message using federated capabilities when appropriate
                return await self._process_regular_message(message)
                
        except Exception as e:
            logger.error(f"Error processing message in FederatedLearningMeshAgent {self.id}: {e}")
            return await self._create_error_response(message, str(e))
    
    async def _handle_model_registration(self, message: UniversalMessage) -> UniversalMessage:
        """
        Handle request to register for a federated learning model
        """
        model_id = message.payload.get("model_id")
        
        if not model_id:
            return await self._create_error_response(message, "No model_id provided")
        
        success = await self.federated_agent.register_for_model(model_id)
        
        if success:
            self.registered_models.add(model_id)
            logger.info(f"Agent {self.id} registered for federated model {model_id}")
            
            return UniversalMessage(
                metadata={
                    "id": f"registration_success_{message.metadata.get('id')}",
                    "timestamp": datetime.now(UTC).isoformat(),
                    "type": "federated_registration_success"
                },
                routing={
                    "source": self.id,
                    "destination": message.routing.get("source"),
                    "reply_to": message.routing.get("source")
                },
                payload={
                    "status": "success",
                    "model_id": model_id,
                    "agent_id": self.id
                },
                context={"registration": True},
                security={"encrypted": True, "access_level": "federated_member"},
                tenant_id=message.tenant_id
            )
        else:
            return await self._create_error_response(message, f"Failed to register for model {model_id}")
    
    async def _handle_training_request(self, message: UniversalMessage) -> UniversalMessage:
        """
        Handle request to perform federated learning training
        """
        model_id = message.payload.get("model_id")
        epochs = message.payload.get("epochs", 1)
        
        if not model_id:
            return await self._create_error_response(message, "No model_id provided")
        
        if model_id not in self.registered_models:
            return await self._create_error_response(message, f"Agent not registered for model {model_id}")
        
        # Start federated learning cycle
        success = await self.federated_agent.federated_learning_cycle(model_id, epochs)
        
        if success:
            logger.info(f"Agent {self.id} completed federated training for model {model_id}")
            
            return UniversalMessage(
                metadata={
                    "id": f"training_success_{message.metadata.get('id')}",
                    "timestamp": datetime.now(UTC).isoformat(),
                    "type": "federated_training_success"
                },
                routing={
                    "source": self.id,
                    "destination": message.routing.get("source"),
                    "reply_to": message.routing.get("source")
                },
                payload={
                    "status": "success",
                    "model_id": model_id,
                    "agent_id": self.id,
                    "epochs_completed": epochs
                },
                context={"training_completed": True},
                security={"encrypted": True, "access_level": "federated_member"},
                tenant_id=message.tenant_id
            )
        else:
            return await self._create_error_response(message, f"Federated training failed for model {model_id}")
    
    async def _handle_model_sync(self, message: UniversalMessage) -> UniversalMessage:
        """
        Handle request to sync with global federated model
        """
        model_id = message.payload.get("model_id")
        
        if not model_id:
            return await self._create_error_response(message, "No model_id provided")
        
        if model_id not in self.registered_models:
            return await self._create_error_response(message, f"Agent not registered for model {model_id}")
        
        success = await self.federated_agent.sync_with_global_model(model_id)
        
        if success:
            logger.info(f"Agent {self.id} synced with global model {model_id}")
            
            return UniversalMessage(
                metadata={
                    "id": f"sync_success_{message.metadata.get('id')}",
                    "timestamp": datetime.now(UTC).isoformat(),
                    "type": "federated_sync_success"
                },
                routing={
                    "source": self.id,
                    "destination": message.routing.get("source"),
                    "reply_to": message.routing.get("source")
                },
                payload={
                    "status": "success",
                    "model_id": model_id,
                    "agent_id": self.id
                },
                context={"sync_completed": True},
                security={"encrypted": True, "access_level": "federated_member"},
                tenant_id=message.tenant_id
            )
        else:
            return await self._create_error_response(message, f"Model sync failed for model {model_id}")
    
    async def _handle_metrics_request(self, message: UniversalMessage) -> UniversalMessage:
        """
        Handle request for federated learning metrics
        """
        model_id = message.payload.get("model_id")
        
        if not model_id:
            # Return metrics for all models
            metrics = {}
            for mdl_id in self.registered_models:
                if self.coordinator:
                    metrics[mdl_id] = self.coordinator.get_model_info(mdl_id)
            
            return UniversalMessage(
                metadata={
                    "id": f"metrics_response_{message.metadata.get('id')}",
                    "timestamp": datetime.now(UTC).isoformat(),
                    "type": "federated_metrics_response"
                },
                routing={
                    "source": self.id,
                    "destination": message.routing.get("source"),
                    "reply_to": message.routing.get("source")
                },
                payload={
                    "status": "success",
                    "metrics": metrics,
                    "agent_id": self.id
                },
                context={"metrics_request": True},
                security={"encrypted": True, "access_level": "federated_member"},
                tenant_id=message.tenant_id
            )
        else:
            # Return metrics for specific model
            if self.coordinator:
                model_metrics = self.coordinator.get_model_info(model_id)
                
                return UniversalMessage(
                    metadata={
                        "id": f"metrics_response_{message.metadata.get('id')}",
                        "timestamp": datetime.now(UTC).isoformat(),
                        "type": "federated_metrics_response"
                    },
                    routing={
                        "source": self.id,
                        "destination": message.routing.get("source"),
                        "reply_to": message.routing.get("source")
                    },
                    payload={
                        "status": "success",
                        "model_id": model_id,
                        "metrics": model_metrics,
                        "agent_id": self.id
                    },
                    context={"metrics_request": True},
                    security={"encrypted": True, "access_level": "federated_member"},
                    tenant_id=message.tenant_id
                )
            else:
                return await self._create_error_response(message, "No coordinator available")
    
    async def _process_regular_message(self, message: UniversalMessage) -> UniversalMessage:
        """
        Process a regular (non-federated) message using standard agent capabilities
        """
        logger.info(f"Processing regular message in federated agent {self.id}")
        
        # For now, return a simple acknowledgment
        # In a real implementation, this would process messages
        # based on the agent's capabilities
        response_payload = {
            "status": "received",
            "processed_by": self.id,
            "message_id": message.metadata.get("id"),
            "capabilities_used": "general_processing"
        }
        
        # If the message has specific requirements that match our capabilities
        if "capability_request" in message.payload:
            requested_capability = message.payload["capability_request"]
            if requested_capability in self.capabilities:
                response_payload["capability_executed"] = requested_capability
                # Execute the capability here (implementation would depend on specific capability)
                response_payload["result"] = f"Executed {requested_capability} capability"
        
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
            payload=response_payload,
            context={"original_message": message.payload},
            security={"encrypted": True, "access_level": "standard"},
            tenant_id=message.tenant_id
        )
    
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
                "original_message_id": message.metadata.get("id")
            },
            context={"error_context": message.context},
            security={"encrypted": True, "access_level": "error"},
            tenant_id=message.tenant_id
        )
    
    async def start(self):
        """
        Start the federated learning agent
        """
        logger.info(f"Starting Federated Learning Mesh Agent: {self.id}")
        
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
            logger.error(f"Error in FederatedLearningMeshAgent {self.id}: {e}")