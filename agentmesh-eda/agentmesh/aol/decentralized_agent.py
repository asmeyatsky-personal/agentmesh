"""
Decentralized Agent for AgentMesh
Implements an agent that participates in decentralized coordination
"""
from agentmesh.aol.agent import Agent
from agentmesh.mal.adapters.base import MessagePlatformAdapter
from agentmesh.mal.message import UniversalMessage
from agentmesh.aol.decentralized_coordination import DecentralizedCoordinator, CoordinationMessage, CoordinationProtocol, initialize_decentralized_coordinator, get_decentralized_coordinator
from typing import Dict, List, Any
import asyncio
import logging
from datetime import UTC, datetime

logger = logging.getLogger(__name__)

class DecentralizedAgent(Agent):
    """
    Agent that participates in decentralized coordination protocols
    """
    
    def __init__(self, agent_id: str, capabilities: List[str], 
                 message_adapter: MessagePlatformAdapter, 
                 cluster_nodes: List[str]):
        super().__init__(agent_id, capabilities)
        self.message_adapter = message_adapter
        self.cluster_nodes = cluster_nodes
        self.coordinator = initialize_decentralized_coordinator(agent_id, cluster_nodes)
        self.status = "active"
        self.task_queue = asyncio.Queue()
        self.running = False
    
    async def process_message(self, message: UniversalMessage) -> UniversalMessage:
        """
        Process incoming messages with decentralized coordination
        """
        try:
            # Check if this is a coordination message
            if message.metadata.get("coordination_message", False):
                await self._process_coordination_message(message)
                # Return a simple acknowledgment for coordination messages
                return await self._create_coordination_ack(message)
            
            # Process as a regular message
            message_type = message.metadata.get("type", "general")
            
            if message_type == "decentralized_status_request":
                return await self._handle_status_request(message)
            elif message_type == "cluster_membership_request":
                return await self._handle_membership_request(message)
            elif message_type == "consensus_proposal":
                return await self._handle_consensus_proposal(message)
            else:
                # Process as a regular message using decentralized coordination
                return await self._process_regular_message(message)
                
        except Exception as e:
            logger.error(f"Error processing message in DecentralizedAgent {self.id}: {e}")
            return await self._create_error_response(message, str(e))
    
    async def _process_coordination_message(self, message: UniversalMessage):
        """
        Process messages specifically for coordination protocols
        """
        try:
            # Convert UniversalMessage to CoordinationMessage
            coordination_msg = CoordinationMessage(
                id=message.metadata.get("id", f"coord_{message.metadata.get('id', 'unknown')}"),
                sender_id=message.routing.get("source", self.id),
                message_type=message.metadata.get("coord_type", "general"),
                content={
                    "payload": message.payload,
                    "original_message": message,
                    "timestamp": message.metadata.get("timestamp")
                },
                protocol=message.metadata.get("protocol", CoordinationProtocol.GOSHIPOP.value)
            )
            
            # Process through coordinator
            if self.coordinator:
                self.coordinator.receive_message(coordination_msg)
            
        except Exception as e:
            logger.error(f"Error processing coordination message: {e}")
    
    async def _create_coordination_ack(self, message: UniversalMessage) -> UniversalMessage:
        """
        Create an acknowledgment for coordination messages
        """
        return UniversalMessage(
            metadata={
                "id": f"coord_ack_{message.metadata.get('id')}",
                "timestamp": datetime.now(UTC).isoformat(),
                "type": "coordination_ack"
            },
            routing={
                "source": self.id,
                "destination": message.routing.get("source"),
                "reply_to": message.routing.get("source")
            },
            payload={
                "status": "received",
                "coordination_processed": True,
                "message_id": message.metadata.get("id")
            },
            context={"original_message_id": message.metadata.get("id")},
            security={"encrypted": True, "access_level": "decentralized"},
            tenant_id=message.tenant_id
        )
    
    async def _handle_status_request(self, message: UniversalMessage) -> UniversalMessage:
        """
        Handle request for decentralized status
        """
        if not self.coordinator:
            return await self._create_error_response(message, "No coordinator available")
        
        cluster_status = self.coordinator.get_cluster_status()
        
        return UniversalMessage(
            metadata={
                "id": f"decentralized_status_response_{message.metadata.get('id')}",
                "timestamp": datetime.now(UTC).isoformat(),
                "type": "decentralized_status_response"
            },
            routing={
                "source": self.id,
                "destination": message.routing.get("source"),
                "reply_to": message.routing.get("source")
            },
            payload={
                "status": "success",
                "cluster_status": cluster_status,
                "agent_id": self.id
            },
            context={"status_request": True},
            security={"encrypted": True, "access_level": "decentralized"},
            tenant_id=message.tenant_id
        )
    
    async def _handle_membership_request(self, message: UniversalMessage) -> UniversalMessage:
        """
        Handle request for cluster membership
        """
        return UniversalMessage(
            metadata={
                "id": f"membership_response_{message.metadata.get('id')}",
                "timestamp": datetime.now(UTC).isoformat(),
                "type": "membership_response"
            },
            routing={
                "source": self.id,
                "destination": message.routing.get("source"),
                "reply_to": message.routing.get("source")
            },
            payload={
                "status": "success",
                "cluster_nodes": self.cluster_nodes,
                "current_node": self.id,
                "membership_status": "active"
            },
            context={"membership_request": True},
            security={"encrypted": True, "access_level": "decentralized"},
            tenant_id=message.tenant_id
        )
    
    async def _handle_consensus_proposal(self, message: UniversalMessage) -> UniversalMessage:
        """
        Handle consensus proposal
        """
        try:
            proposal = message.payload.get("proposal", {})
            proposal_id = proposal.get("id")
            
            if not proposal_id:
                return await self._create_error_response(message, "No proposal ID provided")
            
            # In a real implementation, this would participate in consensus
            # For now, we'll simulate accepting the proposal
            logger.info(f"Agent {self.id} received consensus proposal: {proposal_id}")
            
            # Attempt to broadcast via Raft if we're a leader
            if self.coordinator and self.coordinator.raft_protocol.state.role == "leader":
                coordination_msg = CoordinationMessage(
                    id=f"raft_proposal_{proposal_id}",
                    sender_id=self.id,
                    message_type="proposal",
                    content=proposal,
                    protocol=CoordinationProtocol.RAFT.value
                )
                
                await self.coordinator.broadcast_message(
                    coordination_msg, CoordinationProtocol.RAFT
                )
            
            return UniversalMessage(
                metadata={
                    "id": f"proposal_response_{message.metadata.get('id')}",
                    "timestamp": datetime.now(UTC).isoformat(),
                    "type": "proposal_response"
                },
                routing={
                    "source": self.id,
                    "destination": message.routing.get("source"),
                    "reply_to": message.routing.get("source")
                },
                payload={
                    "status": "accepted",
                    "proposal_id": proposal_id,
                    "agent_id": self.id,
                    "consensus_participation": True
                },
                context={"proposal_response": True},
                security={"encrypted": True, "access_level": "decentralized"},
                tenant_id=message.tenant_id
            )
            
        except Exception as e:
            logger.error(f"Error handling consensus proposal: {e}")
            return await self._create_error_response(message, str(e))
    
    async def _process_regular_message(self, message: UniversalMessage) -> UniversalMessage:
        """
        Process a regular message using decentralized coordination
        """
        logger.info(f"Processing regular message in decentralized agent {self.id}")
        
        # For now, return a simple acknowledgment
        # In a real implementation, this would process messages
        # based on the agent's capabilities while using decentralized coordination
        response_payload = {
            "status": "received_and_processed",
            "processed_by": self.id,
            "message_id": message.metadata.get("id"),
            "decentralized_coordination": "active"
        }
        
        # If the message has specific requirements that match our capabilities
        if "capability_request" in message.payload:
            requested_capability = message.payload["capability_request"]
            if requested_capability in self.capabilities:
                response_payload["capability_executed"] = requested_capability
                # Execute the capability here (implementation would depend on specific capability)
                response_payload["result"] = f"Executed {requested_capability} capability with decentralized coordination"
        
        # If this message requires consensus, initiate it
        if message.context.get("requires_consensus", False):
            logger.info(f"Initiating consensus for message {message.metadata.get('id')}")
            # In a real implementation, this would start a consensus process
            response_payload["consensus_initiated"] = True
        
        return UniversalMessage(
            metadata={
                "id": f"decentralized_ack_{message.metadata.get('id')}",
                "timestamp": datetime.now(UTC).isoformat(),
                "type": "decentralized_acknowledgment",
                "coordination_message": True,  # Mark as coordination-related
                "protocol": CoordinationProtocol.GOSHIPOP.value  # Default protocol
            },
            routing={
                "source": self.id,
                "destination": message.routing.get("source"),
                "reply_to": message.routing.get("source")
            },
            payload=response_payload,
            context={"original_message": message.payload},
            security={"encrypted": True, "access_level": "decentralized"},
            tenant_id=message.tenant_id
        )
    
    async def _create_error_response(self, message: UniversalMessage, error: str) -> UniversalMessage:
        """
        Create an error response message
        """
        return UniversalMessage(
            metadata={
                "id": f"decentralized_error_{message.metadata.get('id')}",
                "timestamp": datetime.now(UTC).isoformat(),
                "type": "decentralized_error"
            },
            routing={
                "source": self.id,
                "destination": message.routing.get("source"),
                "reply_to": message.routing.get("source")
            },
            payload={
                "error": error,
                "original_message_id": message.metadata.get("id"),
                "decentralized_coordination": "maintained"
            },
            context={"error_context": message.context},
            security={"encrypted": True, "access_level": "error"},
            tenant_id=message.tenant_id
        )
    
    async def start(self):
        """
        Start the decentralized agent
        """
        logger.info(f"Starting Decentralized Agent: {self.id}")
        
        self.running = True
        
        # Start the coordinator
        if self.coordinator:
            await self.coordinator.start()
        
        try:
            # Main processing loop
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
            logger.error(f"Error in DecentralizedAgent {self.id}: {e}")
        finally:
            if self.coordinator:
                await self.coordinator.stop()
    
    async def broadcast_to_cluster(self, message_content: Dict[str, Any], 
                                 protocol: CoordinationProtocol = CoordinationProtocol.GOSHIPOP):
        """
        Broadcast a message to the entire cluster
        """
        if not self.coordinator:
            logger.error("No coordinator available for broadcast")
            return False
        
        coordination_msg = CoordinationMessage(
            id=f"broadcast_{secrets.token_hex(8)}",
            sender_id=self.id,
            message_type="broadcast",
            content=message_content,
            protocol=protocol.value
        )
        
        try:
            await self.coordinator.broadcast_message(coordination_msg, protocol)
            logger.info(f"Broadcasted message to cluster using {protocol.value}")
            return True
        except Exception as e:
            logger.error(f"Error broadcasting to cluster: {e}")
            return False
    
    async def join_cluster(self):
        """
        Join the decentralized cluster
        """
        if not self.coordinator:
            logger.error("No coordinator available to join cluster")
            return False
        
        try:
            # Announce presence to cluster
            join_message = {
                "node_id": self.id,
                "capabilities": self.capabilities,
                "timestamp": datetime.now(UTC).isoformat(),
                "action": "join"
            }
            
            success = await self.broadcast_to_cluster(join_message, CoordinationProtocol.GOSHIPOP)
            if success:
                logger.info(f"Agent {self.id} joined cluster successfully")
            
            return success
        except Exception as e:
            logger.error(f"Error joining cluster: {e}")
            return False
    
    async def leave_cluster(self):
        """
        Leave the decentralized cluster
        """
        if not self.coordinator:
            logger.error("No coordinator available to leave cluster")
            return False
        
        try:
            # Announce departure from cluster
            leave_message = {
                "node_id": self.id,
                "timestamp": datetime.now(UTC).isoformat(),
                "action": "leave"
            }
            
            success = await self.broadcast_to_cluster(leave_message, CoordinationProtocol.GOSHIPOP)
            if success:
                logger.info(f"Agent {self.id} left cluster successfully")
            
            return success
        except Exception as e:
            logger.error(f"Error leaving cluster: {e}")
            return False