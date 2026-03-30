"""
Decentralized Coordination Protocols for AgentMesh
Implements distributed consensus and peer-to-peer coordination mechanisms
"""
from typing import Dict, List, Any, Optional, Set, Tuple
import asyncio
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
import hashlib
import json
from enum import Enum
import secrets
from collections import defaultdict
import time

logger = logging.getLogger(__name__)

class CoordinationProtocol(Enum):
    """Types of coordination protocols"""
    GOSHIPOP = "gossip"          # Gossip protocol for information dissemination
    RAFT = "raft"                # Raft consensus algorithm
    PAXOS = "paxos"              # Paxos consensus algorithm
    BYZANTINE = "byzantine"      # Byzantine fault-tolerant protocol
    HEARTBEAT = "heartbeat"      # Heartbeat-based coordination

@dataclass
class CoordinationMessage:
    """Message used in decentralized coordination"""
    id: str
    sender_id: str
    message_type: str  # request, response, proposal, accept, commit, etc.
    content: Dict[str, Any]
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    signatures: List[Dict[str, str]] = field(default_factory=list)  # For multi-signature protocols
    protocol: str = CoordinationProtocol.GOSHIPOP.value

@dataclass
class NodeState:
    """State of a node in the decentralized system"""
    node_id: str
    status: str = "active"  # active, inactive, suspect, failed
    last_heartbeat: datetime = field(default_factory=lambda: datetime.now(UTC))
    role: str = "follower"  # leader, follower, candidate (for consensus protocols)
    term: int = 0
    committed_log_index: int = 0
    last_applied: int = 0
    next_index: Dict[str, int] = field(default_factory=dict)  # For Raft
    match_index: Dict[str, int] = field(default_factory=dict)  # For Raft

class GossipProtocol:
    """
    Gossip protocol for information dissemination in decentralized networks
    """
    
    def __init__(self, node_id: str, peers: List[str], gossip_interval: float = 1.0):
        self.node_id = node_id
        self.peers = set(peers) - {node_id}  # Exclude self from peers
        self.gossip_interval = gossip_interval
        self.data_store = {}  # Local data store
        self.gossip_history = {}  # Track gossip messages
        self.message_queue = asyncio.Queue()
        self.running = False
    
    async def start(self):
        """Start the gossip protocol"""
        self.running = True
        # Start gossip dissemination task
        asyncio.create_task(self._gossip_task())
        logger.info(f"Gossip protocol started for node {self.node_id}")
    
    async def stop(self):
        """Stop the gossip protocol"""
        self.running = False
        logger.info(f"Gossip protocol stopped for node {self.node_id}")
    
    async def _gossip_task(self):
        """Task that periodically gossips information to peers"""
        while self.running:
            try:
                # Select random peers to gossip with
                if self.peers:
                    selected_peers = list(self.peers)[:min(3, len(self.peers))]  # Gossip with up to 3 peers
                    for peer in selected_peers:
                        await self._gossip_to_peer(peer)
                
                await asyncio.sleep(self.gossip_interval)
            except Exception as e:
                logger.error(f"Error in gossip task: {e}")
                await asyncio.sleep(self.gossip_interval)
    
    async def _gossip_to_peer(self, peer: str):
        """Gossip information to a specific peer"""
        try:
            # Prepare gossip message with recent updates
            message = CoordinationMessage(
                id=f"gossip_{secrets.token_hex(8)}",
                sender_id=self.node_id,
                message_type="gossip",
                content={
                    "data_store": self.data_store,
                    "timestamp": datetime.now(UTC).isoformat(),
                    "node_status": "active"
                },
                protocol=CoordinationProtocol.GOSHIPOP.value
            )
            
            # In a real implementation, this would send the message to the peer
            # For now, we'll just log it
            logger.debug(f"Gossiping to peer {peer}: {message.id}")
            
        except Exception as e:
            logger.error(f"Error gossiping to {peer}: {e}")
    
    def receive_gossip(self, message: CoordinationMessage):
        """Receive and process gossip from another node"""
        try:
            # Update our data store with information from the gossip
            remote_data = message.content.get("data_store", {})
            for key, value in remote_data.items():
                if key not in self.data_store or self._is_newer(value, self.data_store[key]):
                    self.data_store[key] = value
            
            # Update gossip history
            self.gossip_history[message.sender_id] = {
                "timestamp": message.timestamp,
                "message_id": message.id
            }
            
            logger.debug(f"Received gossip from {message.sender_id}")
            
        except Exception as e:
            logger.error(f"Error processing gossip from {message.sender_id}: {e}")
    
    def _is_newer(self, new_value: Any, old_value: Any) -> bool:
        """Determine if a value is newer than another"""
        # In a real implementation, this would use vector clocks or timestamps
        return True  # Simplified for demonstration

class RaftProtocol:
    """
    Raft consensus protocol implementation for leader election and log replication
    """
    
    def __init__(self, node_id: str, cluster_nodes: List[str], election_timeout: float = 5.0):
        self.node_id = node_id
        self.cluster_nodes = set(cluster_nodes)
        self.election_timeout = election_timeout
        self.state = NodeState(node_id=node_id)
        
        # Raft-specific state
        self.log = []  # Log entries
        self.commit_index = 0
        self.last_applied = 0
        self.voted_for = None
        self.current_term = 0
        
        # Timer for election
        self.election_timer = None
        self.heartbeat_timer = None
        
        self.running = False
    
    async def start(self):
        """Start the Raft protocol"""
        self.running = True
        # Start election timer
        self.election_timer = asyncio.create_task(self._election_timeout_task())
        logger.info(f"Raft protocol started for node {self.node_id}")
    
    async def stop(self):
        """Stop the Raft protocol"""
        self.running = False
        if self.election_timer:
            self.election_timer.cancel()
        if self.heartbeat_timer:
            self.heartbeat_timer.cancel()
        logger.info(f"Raft protocol stopped for node {self.node_id}")
    
    async def _election_timeout_task(self):
        """Task that handles election timeouts"""
        while self.running:
            try:
                # Wait for election timeout
                await asyncio.sleep(self.election_timeout)
                
                # If not leader, start new election
                if self.state.role != "leader":
                    await self._start_election()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in election timeout task: {e}")
    
    async def _start_election(self):
        """Start a new election cycle"""
        try:
            self.current_term += 1
            self.state.role = "candidate"
            self.voted_for = self.node_id
            
            logger.info(f"Node {self.node_id} starting election for term {self.current_term}")
            
            # Request votes from other nodes
            votes = 1  # Vote for self
            total_nodes = len(self.cluster_nodes)
            
            # In a real implementation, this would send RequestVote RPCs to other nodes
            # For now, we'll simulate the voting process
            for node in self.cluster_nodes:
                if node != self.node_id:
                    # Simulate receiving a vote (in real system, this would be async)
                    vote_granted = await self._request_vote(node)
                    if vote_granted:
                        votes += 1
            
            # Check if we got majority votes
            if votes > total_nodes // 2:
                # Become leader
                self.state.role = "leader"
                self.state.term = self.current_term
                logger.info(f"Node {self.node_id} elected as leader for term {self.current_term}")
                
                # Start sending heartbeats
                if self.heartbeat_timer:
                    self.heartbeat_timer.cancel()
                self.heartbeat_timer = asyncio.create_task(self._heartbeat_task())
            else:
                # Become follower again
                self.state.role = "follower"
                
        except Exception as e:
            logger.error(f"Error in election: {e}")
            self.state.role = "follower"
    
    async def _request_vote(self, candidate_id: str) -> bool:
        """Request a vote from another node (simplified)"""
        # In a real implementation, this would send a RequestVote RPC
        # For simulation, return True with some probability
        import random
        return random.choice([True, False, True, True])  # 75% success rate
    
    async def _heartbeat_task(self):
        """Task that sends heartbeats to followers"""
        while self.running and self.state.role == "leader":
            try:
                # Send AppendEntries RPCs to all followers
                for follower in self.cluster_nodes:
                    if follower != self.node_id:
                        await self._send_heartbeat(follower)
                
                # Wait for heartbeat interval (shorter than election timeout)
                await asyncio.sleep(self.election_timeout / 3)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in heartbeat task: {e}")
                break
    
    async def _send_heartbeat(self, follower: str):
        """Send a heartbeat to a follower"""
        try:
            message = CoordinationMessage(
                id=f"heartbeat_{secrets.token_hex(8)}",
                sender_id=self.node_id,
                message_type="heartbeat",
                content={
                    "term": self.current_term,
                    "leader_commit": self.commit_index,
                    "prev_log_index": len(self.log) - 1 if self.log else 0,
                    "prev_log_term": self.log[-1]["term"] if self.log else 0,
                    "entries": []  # Empty for heartbeat
                },
                protocol=CoordinationProtocol.RAFT.value
            )
            
            # In a real implementation, this would send the message to the follower
            logger.debug(f"Sending heartbeat to follower {follower}")
            
        except Exception as e:
            logger.error(f"Error sending heartbeat to {follower}: {e}")
    
    def receive_append_entries(self, message: CoordinationMessage) -> Dict[str, Any]:
        """Receive AppendEntries RPC from leader"""
        try:
            content = message.content
            term = content.get("term", 0)
            
            # Reply false if term < currentTerm (§5.1)
            if term < self.current_term:
                return {"success": False, "term": self.current_term}
            
            # Update current term and become follower
            if term > self.current_term:
                self.current_term = term
                self.state.role = "follower"
                self.voted_for = None
            
            # Update leader info
            self.state.last_heartbeat = datetime.now(UTC)
            
            return {"success": True, "term": self.current_term}
            
        except Exception as e:
            logger.error(f"Error processing AppendEntries: {e}")
            return {"success": False, "error": str(e)}

class DecentralizedCoordinator:
    """
    Main coordinator for decentralized protocols
    """
    
    def __init__(self, node_id: str, cluster_nodes: List[str]):
        self.node_id = node_id
        self.cluster_nodes = cluster_nodes
        self.protocols: Dict[str, Any] = {}
        self.node_states: Dict[str, NodeState] = {}
        self.message_queue = asyncio.Queue()
        self.running = False
        
        # Initialize default protocols
        self.gossip_protocol = GossipProtocol(node_id, cluster_nodes)
        self.raft_protocol = RaftProtocol(node_id, cluster_nodes)
    
    async def start(self):
        """Start the decentralized coordinator"""
        self.running = True
        
        # Start gossip protocol
        await self.gossip_protocol.start()
        
        # Start Raft protocol
        await self.raft_protocol.start()
        
        # Initialize node states
        for node in self.cluster_nodes:
            self.node_states[node] = NodeState(node_id=node)
        
        logger.info(f"Decentralized coordinator started for node {self.node_id}")
    
    async def stop(self):
        """Stop the decentralized coordinator"""
        self.running = False
        
        # Stop protocols
        await self.gossip_protocol.stop()
        await self.raft_protocol.stop()
        
        logger.info(f"Decentralized coordinator stopped for node {self.node_id}")
    
    async def broadcast_message(self, message: CoordinationMessage, protocol_type: CoordinationProtocol):
        """Broadcast a message using the specified protocol"""
        try:
            if protocol_type == CoordinationProtocol.GOSHIPOP:
                # Use gossip to disseminate message
                for peer in self.gossip_protocol.peers:
                    # In a real implementation, this would send to peer
                    logger.debug(f"Broadcasting to {peer} via gossip")
            
            elif protocol_type == CoordinationProtocol.RAFT:
                # For Raft, only leader can broadcast
                if self.raft_protocol.state.role == "leader":
                    for follower in self.cluster_nodes:
                        if follower != self.node_id:
                            # Send to follower
                            logger.debug(f"Broadcasting to {follower} via Raft")
                else:
                    logger.warning(f"Node {self.node_id} is not leader, cannot broadcast via Raft")
            
            logger.info(f"Broadcast message {message.id} using {protocol_type.value}")
            
        except Exception as e:
            logger.error(f"Error broadcasting message: {e}")
    
    def receive_message(self, message: CoordinationMessage):
        """Receive and process a coordination message"""
        try:
            protocol_type = CoordinationProtocol(message.protocol)
            
            if protocol_type == CoordinationProtocol.GOSHIPOP:
                self.gossip_protocol.receive_gossip(message)
                
            elif protocol_type == CoordinationProtocol.RAFT:
                if message.message_type == "heartbeat":
                    result = self.raft_protocol.receive_append_entries(message)
                    logger.debug(f"Processed Raft heartbeat from {message.sender_id}: {result}")
                
            # Update node state
            if message.sender_id in self.node_states:
                self.node_states[message.sender_id].last_heartbeat = message.timestamp
                self.node_states[message.sender_id].status = "active"
            
            logger.info(f"Received coordination message {message.id} from {message.sender_id}")
            
        except Exception as e:
            logger.error(f"Error processing coordination message: {e}")
    
    def get_cluster_status(self) -> Dict[str, Any]:
        """Get current cluster status"""
        active_nodes = sum(1 for state in self.node_states.values() 
                          if self._is_node_active(state))
        total_nodes = len(self.node_states)
        
        return {
            "node_id": self.node_id,
            "cluster_size": total_nodes,
            "active_nodes": active_nodes,
            "leader": self._get_leader(),
            "protocol_status": {
                "gossip": "running" if hasattr(self.gossip_protocol, 'running') and self.gossip_protocol.running else "stopped",
                "raft": {
                    "role": self.raft_protocol.state.role,
                    "term": self.raft_protocol.current_term,
                    "status": "running" if hasattr(self.raft_protocol, 'running') and self.raft_protocol.running else "stopped"
                }
            }
        }
    
    def _is_node_active(self, state: NodeState) -> bool:
        """Check if a node is considered active"""
        # Consider node active if heartbeat was received in last 30 seconds
        time_diff = datetime.now(UTC) - state.last_heartbeat
        return time_diff < timedelta(seconds=30)
    
    def _get_leader(self) -> Optional[str]:
        """Get the current leader node (for Raft)"""
        # In a real implementation, this would check which node has leader role
        # For now, return a placeholder
        for node_id, state in self.node_states.items():
            if state.role == "leader":
                return node_id
        return None

# Create a global coordinator instance
_global_coordinator: Optional[DecentralizedCoordinator] = None

def get_decentralized_coordinator() -> Optional[DecentralizedCoordinator]:
    """
    Get the global decentralized coordinator instance
    """
    return _global_coordinator

def initialize_decentralized_coordinator(node_id: str, cluster_nodes: List[str]):
    """
    Initialize the global decentralized coordinator
    """
    global _global_coordinator
    _global_coordinator = DecentralizedCoordinator(node_id, cluster_nodes)
    return _global_coordinator