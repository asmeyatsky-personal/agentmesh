"""
Advanced Message Router with Google Cloud Intelligence
Implements intelligent routing based on agent capabilities, load, and context
"""
from agentmesh.mal.message import UniversalMessage
from agentmesh.aol.registry import AgentRegistry
from typing import Dict, List, Optional, Tuple
import asyncio
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class AdvancedMessageRouter:
    """
    Advanced message router with intelligent routing capabilities
    """
    
    def __init__(self, registry: AgentRegistry):
        self.registry = registry
        self.routing_history = {}  # Track routing decisions
        self.load_balancer = LoadBalancer()
        
    async def route_message(self, message: UniversalMessage) -> Optional[str]:
        """
        Determine the best agent to handle the message based on:
        - Agent capabilities
        - Current load
        - Context and domain expertise
        - Historical performance
        """
        try:
            # Get all available agents
            all_agents = self.registry.get_all_agents()
            
            # Filter agents based on message requirements
            candidate_agents = self._filter_by_capabilities(all_agents, message)
            
            if not candidate_agents:
                logger.warning(f"No agents found with required capabilities for message {message.metadata['id']}")
                return None
            
            # Score agents based on multiple factors
            scored_agents = await self._score_agents(candidate_agents, message)
            
            if not scored_agents:
                logger.warning(f"No suitable agents found after scoring for message {message.metadata['id']}")
                return None
            
            # Select the best agent
            best_agent_id = self._select_best_agent(scored_agents, message)
            
            # Record routing decision
            self._record_routing_decision(message.metadata['id'], best_agent_id, scored_agents)
            
            return best_agent_id
            
        except Exception as e:
            logger.error(f"Error routing message {message.metadata['id']}: {e}")
            return None
    
    def _filter_by_capabilities(self, agents: Dict, message: UniversalMessage) -> List[Tuple[str, object]]:
        """
        Filter agents based on whether they have required capabilities
        """
        # Determine required capabilities from message
        required_capabilities = self._extract_required_capabilities(message)
        
        candidate_agents = []
        for agent_id, agent in agents.items():
            # Check if agent has all required capabilities
            has_capabilities = all(
                cap in agent.capabilities for cap in required_capabilities
            )
            
            # If no specific capabilities required, check if agent can handle general requests
            if not required_capabilities:
                if agent.capabilities:  # Any capabilities are acceptable
                    candidate_agents.append((agent_id, agent))
            elif has_capabilities:
                candidate_agents.append((agent_id, agent))
        
        return candidate_agents
    
    async def _score_agents(self, candidate_agents: List[Tuple[str, object]], message: UniversalMessage) -> List[Dict]:
        """
        Score candidate agents based on multiple factors
        """
        scored_agents = []
        
        for agent_id, agent in candidate_agents:
            score = 0.0
            factors = {}
            
            # Capability match score (0.0 to 1.0)
            capability_score = self._calculate_capability_score(agent, message)
            factors["capability_score"] = capability_score
            score += capability_score * 0.4  # 40% weight
            
            # Load score (0.0 to 1.0, lower is better)
            load_score = self._calculate_load_score(agent_id)
            factors["load_score"] = 1.0 - load_score  # Invert since lower load is better
            score += (1.0 - load_score) * 0.3  # 30% weight
            
            # Historical performance score (0.0 to 1.0)
            performance_score = self._calculate_performance_score(agent_id, message)
            factors["performance_score"] = performance_score
            score += performance_score * 0.2  # 20% weight
            
            # Context relevance score (0.0 to 1.0)
            context_score = self._calculate_context_score(agent, message)
            factors["context_score"] = context_score
            score += context_score * 0.1  # 10% weight
            
            scored_agents.append({
                "agent_id": agent_id,
                "agent": agent,
                "score": min(score, 1.0),  # Cap at 1.0
                "factors": factors
            })
        
        # Sort by score descending
        scored_agents.sort(key=lambda x: x["score"], reverse=True)
        return scored_agents
    
    def _select_best_agent(self, scored_agents: List[Dict], message: UniversalMessage) -> str:
        """
        Select the best agent based on scores
        """
        # If top agent has high score, select it
        if scored_agents[0]["score"] >= 0.7:
            return scored_agents[0]["agent_id"]
        
        # Otherwise, apply load balancing for agents with similar scores
        top_scoring_agents = [
            agent for agent in scored_agents 
            if agent["score"] >= scored_agents[0]["score"] * 0.8  # Within 80% of top score
        ]
        
        if len(top_scoring_agents) == 1:
            return top_scoring_agents[0]["agent_id"]
        
        # Use load balancer to pick among top candidates
        agent_ids = [agent["agent_id"] for agent in top_scoring_agents]
        return self.load_balancer.select_agent(agent_ids)
    
    def _extract_required_capabilities(self, message: UniversalMessage) -> List[str]:
        """
        Extract required capabilities from message
        """
        required_capabilities = []
        
        # Extract from routing information
        if "required_capabilities" in message.routing:
            required_capabilities.extend(message.routing["required_capabilities"])
        
        # Extract from payload based on content
        if "intent" in message.payload:
            intent = message.payload["intent"]
            # Map intents to capabilities (this would be expanded with domain knowledge)
            intent_to_capability = {
                "data_processing": ["process-data", "analyze"],
                "query": ["query", "information-retrieval"],
                "task_execution": ["execute-tasks", "workflow"],
                "analysis": ["analyze", "reasoning"],
                "generation": ["generate", "create"]
            }
            capability = intent_to_capability.get(intent)
            if capability:
                required_capabilities.append(capability)
        
        # Remove duplicates
        return list(set(required_capabilities))
    
    def _calculate_capability_score(self, agent, message: UniversalMessage) -> float:
        """
        Calculate how well an agent's capabilities match the message requirements
        """
        required_capabilities = set(self._extract_required_capabilities(message))
        agent_capabilities = set(agent.capabilities)
        
        if not required_capabilities:
            return 1.0  # No specific requirements, agent is acceptable
        
        # Calculate intersection over union (IoU) for capability matching
        intersection = len(required_capabilities.intersection(agent_capabilities))
        union = len(required_capabilities.union(agent_capabilities))
        
        if union == 0:
            return 0.0
        
        # Also consider if agent has extra relevant capabilities
        match_ratio = intersection / len(required_capabilities) if required_capabilities else 1.0
        
        # Bonus for having more capabilities than required
        capability_bonus = min(len(agent_capabilities) / max(len(required_capabilities), 1), 1.0) if required_capabilities else 0.0
        
        return min((match_ratio + capability_bonus) / 2, 1.0)
    
    def _calculate_load_score(self, agent_id: str) -> float:
        """
        Calculate normalized load score for an agent (0.0 to 1.0, higher means more loaded).

        STUB: Estimates load from routing history count. Replace with real
        metrics (e.g. Prometheus agent_load gauge) for production use.
        """
        recent_routes = self._get_recent_routes(agent_id)
        return min(len(recent_routes) / 10.0, 1.0)
    
    def _calculate_performance_score(self, agent_id: str, message: UniversalMessage) -> float:
        """
        Calculate historical performance score for an agent on similar messages.

        STUB: Uses in-memory routing history averages. Replace with real
        performance metrics backend for production use.
        """
        if agent_id in self.routing_history:
            # Calculate based on success rate or other metrics
            history = self.routing_history[agent_id]
            if history:
                # Average performance (simulated)
                return min(sum(h.get("performance_score", 0.8) for h in history) / len(history), 1.0)
        
        # Default performance score
        return 0.8
    
    def _calculate_context_score(self, agent, message: UniversalMessage) -> float:
        """
        Calculate how well an agent's context matches the message
        """
        # Check if agent has been successful with similar contexts
        if "domain" in message.context:
            agent_domain = getattr(agent, "domain", None)
            if agent_domain and agent_domain == message.context["domain"]:
                return 1.0
        
        # Check for tenant-specific agents
        if message.tenant_id and hasattr(agent, "tenant_id"):
            if agent.tenant_id == message.tenant_id:
                return 0.9
        
        # Default context score
        return 0.5
    
    def _get_recent_routes(self, agent_id: str) -> List[Dict]:
        """
        Get recent routing decisions for an agent
        """
        # This would connect to actual metrics in a real implementation
        # For simulation, we'll check routing history
        if agent_id in self.routing_history:
            recent_cutoff = datetime.now() - timedelta(minutes=5)  # Last 5 minutes
            return [
                route for route in self.routing_history[agent_id]
                if route.get("timestamp", datetime.min) > recent_cutoff
            ]
        return []
    
    def _record_routing_decision(self, message_id: str, agent_id: str, scored_agents: List[Dict]):
        """
        Record the routing decision for future analysis
        """
        if agent_id not in self.routing_history:
            self.routing_history[agent_id] = []
        
        self.routing_history[agent_id].append({
            "message_id": message_id,
            "timestamp": datetime.now(),
            "scored_agents": scored_agents,
            "selected": True
        })
        
        # Remove old entries to prevent memory bloat (keep last 1000)
        if len(self.routing_history[agent_id]) > 1000:
            self.routing_history[agent_id] = self.routing_history[agent_id][-1000:]


class LoadBalancer:
    """
    Load balancing component for distributing messages across agents
    """
    
    def __init__(self):
        self.agent_load = {}  # Track load per agent
        self.last_assigned = {}  # For round-robin
    
    def select_agent(self, agent_ids: List[str]) -> str:
        """
        Select an agent based on load balancing strategy
        """
        if not agent_ids:
            return None
        
        if len(agent_ids) == 1:
            return agent_ids[0]
        
        # Implement round-robin selection
        first_agent = agent_ids[0]
        last_selected = self.last_assigned.get(first_agent, -1)
        
        # Calculate next index (round-robin)
        next_index = (last_selected + 1) % len(agent_ids)
        selected_agent = agent_ids[next_index]
        
        # Update last assigned index
        self.last_assigned[first_agent] = next_index
        
        # Update load tracking
        self._update_load(selected_agent)
        
        return selected_agent
    
    def _update_load(self, agent_id: str):
        """
        Update load tracking for an agent
        """
        if agent_id not in self.agent_load:
            self.agent_load[agent_id] = 0
        self.agent_load[agent_id] += 1
        
        # TODO: Implement periodic cleanup of stale load data.
        # Currently load counters grow unbounded for inactive agents.