"""
Swarm Intelligence Orchestrator for AgentMesh
Implements decentralized coordination of multiple agents for complex tasks
"""
from agentmesh.aol.agent import Agent
from agentmesh.mal.message import UniversalMessage
from agentmesh.mal.router import MessageRouter
from agentmesh.aol.registry import AgentRegistry
from typing import Dict, List, Any, Optional
import asyncio
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum

logger = logging.getLogger(__name__)

class SwarmTaskStatus(Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

class SwarmAgentRole(Enum):
    LEADER = "leader"
    WORKER = "worker"
    OBSERVER = "observer"
    COORDINATOR = "coordinator"
    SPECIALIST = "specialist"

@dataclass
class SwarmTask:
    """Represents a task that can be handled by the swarm"""
    id: str
    description: str
    required_capabilities: List[str]
    assigned_agents: List[str] = field(default_factory=list)
    status: SwarmTaskStatus = SwarmTaskStatus.PENDING
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    completed_at: Optional[datetime] = None
    results: Dict[str, Any] = field(default_factory=dict)
    dependencies: List[str] = field(default_factory=list)

@dataclass
class SwarmAgentState:
    """Tracks the state of an agent in the swarm"""
    agent_id: str
    role: SwarmAgentRole
    capabilities: List[str]
    load: float = 0.0
    performance_score: float = 0.8
    last_seen: datetime = field(default_factory=lambda: datetime.now(UTC))
    tasks_completed: int = 0

class SwarmOrchestrator(Agent):
    """
    Advanced orchestrator that coordinates swarm intelligence across multiple agents
    """
    
    def __init__(self, agent_id: str, capabilities: List[str], message_router: MessageRouter):
        super().__init__(agent_id, capabilities)
        self.router = message_router
        self.registry = AgentRegistry()
        self.swarm_agents: Dict[str, SwarmAgentState] = {}
        self.active_tasks: Dict[str, SwarmTask] = {}
        self.task_queue = asyncio.Queue()
        self.swarm_metrics = {
            "total_tasks": 0,
            "completed_tasks": 0,
            "failed_tasks": 0,
            "average_completion_time": 0.0
        }
        
    async def initialize_swarm(self, agent_list: List[Dict[str, Any]]):
        """
        Initialize the swarm with a list of available agents
        """
        for agent_info in agent_list:
            agent_id = agent_info["id"]
            capabilities = agent_info.get("capabilities", [])
            role = SwarmAgentRole(agent_info.get("role", "worker"))
            
            self.swarm_agents[agent_id] = SwarmAgentState(
                agent_id=agent_id,
                role=role,
                capabilities=capabilities
            )
        
        logger.info(f"Initialized swarm with {len(self.swarm_agents)} agents")
    
    async def create_task(self, description: str, required_capabilities: List[str], 
                         dependencies: List[str] = None) -> str:
        """
        Create a new task for the swarm
        """
        import uuid
        task_id = f"swarm_task_{uuid.uuid4()}"
        
        task = SwarmTask(
            id=task_id,
            description=description,
            required_capabilities=required_capabilities,
            dependencies=dependencies or []
        )
        
        self.active_tasks[task_id] = task
        self.swarm_metrics["total_tasks"] += 1
        
        # Add to queue for processing
        await self.task_queue.put(task)
        
        logger.info(f"Created swarm task {task_id}: {description}")
        return task_id
    
    async def assign_task_to_agents(self, task: SwarmTask) -> List[str]:
        """
        Assign a task to appropriate agents based on capabilities and availability
        """
        available_agents = []
        
        # Find agents with required capabilities
        for agent_id, agent_state in self.swarm_agents.items():
            if all(cap in agent_state.capabilities for cap in task.required_capabilities):
                if agent_state.load < 0.8:  # Only assign to agents with less than 80% load
                    available_agents.append(agent_state)
        
        # Sort by performance score (descending) and load (ascending)
        available_agents.sort(key=lambda x: (x.performance_score, -x.load), reverse=True)
        
        assigned_agent_ids = []
        
        # For complex tasks, assign to multiple agents (swarm approach)
        if len(task.description) > 100 or len(task.required_capabilities) > 3:  # Complex task heuristic
            # Assign to multiple agents for parallel processing
            for agent_state in available_agents[:3]:  # Max 3 agents for complex tasks
                self.swarm_agents[agent_state.agent_id].load += 0.3  # Increase load
                self.swarm_agents[agent_state.agent_id].assigned_tasks = task.id
                assigned_agent_ids.append(agent_state.agent_id)
        else:
            # Assign to single best agent
            if available_agents:
                best_agent = available_agents[0]
                self.swarm_agents[best_agent.agent_id].load += 0.5  # Increase load
                self.swarm_agents[best_agent.agent_id].assigned_tasks = task.id
                assigned_agent_ids.append(best_agent.agent_id)
        
        task.assigned_agents = assigned_agent_ids
        logger.info(f"Assigned task {task.id} to agents: {assigned_agent_ids}")
        
        return assigned_agent_ids
    
    async def coordinate_task_execution(self, task: SwarmTask):
        """
        Coordinate the execution of a task across multiple agents
        """
        assigned_agent_ids = task.assigned_agents
        if not assigned_agent_ids:
            logger.warning(f"No agents assigned to task {task.id}")
            return
        
        # Create task assignment messages for each agent
        for agent_id in assigned_agent_ids:
            task_message = UniversalMessage(
                metadata={
                    "id": f"task_assignment_{task.id}",
                    "timestamp": datetime.now(UTC).isoformat(),
                    "type": "task_assignment",
                    "task_id": task.id
                },
                routing={
                    "source": self.id,
                    "destination": agent_id,
                    "reply_to": self.id
                },
                payload={
                    "task_description": task.description,
                    "task_requirements": task.required_capabilities,
                    "task_dependencies": task.dependencies,
                    "assigned_by": self.id
                },
                context={
                    "swarm_task": True,
                    "task_id": task.id
                },
                security={
                    "encrypted": True,
                    "access_level": "swarm_member"
                },
                tenant_id="swarm_tenant"
            )
            
            # Send the task assignment message
            await self.router.route_message(task_message)
    
    async def process_task_results(self, result_message: UniversalMessage):
        """
        Process results from agents and update task status
        """
        task_id = result_message.context.get("task_id")
        if not task_id or task_id not in self.active_tasks:
            logger.warning(f"Received result for unknown task: {task_id}")
            return
        
        task = self.active_tasks[task_id]
        
        # Update task status based on result
        result_status = result_message.payload.get("status", "completed")
        agent_id = result_message.routing.get("source")
        
        if result_status == "completed":
            # Aggregate results
            task.results[agent_id] = result_message.payload.get("result")
            
            # Check if all assigned agents have completed
            completed_agents = [aid for aid in task.assigned_agents 
                              if aid in task.results or self._is_agent_completed_task(aid, task_id)]
            
            if len(completed_agents) >= len(task.assigned_agents):
                task.status = SwarmTaskStatus.COMPLETED
                task.completed_at = datetime.now(UTC)
                self.swarm_metrics["completed_tasks"] += 1
                
                # Update agent performance
                await self._update_agent_performance(task)
                
                logger.info(f"Task {task_id} completed by {len(completed_agents)} agents")
                
                # Send completion notification
                completion_message = UniversalMessage(
                    metadata={
                        "id": f"task_completion_{task_id}",
                        "timestamp": datetime.now(UTC).isoformat(),
                        "type": "task_completion"
                    },
                    routing={
                        "source": self.id,
                        "destination": result_message.routing.get("source"),
                        "reply_to": result_message.routing.get("reply_to", self.id)
                    },
                    payload={
                        "task_id": task_id,
                        "status": "completed",
                        "results": task.results,
                        "summary": await self._summarize_results(task.results)
                    },
                    context={"swarm_task": True},
                    security={"encrypted": True, "access_level": "swarm_leader"},
                    tenant_id="swarm_tenant"
                )
                
                await self.router.route_message(completion_message)
        
        elif result_status == "failed":
            task.status = SwarmTaskStatus.FAILED
            logger.error(f"Task {task_id} failed by agent {agent_id}")
            self.swarm_metrics["failed_tasks"] += 1
            
            # Try to reassign to another agent
            await self._reassign_failed_task(task, agent_id)
    
    def _is_agent_completed_task(self, agent_id: str, task_id: str) -> bool:
        """
        Check if an agent has completed a specific task
        """
        # Implementation would check agent's task completion records
        # This is a simplified version
        return True
    
    async def _update_agent_performance(self, task: SwarmTask):
        """
        Update performance scores for agents that completed the task
        """
        for agent_id in task.assigned_agents:
            if agent_id in self.swarm_agents:
                agent_state = self.swarm_agents[agent_id]
                agent_state.tasks_completed += 1
                agent_state.load = max(0, agent_state.load - 0.3)  # Reduce load after completion
                # Performance could be updated based on task completion time, accuracy, etc.
                agent_state.performance_score = min(1.0, agent_state.performance_score + 0.01)
    
    async def _reassign_failed_task(self, task: SwarmTask, failed_agent_id: str):
        """
        Reassign a failed task to different agents
        """
        logger.info(f"Reassigning failed task {task.id}, removing agent {failed_agent_id}")
        
        # Remove failed agent from assigned agents
        task.assigned_agents = [aid for aid in task.assigned_agents if aid != failed_agent_id]
        
        # Find new agents for the task
        new_agents = await self.assign_task_to_agents(task)
        
        # Coordinate the task execution with new agents
        await self.coordinate_task_execution(task)
    
    async def _summarize_results(self, results: Dict[str, Any]) -> str:
        """
        Create a summary of results from multiple agents
        """
        if not results:
            return "No results available"
        
        # For now, return a simple summary
        # In a real implementation, this would use AI to create a coherent summary
        return f"Aggregated results from {len(results)} agents"
    
    async def start(self):
        """
        Start the swarm orchestrator main loop
        """
        logger.info(f"Starting Swarm Orchestrator: {self.id}")
        
        # Main loop to process tasks from the queue
        while True:
            try:
                # Wait for a task from the queue
                task = await self.task_queue.get()
                
                # Assign the task to appropriate agents
                assigned_agents = await self.assign_task_to_agents(task)
                
                if assigned_agents:
                    # Coordinate task execution
                    await self.coordinate_task_execution(task)
                else:
                    logger.warning(f"No suitable agents found for task {task.id}")
                    task.status = SwarmTaskStatus.FAILED
                
                # Mark task as processed
                self.task_queue.task_done()
                
            except asyncio.CancelledError:
                logger.info("Swarm Orchestrator shutting down")
                break
            except Exception as e:
                logger.error(f"Error in Swarm Orchestrator {self.id}: {e}")
                await asyncio.sleep(1)  # Wait before continuing
    
    async def get_swarm_status(self) -> Dict[str, Any]:
        """
        Get the current status of the swarm
        """
        return {
            "active_agents": len(self.swarm_agents),
            "active_tasks": len([t for t in self.active_tasks.values() if t.status == SwarmTaskStatus.IN_PROGRESS]),
            "pending_tasks": len([t for t in self.active_tasks.values() if t.status == SwarmTaskStatus.PENDING]),
            "metrics": self.swarm_metrics,
            "agent_distribution": {
                role.value: len([a for a in self.swarm_agents.values() if a.role == role])
                for role in SwarmAgentRole
            }
        }