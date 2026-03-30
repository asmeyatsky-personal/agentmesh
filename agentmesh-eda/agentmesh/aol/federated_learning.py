"""
Federated Learning System for AgentMesh
Enables distributed model training across agents while preserving data privacy
"""
from typing import Dict, List, Any, Optional, Tuple
import asyncio
import logging
import numpy as np
from dataclasses import dataclass, field
from datetime import UTC, datetime
import json
import base64
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import secrets
import pickle

logger = logging.getLogger(__name__)

@dataclass
class ModelUpdate:
    """Represents a model update from an agent"""
    agent_id: str
    model_id: str
    update_data: bytes  # Serialized model weights/parameters
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    accuracy: float = 0.0
    data_size: int = 0  # Size of training data used
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class FederatedModel:
    """Represents a federated learning model"""
    model_id: str
    model_type: str
    global_weights: bytes = b""
    agents: List[str] = field(default_factory=list)
    creation_time: datetime = field(default_factory=lambda: datetime.now(UTC))
    current_round: int = 0
    total_updates: int = 0
    accuracy_history: List[float] = field(default_factory=list)
    encryption_key: Optional[bytes] = None

class FederatedLearningCoordinator:
    """
    Coordinates federated learning across multiple agents
    """
    
    def __init__(self, coordinator_id: str):
        self.coordinator_id = coordinator_id
        self.federated_models: Dict[str, FederatedModel] = {}
        self.pending_updates: Dict[str, List[ModelUpdate]] = {}
        self.agents_by_model: Dict[str, List[str]] = {}
        self.encryption_enabled = True
        self.update_threshold = 5  # Number of updates needed to aggregate
        self.aggregation_method = "average"  # Options: average, weighted, etc.
        
    def create_model(self, model_id: str, model_type: str, initial_weights: bytes = None) -> FederatedModel:
        """
        Create a new federated learning model
        """
        if initial_weights is None:
            initial_weights = b""  # In practice, this would be initial model weights
            
        # Generate encryption key for this model
        encryption_key = Fernet.generate_key() if self.encryption_enabled else None
        
        model = FederatedModel(
            model_id=model_id,
            model_type=model_type,
            global_weights=initial_weights,
            encryption_key=encryption_key
        )
        
        self.federated_models[model_id] = model
        self.pending_updates[model_id] = []
        
        logger.info(f"Created federated model: {model_id}")
        return model
    
    def register_agent_for_model(self, agent_id: str, model_id: str) -> bool:
        """
        Register an agent to participate in federated learning for a specific model
        """
        if model_id not in self.federated_models:
            logger.error(f"Model {model_id} does not exist")
            return False
        
        model = self.federated_models[model_id]
        if agent_id not in model.agents:
            model.agents.append(agent_id)
            
        if model_id not in self.agents_by_model:
            self.agents_by_model[model_id] = []
        if agent_id not in self.agents_by_model[model_id]:
            self.agents_by_model[model_id].append(agent_id)
        
        logger.info(f"Agent {agent_id} registered for model {model_id}")
        return True
    
    def submit_model_update(self, agent_id: str, model_update: ModelUpdate) -> bool:
        """
        Submit a model update from an agent
        """
        model_id = model_update.model_id
        if model_id not in self.federated_models:
            logger.error(f"Update for unknown model: {model_id}")
            return False
        
        # Verify the agent is registered for this model
        if agent_id not in self.federated_models[model_id].agents:
            logger.error(f"Agent {agent_id} not registered for model {model_id}")
            return False
        
        # Add update to pending list
        self.pending_updates[model_id].append(model_update)
        self.federated_models[model_id].total_updates += 1
        
        logger.info(f"Received model update from agent {agent_id} for model {model_id}")
        
        # Check if enough updates to trigger aggregation
        if len(self.pending_updates[model_id]) >= self.update_threshold:
            asyncio.create_task(self._aggregate_updates(model_id))
        
        return True
    
    async def _aggregate_updates(self, model_id: str):
        """
        Aggregate model updates when threshold is reached
        """
        try:
            updates = self.pending_updates[model_id]
            if len(updates) < 2:
                logger.warning(f"Not enough updates to aggregate for model {model_id}")
                return
            
            logger.info(f"Aggregating {len(updates)} updates for model {model_id}")
            
            # Perform aggregation based on method
            if self.aggregation_method == "average":
                new_weights = await self._aggregate_by_average(updates)
            elif self.aggregation_method == "weighted":
                new_weights = await self._aggregate_by_weighted_average(updates)
            else:
                new_weights = await self._aggregate_by_average(updates)  # Default
            
            # Update the global model
            self.federated_models[model_id].global_weights = new_weights
            self.federated_models[model_id].current_round += 1
            
            # Calculate and record accuracy
            avg_accuracy = sum(u.accuracy for u in updates) / len(updates)
            self.federated_models[model_id].accuracy_history.append(avg_accuracy)
            
            # Clear pending updates
            self.pending_updates[model_id] = []
            
            logger.info(f"Completed aggregation round {self.federated_models[model_id].current_round} for model {model_id}")
            
            # Notify agents of the new global model
            await self._notify_agents(model_id)
            
        except Exception as e:
            logger.error(f"Error aggregating updates for model {model_id}: {e}")
    
    async def _aggregate_by_average(self, updates: List[ModelUpdate]) -> bytes:
        """
        Aggregate updates using simple averaging
        """
        # In a real implementation, this would deserialize the weight updates,
        # perform federated averaging, and serialize the results
        # For now, we'll simulate the process
        
        # This is a simplified approach - in reality, you'd need to properly
        # average the model weights which requires deserializing them into tensors,
        # averaging, and re-serializing
        
        # For demonstration purposes, we'll combine the updates in a basic way
        combined_weights = b""
        for update in updates:
            combined_weights += update.update_data + b"|"
        
        return combined_weights
    
    async def _aggregate_by_weighted_average(self, updates: List[ModelUpdate]) -> bytes:
        """
        Aggregate updates using weighted averaging based on data size
        """
        # Calculate total data size for weighting
        total_data_size = sum(update.data_size for update in updates)
        if total_data_size == 0:
            return await self._aggregate_by_average(updates)
        
        # In a real implementation, this would perform weighted averaging
        # of model parameters based on each agent's data size contribution
        combined_weights = b""
        for update in updates:
            weight = update.data_size / total_data_size
            # This is a simplified representation - real implementation would
            # apply the weight to the actual model parameters
            combined_weights += update.update_data + f"|weight:{weight}".encode() + b"|"
        
        return combined_weights
    
    async def _notify_agents(self, model_id: str):
        """
        Notify registered agents of the new global model
        """
        # This would send messages to all registered agents
        # with the updated global model weights
        agent_list = self.federated_models[model_id].agents
        logger.info(f"Notifying {len(agent_list)} agents about updated model {model_id}")
        
        # Implementation would send messages to agents
        # For now, we just log the notification
    
    def get_model_info(self, model_id: str) -> Optional[Dict[str, Any]]:
        """
        Get information about a federated learning model
        """
        if model_id not in self.federated_models:
            return None
        
        model = self.federated_models[model_id]
        return {
            "model_id": model.model_id,
            "model_type": model.model_type,
            "current_round": model.current_round,
            "total_updates": model.total_updates,
            "registered_agents": len(model.agents),
            "last_accuracy": model.accuracy_history[-1] if model.accuracy_history else 0.0,
            "accuracy_history": model.accuracy_history
        }
    
    def get_encryption_key(self, model_id: str, agent_id: str) -> Optional[bytes]:
        """
        Get encryption key for a specific agent and model if valid
        """
        if model_id not in self.federated_models:
            return None
        
        model = self.federated_models[model_id]
        if agent_id not in model.agents:
            logger.warning(f"Agent {agent_id} not authorized for model {model_id} key")
            return None
        
        return model.encryption_key

class FederatedLearningAgent:
    """
    Agent that participates in federated learning
    """
    
    def __init__(self, agent_id: str, capabilities: List[str]):
        self.agent_id = agent_id
        self.capabilities = capabilities
        self.registered_models = []
        self.local_model_weights = {}
        self.training_datasets = {}
        self.federated_coordinator = None
        self.is_training = False
        
    def set_coordinator(self, coordinator: FederatedLearningCoordinator):
        """
        Set the federated learning coordinator
        """
        self.federated_coordinator = coordinator
    
    async def register_for_model(self, model_id: str) -> bool:
        """
        Register this agent to participate in federated learning for a model
        """
        if not self.federated_coordinator:
            logger.error("No federated coordinator set")
            return False
        
        success = self.federated_coordinator.register_agent_for_model(self.agent_id, model_id)
        if success:
            self.registered_models.append(model_id)
        
        return success
    
    async def train_on_local_data(self, model_id: str, epochs: int = 1) -> Optional[ModelUpdate]:
        """
        Train on local data and create a model update
        """
        if model_id not in self.registered_models:
            logger.error(f"Agent not registered for model {model_id}")
            return None
        
        logger.info(f"Agent {self.agent_id} training on local data for model {model_id}")
        
        try:
            # Simulate training process
            # In a real implementation, this would:
            # 1. Load local training data
            # 2. Update the model with global weights
            # 3. Train for specified epochs
            # 4. Calculate new weights/parameters
            
            # For simulation, we'll create random update data
            simulated_update = {
                "model_id": model_id,
                "agent_id": self.agent_id,
                "training_round": self._get_current_round(model_id),
                "local_updates": f"simulated_update_epoch_{epochs}",
                "timestamp": datetime.now(UTC).isoformat()
            }
            
            update_bytes = pickle.dumps(simulated_update)
            
            # Create model update object
            model_update = ModelUpdate(
                agent_id=self.agent_id,
                model_id=model_id,
                update_data=update_bytes,
                accuracy=0.85 + secrets.randbelow(100) / 1000,  # Simulated accuracy
                data_size=1000 + secrets.randbelow(5000),  # Simulated data size
                metadata={"epochs": epochs, "training_time": 10.5}  # Simulated metrics
            )
            
            logger.info(f"Agent {self.agent_id} completed local training for model {model_id}")
            return model_update
            
        except Exception as e:
            logger.error(f"Error in local training for agent {self.agent_id}: {e}")
            return None
    
    def _get_current_round(self, model_id: str) -> int:
        """
        Get the current training round for the model
        """
        if not self.federated_coordinator:
            return 0
        
        model_info = self.federated_coordinator.get_model_info(model_id)
        return model_info.get("current_round", 0) if model_info else 0
    
    async def submit_update(self, model_update: ModelUpdate) -> bool:
        """
        Submit a model update to the coordinator
        """
        if not self.federated_coordinator:
            logger.error("No federated coordinator set")
            return False
        
        return self.federated_coordinator.submit_model_update(self.agent_id, model_update)
    
    async def sync_with_global_model(self, model_id: str) -> bool:
        """
        Sync with the global model (download latest weights)
        """
        if not self.federated_coordinator or model_id not in self.federated_models:
            logger.error("No federated coordinator or model not found")
            return False
        
        # Get the global model weights
        global_weights = self.federated_coordinator.federated_models[model_id].global_weights
        
        # Update local model with global weights
        self.local_model_weights[model_id] = global_weights
        
        logger.info(f"Agent {self.agent_id} synced with global model {model_id}")
        return True
    
    async def federated_learning_cycle(self, model_id: str, epochs: int = 1) -> bool:
        """
        Perform one cycle of federated learning: train locally, submit update, sync global
        """
        logger.info(f"Starting federated learning cycle for agent {self.agent_id}, model {model_id}")
        
        # Train on local data
        model_update = await self.train_on_local_data(model_id, epochs)
        if not model_update:
            logger.error(f"Failed to train locally for model {model_id}")
            return False
        
        # Submit the update
        success = await self.submit_update(model_update)
        if not success:
            logger.error(f"Failed to submit update for model {model_id}")
            return False
        
        # Sync with global model
        success = await self.sync_with_global_model(model_id)
        if not success:
            logger.error(f"Failed to sync with global model {model_id}")
            return False
        
        logger.info(f"Completed federated learning cycle for agent {self.agent_id}, model {model_id}")
        return True
    
    async def start_federated_learning(self, model_id: str, cycles: int = 10, epochs_per_cycle: int = 1):
        """
        Start participating in federated learning for a specified number of cycles
        """
        logger.info(f"Agent {self.agent_id} starting federated learning for model {model_id}")
        
        self.is_training = True
        cycle = 0
        
        while cycle < cycles and self.is_training:
            try:
                await self.federated_learning_cycle(model_id, epochs_per_cycle)
                cycle += 1
                
                # Wait a bit between cycles (in a real system, this might be event-driven)
                await asyncio.sleep(5)
                
            except Exception as e:
                logger.error(f"Error in federated learning cycle {cycle}: {e}")
                break
        
        self.is_training = False
        logger.info(f"Agent {self.agent_id} finished federated learning for model {model_id}")
    
    def stop_federated_learning(self):
        """
        Stop the federated learning process
        """
        self.is_training = False