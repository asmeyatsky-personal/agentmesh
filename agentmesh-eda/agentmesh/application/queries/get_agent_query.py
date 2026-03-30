"""
Get Agent Query

Architectural Intent:
- Query handler to retrieve a single agent
- Returns domain aggregate for internal use
- Can be wrapped to return DTO for external consumption
"""

from typing import Optional
from loguru import logger

from agentmesh.domain.entities.agent_aggregate import AgentAggregate
from agentmesh.domain.ports.agent_repository_port import AgentRepositoryPort


class GetAgentQuery:
    """
    Query: Get agent by ID.

    Flow:
    1. Retrieve agent from repository
    2. Return domain aggregate
    """

    def __init__(self, agent_repository: AgentRepositoryPort):
        self._repository = agent_repository

    async def execute(self, agent_id: str, tenant_id: str) -> Optional[AgentAggregate]:
        """
        Execute get agent query.

        Args:
            agent_id: Agent ID to retrieve
            tenant_id: Tenant ID for isolation

        Returns:
            AgentAggregate if found, None otherwise
        """
        logger.info(f"Getting agent {agent_id} for tenant {tenant_id}")

        agent = await self._repository.get_by_id(agent_id, tenant_id)

        if agent:
            logger.info(f"Agent {agent_id} found")
        else:
            logger.warning(f"Agent {agent_id} not found")

        return agent
