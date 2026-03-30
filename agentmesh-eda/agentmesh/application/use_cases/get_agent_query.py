"""
Get Agent Query

Application layer query for retrieving a single agent by ID.
Follows CQRS read-side pattern.
"""

from typing import Optional
from agentmesh.domain.ports.agent_repository_port import AgentRepositoryPort
from agentmesh.domain.entities.agent_aggregate import AgentAggregate


class GetAgentQuery:
    """Query to retrieve a single agent by ID with tenant isolation."""

    def __init__(self, repository: AgentRepositoryPort):
        self._repository = repository

    async def execute(self, agent_id: str, tenant_id: str) -> Optional[AgentAggregate]:
        """
        Get agent by ID.

        Args:
            agent_id: Agent to retrieve
            tenant_id: Tenant scope for isolation

        Returns:
            AgentAggregate if found, None otherwise
        """
        return await self._repository.get_by_id(agent_id, tenant_id)
