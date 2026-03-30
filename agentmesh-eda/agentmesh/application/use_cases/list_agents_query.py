"""
List Agents Query

Application layer query for listing agents with optional status filter.
Follows CQRS read-side pattern.
"""

from typing import List, Optional
from agentmesh.domain.ports.agent_repository_port import AgentRepositoryPort
from agentmesh.domain.entities.agent_aggregate import AgentAggregate


class ListAgentsQuery:
    """Query to list agents for a tenant, optionally filtered by status."""

    def __init__(self, repository: AgentRepositoryPort):
        self._repository = repository

    async def execute(self, tenant_id: str, status: Optional[str] = None) -> List[AgentAggregate]:
        """
        List agents for a tenant.

        Args:
            tenant_id: Tenant scope
            status: Optional status filter (AVAILABLE, BUSY, etc.)

        Returns:
            List of matching agents
        """
        if status:
            return await self._repository.find_by_status(status, tenant_id)
        return await self._repository.find_all(tenant_id)
