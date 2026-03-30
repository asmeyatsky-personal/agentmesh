"""
List Agents Query

Architectural Intent:
- Query handler to list agents with optional filtering
- Returns list of domain aggregates
- Supports filtering by status
"""

from typing import List, Optional
from loguru import logger

from agentmesh.domain.entities.agent_aggregate import AgentAggregate
from agentmesh.domain.ports.agent_repository_port import AgentRepositoryPort


class ListAgentsQuery:
    """
    Query: List agents for a tenant.

    Flow:
    1. Retrieve agents from repository (optionally filtered)
    2. Return list of domain aggregates
    """

    def __init__(self, agent_repository: AgentRepositoryPort):
        self._repository = agent_repository

    async def execute(
        self,
        tenant_id: str,
        status: Optional[str] = None,
    ) -> List[AgentAggregate]:
        """
        Execute list agents query.

        Args:
            tenant_id: Tenant ID for isolation
            status: Optional status filter

        Returns:
            List of AgentAggregate matching criteria
        """
        logger.info(
            f"Listing agents for tenant {tenant_id}"
            + (f" with status {status}" if status else "")
        )

        if status:
            agents = await self._repository.find_by_status(status, tenant_id)
        else:
            agents = await self._repository.find_all(tenant_id)

        logger.info(f"Found {len(agents)} agents")

        return agents
