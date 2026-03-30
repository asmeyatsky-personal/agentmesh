"""
Application Queries

Architectural Intent:
- Query handlers following CQRS pattern
- Read-optimized data access
- Return DTOs, not domain aggregates
"""

from agentmesh.application.queries.get_agent_query import GetAgentQuery
from agentmesh.application.queries.list_agents_query import ListAgentsQuery

__all__ = ["GetAgentQuery", "ListAgentsQuery"]
