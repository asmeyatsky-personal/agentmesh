"""
Integration Tests: Event-Driven Architecture

Tests CQRS patterns and event sourcing functionality.

Architectural Intent:
- Verify command dispatching works end-to-end
- Verify event publishing and handling
- Verify query processing
- Verify eventual consistency
"""

import pytest
from unittest.mock import AsyncMock, MagicMock
from agentmesh.cqrs.bus import CqrsBus
from agentmesh.cqrs.command import Command
from agentmesh.cqrs.query import Query
from agentmesh.cqrs.handler import CommandHandler, QueryHandler
from agentmesh.cqrs.event_store import InMemoryEventStore


# Test command and event
class CreateTestCommand(Command):
    def __init__(self, name: str, tenant_id: str):
        self.name = name
        self.tenant_id = tenant_id


class GetTestQuery(Query):
    def __init__(self, name: str, tenant_id: str):
        self.name = name
        self.tenant_id = tenant_id


class TestCreatedEvent:
    def __init__(self, name: str, tenant_id: str):
        self.name = name
        self.tenant_id = tenant_id
        self.created_at = "now"


# Test handlers
class TestCommandHandler(CommandHandler):
    def __init__(self, event_store):
        self.event_store = event_store
        self.handled_commands = []

    def handle(self, command: CreateTestCommand):
        self.handled_commands.append(command)

        # Create and store event
        event = TestCreatedEvent(command.name, command.tenant_id)
        self.event_store.save_events(command.tenant_id, command.name, [event])


class TestQueryHandler(QueryHandler):
    def __init__(self, event_store):
        self.event_store = event_store
        self.handled_queries = []

    async def handle(self, query: GetTestQuery):
        self.handled_queries.append(query)

        # Load events from store
        events = self.event_store.load_events(query.tenant_id, query.name)

        # Return materialized view
        if events:
            latest_event = events[-1]
            return {
                "name": latest_event.name,
                "tenant_id": latest_event.tenant_id,
                "created_at": latest_event.created_at,
                "exists": True,
            }

        return {"name": query.name, "exists": False}


@pytest.mark.asyncio
class TestCQRSIntegration:
    """Integration tests for CQRS patterns"""

    @pytest.fixture
    def setup(self):
        """Setup CQRS components"""
        event_store = InMemoryEventStore()
        command_bus = CqrsBus()

        # Register handlers
        command_handler = TestCommandHandler(event_store)
        query_handler = TestQueryHandler(event_store)

        command_bus.register_command_handler(CreateTestCommand, command_handler)
        command_bus.register_query_handler(GetTestQuery, query_handler)

        return command_bus, event_store, command_handler, query_handler

    async def test_command_dispatch_and_event_creation(self, setup):
        """Test command dispatch creates events"""
        command_bus, event_store, command_handler, query_handler = setup

        command = CreateTestCommand("test-entity", "tenant-1")

        # Dispatch command
        await command_bus.dispatch_command(command)

        # Verify command was handled
        assert len(command_handler.handled_commands) == 1
        assert command_handler.handled_commands[0] == command

        # Verify event was created and stored
        events = event_store.load_events("tenant-1", "test-entity")
        assert len(events) == 1
        assert events[0].name == "test-entity"
        assert events[0].tenant_id == "tenant-1"

    async def test_query_returns_materialized_view(self, setup):
        """Test query returns materialized view from events"""
        command_bus, event_store, command_handler, query_handler = setup

        # First create entity via command
        command = CreateTestCommand("query-test", "tenant-1")
        await command_bus.dispatch_command(command)

        # Then query for it
        query = GetTestQuery("query-test", "tenant-1")
        result = await command_bus.dispatch_query(query)

        # Verify query was handled
        assert len(query_handler.handled_queries) == 1
        assert query_handler.handled_queries[0] == query

        # Verify result
        assert result["name"] == "query-test"
        assert result["tenant_id"] == "tenant-1"
        assert result["exists"] is True

    async def test_multiple_commands_create_multiple_events(self, setup):
        """Test multiple commands create multiple events"""
        command_bus, event_store, command_handler, query_handler = setup

        # Create multiple entities
        commands = [CreateTestCommand(f"entity-{i}", "tenant-1") for i in range(3)]

        for command in commands:
            await command_bus.dispatch_command(command)

        # Verify all commands handled
        assert len(command_handler.handled_commands) == 3

        # Verify all events created
        events = event_store.load_events("tenant-1", "entity-0")
        events_1 = event_store.load_events("tenant-1", "entity-1")
        events_2 = event_store.load_events("tenant-1", "entity-2")

        assert len(events) == 1 and events[0].name == "entity-0"
        assert len(events_1) == 1 and events_1[0].name == "entity-1"
        assert len(events_2) == 1 and events_2[0].name == "entity-2"

    async def test_eventual_consistency(self, setup):
        """Test eventual consistency between command and query sides"""
        command_bus, event_store, command_handler, query_handler = setup

        # Create entity
        command = CreateTestCommand("consistency-test", "tenant-1")
        await command_bus.dispatch_command(command)

        # Query immediately (should see the entity)
        query = GetTestQuery("consistency-test", "tenant-1")
        result = await command_bus.dispatch_query(query)

        assert result["exists"] is True

        # Query again (should still be consistent)
        result2 = await command_bus.dispatch_query(query)
        assert result2["exists"] is True
        assert result2["name"] == result["name"]

    async def test_tenant_isolation_in_cqrs(self, setup):
        """Test tenant isolation in CQRS patterns"""
        command_bus, event_store, command_handler, query_handler = setup

        # Create entities in different tenants
        command1 = CreateTestCommand("shared-name", "tenant-1")
        command2 = CreateTestCommand("shared-name", "tenant-2")

        await command_bus.dispatch_command(command1)
        await command_bus.dispatch_command(command2)

        # Query tenant 1
        query1 = GetTestQuery("shared-name", "tenant-1")
        result1 = await command_bus.dispatch_query(query1)

        # Query tenant 2
        query2 = GetTestQuery("shared-name", "tenant-2")
        result2 = await command_bus.dispatch_query(query2)

        # Both should exist but be separate
        assert result1["exists"] is True
        assert result2["exists"] is True
        assert result1["tenant_id"] == "tenant-1"
        assert result2["tenant_id"] == "tenant-2"

    async def test_query_nonexistent_entity(self, setup):
        """Test query for non-existent entity"""
        command_bus, event_store, command_handler, query_handler = setup

        # Query without creating entity first
        query = GetTestQuery("nonexistent", "tenant-1")
        result = await command_bus.dispatch_query(query)

        # Should indicate non-existence
        assert result["exists"] is False
        assert result["name"] == "nonexistent"


@pytest.mark.asyncio
class TestEventSourcingIntegration:
    """Integration tests for event sourcing"""

    @pytest.fixture
    def setup(self):
        """Setup event store"""
        return InMemoryEventStore()

    async def test_event_store_persistence(self, setup):
        """Test event store persistence"""
        event_store = setup

        # Save events
        events = [TestCreatedEvent(f"event-{i}", "tenant-1") for i in range(3)]

        event_store.save_events("tenant-1", "aggregate-1", events)

        # Load events back
        loaded_events = event_store.load_events("tenant-1", "aggregate-1")

        assert len(loaded_events) == 3
        assert loaded_events[0].name == "event-0"
        assert loaded_events[1].name == "event-1"
        assert loaded_events[2].name == "event-2"

    async def test_event_store_isolation(self, setup):
        """Test tenant isolation in event store"""
        event_store = setup

        # Save events for different tenants
        events_t1 = [TestCreatedEvent("tenant1-event", "tenant-1")]
        events_t2 = [TestCreatedEvent("tenant2-event", "tenant-2")]

        event_store.save_events("tenant-1", "aggregate-1", events_t1)
        event_store.save_events("tenant-2", "aggregate-1", events_t2)

        # Verify isolation
        loaded_t1 = event_store.load_events("tenant-1", "aggregate-1")
        loaded_t2 = event_store.load_events("tenant-2", "aggregate-1")

        assert len(loaded_t1) == 1
        assert len(loaded_t2) == 1
        assert loaded_t1[0].tenant_id == "tenant-1"
        assert loaded_t2[0].tenant_id == "tenant-2"

    async def test_event_store_empty_results(self, setup):
        """Test event store handles non-existent data"""
        event_store = setup

        # Load events for non-existent tenant/aggregate
        loaded_events = event_store.load_events("nonexistent", "nonexistent")

        assert loaded_events == []
