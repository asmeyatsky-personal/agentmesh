"""
Shared test fixtures and configuration for comprehensive test suite.

Provides:
- Authentication fixtures
- Mock database, cache, broker, event store
- In-memory implementations for testing
- Test utilities and helpers
- Pytest configuration and marks
"""

import os
import pytest
import asyncio
import logging
from datetime import timedelta, datetime, UTC
from unittest.mock import AsyncMock, MagicMock

# Set required environment variables before any lazy initialization
os.environ.setdefault("ENCRYPTION_KEY", "pQKcaduh-gF3PDrZ56uJzcJOt4X10AgthjJETVxVNQM=")
os.environ.setdefault("JWT_SECRET_KEY", "test-jwt-secret-key-for-testing-only")

from agentmesh.security.auth import create_access_token


# ===== PYTEST CONFIGURATION =====

# ===== AUTHENTICATION FIXTURES =====

@pytest.fixture
def valid_token():
    """Generate valid auth token"""
    data = {"sub": "testuser", "roles": ["agent"]}
    token = create_access_token(data, tenant_id="test_tenant", expires_delta=timedelta(days=3650))
    return token


@pytest.fixture
def test_user():
    """Test user context"""
    return {
        "user_id": "user-123",
        "tenant_id": "tenant-1",
        "roles": ["admin"],
        "permissions": ["read", "write", "delete"]
    }


@pytest.fixture
def test_request_id():
    """Test request ID for tracing"""
    return "req-abc-123"


# ===== DATABASE FIXTURES =====

@pytest.fixture
async def mock_db():
    """Mock database connection"""
    db = AsyncMock()
    db.execute = AsyncMock(return_value=True)
    db.query = AsyncMock(return_value=[])
    db.pool_size = 20
    db.health_check = AsyncMock(return_value=True)
    return db


@pytest.fixture
async def mock_postgres():
    """Mock PostgreSQL adapter"""
    adapter = AsyncMock()
    adapter.connect = AsyncMock(return_value=None)
    adapter.disconnect = AsyncMock(return_value=None)
    adapter.execute = AsyncMock(return_value=None)
    adapter.query = AsyncMock(return_value=[])
    return adapter


# ===== CACHE FIXTURES =====

@pytest.fixture
async def mock_cache():
    """Mock cache (Redis)"""
    cache = AsyncMock()
    cache.get = AsyncMock(return_value=None)
    cache.set = AsyncMock(return_value=None)
    cache.delete = AsyncMock(return_value=None)
    cache.exists = AsyncMock(return_value=False)
    cache.invalidate_pattern = AsyncMock(return_value=0)
    return cache


@pytest.fixture
async def in_memory_cache():
    """In-memory cache implementation for testing"""
    class InMemoryCache:
        def __init__(self):
            self.data = {}
            self.operations = []

        async def get(self, key: str):
            self.operations.append(("get", key))
            return self.data.get(key)

        async def set(self, key: str, value, ttl_seconds: int = 3600):
            self.operations.append(("set", key))
            self.data[key] = value

        async def delete(self, key: str):
            self.operations.append(("delete", key))
            self.data.pop(key, None)

        async def exists(self, key: str):
            return key in self.data

        async def invalidate_pattern(self, pattern: str):
            count = 0
            keys_to_delete = []
            for key in self.data.keys():
                if pattern.replace("*", "") in key:
                    keys_to_delete.append(key)
                    count += 1
            for key in keys_to_delete:
                del self.data[key]
            return count

    return InMemoryCache()


# ===== MESSAGE BROKER FIXTURES =====

@pytest.fixture
async def mock_message_broker():
    """Mock message broker"""
    broker = AsyncMock()
    broker.publish = AsyncMock(return_value="delivery-id-123")
    broker.subscribe = AsyncMock(return_value="subscription-id-456")
    broker.unsubscribe = AsyncMock(return_value=None)
    broker.health_check = AsyncMock(return_value=True)
    return broker


@pytest.fixture
async def in_memory_message_broker():
    """In-memory message broker for testing"""
    class InMemoryBroker:
        def __init__(self):
            self.messages = []
            self.subscriptions = {}

        async def publish(self, message, topic: str = None):
            self.messages.append({"message": message, "topic": topic, "published_at": datetime.now(UTC)})
            return f"delivery-{len(self.messages)}"

        async def subscribe(self, topic: str, callback):
            if topic not in self.subscriptions:
                self.subscriptions[topic] = []
            self.subscriptions[topic].append(callback)
            return f"sub-{len(self.subscriptions)}"

        async def health_check(self):
            return True

        def get_messages(self, topic: str = None):
            if topic:
                return [m for m in self.messages if m.get("topic") == topic]
            return self.messages

    return InMemoryBroker()


# ===== EVENT STORE FIXTURES =====

@pytest.fixture
async def mock_event_store():
    """Mock event store"""
    store = AsyncMock()
    store.append_events = AsyncMock(return_value=None)
    store.get_events = AsyncMock(return_value=[])
    store.health_check = AsyncMock(return_value=True)
    return store


@pytest.fixture
async def in_memory_event_store():
    """In-memory event store for testing"""
    class InMemoryEventStore:
        def __init__(self):
            self.events = {}

        async def append_events(self, aggregate_id: str, events):
            if aggregate_id not in self.events:
                self.events[aggregate_id] = []
            self.events[aggregate_id].extend(events)

        async def get_events(self, aggregate_id: str, from_version: int = 0):
            return self.events.get(aggregate_id, [])[from_version:]

        async def health_check(self):
            return True

    return InMemoryEventStore()


# ===== AUDIT STORE FIXTURES =====

@pytest.fixture
async def in_memory_audit_store():
    """In-memory audit store for testing"""
    class InMemoryAuditStore:
        def __init__(self):
            self.entries = []

        async def append(self, entry):
            entry_id = f"entry-{len(self.entries)}"
            from dataclasses import replace
            entry_with_id = replace(entry, entry_id=entry_id)
            self.entries.append(entry_with_id)
            return entry_id

        async def query(self, query):
            results = self.entries
            if query.tenant_id:
                results = [e for e in results if e.tenant_id == query.tenant_id]
            if query.actor_id:
                results = [e for e in results if e.actor_id == query.actor_id]
            if query.action:
                results = [e for e in results if e.action.value == query.action]
            return results[query.offset:query.offset + query.limit]

        async def get_by_id(self, entry_id: str):
            for entry in self.entries:
                if entry.entry_id == entry_id:
                    return entry
            return None

        async def get_by_request_id(self, request_id: str):
            return [e for e in self.entries if e.request_id == request_id]

        async def get_actor_history(self, tenant_id: str, actor_id: str, limit: int = 100):
            return [
                e for e in self.entries
                if e.tenant_id == tenant_id and e.actor_id == actor_id
            ][:limit]

        async def get_resource_history(self, tenant_id: str, resource_id: str, limit: int = 100):
            return [
                e for e in self.entries
                if e.tenant_id == tenant_id and e.resource_id == resource_id
            ][:limit]

        async def export_range(self, tenant_id: str, start_date, end_date):
            import json
            entries = [e for e in self.entries if e.tenant_id == tenant_id]
            return json.dumps([e.to_dict() for e in entries]).encode()

        async def verify_integrity(self):
            return True

    return InMemoryAuditStore()


# ===== COMBINED FIXTURES =====

@pytest.fixture
async def mock_dependencies(mock_db, mock_cache, mock_message_broker, mock_event_store):
    """All mock dependencies together"""
    return {
        'db': mock_db,
        'cache': mock_cache,
        'message_broker': mock_message_broker,
        'event_store': mock_event_store
    }


@pytest.fixture
async def in_memory_dependencies(in_memory_cache, in_memory_message_broker,
                                 in_memory_event_store, in_memory_audit_store):
    """All in-memory implementations together"""
    return {
        'cache': in_memory_cache,
        'message_broker': in_memory_message_broker,
        'event_store': in_memory_event_store,
        'audit_store': in_memory_audit_store
    }


# ===== TEST UTILITIES =====

@pytest.fixture
def assert_observable():
    """Helper to assert observability properties"""
    def _assert(context, has_trace_id=True, has_span_id=True):
        if has_trace_id:
            assert 'trace_id' in context, "trace_id not in context"
        if has_span_id:
            assert 'span_id' in context, "span_id not in context"
    return _assert


@pytest.fixture
def assert_audit_logged():
    """Helper to assert audit logging"""
    def _assert(audit_store, action, actor_id, resource_id, status):
        found = False
        for entry in audit_store.entries:
            if (entry.action.value == action and
                entry.actor_id == actor_id and
                entry.resource_id == resource_id and
                entry.status.value == status):
                found = True
                break
        assert found, f"Audit entry not found: {action} by {actor_id} on {resource_id}"
    return _assert


# ===== PYTEST MARKS =====

def pytest_configure(config):
    """Register custom pytest marks"""
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests"
    )
    config.addinivalue_line(
        "markers", "async_test: marks tests as async"
    )
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )
    config.addinivalue_line(
        "markers", "security: marks tests as security-related"
    )
    config.addinivalue_line(
        "markers", "resilience: marks tests as resilience-related"
    )
    config.addinivalue_line(
        "markers", "property_based: marks tests as property-based"
    )
