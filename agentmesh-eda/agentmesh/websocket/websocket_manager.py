"""
WebSocket Support for Real-Time Updates

Provides WebSocket functionality for real-time agent and system updates.

Architectural Intent:
- Real-time agent status updates
- Live task execution progress
- System event broadcasting
- Bi-directional communication
- Connection management and scaling
- Message queuing and retries
"""

import asyncio
import json
import logging
import time
from typing import Dict, List, Optional, Callable, Any, Set
from dataclasses import dataclass, asdict
from enum import Enum
from datetime import UTC, datetime
import hashlib
import secrets
from fastapi import WebSocket, WebSocketDisconnect
from fastapi.websockets import WebSocketDisconnect as FastAPIWebSocketDisconnect
from starlette.websockets import WebSocketState
from loguru import logger
from agentmesh.middleware.rate_limiting import APIKeyValidator, generate_secure_token


class MessageType(str, Enum):
    """WebSocket message types"""
    # Connection management
    CONNECT = "connect"
    DISCONNECT = "disconnect"
    PING = "ping"
    PONG = "pong"
    AUTH = "auth"
    AUTH_RESPONSE = "auth_response"
    ERROR = "error"
    
    # Real-time data
    AGENT_STATUS_UPDATE = "agent_status_update"
    TASK_UPDATE = "task_update"
    SYSTEM_EVENT = "system_event"
    METRICS_UPDATE = "metrics_update"
    NOTIFICATION = "notification"


@dataclass
class WebSocketMessage:
    """WebSocket message structure"""
    type: MessageType
    data: Optional[Dict[str, Any]] = None
    timestamp: str = None
    message_id: Optional[str] = None
    connection_id: Optional[str] = None


@dataclass
class ClientConnection:
    """Represents a WebSocket client connection"""
    websocket: WebSocket
    connection_id: str
    user_id: Optional[str] = None
    tenant_id: Optional[str] = None
    api_permissions: List[str] = None
    subscribed_channels: Set[str] = None
    last_ping: float = 0
    is_authenticated: bool = False
    connected_at: datetime = None


@dataclass
class ChannelSubscription:
    """Channel subscription information"""
    name: str
    subscribers: Set[str] = Set[str]()
    permissions: List[str] = []
    last_activity: datetime


class WebSocketManager:
    """Manages WebSocket connections and message routing"""
    
    def __init__(self):
        self.connections: Dict[str, ClientConnection] = {}
        self.channels: Dict[str, ChannelSubscription] = {}
        self.message_handlers: Dict[MessageType, List[Callable]] = {}
        self.broadcast_handlers: List[Callable] = []
        self.redis_available = False  # Could add Redis for distributed WebSocket
        self.connection_timeout = 300  # 5 minutes
        self.max_connections_per_user = 5
        self.max_global_connections = 1000
        
    def register_handler(self, message_type: MessageType, handler: Callable):
        """Register a message handler"""
        if message_type not in self.message_handlers:
            self.message_handlers[message_type] = []
        self.message_handlers[message_type].append(handler)
    
    def register_broadcast_handler(self, handler: Callable):
        """Register a broadcast handler"""
        self.broadcast_handlers.append(handler)
    
    async def connect(self, websocket: WebSocket, connection_id: str, token: Optional[str] = None):
        """Handle new WebSocket connection"""
        try:
            # Validate API key if provided
            user_id = None
            tenant_id = None
            api_permissions = ["read", "write"]
            
            if token:
                # Validate token and extract user info
                user_id = self._extract_user_id_from_token(token)
                tenant_id = self._extract_tenant_id_from_token(token)
                api_permissions = self._extract_permissions_from_token(token)
                
                if not self._validate_user_connections(user_id, tenant_id):
                    await websocket.close(code=4001, reason="Too many connections")
                    return
            
            client = ClientConnection(
                websocket=websocket,
                connection_id=connection_id,
                user_id=user_id,
                tenant_id=tenant_id,
                api_permissions=api_permissions,
                connected_at=datetime.now(UTC)
            )
            
            self.connections[connection_id] = client
            
            # Send connection confirmation
            await self._send_to_client(client, {
                "type": MessageType.CONNECT,
                "data": {"connection_id": connection_id, "status": "connected"},
                "timestamp": datetime.now(UTC).isoformat(),
                "message_id": self._generate_message_id()
            })
            
            # Setup ping/pong for connection health
            asyncio.create_task(self._ping_loop(client))
            
            logger.info(f"WebSocket connected: {connection_id} for user: {user_id}")
            
        except Exception as e:
            logger.error(f"WebSocket connection error: {e}")
            await websocket.close(code=1011, reason="Connection failed")
    
    async def disconnect(self, connection_id: str):
        """Handle WebSocket disconnection"""
        if connection_id in self.connections:
            client = self.connections[connection_id]
            
            # Unsubscribe from all channels
            for channel_name in client.subscribed_channels:
                if channel_name in self.channels:
                    self.channels[channel_name].subscribers.discard(connection_id)
            
            del self.connections[connection_id]
            
            # Notify other handlers
            await self._notify_handlers({
                "type": MessageType.DISCONNECT,
                "data": {
                    "connection_id": connection_id,
                    "user_id": client.user_id,
                    "reason": "Disconnected",
                    "timestamp": datetime.now(UTC).isoformat(),
                    "message_id": self._generate_message_id()
                }
            }, connection_id=connection_id)
            
            logger.info(f"WebSocket disconnected: {connection_id}")
    
    async def _send_to_client(self, client: ClientConnection, message: WebSocketMessage):
        """Send message to specific client"""
        try:
            message_json = json.dumps(asdict(message))
            await client.websocket.send_text(message_json)
        except Exception as e:
            logger.error(f"Failed to send WebSocket message: {e}")
    
    async def _broadcast_to_all(self, message: WebSocketMessage, exclude_connection: Optional[str] = None):
        """Broadcast message to all connected clients"""
        for conn_id, client in self.connections.items():
            if exclude_connection and conn_id == exclude_connection:
                continue
            
            await self._send_to_client(client, message)
    
    async def _broadcast_to_channel(self, channel_name: str, message: WebSocketMessage, exclude_connection: Optional[str] = None):
        """Broadcast message to channel subscribers"""
        if channel_name in self.channels:
            for conn_id in self.channels[channel_name].subscribers:
                if exclude_connection and conn_id == exclude_connection:
                    continue
                    
                await self._send_to_client(self.connections[conn_id], message)
    
    async def _notify_handlers(self, message: WebSocketMessage, connection_id: Optional[str] = None):
        """Notify all registered handlers"""
        for handler in self.message_handlers.get(message.type, []):
            try:
                await handler(message, connection_id)
            except Exception as e:
                logger.error(f"WebSocket handler error: {e}")
        
        for handler in self.broadcast_handlers:
            try:
                await handler(message, connection_id)
            except Exception as e:
                logger.error(f"WebSocket broadcast handler error: {e}")
    
    async def _ping_loop(self, client: ClientConnection):
        """Maintain connection with ping/pong"""
        while client.connection_id in self.connections:
            try:
                await asyncio.sleep(30)  # Ping every 30 seconds
                
                ping_message = WebSocketMessage(
                    type=MessageType.PING,
                    timestamp=datetime.now(UTC).isoformat(),
                    message_id=self._generate_message_id()
                )
                
                await self._send_to_client(client, ping_message)
                
                # Wait for pong with timeout
                try:
                    pong_response = await asyncio.wait_for(
                        self._wait_for_pong(client),
                        timeout=10.0
                    )
                    logger.debug(f"Pong received for connection {client.connection_id}")
                except asyncio.TimeoutError:
                    logger.warning(f"Pong timeout for connection {client.connection_id}")
                    # Connection may be dead, consider disconnecting
                    break
                
            except Exception as e:
                logger.error(f"Ping loop error for connection {client.connection_id}: {e}")
                break
    
    async def _wait_for_pong(self, client: ClientConnection, timeout: float = 10.0):
        """Wait for pong response"""
        start_time = time.time()
        
        while client.connection_id in self.connections and time.time() - start_time < timeout:
            try:
                message = await client.websocket.receive_text()
                data = json.loads(message)
                
                if data.get("type") == MessageType.PONG and data.get("message_id") == client.last_ping_id:
                    return data
            except Exception as e:
                logger.error(f"Pong wait error: {e}")
                raise asyncio.TimeoutError()
        
        raise asyncio.TimeoutError("Pong timeout")
    
    def _generate_message_id(self) -> str:
        """Generate unique message ID"""
        return secrets.token_urlsafe(16)
    
    def _extract_user_id_from_token(self, token: str) -> Optional[str]:
        """Extract user ID from JWT token"""
        try:
            # Simple token parsing - in production, use proper JWT library
            # This is a simplified implementation
            if "." in token:
                parts = token.split(".")
                if len(parts) >= 2:
                    user_part = parts[0]
                    if user_part.startswith("user_"):
                        return user_part[5:]  # Remove "user_" prefix
                    return user_part
            return None
        except Exception:
            return None
    
    def _extract_tenant_id_from_token(self, token: str) -> Optional[str]:
        """Extract tenant ID from JWT token"""
        try:
            if "." in token:
                parts = token.split(".")
                if len(parts) >= 2:
                    tenant_part = parts[0]
                    if tenant_part.startswith("tenant_"):
                        return tenant_part[5:]  # Remove "tenant_" prefix
                    return tenant_part
            return None
        except Exception:
            return None
    
    def _extract_permissions_from_token(self, token: str) -> List[str]:
        """Extract permissions from JWT token"""
        try:
            # Simple token parsing for demo
            return ["read", "write"]  # In production, extract from JWT claims
        except Exception:
            return ["read"]  # Default permissions
    
    def _validate_user_connections(self, user_id: str, tenant_id: str) -> bool:
        """Validate user connection limits"""
        if not user_id or not tenant_id:
            return True
        
        user_connections = len([
            conn for conn in self.connections.values()
            if conn.user_id == user_id
        ])
        
        tenant_connections = len([
            conn for conn in self.connections.values()
            if conn.tenant_id == tenant_id
        ])
        
        return (user_connections < self.max_connections_per_user and 
                tenant_connections < self.max_connections_per_user)
    
    async def handle_message(self, websocket: WebSocket, message: str, connection_id: str):
        """Handle incoming WebSocket message"""
        try:
            client = self.connections.get(connection_id)
            if not client:
                logger.warning(f"Received message for unknown connection: {connection_id}")
                return
            
            data = json.loads(message)
            message_obj = WebSocketMessage(**data)
            
            # Update last ping time
            if message_obj.type == MessageType.PONG:
                client.last_ping = time.time()
            
            # Route message to appropriate handler
            await self._notify_handlers(message_obj, connection_id)
            
        except Exception as e:
            logger.error(f"WebSocket message handling error: {e}")
            await websocket.close(code=4000, reason="Invalid message")
    
    async def authenticate_connection(self, websocket: WebSocket, connection_id: str, token: str):
        """Authenticate WebSocket connection"""
        user_id = self._extract_user_id_from_token(token)
        tenant_id = self._extract_tenant_id_from_token(token)
        
        if not self._validate_user_connections(user_id, tenant_id):
            await websocket.close(code=4001, reason="Too many connections")
            return False
        
        # Update client with authentication info
        client = self.connections[connection_id]
        client.user_id = user_id
        client.tenant_id = tenant_id
        client.is_authenticated = True
        
        # Send authentication success
        auth_response = WebSocketMessage(
            type=MessageType.AUTH_RESPONSE,
            data={"status": "authenticated", "user_id": user_id, "tenant_id": tenant_id},
            timestamp=datetime.now(UTC).isoformat(),
            message_id=self._generate_message_id()
        )
        
        await self._send_to_client(client, auth_response)
        logger.info(f"WebSocket authenticated: {connection_id} for user: {user_id}")
        return True
    
    async def subscribe_to_channel(self, connection_id: str, channel_name: str, permissions: List[str] = None):
        """Subscribe client to a channel"""
        client = self.connections.get(connection_id)
        if not client:
            return False
        
        if channel_name not in self.channels:
            self.channels[channel_name] = ChannelSubscription(
                name=channel_name,
                subscribers=set(),
                permissions=permissions or [],
                last_activity=datetime.now(UTC)
            )
        
        # Validate permissions
        if not self._validate_channel_permissions(client, self.channels[channel_name], permissions):
            return False
        
        self.channels[channel_name].subscribers.add(connection_id)
        client.subscribed_channels.add(channel_name)
        
        # Send subscription confirmation
        response = WebSocketMessage(
            type=MessageType.NOTIFICATION,
            data={
                "action": "subscribed",
                "channel": channel_name,
                "timestamp": datetime.now(UTC).isoformat(),
                "message_id": self._generate_message_id()
            }
        )
        
        await self._send_to_client(client, response)
    
    def _validate_channel_permissions(self, client: ClientConnection, channel: ChannelSubscription, permissions: List[str]) -> bool:
        """Validate if client has required permissions for channel"""
        if not permissions:
            return True  # No restrictions if no permissions specified
        
        client_perms = set(client.api_permissions or [])
        required_perms = set(permissions)
        
        # Check if client has all required permissions
        for perm in required_perms:
            if perm not in client_perms:
                return False
        return True


# WebSocket endpoint
class WebSocketEndpoint:
    """FastAPI WebSocket endpoint with authentication"""
    
    def __init__(self, websocket_manager: WebSocketManager):
        self.manager = websocket_manager
    
    async def __call__(self, websocket: WebSocket, token: Optional[str] = None):
        """Handle WebSocket connection"""
        connection_id = self.manager._generate_message_id()
        
        # Authenticate if token provided
        if token:
            if not await self.manager.authenticate_connection(websocket, connection_id, token):
                return
        
        # Connect without authentication (public connections)
        await self.manager.connect(websocket, connection_id)
    
    async def send_agent_update(self, agent_id: str, status: str, metadata: Dict[str, Any] = None):
        """Broadcast agent status update to relevant subscribers"""
        message = WebSocketMessage(
            type=MessageType.AGENT_STATUS_UPDATE,
            data={
                "agent_id": agent_id,
                "status": status,
                "metadata": metadata or {},
                "timestamp": datetime.now(UTC).isoformat(),
                "message_id": self.manager._generate_message_id()
            }
        )
        
        await self.manager._broadcast_to_all(message)
    
    async def send_task_update(self, task_id: str, status: str, agent_id: str, metadata: Dict[str, Any] = None):
        """Send task update to subscribers"""
        message = WebSocketMessage(
            type=MessageType.TASK_UPDATE,
            data={
                "task_id": task_id,
                "status": status,
                "agent_id": agent_id,
                "metadata": metadata or {},
                "timestamp": datetime.now(UTC).isoformat(),
                "message_id": self.manager._generate_message_id()
            }
        )
        
        await self.manager._broadcast_to_all(message)
    
    async def send_system_event(self, event_type: str, data: Dict[str, Any]):
        """Broadcast system event"""
        message = WebSocketMessage(
            type=MessageType.SYSTEM_EVENT,
            data={
                "event_type": event_type,
                "data": data,
                "timestamp": datetime.now(UTC).isoformat(),
                "message_id": self.manager._generate_message_id()
            }
        )
        
        await self.manager._broadcast_to_all(message)
    
    async def send_notification(self, message: str, level: str = "info", target: Optional[List[str]] = None):
        """Send notification to specific users or channels"""
        message_obj = WebSocketMessage(
            type=MessageType.NOTIFICATION,
            data={
                "message": message,
                "level": level,
                "target": target,
                "timestamp": datetime.now(UTC).isoformat(),
                "message_id": self.manager._generate_message_id()
            }
        )
        
        if target:
            # Send to specific users by connection ID
            for target_connection_id in target:
                if target_connection_id in self.manager.connections:
                    await self.manager._send_to_client(self.manager.connections[target_connection_id], message_obj)
        else:
            # Broadcast to all
            await self.manager._broadcast_to_all(message_obj)
    
    async def send_metrics_update(self, metrics: Dict[str, Any]):
        """Broadcast metrics update"""
        message = WebSocketMessage(
            type=MessageType.METRICS_UPDATE,
            data=metrics,
            "timestamp": datetime.now(UTC).isoformat(),
            "message_id": self.manager._generate_message_id()
            }
        )
        
        await self.manager._broadcast_to_all(message)


# Utility functions
async def create_websocket_endpoint(
    manager: WebSocketManager,
    path: str = "/ws",
    token_required: bool = False
):
    """Create and return a WebSocket endpoint"""
    
    async def websocket_endpoint(websocket: WebSocket, token: Optional[str] = None):
        connection_id = manager._generate_message_id()
        
        if token_required:
            if not await manager.authenticate_connection(websocket, connection_id, token):
                return
        
        await manager.connect(websocket, connection_id)
    
    return websocket_endpoint