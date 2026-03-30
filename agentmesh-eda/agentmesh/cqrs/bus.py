from agentmesh.cqrs.event import Event
from agentmesh.cqrs.command import Command
from agentmesh.cqrs.handler import CommandHandler
from agentmesh.cqrs.query import Query
from agentmesh.cqrs.query_handler import QueryHandler
from agentmesh.mal.router import MessageRouter
from agentmesh.mal.message import UniversalMessage
from typing import Dict, List, Type, Any
from agentmesh.utils.metrics import COMMANDS_DISPATCHED


class EventBus:
    def __init__(self, router: MessageRouter):
        self.router = router

    async def publish(self, events: List[Event]):
        for event in events:
            message = UniversalMessage(
                payload=event.__dict__,
                routing={"targets": [f"nats:events.{type(event).__name__}"]},
            )
            await self.router.route_message(message)


class CqrsBus:
    def __init__(self):
        self.command_handlers: Dict[Type[Command], CommandHandler] = {}
        self.query_handlers: Dict[Type[Query], QueryHandler] = {}

    def register_command_handler(self, command_type: Type[Command], handler: CommandHandler):
        self.command_handlers[command_type] = handler

    def register_query_handler(self, query_type: Type[Query], handler: QueryHandler):
        self.query_handlers[query_type] = handler

    async def dispatch_command(self, command: Command):
        handler = self.command_handlers.get(type(command))
        if handler is None:
            raise ValueError(f"No handler registered for command type: {type(command).__name__}")
        handler.handle(command)
        COMMANDS_DISPATCHED.labels(command_type=type(command).__name__).inc()

    async def dispatch_query(self, query: Query) -> Any:
        handler = self.query_handlers.get(type(query))
        if handler is None:
            raise ValueError(f"No handler registered for query type: {type(query).__name__}")
        return await handler.handle(query)
