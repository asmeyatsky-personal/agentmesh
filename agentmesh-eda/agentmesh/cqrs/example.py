from dataclasses import dataclass
from datetime import UTC, datetime
import uuid
from agentmesh.cqrs.command import Command
from agentmesh.cqrs.event import Event
from agentmesh.cqrs.handler import CommandHandler, EventHandler
from agentmesh.cqrs.event_store import InMemoryEventStore
from agentmesh.cqrs.bus import CommandBus
from agentmesh.utils.logging_config import setup_logging


# 1. Define Commands
@dataclass
class CreateAccount(Command):
    tenant_id: str
    account_id: str
    initial_balance: float


@dataclass
class CreditAccount(Command):
    tenant_id: str
    account_id: str
    amount: float


# 2. Define Events
@dataclass
class AccountCreated(Event):
    account_id: str
    initial_balance: float


@dataclass
class AccountCredited(Event):
    account_id: str
    amount: float


# 3. Define Aggregate
class BankAccount:
    def __init__(self, account_id: str):
        self.account_id = account_id
        self.balance = 0.0
        self.changes: list[Event] = []

    def create(self, initial_balance: float):
        event = AccountCreated(
            event_id=str(uuid.uuid4()),
            created_at=datetime.now(UTC),
            account_id=self.account_id,
            initial_balance=initial_balance,
        )
        self._apply(event)
        self.changes.append(event)

    def credit(self, amount: float):
        event = AccountCredited(
            event_id=str(uuid.uuid4()),
            created_at=datetime.now(UTC),
            account_id=self.account_id,
            amount=amount,
        )
        self._apply(event)
        self.changes.append(event)

    def _apply(self, event: Event):
        if isinstance(event, AccountCreated):
            self.balance = event.initial_balance
        elif isinstance(event, AccountCredited):
            self.balance += event.amount

    def load_from_history(self, events: list[Event]):
        for event in events:
            self._apply(event)


# 4. Define Command Handler
class BankAccountCommandHandler(CommandHandler):
    def __init__(self, event_store: InMemoryEventStore):
        self.event_store = event_store

    def handle(self, command: Command):
        if isinstance(command, CreateAccount):
            account = BankAccount(command.account_id)
            account.create(command.initial_balance)
            self.event_store.save_events(
                command.tenant_id, account.account_id, account.changes
            )
        elif isinstance(command, CreditAccount):
            events = self.event_store.load_events(
                command.tenant_id, command.account_id
            )
            account = BankAccount(command.account_id)
            account.load_from_history(events)
            account.credit(command.amount)
            self.event_store.save_events(
                command.tenant_id, account.account_id, account.changes
            )


# 5. Define Read Model and Event Handler
class BankAccountReadModel:
    def __init__(self):
        self.accounts: dict[str, dict[str, float]] = {}

    def get_balance(self, tenant_id: str, account_id: str) -> float:
        return self.accounts.get(tenant_id, {}).get(account_id, 0.0)


class BankAccountEventHandler(EventHandler):
    def __init__(self, read_model: BankAccountReadModel):
        self.read_model = read_model

    def handle(self, event: Event, tenant_id: str):
        if tenant_id not in self.read_model.accounts:
            self.read_model.accounts[tenant_id] = {}

        if isinstance(event, AccountCreated):
            self.read_model.accounts[tenant_id][event.account_id] = (
                event.initial_balance
            )
        elif isinstance(event, AccountCredited):
            self.read_model.accounts[tenant_id][event.account_id] += event.amount


def main():
    setup_logging()
    # Setup
    event_store = InMemoryEventStore()
    command_bus = CommandBus()

    command_handler = BankAccountCommandHandler(event_store)
    command_bus.register_handler(CreateAccount, command_handler)
    command_bus.register_handler(CreditAccount, command_handler)

    # Execute commands for tenant 1
    tenant_1_id = "tenant1"
    account_1_id = "123"
    command_bus.dispatch(
        CreateAccount(
            tenant_id=tenant_1_id, account_id=account_1_id, initial_balance=100.0
        )
    )
    command_bus.dispatch(
        CreditAccount(tenant_id=tenant_1_id, account_id=account_1_id, amount=50.0)
    )

    # Execute commands for tenant 2
    tenant_2_id = "tenant2"
    account_2_id = "456"
    command_bus.dispatch(
        CreateAccount(
            tenant_id=tenant_2_id, account_id=account_2_id, initial_balance=200.0
        )
    )

    # Verify event store for tenant 1
    events_t1 = event_store.load_events(tenant_1_id, account_1_id)
    print(f"Events for tenant 1: {events_t1}")

    # Verify event store for tenant 2
    events_t2 = event_store.load_events(tenant_2_id, account_2_id)
    print(f"Events for tenant 2: {events_t2}")

    # Verify read model
    read_model = BankAccountReadModel()
    event_handler = BankAccountEventHandler(read_model)
    for event in events_t1:
        event_handler.handle(event, tenant_1_id)
    for event in events_t2:
        event_handler.handle(event, tenant_2_id)

    print(f"Read model for tenant 1: {read_model.get_balance(tenant_1_id, account_1_id)}")
    print(f"Read model for tenant 2: {read_model.get_balance(tenant_2_id, account_2_id)}")


if __name__ == "__main__":
    main()
