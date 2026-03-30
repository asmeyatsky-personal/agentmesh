"""
PostgreSQL Message Persistence Adapter

Architectural Intent:
- Implements MessagePersistencePort using PostgreSQL
- Handles encryption of sensitive message data
- Provides message audit trail and history queries
- Maintains referential integrity and performance indexes

Key Design Decisions:
1. Encrypts message payload for confidentiality
2. Stores plaintext metadata for efficient queries
3. Creates indexes on tenant_id, message_id, type for performance
4. Uses SQLAlchemy ORM for type safety and portability
"""

from datetime import UTC, datetime
from typing import Optional, List
from loguru import logger
from agentmesh.domain.ports.message_persistence_port import (
    MessagePersistencePort,
    MessagePersistenceResult,
    MessageNotPersistedException
)
from agentmesh.mal.message import UniversalMessage
from agentmesh.security.encryption import encrypt_data, decrypt_data
from agentmesh.db.database import SessionLocal, Message
import json


class PostgresMessagePersistenceAdapter(MessagePersistencePort):
    """
    PostgreSQL adapter for message persistence.

    Encrypts sensitive payload data while maintaining queryable metadata.
    Ensures tenant isolation and maintains audit trail.
    """

    def __init__(self):
        """Initialize adapter with database session"""
        self.Session = SessionLocal
        logger.info("PostgresMessagePersistenceAdapter initialized")

    async def persist_message(self,
                            message: UniversalMessage) -> MessagePersistenceResult:
        """
        Persist message to PostgreSQL with encryption.

        Algorithm:
        1. Validate message structure
        2. Encrypt sensitive payload
        3. Extract plaintext metadata for indexing
        4. Insert into database
        5. Return persistence result

        Args:
            message: UniversalMessage to persist

        Returns:
            MessagePersistenceResult with storage details

        Raises:
            MessageNotPersistedException: If persistence fails
        """
        db = self.Session()
        try:
            message_id = message.metadata.get("id")
            if not message_id:
                raise ValueError("Message must have 'id' in metadata")

            # Serialize message
            message_bytes = message.serialize().encode('utf-8')

            # Encrypt payload
            encrypted_payload = encrypt_data(message_bytes)

            # Create database record
            db_message = Message(
                id=message_id,
                tenant_id=message.tenant_id,
                message_type=message.metadata.get("type", "UNKNOWN"),
                source=message.metadata.get("source", "UNKNOWN"),
                priority=message.metadata.get("priority", "NORMAL"),
                correlation_id=message.metadata.get("correlation_id"),
                encrypted_payload=encrypted_payload,
                persisted_at=datetime.now(UTC)
            )

            db.add(db_message)
            db.commit()
            db.refresh(db_message)

            logger.info(
                f"Message {message_id} persisted successfully "
                f"(tenant: {message.tenant_id}, type: {db_message.message_type})"
            )

            return MessagePersistenceResult(
                persisted_id=message_id,
                tenant_id=message.tenant_id,
                persisted_at=db_message.persisted_at,
                storage_location=f"postgresql://messages/{message.tenant_id}/{message_id}"
            )

        except Exception as e:
            logger.error(f"Failed to persist message: {e}")
            raise MessageNotPersistedException(f"Failed to persist message: {e}") from e
        finally:
            db.close()

    async def get_message(self,
                         message_id: str,
                         tenant_id: str) -> Optional[UniversalMessage]:
        """
        Retrieve and decrypt persisted message.

        Args:
            message_id: ID of message to retrieve
            tenant_id: Tenant ID for isolation

        Returns:
            UniversalMessage if found, None otherwise

        Raises:
            MessageNotPersistedException: If decryption fails
        """
        db = self.Session()
        try:
            db_message = db.query(Message).filter(
                Message.id == message_id,
                Message.tenant_id == tenant_id
            ).first()

            if not db_message:
                logger.debug(f"Message {message_id} not found for tenant {tenant_id}")
                return None

            # Decrypt payload
            decrypted_bytes = decrypt_data(db_message.encrypted_payload)
            decrypted_str = decrypted_bytes.decode('utf-8')

            # Reconstruct message
            message = UniversalMessage.deserialize(decrypted_str)
            logger.debug(f"Retrieved and decrypted message {message_id}")

            return message

        except ValueError as e:
            logger.error(f"Failed to decrypt message {message_id}: {e}")
            raise MessageNotPersistedException(f"Failed to decrypt message: {e}") from e
        except Exception as e:
            logger.error(f"Failed to retrieve message {message_id}: {e}")
            raise MessageNotPersistedException(f"Failed to retrieve message: {e}") from e
        finally:
            db.close()

    async def get_message_history(self,
                                 tenant_id: str,
                                 limit: int = 100,
                                 offset: int = 0) -> List[UniversalMessage]:
        """
        Retrieve message history for tenant.

        Returns messages ordered by newest first (DESC).

        Args:
            tenant_id: Tenant ID for isolation
            limit: Maximum messages to return
            offset: Number of messages to skip

        Returns:
            List of UniversalMessage ordered by timestamp DESC

        Raises:
            MessageNotPersistedException: If query fails
        """
        db = self.Session()
        try:
            db_messages = db.query(Message).filter(
                Message.tenant_id == tenant_id
            ).order_by(
                Message.persisted_at.desc()
            ).limit(limit).offset(offset).all()

            messages = []
            corrupted_ids = []
            for db_msg in db_messages:
                try:
                    decrypted_bytes = decrypt_data(db_msg.encrypted_payload)
                    decrypted_str = decrypted_bytes.decode('utf-8')
                    message = UniversalMessage.deserialize(decrypted_str)
                    messages.append(message)
                except Exception as e:
                    corrupted_ids.append(db_msg.id)
                    logger.error(
                        f"Failed to decrypt message {db_msg.id} for tenant {tenant_id}: {e}. "
                        f"Message will be excluded from results."
                    )

            if corrupted_ids:
                logger.warning(
                    f"Skipped {len(corrupted_ids)} corrupted messages for tenant {tenant_id}: "
                    f"{corrupted_ids}"
                )

            logger.debug(f"Retrieved {len(messages)} messages for tenant {tenant_id}")
            return messages

        except Exception as e:
            logger.error(f"Failed to retrieve message history: {e}")
            raise MessageNotPersistedException(f"Failed to retrieve history: {e}") from e
        finally:
            db.close()

    async def get_messages_by_type(self,
                                  message_type: str,
                                  tenant_id: str,
                                  limit: int = 100) -> List[UniversalMessage]:
        """
        Retrieve messages by type (uses plaintext index for performance).

        Args:
            message_type: Type of message to filter by
            tenant_id: Tenant ID for isolation
            limit: Maximum messages to return

        Returns:
            List of UniversalMessage matching type

        Raises:
            MessageNotPersistedException: If query fails
        """
        db = self.Session()
        try:
            db_messages = db.query(Message).filter(
                Message.tenant_id == tenant_id,
                Message.message_type == message_type
            ).order_by(
                Message.persisted_at.desc()
            ).limit(limit).all()

            messages = []
            corrupted_ids = []
            for db_msg in db_messages:
                try:
                    decrypted_bytes = decrypt_data(db_msg.encrypted_payload)
                    decrypted_str = decrypted_bytes.decode('utf-8')
                    message = UniversalMessage.deserialize(decrypted_str)
                    messages.append(message)
                except Exception as e:
                    corrupted_ids.append(db_msg.id)
                    logger.error(
                        f"Failed to decrypt message {db_msg.id} (type={message_type}) "
                        f"for tenant {tenant_id}: {e}. Message will be excluded from results."
                    )

            if corrupted_ids:
                logger.warning(
                    f"Skipped {len(corrupted_ids)} corrupted messages of type {message_type} "
                    f"for tenant {tenant_id}: {corrupted_ids}"
                )

            logger.debug(
                f"Retrieved {len(messages)} messages of type {message_type} "
                f"for tenant {tenant_id}"
            )
            return messages

        except Exception as e:
            logger.error(f"Failed to retrieve messages by type: {e}")
            raise MessageNotPersistedException(f"Failed to retrieve by type: {e}") from e
        finally:
            db.close()

    async def delete_message(self,
                           message_id: str,
                           tenant_id: str) -> bool:
        """
        Delete persisted message.

        Args:
            message_id: ID of message to delete
            tenant_id: Tenant ID for isolation

        Returns:
            True if deleted, False if not found

        Raises:
            MessageNotPersistedException: If deletion fails
        """
        db = self.Session()
        try:
            result = db.query(Message).filter(
                Message.id == message_id,
                Message.tenant_id == tenant_id
            ).delete()

            db.commit()

            if result > 0:
                logger.info(f"Deleted message {message_id} for tenant {tenant_id}")
                return True
            else:
                logger.debug(f"Message {message_id} not found for deletion")
                return False

        except Exception as e:
            logger.error(f"Failed to delete message {message_id}: {e}")
            raise MessageNotPersistedException(f"Failed to delete message: {e}") from e
        finally:
            db.close()
