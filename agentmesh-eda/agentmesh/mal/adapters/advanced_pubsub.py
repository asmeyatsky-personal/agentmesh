"""
Advanced Google Cloud Pub/Sub Adapter for AgentMesh
Implements enhanced features like dead letter queues, ordering, and error handling
"""
from agentmesh.mal.adapters.base import MessagePlatformAdapter
from agentmesh.mal.message import UniversalMessage
from google.cloud import pubsub_v1, monitoring_v3
from google.api_core import exceptions
from typing import Dict, Any, AsyncGenerator
from concurrent import futures
import asyncio
import json
import time
import logging
from datetime import UTC, datetime

logger = logging.getLogger(__name__)

class AdvancedPubSubAdapter(MessagePlatformAdapter):
    """
    Advanced Google Cloud Pub/Sub adapter with enhanced features
    """
    
    def __init__(self, project_id: str, enable_ordering: bool = False, 
                 enable_dead_letter_queue: bool = True):
        self.project_id = project_id
        self.enable_ordering = enable_ordering
        self.enable_dead_letter_queue = enable_dead_letter_queue
        
        # Initialize Pub/Sub clients
        self.publisher = pubsub_v1.PublisherClient()
        self.subscriber = pubsub_v1.SubscriberClient()
        
        # Initialize monitoring client for metrics
        self.monitoring_client = monitoring_v3.MetricServiceClient()
        
        # Thread pool for handling callbacks
        self.executor = futures.ThreadPoolExecutor(max_workers=10)
        
        # Track metrics
        self.message_count = 0
        self.error_count = 0

    async def send(self, message: UniversalMessage, target: str):
        """
        Send a message to a Pub/Sub topic with enhanced features
        """
        try:
            topic_path = self.publisher.topic_path(self.project_id, target)
            
            # Prepare message data
            message_data = message.serialize()
            
            # Add attributes for enhanced routing and tracking
            attributes = {
                "tenant_id": message.tenant_id,
                "message_id": message.metadata.get("id", ""),
                "timestamp": message.metadata.get("timestamp", datetime.now(UTC).isoformat()),
                "source": message.routing.get("source", "unknown"),
                "destination": message.routing.get("destination", target)
            }
            
            # Add ordering key if enabled and available
            if self.enable_ordering:
                ordering_key = message.metadata.get("ordering_key", "")
                if ordering_key:
                    attributes["ordering_key"] = ordering_key
            
            # Publish message
            future = self.publisher.publish(
                topic_path,
                message_data,
                **attributes
            )
            
            # Wait for publish to complete
            message_id = future.result()
            
            # Update metrics
            self.message_count += 1
            
            logger.info(f"Published message {message_id} to topic {target}")
            
            # Optionally, publish metrics to Cloud Monitoring
            await self._publish_metrics()
            
        except exceptions.DeadlineExceeded:
            logger.error(f"Deadline exceeded when publishing to topic {target}")
            if self.enable_dead_letter_queue:
                await self._send_to_dead_letter_queue(message, target, "DEADLINE_EXCEEDED")
            raise
        except exceptions.ResourceExhausted:
            logger.error(f"Resource exhausted when publishing to topic {target}")
            if self.enable_dead_letter_queue:
                await self._send_to_dead_letter_queue(message, target, "RESOURCE_EXHAUSTED")
            raise
        except Exception as e:
            logger.error(f"Error publishing message to topic {target}: {e}")
            if self.enable_dead_letter_queue:
                await self._send_to_dead_letter_queue(message, target, str(e))
            raise

    async def consume(self, subscription_name: str) -> AsyncGenerator[UniversalMessage, None]:
        """
        Consume messages from a Pub/Sub subscription with enhanced error handling
        """
        subscription_path = self.subscriber.subscription_path(
            self.project_id, subscription_name
        )
        
        # Queue to hold messages received from Pub/Sub
        message_queue = asyncio.Queue()
        
        def callback_wrapper(message):
            """
            Callback function to process messages from Pub/Sub
            """
            try:
                # Deserialize the message
                deserialized_message = UniversalMessage.deserialize(message.data)
                
                # Add Pub/Sub specific attributes to the message
                deserialized_message.context["pubsub_attributes"] = dict(message.attributes)
                deserialized_message.context["pubsub_message_id"] = message.message_id
                deserialized_message.context["pubsub_publish_time"] = str(message.publish_time)
                
                # Put the message in the queue for the async generator
                asyncio.run_coroutine_threadsafe(
                    message_queue.put(deserialized_message), 
                    asyncio.get_event_loop()
                )
                
                # Acknowledge the message
                message.ack()
                
            except Exception as e:
                logger.error(f"Error processing message from subscription {subscription_name}: {e}")
                # Log to dead letter queue if enabled
                if self.enable_dead_letter_queue:
                    asyncio.run_coroutine_threadsafe(
                        self._handle_message_error(message, str(e)), 
                        asyncio.get_event_loop()
                    )
                # Don't ack the message so it can be retried
                
        # Start the subscriber
        streaming_pull_future = self.subscriber.subscribe(
            subscription_path, callback=callback_wrapper
        )
        
        logger.info(f"Listening for messages on {subscription_path}...")

        try:
            # Continuously yield messages from the queue
            while True:
                try:
                    # Wait for a message with a timeout
                    message = await asyncio.wait_for(message_queue.get(), timeout=5.0)
                    yield message
                except asyncio.TimeoutError:
                    # Check if subscription is still active
                    continue
        except Exception as e:
            logger.error(f"Error in Pub/Sub consumer: {e}")
            streaming_pull_future.cancel()  # Cancel the streaming pull future
            raise
        finally:
            streaming_pull_future.cancel()

    async def _publish_metrics(self):
        """
        Publish custom metrics to Google Cloud Monitoring
        """
        try:
            # Create a time series for our custom metric
            series = monitoring_v3.TimeSeries()
            series.metric.type = f"custom.googleapis.com/agentmesh/messages_processed"
            series.resource.type = "global"
            series.resource.labels["project_id"] = self.project_id
            
            # Add the current value
            point = monitoring_v3.Point()
            point.interval = monitoring_v3.TimeInterval(
                {
                    "end_time": {"seconds": int(time.time())},
                }
            )
            point.value = monitoring_v3.TypedValue()
            point.value.int64_value = self.message_count
            
            series.points = [point]
            
            # Publish the metric
            project_name = self.monitoring_client.project_path(self.project_id)
            self.monitoring_client.create_time_series(project_name, [series])
            
        except Exception as e:
            logger.error(f"Error publishing metrics: {e}")

    async def _send_to_dead_letter_queue(self, message: UniversalMessage, original_target: str, error: str):
        """
        Send a failed message to a dead letter queue
        """
        try:
            # Create a dead letter topic name
            dlq_topic = f"{original_target}-dlq"
            
            # Add error context to the message
            error_context = {
                "original_target": original_target,
                "error": error,
                "failed_at": datetime.now(UTC).isoformat(),
                "retry_count": message.context.get("retry_count", 0) + 1
            }
            
            message.context["error_context"] = error_context
            
            # Send to dead letter queue
            await self.send(message, dlq_topic)
            
            logger.warning(f"Message sent to dead letter queue: {dlq_topic}")
            
        except Exception as e:
            logger.error(f"Error sending message to dead letter queue: {e}")

    async def _handle_message_error(self, pubsub_message, error: str):
        """
        Handle an error during message processing
        """
        try:
            if self.enable_dead_letter_queue:
                # Send the original message to the dead letter queue
                original_message = UniversalMessage.deserialize(pubsub_message.data)
                await self._send_to_dead_letter_queue(
                    original_message, 
                    pubsub_message.attributes.get('destination', 'unknown'),
                    error
                )
            
            # Log the error for monitoring
            logger.error(f"Message processing error: {error}, message_id: {pubsub_message.message_id}")
        except Exception as e:
            logger.error(f"Error in _handle_message_error: {e}")