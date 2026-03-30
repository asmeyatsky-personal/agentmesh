from loguru import logger
from typing import Any, Dict, List
import asyncio
import math
import random

try:
    import numpy as np
    _NUMPY_AVAILABLE = True
except ImportError:
    _NUMPY_AVAILABLE = False

from agentmesh.epe.stream_processor import StreamProcessor
from agentmesh.mal.message import UniversalMessage


class AnomalyDetector:
    def __init__(self, window_size: int = 30, std_dev_threshold: float = 3.0):
        self.window_size = window_size
        self.std_dev_threshold = std_dev_threshold
        self.data_window: List[float] = []
        logger.info(
            "AnomalyDetector initialized with window_size={} and std_dev_threshold={}",
            window_size,
            std_dev_threshold,
        )

    def detect(self, data_point: float) -> bool:
        """
        Detects anomalies in the given data point using a statistical approach.
        """
        is_anomaly = False
        if len(self.data_window) >= self.window_size:
            if _NUMPY_AVAILABLE:
                mean = np.mean(self.data_window)
                std_dev = np.std(self.data_window)
            else:
                n = len(self.data_window)
                mean = sum(self.data_window) / n
                std_dev = math.sqrt(sum((x - mean) ** 2 for x in self.data_window) / n)
            if std_dev == 0: # All data points are the same, use fallback
                effective_std = max(abs(mean) * 0.1, 1.0)
                is_anomaly = abs(data_point - mean) > self.std_dev_threshold * effective_std
            elif abs(data_point - mean) > self.std_dev_threshold * std_dev:
                is_anomaly = True
                logger.warning(
                    "Anomaly detected! Data point {} is outside the threshold.", data_point
                )

        self.data_window.append(data_point)
        if len(self.data_window) > self.window_size:
            self.data_window.pop(0)

        return is_anomaly

    def train_model(self, training_data: List[float]):
        """
        Trains the anomaly detection model by pre-populating the window.
        """
        self.data_window.extend(training_data)
        if len(self.data_window) > self.window_size:
            self.data_window = self.data_window[-self.window_size :]
        logger.info("Anomaly detection model trained with {} data points.", len(training_data))


async def anomaly_detection_processor_func(message: UniversalMessage, detector: AnomalyDetector):
    """
    A processor function to integrate AnomalyDetector with StreamProcessor.
    Assumes the message payload contains a 'value' key with the data point.
    """
    data_point = message.payload.get("value")
    if data_point is not None:
        if detector.detect(data_point):
            logger.warning(f"EPE Anomaly Detected: Message ID: {message.metadata.get('id')}, Data: {data_point}")
        else:
            logger.info(f"EPE Normal: Message ID: {message.metadata.get('id')}, Data: {data_point}")
    else:
        logger.warning(f"EPE Anomaly Processor: No 'value' in payload for message {message.metadata.get('id')}")


async def main():
    # --- Setup Anomaly Detector ---
    detector = AnomalyDetector(window_size=5, std_dev_threshold=2.0)
    # Pre-train the detector with some initial data
    initial_data = [random.uniform(9.0, 11.0) for _ in range(detector.window_size)]
    detector.train_model(initial_data)
    logger.info(f"AnomalyDetector initialized and pre-trained with {len(initial_data)} points.")

    # --- Setup Kafka Adapter (example) ---
    # In a real scenario, bootstrap_servers would come from configuration
    from agentmesh.mal.adapters.kafka import KafkaAdapter
    kafka_adapter = KafkaAdapter(bootstrap_servers="localhost:9092")
    
    # --- Setup Stream Processor ---
    # We need to wrap the anomaly_detection_processor_func to pass the detector instance
    async def processor_with_detector(message: UniversalMessage):
        await anomaly_detection_processor_func(message, detector)

    stream_processor = StreamProcessor(
        adapter=kafka_adapter,
        subscription="sensor_data_stream", # Example topic
        processors=[processor_with_detector]
    )

    # --- Simulate sending messages to Kafka (for demonstration) ---
    async def simulate_kafka_producer():
        from agentmesh.mal.adapters.kafka import KafkaAdapter
        producer_adapter = KafkaAdapter(bootstrap_servers="localhost:9092")
        logger.info("Starting Kafka producer simulation...")
        for i in range(20):
            data_point = random.uniform(9.0, 11.0)
            if i == 5 or i == 15: # Inject anomalies
                data_point = random.uniform(20.0, 25.0)
                logger.warning(f"Simulating anomaly injection: {data_point:.2f}")

            message = UniversalMessage(
                payload={"value": data_point, "timestamp": datetime.now(UTC).isoformat()},
                metadata={"type": "sensor_reading", "source": "simulated_sensor"}
            )
            await producer_adapter.send(message, "sensor_data_stream")
            logger.info(f"Sent simulated message: {message.metadata.get('id')} with value {data_point:.2f}")
            await asyncio.sleep(0.5)
        logger.info("Kafka producer simulation finished.")

    # Run both producer and consumer concurrently
    await asyncio.gather(
        simulate_kafka_producer(),
        stream_processor.start()
    )


if __name__ == "__main__":
    from datetime import UTC, datetime # Import datetime here for main function
    asyncio.run(main())
