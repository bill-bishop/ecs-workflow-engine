"""Kafka message queue interface for the ECS workflow engine.

This module provides a thin wrapper around a Kafka producer and consumer
to allow external systems to submit tasks into the workflow engine and
receive notifications when tasks complete.  It uses the kafka-python
library.  You must install the `kafka-python` package (e.g.
`pip install kafka-python`) for this module to work.

Example usage::

    from workflow_engine.ecs_workflow import ECSWorkflowEngine
    from workflow_engine.kafka_interface import KafkaMQInterface

    engine = ECSWorkflowEngine()
    kafka_interface = KafkaMQInterface(brokers='localhost:9092')

    # Publish tasks when submitting them to the engine
    task = await engine.submit_task(data={'hello': 'world'}, workflow_name='my_workflow')
    await kafka_interface.publish_task(task)

    # Register hook to publish results
    engine.register_task_completed_hook(kafka_interface.publish_result)

    # Start consuming tasks from Kafka (runs in background)
    asyncio.create_task(kafka_interface.consume_tasks(engine))

This example will send tasks to the ``ecs_tasks`` topic and send completion
messages to the ``ecs_tasks_completed`` topic.  You can customise topic names
via the constructor.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

try:
    from kafka import KafkaProducer, KafkaConsumer  # type: ignore
except ImportError:
    # kafka-python is not installed; define dummy names so type checkers do not fail.
    KafkaProducer = None  # type: ignore
    KafkaConsumer = None  # type: ignore

from .ecs_workflow import Task, ECSWorkflowEngine


class KafkaMQInterface:
    """Simple Kafka-based message queue for tasks and results.

    This class creates a producer and consumer for sending and receiving JSON
    messages to and from Kafka topics.  Tasks are serialised to JSON before
    publishing.  When consuming tasks, the JSON payload is deserialised and
    submitted to the workflow engine.
    """

    def __init__(
        self,
        *,
        brokers: str,
        tasks_topic: str = "ecs_tasks",
        results_topic: str = "ecs_tasks_completed",
        group_id: str = "ecs_workflow_consumer",
        auto_offset_reset: str = "earliest",
    ) -> None:
        """Initialise the Kafka interface.

        Args:
            brokers: Comma-separated list of Kafka bootstrap servers.
            tasks_topic: Topic to which incoming tasks are published.
            results_topic: Topic to which completed task results are published.
            group_id: Consumer group id for the task consumer.
            auto_offset_reset: Behaviour when no initial offset is present.
        """
        if KafkaProducer is None or KafkaConsumer is None:
            raise RuntimeError(
                "kafka-python must be installed to use KafkaMQInterface"
            )
        self.tasks_topic = tasks_topic
        self.results_topic = results_topic
        # Create producer and consumer
        self._producer = KafkaProducer(
            bootstrap_servers=brokers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        self._consumer = KafkaConsumer(
            self.tasks_topic,
            bootstrap_servers=brokers,
            group_id=group_id,
            auto_offset_reset=auto_offset_reset,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )
        # Control flag for consumption loop
        self._consume_running = False

    async def publish_task(self, task: Task) -> None:
        """Publish a task to the tasks topic.

        The payload contains the task id, workflow name, assigned_to,
        classification and data.  Additional fields may be added as
        required by consumers.
        """
        payload = {
            "id": task.id,
            "workflow_name": task.workflow_name,
            "assigned_to": task.assigned_to,
            "classification": task.classification,
            "data": task.data,
        }
        loop = asyncio.get_event_loop()
        # Use executor to avoid blocking event loop
        await loop.run_in_executor(
            None,
            lambda: self._producer.send(self.tasks_topic, value=payload),
        )

    async def publish_result(self, task: Task) -> None:
        """Publish a completed task result to the results topic.

        Includes task id, final status, output, context and artifacts
        metadata.  Artifacts are represented by their metadata only (not the
        file contents).  Consumers can retrieve the files via a separate
        mechanism if needed.
        """
        payload: dict[str, Any] = {
            "id": task.id,
            "status": task.status.value if hasattr(task.status, "value") else str(task.status),
            "output": task.output,
            "context": task.context,
            "artifacts": [art.__dict__ for art in getattr(task, "artifacts", [])],
        }
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            lambda: self._producer.send(self.results_topic, value=payload),
        )

    async def consume_tasks(self, engine: ECSWorkflowEngine) -> None:
        """Continuously consume tasks from Kafka and submit them to the engine.

        This method runs a loop until ``stop_consuming`` is called.  Incoming
        messages must contain at least ``workflow_name`` and ``data`` fields.
        Optional ``classification`` and ``assigned_to`` fields are also
        honoured.

        Args:
            engine: The workflow engine to which consumed tasks are submitted.
        """
        self._consume_running = True

        def _consume_loop() -> None:
            for msg in self._consumer:
                if not self._consume_running:
                    break
                value = msg.value
                try:
                    workflow_name = value.get("workflow_name")
                    data = value.get("data")
                    classification = value.get("classification")
                    assigned_to = value.get("assigned_to", "ai")
                    if workflow_name:
                        # Submit task asynchronously using the engine's loop
                        asyncio.run_coroutine_threadsafe(
                            engine.submit_task(
                                data=data,
                                workflow_name=workflow_name,
                                classification=classification,
                                assigned_to=assigned_to,
                            ),
                            asyncio.get_event_loop(),
                        )
                except Exception:
                    continue

        loop = asyncio.get_event_loop()
        # Run consumer loop in executor to avoid blocking
        await loop.run_in_executor(None, _consume_loop)

    def stop_consuming(self) -> None:
        """Stop consuming tasks from Kafka.

        This sets the internal flag used by the consumer loop.
        """
        self._consume_running = False
