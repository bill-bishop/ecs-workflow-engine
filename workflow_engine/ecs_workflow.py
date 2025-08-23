"""
Entity‑Component System (ECS) implementation for modular workflow processing.

This module defines a minimal framework for queuing arbitrary tasks, routing
them through predefined workflows and invoking external handlers based on
task classifications. Each task is treated as an entity with a set of
components (classification, assignment, current workflow phase, etc.). Systems
registered with the engine operate over these tasks asynchronously.

Key concepts:

* **Task** – An entity containing arbitrary input data plus a component map
  holding its current classification, assignee and workflow state. Tasks can
  be pre‑classified or left unclassified. A UUID is generated automatically.

* **External handlers** – Functions or callables registered to the engine
  keyed by classification. When a task reaches a workflow phase whose
  classification matches a registered handler, the handler is invoked to
  produce an output. Handlers may be synchronous or asynchronous and may
  spawn subprocesses or call external services. Register your own handlers
  by calling ``register_handler``.

* **Workflow definitions** – A workflow is a list of phases, where each
  phase defines a target classification and optionally a list of next
  classifications. The engine uses this list to advance a task through
  its lifecycle. Workflows can be registered via ``register_workflow``.

* **Engine** – Manages the task queue and orchestrates processing. Tasks
  submitted via ``submit_task`` enter an asynchronous event loop. The
  engine inspects the current phase of the task, invokes the appropriate
  handler and determines the next phase based on the handler’s outcome.

This framework is intentionally lightweight and extensible. You can
incorporate sophisticated classification logic, persistence layers or
additional systems without changing the core design. See the example at
the bottom of the file for usage.
"""

from __future__ import annotations

import asyncio
import json
import os
import uuid
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, List, Optional

# Type alias for handler functions
HandlerType = Callable[["Task"], Awaitable[None]]


@dataclass
class Phase:
    """Represents a single phase in a workflow.

    Each phase has a target classification and a list of possible next
    classifications. The engine will assign the task's classification to
    ``target_classification`` upon entering the phase and subsequently
    transition it into one of the ``next_classifications`` after the handler
    finishes.
    """
    target_classification: str
    next_classifications: List[str] = field(default_factory=list)


@dataclass
class Task:
    """A unit of work flowing through the ECS system.

    Attributes:
        data: Arbitrary payload associated with the task.
        classification: Current classification of the task. This is used to
            route the task to the appropriate external handler.
        assigned_to: Identifies whether the task should be processed by an
            agentic assistant (``"ai"``) or a human (``"human"``). This flag
            does not impact processing logic directly but can be used by
            handlers to delegate work.
        workflow_name: Name of the workflow this task follows.
        phase_index: Current index within the workflow’s phases.
        output: Stores the handler’s output once processing is complete.
        id: Unique identifier for the task. Generated automatically.
    """
    data: Any
    classification: Optional[str] = None
    assigned_to: str = "ai"
    workflow_name: Optional[str] = None
    phase_index: int = 0
    output: Any = None
    id: str = field(default_factory=lambda: str(uuid.uuid4()))


class ECSWorkflowEngine:
    """The core engine managing tasks, workflows and handlers."""

    def __init__(self, *, storage_path: Optional[str] = None) -> None:
        """Initialize the engine.

        Args:
            storage_path: Optional path to a JSON file used to persist
                tasks across restarts. If provided, the engine will
                automatically load any pending tasks from this file on
                startup and save the state of queued tasks whenever the
                queue changes. Defaults to ``"tasks.json"`` in the
                current working directory.
        """
        # Queue used for tasks awaiting processing
        self._queue: asyncio.Queue[Task] = asyncio.Queue()
        # Registry mapping classifications to handler callables
        self._handlers: Dict[str, HandlerType] = {}
        # Registry mapping workflow names to lists of phases
        self._workflows: Dict[str, List[Phase]] = {}
        # Event loop running flag
        self._running: bool = False
        # Storage path for persistence
        self._storage_path: str = storage_path or "tasks.json"
        # List of pending tasks (those enqueued but not yet finished)
        self._pending_tasks: List[Task] = []
        # Load any persisted tasks
        self._load_pending_tasks()

    # ------------------------------------------------------------------
    # Persistence helpers
    def _load_pending_tasks(self) -> None:
        """Load pending tasks from the storage file and enqueue them.

        If the storage file does not exist or is invalid, no tasks are
        loaded. Loaded tasks are appended to ``_pending_tasks`` and
        enqueued into the internal asyncio queue.  Errors while
        deserialising tasks are silently ignored.
        """
        if not self._storage_path or not os.path.exists(self._storage_path):
            return
        try:
            with open(self._storage_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                for item in data:
                    try:
                        # Reconstruct Task object. Cast data/output back to
                        # their original form if possible; fall back to raw value.
                        task = Task(
                            data=item.get("data"),
                            classification=item.get("classification"),
                            assigned_to=item.get("assigned_to", "ai"),
                            workflow_name=item.get("workflow_name"),
                            phase_index=item.get("phase_index", 0),
                            output=item.get("output"),
                            id=item.get("id") or str(uuid.uuid4()),
                        )
                    except Exception:
                        continue
                    # Append to pending tasks and queue
                    self._pending_tasks.append(task)
                    try:
                        # Use put_nowait because __init__ is synchronous
                        self._queue.put_nowait(task)
                    except Exception:
                        # If queue is full or something unexpected happens, skip
                        pass
        except Exception:
            # Ignore errors reading storage file
            pass

    def _save_pending_tasks(self) -> None:
        """Persist the current pending tasks to the storage file.

        The tasks are serialised to a list of dictionaries containing
        serialisable representations of each Task's attributes.  The
        ``data`` and ``output`` fields are stored using ``repr()`` to
        provide a simple string representation if they are not
        JSON-serialisable. If no storage path is configured, this is a
        no-op.
        """
        if not self._storage_path:
            return
        serialised = []
        for task in self._pending_tasks:
            try:
                # Attempt to serialise data and output. If they raise a
                # TypeError, fall back to repr().
                data_val = task.data
                output_val = task.output
                # Basic serialisation fallback
                try:
                    json.dumps(data_val)
                except Exception:
                    data_val = repr(data_val)
                try:
                    json.dumps(output_val)
                except Exception:
                    output_val = repr(output_val)
                serialised.append(
                    {
                        "data": data_val,
                        "classification": task.classification,
                        "assigned_to": task.assigned_to,
                        "workflow_name": task.workflow_name,
                        "phase_index": task.phase_index,
                        "output": output_val,
                        "id": task.id,
                    }
                )
            except Exception:
                continue
        try:
            with open(self._storage_path, "w", encoding="utf-8") as f:
                json.dump(serialised, f, indent=2)
        except Exception:
            # Ignore write errors
            pass

    def register_handler(self, classification: str, handler: HandlerType) -> None:
        """Register a handler for a specific classification.

        Args:
            classification: The classification string to handle.
            handler: An async callable that accepts a ``Task`` and writes
                results to ``task.output`` and potentially mutates other
                components.
        """
        if not asyncio.iscoroutinefunction(handler):
            raise TypeError("Handlers must be async functions")
        self._handlers[classification] = handler

    def register_workflow(self, name: str, phases: List[Phase]) -> None:
        """Register a named workflow comprised of ordered phases."""
        if not phases:
            raise ValueError("Workflow must contain at least one phase")
        self._workflows[name] = phases

    async def submit_task(
        self,
        data: Any,
        workflow_name: str,
        classification: Optional[str] = None,
        assigned_to: str = "ai",
    ) -> Task:
        """Create a new task and enqueue it for processing.

        Args:
            data: Payload associated with the task.
            workflow_name: Name of the workflow this task should follow.
            classification: Optional starting classification. If omitted, the
                first phase’s target classification will be used.
            assigned_to: Indicates AI or human processing. Defaults to "ai".

        Returns:
            The enqueued ``Task`` instance.
        """
        if workflow_name not in self._workflows:
            raise ValueError(f"Unknown workflow: {workflow_name}")
        task = Task(
            data=data,
            classification=classification,
            assigned_to=assigned_to,
            workflow_name=workflow_name,
        )
        await self._queue.put(task)
        # Add to pending tasks and persist
        self._pending_tasks.append(task)
        self._save_pending_tasks()
        return task

    async def _process_task(self, task: Task) -> None:
        """Process a single task through its workflow.

        This method iterates through the task's workflow phases. For each
        phase, it assigns the task's classification, invokes the handler and
        decides the next phase based on ``next_classifications``. Once all
        phases have been completed, the task is considered finished.
        """
        workflow = self._workflows[task.workflow_name]
        # If task has no classification set, use the first phase's target
        if task.classification is None:
            task.classification = workflow[0].target_classification

        while task.phase_index < len(workflow):
            phase = workflow[task.phase_index]
            # Update classification to match current phase
            task.classification = phase.target_classification

            handler = self._handlers.get(task.classification)
            if handler is None:
                raise RuntimeError(
                    f"No handler registered for classification '{task.classification}'"
                )
            # Invoke handler
            await handler(task)

            # Determine next classification and advance phase
            if phase.next_classifications:
                # Simple policy: use the first next classification. Real
                # implementations could implement decision logic based on
                # handler results here.
                next_class = phase.next_classifications[0]
                task.classification = next_class
            task.phase_index += 1

            # Persist updated task state after each phase
            # Ensure task is still in pending list
            if task in self._pending_tasks:
                self._save_pending_tasks()

        # Task completed. At this point, task.output may be set by handler.
        # Remove from pending tasks and persist
        if task in self._pending_tasks:
            try:
                self._pending_tasks.remove(task)
            except ValueError:
                pass
        self._save_pending_tasks()

    async def run(self) -> None:
        """Continuously process tasks from the queue until stopped."""
        self._running = True
        while self._running:
            task: Task = await self._queue.get()
            try:
                await self._process_task(task)
            except Exception as e:
                # In production, you'd log this error or handle it gracefully
                print(f"Error processing task {task.id}: {e}")

    def stop(self) -> None:
        """Signal the engine to stop processing tasks."""
        self._running = False


# Example usage function
async def _example_usage() -> None:
    engine = ECSWorkflowEngine()

    # Define a simple handler that echoes the task data and appends the phase
    async def echo_handler(task: Task) -> None:
        # Simulate async processing delay
        await asyncio.sleep(0.1)
        task.output = f"handled {task.classification}: {task.data}"

    # Register handlers for two classifications
    engine.register_handler("phase1", echo_handler)
    engine.register_handler("phase2", echo_handler)

    # Register a two‑phase workflow
    engine.register_workflow(
        "simple_workflow",
        [
            Phase(target_classification="phase1", next_classifications=["phase2"]),
            Phase(target_classification="phase2"),
        ],
    )

    # Submit a task
    await engine.submit_task(data="hello world", workflow_name="simple_workflow")

    # Run the engine for a short period to process the task
    async def shutdown_after(delay: float) -> None:
        await asyncio.sleep(delay)
        engine.stop()

    await asyncio.gather(engine.run(), shutdown_after(1))


if __name__ == "__main__":
    asyncio.run(_example_usage())
