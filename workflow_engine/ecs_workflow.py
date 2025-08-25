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
from enum import Enum
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Optional

# Type alias for handler functions
HandlerType = Callable[["Task"], Awaitable[None]]


class TaskType(str, Enum):
    """Enumeration of core task types.

    A task's type determines which adapter is used to execute it.  The
    default mapping follows the ``assigned_to`` field on a task, but
    individual phases may override this via the ``assigned_to``
    attribute.
    """
    HUMAN = "human"
    AGENTIC = "agentic"
    FUNCTIONAL = "functional"


class TaskStatus(str, Enum):
    """Lifecycle status for tasks.

    These values indicate the current state of a task as it flows
    through the engine.  They are persisted to disk and can be used to
    resume processing after a restart.
    """
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    WAITING_ON_HUMAN = "waiting_on_human"
    WAITING_ON_AGENT = "waiting_on_agent"
    WAITING_ON_IO = "waiting_on_io"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class Artifact:
    """Represents a file or blob produced during task execution.

    Artifacts are stored on disk under ``artifacts_dir/<task_id>``.
    Only metadata is kept in memory and persisted in the task record.
    """
    id: str
    name: str
    kind: str  # e.g. "text", "binary", "json"
    mime: str
    rel_path: str
    size: int


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
    # Optional assignment override: "human", "agentic" or "functional".
    # When provided, this value determines which adapter to use for this phase.
    assigned_to: Optional[str] = None


@dataclass
class Task:
    """A unit of work flowing through the ECS system.

    Attributes:
        data: Arbitrary payload associated with the task.
        classification: Current classification of the task. This is used to
            route the task to the appropriate external handler.
        assigned_to: Identifies whether the task should be processed by an
            agentic assistant (``"ai"``) or a human (``"human"``). This flag
            does not directly impact processing logic but can be used by
            handlers to delegate work and to infer the ``task_type``.
        workflow_name: Name of the workflow this task follows.
        phase_index: Current index within the workflow’s phases.
        output: Stores the handler’s output once processing is complete.
        status: Tracks the lifecycle state of the task (e.g. pending,
            in-progress, waiting for human or agentic input, completed).
        context: Free-form key/value store to carry state between phases.
        artifacts: List of artifacts produced by the task’s handlers.
        id: Unique identifier for the task. Generated automatically.
    """
    data: Any
    classification: Optional[str] = None
    assigned_to: str = "ai"
    workflow_name: Optional[str] = None
    phase_index: int = 0
    output: Any = None
    status: TaskStatus = TaskStatus.PENDING
    context: Dict[str, Any] = field(default_factory=dict)
    artifacts: List[Artifact] = field(default_factory=list)
    id: str = field(default_factory=lambda: str(uuid.uuid4()))


class ECSWorkflowEngine:
    """The core engine managing tasks, workflows and handlers."""

    def __init__(self, *, storage_path: Optional[str] = None, artifacts_dir: Optional[str] = None) -> None:
        """Initialize the engine.

        Args:
            storage_path: Optional path to a JSON file used to persist
                tasks across restarts. If provided, the engine will
                automatically load any pending tasks from this file on
                startup and save the state of queued tasks whenever the
                queue changes. Defaults to ``"tasks.json"`` in the
                current working directory.
            artifacts_dir: Directory where task artifacts will be stored.
                Defaults to ``"artifacts"`` relative to the current
                working directory.
        """
        # Queue used for tasks awaiting processing
        self._queue: asyncio.Queue[Task] = asyncio.Queue()
        # Registry mapping classifications to handler callables
        self._handlers: Dict[str, HandlerType] = {}
        # Registry mapping workflow names to lists of phases
        self._workflows: Dict[str, List[Phase]] = {}
        # Registry mapping task types to adapters
        self._adapters: Dict[TaskType, Any] = {}
        # Event loop running flag
        self._running: bool = False
        # Storage path for persistence
        self._storage_path: str = storage_path or "tasks.json"
        # Directory for storing artifacts
        self._artifacts_dir: Path = Path(artifacts_dir or "artifacts")
        self._artifacts_dir.mkdir(parents=True, exist_ok=True)
        # List of pending tasks (those enqueued but not yet finished)
        self._pending_tasks: List[Task] = []
              # Hooks invoked when a task reaches the COMPLETED status
        self._task_completed_hooks: List[Callable[[Task], Awaitable[None]]] = []

        # Load any persisted tasks
        self._load_pending_tasks()

        # Register default adapters
        # If users do not register their own adapters, these provide
        # baseline behaviour for the three built-in task types.
        self.register_adapter(TaskType.HUMAN, HumanAdapter())
        self.register_adapter(TaskType.AGENTIC, AgentAdapter())
        self.register_adapter(TaskType.FUNCTIONAL, FunctionalAdapter())

        # Register a built-in handler for "define_workflow" tasks.  This
        # handler expects ``task.output`` (or ``task.data`` if no
        # ``output`` is present) to contain a WorkflowSpec dict with the
        # keys ``name`` and ``phases``.  Each phase entry should include
        # ``classification`` and optionally ``next`` (a list of
        # classifications) and ``assigned_to``.  When invoked, the
        # handler registers the workflow and rebinds the task to that
        # workflow.  Subsequent processing will follow the new phases.
        async def _define_workflow_handler(task: Task) -> None:
            # Determine the specification source
            spec = None
            if task.output:
                spec = task.output
            elif isinstance(task.data, dict) and "spec" in task.data:
                spec = task.data["spec"]
            if not spec:
                # Nothing to do
                return
            name = spec.get("name")
            phase_defs = spec.get("phases", [])
            if not name or not phase_defs:
                return
            phases: List[Phase] = []
            for pd in phase_defs:
                try:
                    phases.append(
                        Phase(
                            target_classification=pd.get("classification"),
                            next_classifications=pd.get("next", []),
                            assigned_to=pd.get("assigned_to"),
                        )
                    )
                except Exception:
                    continue
            if not phases:
                return
            # Register the workflow
            self.register_workflow(name, phases)
            # Rebind the current task to the new workflow
            task.workflow_name = name
            task.phase_index = 0
            task.classification = None
            # Persist changes
            self._save_pending_tasks()

        # Register the define_workflow handler
        self.register_handler("define_workflow", _define_workflow_handler)

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
                        # Restore status
                        status_name = item.get("status")
                        if status_name:
                            try:
                                task.status = TaskStatus(status_name)
                            except Exception:
                                task.status = TaskStatus.PENDING
                        # Restore context
                        ctx = item.get("context")
                        if isinstance(ctx, dict):
                            task.context = ctx
                        # Restore artifacts
                        arts = item.get("artifacts")
                        if isinstance(arts, list):
                            for a in arts:
                                try:
                                    art = Artifact(
                                        id=a.get("id"),
                                        name=a.get("name"),
                                        kind=a.get("kind"),
                                        mime=a.get("mime"),
                                        rel_path=a.get("rel_path"),
                                        size=a.get("size", 0),
                                    )
                                    task.artifacts.append(art)
                                except Exception:
                                    continue
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
                        # Persist status name
                        "status": task.status.value if isinstance(task.status, TaskStatus) else task.status,
                        # Persist context (shallow copy)
                        "context": task.context,
                        # Persist artifacts metadata
                        "artifacts": [
                            {
                                "id": art.id,
                                "name": art.name,
                                "kind": art.kind,
                                "mime": art.mime,
                                "rel_path": art.rel_path,
                                "size": art.size,
                            }
                            for art in task.artifacts
                        ],
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
        # New tasks start in the pending state
        task.status = TaskStatus.PENDING
        await self._queue.put(task)
        # Add to pending tasks and persist
        self._pending_tasks.append(task)
        self._save_pending_tasks()
        return task

    async def _process_task(self, task: Task) -> None:
        """Process a single task through its workflow.

        This method iterates through the task's workflow phases. For each
        phase it determines the task type, dispatches to the appropriate
        adapter (or directly to the handler) and updates the task's
        classification and phase index according to the workflow.
        If a phase suspends the task (e.g. waiting for human input), the
        method returns early and the task remains in ``_pending_tasks``.
        Once all phases are complete, the task is marked ``COMPLETED`` and
        removed from the pending list.
        """
        workflow = self._workflows[task.workflow_name]
        # If task has no classification set, use the first phase's target
        if task.classification is None:
            task.classification = workflow[0].target_classification

        # Mark task as in progress
        task.status = TaskStatus.IN_PROGRESS

        while task.phase_index < len(workflow):
            phase = workflow[task.phase_index]
            # Override assigned_to from phase if provided
            assigned_to = phase.target_classification  # not used; we need to use phase.assigned_to if available
            # Determine classification for this phase
            task.classification = phase.target_classification
            # Look up handler
            handler = self._handlers.get(task.classification)
            if handler is None:
                raise RuntimeError(
                    f"No handler registered for classification '{task.classification}'"
                )
            # Determine the task type for adapter dispatch
            # Prefer phase.assigned_to (if set), else task.assigned_to
            target_assigned = getattr(phase, "assigned_to", None) or task.assigned_to
            try:
                task_type = TaskType(target_assigned)
            except Exception:
                # Default to AGENTIC if unknown
                task_type = TaskType.AGENTIC
            adapter = self._adapters.get(task_type)
            if adapter is not None:
                # Dispatch to adapter. Adapter may suspend the task.
                await adapter.handle(task, handler, self)
            else:
                # Default: call the handler directly
                await handler(task)

            # If the task was suspended (status set to a waiting state), stop processing
            if task.status in {
                TaskStatus.WAITING_ON_HUMAN,
                TaskStatus.WAITING_ON_AGENT,
                TaskStatus.WAITING_ON_IO,
            }:
                self._save_pending_tasks()
                return

            # Determine next classification and advance phase
            if phase.next_classifications:
                # Simple policy: use the first next classification unless
                # a transition function is provided.
                next_class = phase.next_classifications[0]
                task.classification = next_class
            task.phase_index += 1

            # Persist updated task state after each phase
            if task in self._pending_tasks:
                self._save_pending_tasks()

        # Task completed. Remove from pending list
        task.status = TaskStatus.COMPLETED
        if task in self._pending_tasks:
            try:
                self._pending_tasks.remove(task)
            except ValueError:
                pass
        # Persist final state
        self._save_pending_tasks()
              # Invoke any registered completion hooks
        if self._task_completed_hooks:
            for hook in list(self._task_completed_hooks):
                try:
                    await hook(task)
                except Exception:
                    pass


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

    # ------------------------------------------------------------------
    def register_task_completed_hook(self, hook: Callable[[Task], Awaitable[None]]) -> None:
        """Register a coroutine to be invoked when a task reaches the COMPLETED status.

        Hooks will be awaited in the order they are registered. Exceptions
        raised by hooks are caught and ignored to avoid interfering with
        task processing.

        Args:
            hook: An async callable that accepts a completed Task.
        """
        if not asyncio.iscoroutinefunction(hook):
            raise TypeError("Task completed hooks must be async functions")
        self._task_completed_hooks.append(hook)


    # Adapter registration and artifact helpers

    def register_adapter(self, task_type: TaskType, adapter: Any) -> None:
        """Register an adapter to handle tasks of a given type.

        The adapter must implement an asynchronous ``handle(task, handler)``
        method.  When a task is processed, the engine will look up the
        appropriate adapter based on the task's type (derived from
        ``assigned_to`` or ``phase.assigned_to``) and delegate
        execution to it.
        """
        self._adapters[task_type] = adapter

    def _ensure_task_dir(self, task_id: str) -> Path:
        """Ensure the artifact directory for a task exists and return it."""
        task_dir = self._artifacts_dir / task_id
        task_dir.mkdir(parents=True, exist_ok=True)
        return task_dir

    def add_artifact_text(self, task: Task, name: str, text: str, mime: str = "text/plain") -> Artifact:
        """Persist a text artifact and register it on the task.

        Args:
            task: The task producing the artifact.
            name: Logical name for the artifact.
            text: The textual contents to write.
            mime: Optional MIME type. Defaults to ``text/plain``.

        Returns:
            The created ``Artifact`` metadata.
        """
        task_dir = self._ensure_task_dir(task.id)
        artifact_id = str(uuid.uuid4())
        filename = f"{artifact_id}.txt"
        file_path = task_dir / filename
        file_path.write_text(text, encoding="utf-8")
        artifact = Artifact(
            id=artifact_id,
            name=name,
            kind="text",
            mime=mime,
            rel_path=str(file_path.relative_to(self._artifacts_dir)),
            size=file_path.stat().st_size,
        )
        task.artifacts.append(artifact)
        self._save_pending_tasks()
        return artifact

    def add_artifact_bytes(self, task: Task, name: str, data: bytes, mime: str = "application/octet-stream") -> Artifact:
        """Persist a binary artifact and register it on the task."""
        task_dir = self._ensure_task_dir(task.id)
        artifact_id = str(uuid.uuid4())
        filename = f"{artifact_id}.bin"
        file_path = task_dir / filename
        file_path.write_bytes(data)
        artifact = Artifact(
            id=artifact_id,
            name=name,
            kind="binary",
            mime=mime,
            rel_path=str(file_path.relative_to(self._artifacts_dir)),
            size=file_path.stat().st_size,
        )
        task.artifacts.append(artifact)
        self._save_pending_tasks()
        return artifact

    def list_artifacts(self, task: Task) -> List[Artifact]:
        """Return a list of artifacts associated with a task."""
        return list(task.artifacts)

    def get_artifact_path(self, artifact: Artifact) -> Path:
        """Get the absolute filesystem path to an artifact."""
        return self._artifacts_dir / artifact.rel_path

    def find_task_by_id(self, task_id: str) -> Optional[Task]:
        """Return the pending task with the given id, or ``None`` if not found."""
        for t in self._pending_tasks:
            if t.id == task_id:
                return t
        return None

    def suspend(self, task: Task, status: TaskStatus, reason: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None) -> None:
        """Suspend a task awaiting external input.

        The task is not re-queued for processing until ``resume`` is
        called.  The task's status and optional reason/metadata are
        stored in its context for later reference.
        """
        task.status = status
        if reason:
            task.context.setdefault("suspend_reason", reason)
        if metadata:
            task.context.setdefault("suspend_metadata", metadata)
        # Persist update
        self._save_pending_tasks()

    async def resume(self, task_id: str, *, output: Any = None, classification: Optional[str] = None) -> None:
        """Resume a suspended task.

        Args:
            task_id: The identifier of the task to resume.
            output: Optional output to attach to the task before resuming.
            classification: Optional classification to set as the new
                phase. If omitted, the task will continue at its current
                phase using the existing classification or the next
                classification specified by the workflow.
        """
        task = self.find_task_by_id(task_id)
        if task is None:
            raise KeyError(f"Task {task_id} not found")
        # Attach output if provided
        if output is not None:
            task.output = output
        # Update classification if provided
        if classification is not None:
            task.classification = classification
        # Clear suspension metadata
        task.context.pop("suspend_reason", None)
        task.context.pop("suspend_metadata", None)
        # Mark as pending/in-progress
        task.status = TaskStatus.IN_PROGRESS
        await self._queue.put(task)
        self._save_pending_tasks()


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


# ----------------------------------------------------------------------
# Default adapter implementations

class BaseAdapter:
    """Base adapter class.

    Adapters encapsulate the execution environment for tasks of a
    particular type.  They receive the task, the handler registered for
    the task's classification and a reference to the engine itself.  An
    adapter may run the handler directly, or it may perform additional
    asynchronous work (e.g. call an LLM, spawn a subprocess, send a
    notification to a human) before or after invoking the handler.
    """

    async def handle(self, task: Task, handler: HandlerType, engine: ECSWorkflowEngine) -> None:
        raise NotImplementedError


class DefaultAdapter(BaseAdapter):
    """A simple adapter that directly invokes the handler.

    This adapter does not change task status or suspend processing.
    """

    async def handle(self, task: Task, handler: HandlerType, engine: ECSWorkflowEngine) -> None:
        await handler(task)


class HumanAdapter(BaseAdapter):
    """Adapter for tasks requiring human input.

    The default implementation simply suspends the task with a
    WAITING_ON_HUMAN status.  In a real system, this would send a
    notification to a human and expect a later call to ``engine.resume``.
    """

    async def handle(self, task: Task, handler: HandlerType, engine: ECSWorkflowEngine) -> None:
        # Optionally call handler to prepare a prompt or gather context
        await handler(task)
        # Suspend the task awaiting human response
        engine.suspend(task, TaskStatus.WAITING_ON_HUMAN, reason="Awaiting human input")


class AgentAdapter(BaseAdapter):
    """Adapter for tasks executed by an agentic assistant (e.g. LLM).

    The default implementation directly calls the handler.  More complex
    implementations could interface with an LLM or other agent.
    """

    async def handle(self, task: Task, handler: HandlerType, engine: ECSWorkflowEngine) -> None:
        await handler(task)
        # If the handler chooses to suspend, it should set task.status
        # accordingly via ``engine.suspend``.  Otherwise, status remains
        # IN_PROGRESS/COMPLETED.


class FunctionalAdapter(BaseAdapter):
    """Adapter for programmatic or functional tasks.

    By default this simply calls the handler.  In a real system this
    could spawn subprocesses, call external services or interact with
    other APIs.
    """

    async def handle(self, task: Task, handler: HandlerType, engine: ECSWorkflowEngine) -> None:
        await handler(task)
