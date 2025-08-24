# Workflow Engine (Entity‑Component System)

This project contains a modular Entity‑Component System (ECS) implementation
for orchestrating complex workflows. It allows you to queue arbitrary tasks,
route them through predefined phases, and delegate work to registered
handlers. Tasks can be processed by a human, an agentic assistant, or a
functional subprocess, and artifacts produced in one phase can be mounted
into subsequent phases. Persistent state ensures tasks survive restarts.

## Features

- **Asynchronous event loop** – Uses Python’s `asyncio` to process tasks
  concurrently without blocking throughput.
- **Flexible classification & workflows** – Tasks can be pre‑classified or
  unclassified. Each workflow phase defines its target classification, an
  optional list of next classifications and an optional `assigned_to`
  override ("human", "agentic", or "functional"). The engine advances
  tasks through the phases automatically.
- **Task types & adapters** – Built‑in adapters for three task types
  (human, agentic and functional). Adapters encapsulate how to execute a
  handler: sending a request to a human, invoking an LLM/tool agent, or
  running a subprocess. You can register your own adapters.
- **Rich task lifecycle** – Tasks carry a `status` (pending, in
  progress, waiting on human/agent/IO, completed, failed), a context
  dictionary and a list of artifacts. Suspend and resume tasks waiting for
  external input via `engine.suspend`/`engine.resume`.
- **Persistent queue & artifacts** – Tasks and their artifacts are
  persisted to JSON and disk. Pending tasks survive process restarts and
  resume processing where they left off. Artifacts (text/binary files)
  produced in one phase can be mounted in later phases.
- **Meta “define_workflow”** – A built‑in classification that accepts a
  `WorkflowSpec` dict and dynamically registers a new workflow. The current
  task is rebound to the defined workflow and continues processing.
- **Handler registry** – Register async functions as handlers for
  classifications. Handlers may call external processes or services and
  interact with adapters.
- **Extensible design** – The core is intentionally lightweight. You can
  plug in your own classification logic, adapters, storage backends or
  additional lifecycle
### Kafka integration

While the engine ships with an in memory queue by default, real‑world
deployments often benefit from using an external message broker. This
repository includes a `KafkaMQInterface` in
`workflow_engine/kafka_interface.py` that leverages
[`kafka-python`](https://pypi.org/project/kafka-python/). It provides

1. **Publishing tasks** – After creating and submitting a `Task` via
   `engine.submit_task`, you can call `await kafka_interface.publish_task(task)`
   to serialize and send the task description to a Kafka topic. This
   allows producers outside the Python process (e.g. other services) to
   consume pending tasks.

2. **Publishing results** – The engine exposes a method
   `register_task_completed_hook` that accepts an async callback.
   Registering `kafka_interface.publish_result` as such a hook will
   automatically serialize and publish completed tasks (including their
   status, output and artifact metadata) to a Kafka results topic.

3. **Consuming tasks** – To integrate with an external producer, call
   `await kafka_interface.consume_tasks(engine)` in the background.
   It will continuously poll the Kafka tasks topic, deserialize
   incoming messages and submit them to the workflow engine via
   `engine.submit_task`. When finished, call `kafka_interface.stop_consuming()`
   to terminate the loop.

You can customise the broker address, topic names and consumer group via
constructor parameters. See the docstring in
`workflow_engine/kafka_interface.py` for usage examples and details.

 hooks without modifying the engine.

## Getting started

1. **Install dependencies**. This project requires Python 3.8+ and no
   external packages.
2. **Define handlers** for your classifications. Handlers must be
   `async` functions that accept a `Task` object and assign output to
   `task.output`. They may also call `engine.add_artifact_text/bytes` to
   persist files or call `engine.suspend` to wait for human/agentic
   input.
3. **Register workflows** as lists of `Phase` objects. Each phase
   specifies a target classification, optional list of next classifications
   and optional `assigned_to` override.
4. **Register adapters** for your task types if you need custom behaviour.
   The engine provides default adapters for human (suspend), agentic and
   functional tasks.
5. **Submit tasks** with `submit_task(data, workflow_name, classification=None,
   assigned_to="ai")`.
6. **Run the engine** using `await engine.run()`. Stop it by calling
   `engine.stop()`.

See `ecs_workflow.py` for a complete example in the `__main__` block.

## Example

```python
import asyncio
from workflow_engine.ecs_workflow import ECSWorkflowEngine, Phase, Task

async def echo_handler(task: Task) -> None:
    await asyncio.sleep(0.5)
    task.output = f"Processed {task.classification}: {task.data}"

async def main():
    engine = ECSWorkflowEngine()
    engine.register_handler("phase1", echo_handler)
    engine.register_handler("phase2", echo_handler)
    engine.register_workflow(
        "my_workflow",
        [
            Phase(target_classification="phase1", next_classifications=["phase2"]),
            Phase(target_classification="phase2"),
        ],
    )
    await engine.submit_task({"message": "Hello"}, workflow_name="my_workflow")
    async def stop_after(delay):
        await asyncio.sleep(delay)
        engine.stop()
    await asyncio.gather(engine.run(), stop_after(2))

asyncio.run(main())
```

## Deploying to GitHub

To deploy this project to your GitHub:

1. Create a new repository on GitHub via the web UI.
2. Clone the repository locally or initialise a new git repo in this
   directory with `git init`.
3. Commit the contents of `workflow_engine/`:
   ```bash
   git add workflow_engine
   git commit -m "Add ECS workflow engine implementation"
   git branch -M main
   git remote add origin YOUR_REMOTE_URL
   git push -u origin main
   ```
4. Add a license and update this README as needed.

You will need to authenticate with GitHub when pushing; use a Personal
Access Token (PAT) if two‑factor authentication is enabled on your
account.


## OpenAI agent integration

You can delegate agentic tasks to OpenAI's Chat API using the provided `OpenAIAgentAdapter`. The adapter reads your API key from the `OPENAI_API_KEY` environment variable and uses OpenAI's chat completion models to process tasks. To enable it, register the adapter for the agentic task type:

```python
from workflow_engine.openai_agent_adapter import OpenAIAgentAdapter
from workflow_engine.ecs_workflow import TaskType

engine.register_adapter(TaskType.AGENTIC, OpenAIAgentAdapter(model="gpt-4o"))
```


Make sure you set the `OPENAI_API_KEY` environment variable before running the engine.

## Web UI

A simple Flask-based dashboard is provided in `workflow_engine/ui_server.py` for interacting with the workflow engine and Kafka message queue. The UI lets you:

- View pending and in-progress tasks and their status.
- Submit new tasks to a workflow.
- Provide outputs for tasks waiting on human input (resume tasks).

To start the web server:

```bash
export OPENAI_API_KEY=sk-...
export KAFKA_BROKERS=localhost:9092  # optional; omit if not using Kafka
python -m workflow_engine.ui_server
```

The server will run on `http://127.0.0.1:5000` by default. Navigate to that URL in your browser to access the dashboard.

If `KAFKA_BROKERS` is set, the UI will publish new tasks to Kafka and consume completed-task messages automatically. Otherwise, tasks remain in memory.
