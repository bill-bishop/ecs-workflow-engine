# Workflow Engine (Entity‑Component System)

This project contains a simple Entity‑Component System (ECS) implementation
tailored for building modular workflows. It allows you to queue arbitrary
tasks, route them through predefined phases, and delegate work to
registered handlers. Tasks can be processed by either an agentic assistant
or a human, and external subprocesses can be invoked to handle specific
classifications.

## Features

- **Asynchronous event loop** – Uses Python’s `asyncio` to process tasks
  concurrently without blocking throughput.
- **Flexible classification** – Tasks can be pre‑classified or left
  unclassified. Each workflow phase defines its target classification and
  possible next states.
- **Handler registry** – Register async functions as handlers for
  classifications. Handlers may call external processes or services.
- **Workflow definitions** – Register workflows as ordered lists of phases.
  The engine advances tasks through the phases automatically.
- **Assignment concept** – Tasks include an `assigned_to` attribute
  (`"ai"` or `"human"`) so handlers can decide whether to run automated
  logic or request human input.
- **Extensible** – Designed to be minimal; additional systems (e.g.,
  classification logic, persistence layers) can be layered on top.

## Getting started

1. **Install dependencies**. This project requires Python 3.8+ and no
   external packages.
2. **Define handlers** for your classifications. Handlers must be
   `async` functions that accept a `Task` object and assign output to
   `task.output`.
3. **Register workflows** as lists of `Phase` objects. Each phase
   specifies a target classification and an optional list of next
   classifications.
4. **Submit tasks** with `submit_task(data, workflow_name, classification=None,
   assigned_to="ai")`.
5. **Run the engine** using `await engine.run()`. Stop it by calling
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
