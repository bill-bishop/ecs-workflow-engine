"""Simple web UI for interacting with the workflow engine.

This module starts a Flask web server that allows users to:

* View pending and in-progress tasks and their statuses.
* Submit new tasks to a workflow.
* Provide output for tasks waiting on human input (resume tasks).

It integrates with the Kafka message queue interface (if available) by
publishing tasks to a Kafka topic and publishing results on task
completion. It also starts the workflow engine and Kafka consumer loops
in background threads. To use the OpenAI agentic adapter, ensure you
have registered it on the engine before submitting agentic tasks.

To run this server:

.. code-block:: bash

    export OPENAI_API_KEY=sk-...
    export KAFKA_BROKERS=localhost:9092
    python -m workflow_engine.ui_server

This will start the Flask server on http://127.0.0.1:5000. Navigate
there in your browser to interact with your workflows.
"""

from __future__ import annotations

import asyncio
import json
import os
import threading
from typing import Any, List, Optional
from .ecs_workflow import Phase, Task
from flask import Flask, jsonify, redirect, render_template_string, request, url_for, current_app

from .ecs_workflow import ECSWorkflowEngine, Task, TaskStatus, Phase, TaskType
from .kafka_interface import KafkaMQInterface  # noqa: F401

# Create the Flask application
app = Flask(__name__)

# Initialise the workflow engine
engine = ECSWorkflowEngine()

# Determine Kafka configuration from environment
_brokers = os.environ.get("KAFKA_BROKERS")
kafka_interface: Optional[KafkaMQInterface] = None
if _brokers:
    try:
        kafka_interface = KafkaMQInterface(brokers=_brokers)
        # Register result publisher hook
        engine.register_task_completed_hook(kafka_interface.publish_result)
    except Exception:
        kafka_interface = None

# Create and start an asyncio event loop for the engine and Kafka consumer
_loop = asyncio.new_event_loop()


# Simple echo handler
async def echo_handler(task: Task) -> None:
    task.output = {"handled": task.classification, "data": task.data}


# Register a minimal workflow you can submit to from the form
engine.register_handler("process", echo_handler)
engine.register_workflow("default", [Phase(target_classification="process")])

def _start_async_services() -> None:
    """Start the workflow engine and optionally the Kafka consumer."""
    asyncio.set_event_loop(_loop)
    tasks: List[asyncio.Task[Any]] = []
    # Run the engine loop
    tasks.append(_loop.create_task(engine.run()))
    # If Kafka is configured, start consuming tasks
    if kafka_interface is not None:
        tasks.append(_loop.create_task(kafka_interface.consume_tasks(engine)))
    _loop.run_until_complete(asyncio.gather(*tasks))

# Start the async services in a background thread
threading.Thread(target=_start_async_services, daemon=True).start()

@app.route("/")
def index() -> str:
    """Render the dashboard with a list of current tasks."""
    tasks = list(engine._pending_tasks)  # type: ignore[attr-defined]
    # Sort tasks by creation time or id for consistent ordering
    tasks.sort(key=lambda t: t.id)
    return render_template_string(
        """
        <h1>Workflow Engine Dashboard</h1>
        <p><a href="{{ url_for('submit_task') }}">Submit new task</a></p>
        <h2>Tasks</h2>
        <table border="1" cellpadding="4" cellspacing="0">
            <tr><th>ID</th><th>Workflow</th><th>Classification</th><th>Status</th><th>Assigned to</th><th>Actions</th></tr>
            {% for task in tasks %}
            <tr>
                <td>{{ task.id }}</td>
                <td>{{ task.workflow_name }}</td>
                <td>{{ task.classification }}</td>
                <td>{{ task.status.name if hasattr(task.status, 'name') else task.status }}</td>
                <td>{{ task.assigned_to }}</td>
                <td>
                    {% if hasattr(task.status, 'name') and task.status.name.startswith('WAITING_ON_HUMAN') %}
                        <a href="{{ url_for('resume_task', task_id=task.id) }}">Provide input</a>
                    {% else %}
                        &nbsp;
                    {% endif %}
                </td>
            </tr>
            {% endfor %}
        </table>
        """,
        tasks=tasks,
    )

@app.route("/submit", methods=["GET", "POST"])
def submit_task():
    if request.method == "GET":
        return render_template_string(
            """
            <h1>Submit New Task</h1>
            <form method="post">
                <p>Workflow name: <input type="text" name="workflow_name" required></p>
                <p>Classification (optional): <input type="text" name="classification"></p>
                <p>Assigned to (ai, human, functional): <input type="text" name="assigned_to" value="ai"></p>
                <p>Data (JSON or text):<br><textarea name="data" rows="6" cols="60"></textarea></p>
                <p><input type="submit" value="Submit"></p>
            </form>
            <p><a href="{{ url_for('index') }}">Back to dashboard</a></p>
            """,
        )


    workflow_name = (request.form.get("workflow_name") or "").strip()
    classification = (request.form.get("classification") or None)
    assigned_to = (request.form.get("assigned_to") or "ai").strip() or "ai"
    raw = (request.form.get("data") or "").strip()


    if not workflow_name:
        return ("Missing workflow_name", 400)


    try:
        data = json.loads(raw)
    except Exception:
        data = raw


    # Schedule and WAIT so we can surface errors (e.g., unknown workflow)
    fut = asyncio.run_coroutine_threadsafe(
        engine.submit_task(
            data=data,
            workflow_name=workflow_name,
            classification=classification,
            assigned_to=assigned_to,
        ),
        _loop,
    )
    try:
        task = fut.result(timeout=5)
        current_app.logger.info("Enqueued task %s", task.id)
    except Exception as e:
        current_app.logger.exception("Submit failed")
        return (f"Submit failed: {e}", 400)


    # Classic PRG but explicit
    return redirect(url_for("index"), code=303)

@app.route("/resume/<task_id>", methods=["GET", "POST"])
def resume_task(task_id: str) -> str:
    """Provide output for a task waiting on human input."""
    # Find the task
    task = engine.find_task_by_id(task_id)
    if task is None:
        return "Task not found", 404
    if request.method == "POST":
        output = request.form["output"].strip()
        next_classification = request.form.get("next_classification", "").strip() or None
        asyncio.run_coroutine_threadsafe(
            engine.resume(task_id, output, next_classification),
            _loop,
        )
        return redirect(url_for("index"))
    # GET: display a form to provide output
    return render_template_string(
        """
        <h1>Resume Task {{ task.id }}</h1>
        <p>Workflow: {{ task.workflow_name }}</p>
        <p>Classification: {{ task.classification }}</p>
        <p>Context: {{ task.context }}</p>
        <form method="post">
            <p>Output:<br><textarea name="output" rows="6" cols="60"></textarea></p>
            <p>Next classification (optional): <input type="text" name="next_classification"></p>
            <p><input type="submit" value="Submit"></p>
        </form>
        <p><a href="{{ url_for('index') }}">Back to dashboard</a></p>
        """,
        task=task,
    )

if __name__ == "__main__":
    # Start the Flask development server
    app.run(debug=True)
