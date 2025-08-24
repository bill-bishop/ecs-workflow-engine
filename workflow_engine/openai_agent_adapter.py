from __future__ import annotations

import os
from typing import Optional

try:
    import openai  # type: ignore
except ImportError:
    openai = None  # type: ignore

from .ecs_workflow import BaseAdapter, Task


class OpenAIAgentAdapter(BaseAdapter):
    """Adapter that delegates agentic tasks to the OpenAI Chat API.

    Reads the API key from the ``OPENAI_API_KEY`` environment variable
    and sends the task's data (and context) to OpenAI's chat model. The
    model's response is stored in ``task.output``.
    """

    def __init__(self, model: str = "gpt-4o", system_prompt: Optional[str] = None) -> None:
        if openai is None:
            raise RuntimeError(
                "The 'openai' package is not installed. Install it with 'pip install openai'"
            )
        api_key = os.environ.get("OPENAI_API_KEY")
        if not api_key:
            raise RuntimeError(
                "OPENAI_API_KEY environment variable must be set to use OpenAIAgentAdapter"
            )
        openai.api_key = api_key
        self.model = model
        self.system_prompt = system_prompt or "You are a helpful assistant."

    async def handle(self, task: Task) -> None:
        """Handle an agentic task by calling the OpenAI Chat API."""
        user_content = str(task.data)
        if task.context:
            try:
                user_content += "\n\nContext:\n" + "\n".join(
                    f"{k}: {v}" for k, v in task.context.items()
                )
            except Exception:
                pass
        messages = [
            {"role": "system", "content": self.system_prompt},
            {"role": "user", "content": user_content},
        ]
        response = await openai.ChatCompletion.acreate(model=self.model, messages=messages)
        task.output = response.choices[0].message.content if response.choices else None
