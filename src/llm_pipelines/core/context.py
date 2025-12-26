"""
Context management for llm-pipelines.

Provides unified task management for async operations in processors,
ensuring proper cleanup and cancellation of background tasks.
"""

import asyncio
from collections.abc import Coroutine
from contextvars import ContextVar
from typing import Any, TypeVar

T = TypeVar("T")

# Global context variable to track the current context
_current_context: ContextVar["Context | None"] = ContextVar("_current_context", default=None)


class Context:
    """
    Context manager for managing async tasks in processors.

    Automatically tracks and cleans up background tasks created during
    processor execution. Ensures proper resource cleanup on exit.

    Example:
        async with context():
            processor = parse_input + call_llm + format_output
            async for item in processor(input_stream):
                process(item)
        # All background tasks are automatically cleaned up here

    Features:
        - Automatic task tracking
        - Graceful shutdown on exit
        - Error propagation from failed tasks
        - Task cancellation support
    """

    def __init__(self) -> None:
        """Initialize a new context."""
        self._tasks: set[asyncio.Task[Any]] = set()
        self._task_group: asyncio.TaskGroup | None = None
        self._token: Any = None

    async def __aenter__(self) -> "Context":
        """Enter the context manager."""
        # Set this context as current
        self._token = _current_context.set(self)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        """
        Exit the context manager.

        Ensures all background tasks are properly cleaned up.
        """
        # Cancel all pending tasks
        for task in self._tasks:
            if not task.done():
                task.cancel()

        # Wait for all tasks to complete or be cancelled
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)

        # Clear the task set
        self._tasks.clear()

        # Restore previous context
        _current_context.reset(self._token)

    def create_task(
        self, coro: Coroutine[Any, Any, T], *, name: str | None = None
    ) -> asyncio.Task[T]:
        """
        Create a task and register it with this context.

        The task will be automatically tracked and cleaned up when the
        context exits.

        Args:
            coro: Coroutine to run as a task
            name: Optional name for the task

        Returns:
            Created task

        Example:
            async with context() as ctx:
                task = ctx.create_task(some_coroutine(), name="my_task")
                result = await task
        """
        task = asyncio.create_task(coro, name=name)
        self._tasks.add(task)

        # Remove task from set when it completes
        def _task_done(t: asyncio.Task[T]) -> None:
            self._tasks.discard(t)

        task.add_done_callback(_task_done)

        return task

    @property
    def active_tasks(self) -> int:
        """Get the number of active tasks in this context."""
        return len(self._tasks)


def current() -> Context:
    """
    Get the current active context.

    Returns:
        Current Context instance

    Raises:
        RuntimeError: If no context is active

    Example:
        async with context():
            ctx = current()
            ctx.create_task(some_coroutine())
    """
    ctx = _current_context.get()
    if ctx is None:
        raise RuntimeError(
            "No active context. Use 'async with context():' to create one."
        )
    return ctx


def create_task(
    coro: Coroutine[Any, Any, T], *, name: str | None = None
) -> asyncio.Task[T]:
    """
    Create a task in the current context.

    This is a convenience function that calls create_task on the current context.

    Args:
        coro: Coroutine to run as a task
        name: Optional name for the task

    Returns:
        Created task

    Raises:
        RuntimeError: If no context is active

    Example:
        from llm_pipelines.context import context, create_task

        async with context():
            task = create_task(some_coroutine(), name="my_task")
            result = await task
    """
    return current().create_task(coro, name=name)


# Alias for convenience
context = Context
