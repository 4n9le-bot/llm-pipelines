"""
Context Management Example for llm-pipelines.

This example demonstrates how to use Context for managing async tasks
in processors, ensuring proper cleanup and resource management.
"""

import asyncio

from llm_pipelines import (
    Processor,
    StreamItem,
    gather_stream,
    merge,
    processor_function,
    split,
    stream_content,
)
from llm_pipelines.context import context, create_task, current


async def example_basic_context():
    """Example 1: Basic context usage."""
    print("\n=== Example 1: Basic Context Usage ===")
    print("Creating and managing async tasks\n")

    async def background_task(name: str, duration: float) -> str:
        """Simulate a background task."""
        print(f"Task {name} started")
        await asyncio.sleep(duration)
        print(f"Task {name} completed")
        return f"Result from {name}"

    # Use context manager
    async with context() as ctx:
        print(f"Active tasks: {ctx.active_tasks}")

        # Create tasks
        task1 = ctx.create_task(background_task("A", 0.1), name="task_a")
        task2 = ctx.create_task(background_task("B", 0.15), name="task_b")

        print(f"Active tasks: {ctx.active_tasks}")

        # Wait for results
        result1 = await task1
        result2 = await task2

        print(f"\nResults: {result1}, {result2}")
        print(f"Active tasks: {ctx.active_tasks}")

    print("Context exited - all tasks cleaned up\n")


async def example_automatic_cleanup():
    """Example 2: Automatic task cleanup on exit."""
    print("\n=== Example 2: Automatic Cleanup ===")
    print("Tasks are cancelled when context exits\n")

    cancelled_tasks = []

    async def long_running_task(name: str) -> None:
        """Simulate a long-running task."""
        try:
            print(f"Task {name} started (will run for 5 seconds)")
            await asyncio.sleep(5)
            print(f"Task {name} completed normally")
        except asyncio.CancelledError:
            print(f"Task {name} was cancelled")
            cancelled_tasks.append(name)
            raise

    async with context() as ctx:
        # Start long-running tasks
        ctx.create_task(long_running_task("Long-1"))
        ctx.create_task(long_running_task("Long-2"))
        print(f"Started {ctx.active_tasks} long-running tasks")

        # Exit context quickly
        await asyncio.sleep(0.1)
        print("Exiting context (tasks will be cancelled)...")

    # Give time for cancellation to process
    await asyncio.sleep(0.1)
    print(f"\nCancelled tasks: {cancelled_tasks}\n")


async def example_with_split():
    """Example 3: Context with split operation."""
    print("\n=== Example 3: Context with Split ===")
    print("Using context to manage split producer tasks\n")

    @processor_function
    async def add_prefix(content):
        """Add prefix to items."""
        async for item in content:
            yield StreamItem(data=f"[PREFIX] {item.data}")

    @processor_function
    async def add_suffix(content):
        """Add suffix to items."""
        async for item in content:
            yield StreamItem(data=f"{item.data} [SUFFIX]")

    items = [StreamItem(data=f"item-{i}") for i in range(3)]

    async with context() as ctx:
        print(f"Initial active tasks: {ctx.active_tasks}")

        # Split stream (creates producer task)
        input_stream = stream_content(items)
        stream1, stream2 = await split(input_stream, n=2)

        print(f"After split active tasks: {ctx.active_tasks}")

        # Process streams differently
        processed1 = add_prefix(stream1)
        processed2 = add_suffix(stream2)

        results1 = await gather_stream(processed1)
        results2 = await gather_stream(processed2)

        print("\nStream 1 (with prefix):")
        for item in results1:
            print(f"  {item.data}")

        print("\nStream 2 (with suffix):")
        for item in results2:
            print(f"  {item.data}")

    print("\nContext exited - producer task cleaned up\n")


async def example_with_merge():
    """Example 4: Context with merge operation."""
    print("\n=== Example 4: Context with Merge ===")
    print("Using context to manage merge consumer tasks\n")

    async def delayed_stream(name: str, items: list[int], delay: float):
        """Create a stream with delayed items."""
        for i in items:
            await asyncio.sleep(delay)
            yield StreamItem(data=f"{name}-{i}")

    async with context() as ctx:
        print(f"Initial active tasks: {ctx.active_tasks}")

        # Create multiple streams
        streams = [
            delayed_stream("Stream-A", [1, 2, 3], 0.05),
            delayed_stream("Stream-B", [4, 5, 6], 0.05),
            delayed_stream("Stream-C", [7, 8, 9], 0.05),
        ]

        # Merge streams (creates consumer tasks)
        merged = merge(streams)

        print(f"After merge active tasks: {ctx.active_tasks}")

        # Process merged stream
        print("\nMerged results (may arrive in any order):")
        async for item in merged:
            print(f"  Received: {item.data}")
            print(f"  Active tasks: {ctx.active_tasks}")

    print("\nContext exited - consumer tasks cleaned up\n")


async def example_nested_contexts():
    """Example 5: Nested contexts."""
    print("\n=== Example 5: Nested Contexts ===")
    print("Using nested contexts for hierarchical task management\n")

    async def task_with_subtasks(name: str) -> list[str]:
        """Task that creates its own subtasks."""
        print(f"Task {name} started")

        results = []

        # Create nested context for subtasks
        async with context() as inner_ctx:
            async def subtask(n: int) -> str:
                await asyncio.sleep(0.05)
                return f"{name}-subtask-{n}"

            # Create subtasks
            tasks = [
                inner_ctx.create_task(subtask(i), name=f"{name}_sub_{i}")
                for i in range(3)
            ]

            print(f"  {name} created {inner_ctx.active_tasks} subtasks")

            # Wait for all subtasks
            results = await asyncio.gather(*tasks)

        print(f"Task {name} completed with {len(results)} subtask results")
        return results

    # Outer context
    async with context() as outer_ctx:
        print(f"Outer context active tasks: {outer_ctx.active_tasks}")

        # Create main tasks
        task1 = outer_ctx.create_task(task_with_subtasks("Main-1"))
        task2 = outer_ctx.create_task(task_with_subtasks("Main-2"))

        result1 = await task1
        result2 = await task2

        print(f"\nMain-1 results: {result1}")
        print(f"Main-2 results: {result2}")

    print("\nBoth contexts exited - all tasks cleaned up\n")


async def example_create_task_convenience():
    """Example 6: Using create_task convenience function."""
    print("\n=== Example 6: Convenience Functions ===")
    print("Using module-level convenience functions\n")

    async def work(n: int) -> int:
        await asyncio.sleep(0.05)
        return n * n

    async with context():
        # Use module-level create_task function
        task1 = create_task(work(2), name="square_2")
        task2 = create_task(work(3), name="square_3")
        task3 = create_task(work(4), name="square_4")

        # Get current context
        ctx = current()
        print(f"Active tasks in current context: {ctx.active_tasks}")

        results = await asyncio.gather(task1, task2, task3)
        print(f"\nResults: {results}")

    print("Context exited\n")


async def example_error_handling():
    """Example 7: Error handling in context."""
    print("\n=== Example 7: Error Handling ===")
    print("Handling errors while maintaining cleanup\n")

    async def failing_task() -> None:
        await asyncio.sleep(0.05)
        raise ValueError("Something went wrong!")

    async def successful_task() -> str:
        await asyncio.sleep(0.1)
        return "Success"

    try:
        async with context() as ctx:
            task1 = ctx.create_task(successful_task())
            task2 = ctx.create_task(failing_task())

            print(f"Created {ctx.active_tasks} tasks")

            # This will raise an exception
            result1 = await task1
            print(f"Task 1 result: {result1}")

            result2 = await task2  # This will raise
            print(f"Task 2 result: {result2}")

    except ValueError as e:
        print(f"Caught error: {e}")

    print("Context exited despite error - cleanup still occurred\n")


async def main():
    """Run all examples."""
    print("=" * 60)
    print("llm-pipelines Context Management Examples")
    print("=" * 60)

    await example_basic_context()
    await example_automatic_cleanup()
    await example_with_split()
    await example_with_merge()
    await example_nested_contexts()
    await example_create_task_convenience()
    await example_error_handling()

    print("=" * 60)
    print("All examples completed!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
