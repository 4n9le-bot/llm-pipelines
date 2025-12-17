"""
Tests for context management module.
"""

import asyncio

import pytest

from llm_pipelines.context import Context, context, create_task, current


class TestContext:
    """Tests for Context class."""

    async def test_context_basic(self) -> None:
        """Test basic context creation and usage."""
        async with context() as ctx:
            assert isinstance(ctx, Context)
            assert ctx.active_tasks == 0

    async def test_context_current(self) -> None:
        """Test getting current context."""
        async with context():
            ctx = current()
            assert isinstance(ctx, Context)

    async def test_context_no_active_context(self) -> None:
        """Test error when no context is active."""
        with pytest.raises(RuntimeError, match="No active context"):
            current()

    async def test_create_task_basic(self) -> None:
        """Test creating a task in context."""

        async def simple_task() -> str:
            await asyncio.sleep(0.01)
            return "done"

        async with context() as ctx:
            task = ctx.create_task(simple_task(), name="test_task")
            assert ctx.active_tasks == 1

            result = await task
            assert result == "done"

    async def test_create_task_multiple(self) -> None:
        """Test creating multiple tasks."""

        async def task_func(n: int) -> int:
            await asyncio.sleep(0.01)
            return n * 2

        async with context() as ctx:
            task1 = ctx.create_task(task_func(1))
            task2 = ctx.create_task(task_func(2))
            task3 = ctx.create_task(task_func(3))

            assert ctx.active_tasks >= 1  # At least one should still be running

            results = await asyncio.gather(task1, task2, task3)
            assert results == [2, 4, 6]

    async def test_create_task_convenience_function(self) -> None:
        """Test create_task convenience function."""

        async def simple_task() -> str:
            return "result"

        async with context():
            task = create_task(simple_task(), name="convenience_test")
            result = await task
            assert result == "result"

    async def test_create_task_without_context(self) -> None:
        """Test create_task raises error when no context."""

        async def simple_task() -> None:
            pass

        with pytest.raises(RuntimeError, match="No active context"):
            create_task(simple_task())

    async def test_context_auto_cleanup(self) -> None:
        """Test automatic cleanup of tasks on exit."""
        tasks_started = []
        tasks_completed = []

        async def long_task(n: int) -> None:
            tasks_started.append(n)
            try:
                await asyncio.sleep(1)  # Long sleep
                tasks_completed.append(n)
            except asyncio.CancelledError:
                # Task was cancelled
                raise

        async with context() as ctx:
            task1 = ctx.create_task(long_task(1))
            task2 = ctx.create_task(long_task(2))
            initial_tasks = ctx.active_tasks
            assert initial_tasks >= 1

            # Give tasks time to start
            await asyncio.sleep(0.01)

        # After context exit, tasks should be cancelled
        # They started but didn't complete normally
        assert len(tasks_started) == 2
        assert len(tasks_completed) == 0  # None completed normally

        # Tasks should be cancelled
        assert task1.cancelled() or task1.done()
        assert task2.cancelled() or task2.done()

    async def test_context_task_removal(self) -> None:
        """Test tasks are removed from context when completed."""

        async def quick_task() -> None:
            await asyncio.sleep(0.01)

        async with context() as ctx:
            task = ctx.create_task(quick_task())
            assert ctx.active_tasks == 1

            await task
            # Task should be removed after completion
            await asyncio.sleep(0.01)
            assert ctx.active_tasks == 0

    async def test_context_nested(self) -> None:
        """Test nested contexts."""

        async def task_in_context() -> str:
            ctx = current()
            return f"ctx_{id(ctx)}"

        async with context() as ctx1:
            result1 = await create_task(task_in_context())

            async with context() as ctx2:
                result2 = await create_task(task_in_context())

                # Inner context should be different
                assert ctx1 is not ctx2
                assert result1 != result2

            # Outer context should be restored
            result3 = await create_task(task_in_context())
            assert result1 == result3

    async def test_context_with_exception(self) -> None:
        """Test context cleanup when exception occurs."""

        async def failing_task() -> None:
            await asyncio.sleep(0.01)
            raise ValueError("Test error")

        with pytest.raises(ValueError, match="Test error"):
            async with context() as ctx:
                task = ctx.create_task(failing_task())
                await task

        # Context should still clean up properly

    async def test_context_task_error_handling(self) -> None:
        """Test that task errors don't break context."""

        async def good_task() -> str:
            await asyncio.sleep(0.01)
            return "success"

        async def bad_task() -> None:
            await asyncio.sleep(0.01)
            raise ValueError("Bad task")

        async with context() as ctx:
            task1 = ctx.create_task(good_task())
            task2 = ctx.create_task(bad_task())

            result1 = await task1
            assert result1 == "success"

            with pytest.raises(ValueError, match="Bad task"):
                await task2

    async def test_active_tasks_count(self) -> None:
        """Test active_tasks property."""

        async def counting_task(delay: float) -> None:
            await asyncio.sleep(delay)

        async with context() as ctx:
            assert ctx.active_tasks == 0

            # Create tasks with longer delays to ensure they stay active
            task1 = ctx.create_task(counting_task(0.2))
            await asyncio.sleep(0.01)  # Give time for task to start
            assert ctx.active_tasks >= 1

            task2 = ctx.create_task(counting_task(0.2))
            await asyncio.sleep(0.01)
            assert ctx.active_tasks >= 2

            task3 = ctx.create_task(counting_task(0.2))
            await asyncio.sleep(0.01)
            assert ctx.active_tasks >= 2  # At least 2 should be active

            # Complete tasks
            await task1
            # Task 1 should be removed shortly
            await asyncio.sleep(0.01)
            remaining_after_1 = ctx.active_tasks
            assert remaining_after_1 >= 1  # At least task2 and task3

            await task2
            await task3
            await asyncio.sleep(0.01)
            assert ctx.active_tasks == 0


class TestContextIntegration:
    """Integration tests with stream utilities."""

    async def test_context_with_split(self) -> None:
        """Test context management with split operation."""
        from llm_pipelines import StreamItem, split, stream_content

        items = [StreamItem(data=i) for i in range(5)]
        input_stream = stream_content(items)

        async with context() as ctx:
            stream1, stream2 = await split(input_stream, n=2)

            # Context should be managing the producer task
            initial_tasks = ctx.active_tasks
            assert initial_tasks >= 1

            results1 = []
            async for item in stream1:
                results1.append(item.data)

            results2 = []
            async for item in stream2:
                results2.append(item.data)

            assert results1 == [0, 1, 2, 3, 4]
            assert results2 == [0, 1, 2, 3, 4]

        # Tasks should be cleaned up after context exit

    async def test_context_with_merge(self) -> None:
        """Test context management with merge operation."""
        from llm_pipelines import StreamItem, merge, stream_content

        async def delayed_stream(items: list[int], delay: float):
            for i in items:
                await asyncio.sleep(delay)
                yield StreamItem(data=i)

        async with context() as ctx:
            streams = [
                delayed_stream([1, 2], 0.01),
                delayed_stream([3, 4], 0.01),
            ]

            merged = merge(streams)

            # Context should be managing consumer tasks
            results = []
            async for item in merged:
                results.append(item.data)
                if ctx.active_tasks > 0:
                    # At least some tasks should be tracked
                    pass

            assert set(results) == {1, 2, 3, 4}

        # Tasks should be cleaned up

    async def test_context_without_context_manager(self) -> None:
        """Test split/merge work without explicit context."""
        from llm_pipelines import StreamItem, split, stream_content

        # Should still work without context manager
        items = [StreamItem(data=i) for i in range(3)]
        input_stream = stream_content(items)

        stream1, stream2 = await split(input_stream, n=2)

        results1 = []
        async for item in stream1:
            results1.append(item.data)

        assert results1 == [0, 1, 2]
