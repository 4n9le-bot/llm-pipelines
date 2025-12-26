"""
Stream utility functions for managing async iterables.
"""

import asyncio
from collections.abc import AsyncIterable, AsyncIterator, Iterable
from typing import TypeVar

from llm_pipelines.core import StreamItem

try:
    from llm_pipelines import context as context_module

    _CONTEXT_AVAILABLE = True
except ImportError:
    _CONTEXT_AVAILABLE = False
    context_module = None  # type: ignore

T = TypeVar("T")


async def stream_content(items: Iterable[StreamItem]) -> AsyncIterator[StreamItem]:
    """
    Convert an iterable of StreamItems into an async stream.

    Example:
        items = [
            StreamItem(data="hello", role="user"),
            StreamItem(data="world", role="assistant")
        ]
        async for item in stream_content(items):
            print(item.data)

    Args:
        items: Iterable of StreamItems

    Yields:
        StreamItems from the iterable
    """
    for item in items:
        yield item


async def gather_stream(content: AsyncIterable[StreamItem]) -> list[StreamItem]:
    """
    Collect an entire stream into a list.

    Note: This buffers all items in memory. Use with caution on large streams.

    Example:
        items = await gather_stream(processor(input_stream))

    Args:
        content: Async stream of StreamItems

    Returns:
        List of all StreamItems
    """
    items: list[StreamItem] = []
    async for item in content:
        items.append(item)
    return items


async def split(
    content: AsyncIterable[StreamItem], n: int = 2
) -> tuple[AsyncIterable[StreamItem], ...]:
    """
    Split a stream into n identical copies.

    Each output stream receives all items from the input stream.
    This is useful for applying different processors to the same data.

    Example:
        stream1, stream2 = split(input_stream, n=2)
        results1 = await processor1(stream1)
        results2 = await processor2(stream2)

    Args:
        content: Input stream
        n: Number of output streams (default: 2)

    Returns:
        Tuple of n identical streams
    """
    if n < 1:
        raise ValueError(f"n must be >= 1, got {n}")

    queues: list[asyncio.Queue[StreamItem | None]] = [
        asyncio.Queue() for _ in range(n)
    ]

    async def producer() -> None:
        """Read from input and distribute to all queues."""
        try:
            async for item in content:
                for queue in queues:
                    await queue.put(item)
        finally:
            # Signal completion to all queues
            for queue in queues:
                await queue.put(None)

    # Start producer task (use context if available)
    if _CONTEXT_AVAILABLE:
        try:
            ctx = context_module.current()
            ctx.create_task(producer(), name="split_producer")
        except RuntimeError:
            # No active context, use regular create_task
            asyncio.create_task(producer())
    else:
        asyncio.create_task(producer())

    async def consumer(queue: asyncio.Queue[StreamItem | None]) -> AsyncIterator[StreamItem]:
        """Yield items from a queue until completion signal."""
        while True:
            item = await queue.get()
            if item is None:
                break
            yield item

    return tuple(consumer(q) for q in queues)


async def concat(
    *streams: AsyncIterable[StreamItem],
) -> AsyncIterator[StreamItem]:
    """
    Concatenate multiple streams sequentially.

    Yields all items from the first stream, then all items from the second, etc.

    Example:
        combined = concat(
            process_file1(file1),
            process_file2(file2),
            process_file3(file3)
        )

    Args:
        *streams: Variable number of streams to concatenate

    Yields:
        Items from all streams in order
    """
    for stream in streams:
        async for item in stream:
            yield item


async def merge(
    streams: list[AsyncIterable[StreamItem]], stop_on_first: bool = False
) -> AsyncIterator[StreamItem]:
    """
    Merge multiple streams, yielding items as they arrive.

    Unlike concat, this processes streams concurrently and yields
    items in the order they become available.

    Example:
        merged = merge([
            user_input_stream,
            ai_response_stream,
            system_events_stream
        ])

    Args:
        streams: List of streams to merge
        stop_on_first: If True, stop when the first stream completes

    Yields:
        Items from all streams as they arrive
    """
    if not streams:
        return

    queue: asyncio.Queue[StreamItem | None] = asyncio.Queue()
    active_streams = len(streams)
    stop_event = asyncio.Event()

    async def consume_stream(stream: AsyncIterable[StreamItem]) -> None:
        """Consume a stream and put items in the shared queue."""
        nonlocal active_streams
        try:
            async for item in stream:
                if stop_event.is_set():
                    break
                await queue.put(item)
        finally:
            active_streams -= 1
            if active_streams == 0 or stop_on_first:
                # Signal completion
                await queue.put(None)
                stop_event.set()

    # Start all consumer tasks (use context if available)
    tasks: list[asyncio.Task[None]] = []
    if _CONTEXT_AVAILABLE:
        try:
            ctx = context_module.current()
            for i, stream in enumerate(streams):
                task = ctx.create_task(consume_stream(stream), name=f"merge_consumer_{i}")
                tasks.append(task)
        except RuntimeError:
            # No active context, use regular create_task
            tasks = [asyncio.create_task(consume_stream(stream)) for stream in streams]
    else:
        tasks = [asyncio.create_task(consume_stream(stream)) for stream in streams]

    try:
        # Yield items from queue
        while True:
            item = await queue.get()
            if item is None:
                break
            yield item
    finally:
        # Ensure all tasks are cleaned up
        stop_event.set()
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
