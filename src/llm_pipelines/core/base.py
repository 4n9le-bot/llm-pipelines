"""
Core abstractions for llm-pipelines: StreamItem, Processor, ItemProcessor.
"""

import abc
import asyncio
from collections.abc import AsyncIterable, AsyncIterator
from dataclasses import dataclass, field
from typing import Any

# Substream names
MAIN_STREAM = ""
DEBUG_STREAM = "debug:"
STATUS_STREAM = "status:"


@dataclass
class StreamItem:
    """
    Basic data unit in a stream.

    Attributes:
        data: The actual content (text, image, JSON, etc.)
        mimetype: Content type (text/plain, image/png, application/json, etc.)
        role: Message role (user, assistant, system)
        metadata: Custom attributes
        substream_name: Routing identifier (main, debug, status)
    """

    data: Any
    mimetype: str = "text/plain"
    role: str = "user"
    metadata: dict[str, Any] = field(default_factory=dict)
    substream_name: str = MAIN_STREAM


class Processor(abc.ABC):
    """
    Abstract base class for stream processors.

    A Processor transforms an input stream (AsyncIterable[StreamItem])
    into an output stream (AsyncIterable[StreamItem]).
    """

    @abc.abstractmethod
    async def call(
        self, content: AsyncIterable[StreamItem]
    ) -> AsyncIterator[StreamItem]:
        """
        Process an input stream and yield output items.

        Args:
            content: Input stream of StreamItems

        Yields:
            Processed StreamItems
        """
        ...

    async def __call__(
        self, content: AsyncIterable[StreamItem]
    ) -> AsyncIterator[StreamItem]:
        """Make processor callable."""
        async for item in self.call(content):
            yield item

    def __add__(self, other: "Processor") -> "Processor":
        """
        Chain two processors sequentially using the + operator.

        Example:
            pipeline = processor1 + processor2 + processor3
        """
        return ChainedProcessor(self, other)

    def __floordiv__(self, other: "ItemProcessor") -> "ItemProcessor":
        """
        Combine two ItemProcessors in parallel using the // operator.

        Note: Only ItemProcessors can be combined in parallel.
        This method is defined here but will raise TypeError if called
        on a non-ItemProcessor.
        """
        raise TypeError(
            "Only ItemProcessors can be combined with // operator. "
            "Convert Processor to ItemProcessor first if needed."
        )


class ItemProcessor(abc.ABC):
    """
    Abstract base class for item-level processors.

    An ItemProcessor processes individual StreamItems independently,
    enabling automatic concurrent execution.
    """

    def __init__(self, max_concurrency: int = 10) -> None:
        """
        Initialize ItemProcessor.

        Args:
            max_concurrency: Maximum number of concurrent item processing tasks
        """
        self.max_concurrency = max_concurrency

    @abc.abstractmethod
    async def call(self, item: StreamItem) -> AsyncIterator[StreamItem]:
        """
        Process a single StreamItem and yield output items.

        Args:
            item: Input StreamItem

        Yields:
            Processed StreamItems
        """
        ...

    async def __call__(self, item: StreamItem) -> AsyncIterator[StreamItem]:
        """Make item processor callable."""
        async for result in self.call(item):
            yield result

    def to_processor(self) -> Processor:
        """
        Convert this ItemProcessor to a Processor.

        Returns:
            A Processor that applies this ItemProcessor to each item
            with controlled concurrency.
        """
        return ItemProcessorAdapter(self)

    def __floordiv__(self, other: "ItemProcessor") -> "ItemProcessor":
        """
        Combine two ItemProcessors in parallel using the // operator.

        Example:
            parallel = processor1 // processor2 // processor3
        """
        return ParallelItemProcessor(self, other)


class ChainedProcessor(Processor):
    """
    A Processor that chains two processors sequentially.

    The output of the first processor becomes the input of the second.
    """

    def __init__(self, first: Processor, second: Processor) -> None:
        self.first = first
        self.second = second

    async def call(
        self, content: AsyncIterable[StreamItem]
    ) -> AsyncIterator[StreamItem]:
        """Chain processors: first -> second."""
        intermediate = self.first(content)
        async for item in self.second(intermediate):
            yield item


class ItemProcessorAdapter(Processor):
    """
    Adapter that converts an ItemProcessor to a Processor.

    Applies the ItemProcessor to each item in the stream with
    controlled concurrency.
    """

    def __init__(self, item_processor: ItemProcessor) -> None:
        self.item_processor = item_processor

    async def call(
        self, content: AsyncIterable[StreamItem]
    ) -> AsyncIterator[StreamItem]:
        """
        Apply ItemProcessor to each item with concurrency control.
        """
        semaphore = asyncio.Semaphore(self.item_processor.max_concurrency)
        queue: asyncio.Queue[StreamItem | None] = asyncio.Queue()

        async def process_item(item: StreamItem) -> None:
            """Process a single item and put results in queue."""
            async with semaphore:
                try:
                    async for result in self.item_processor(item):
                        await queue.put(result)
                except Exception as e:
                    # Put error as status stream item
                    await queue.put(
                        StreamItem(
                            data=f"Error processing item: {str(e)}",
                            mimetype="text/error",
                            substream_name=STATUS_STREAM,
                            metadata={"exception": type(e).__name__},
                        )
                    )

        async def producer() -> None:
            """Consume input stream and create processing tasks."""
            tasks: list[asyncio.Task[None]] = []
            try:
                async for item in content:
                    task = asyncio.create_task(process_item(item))
                    tasks.append(task)

                # Wait for all tasks to complete
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)
            finally:
                # Signal completion
                await queue.put(None)

        # Start producer in background
        producer_task = asyncio.create_task(producer())

        try:
            # Yield items from queue as they arrive
            while True:
                item = await queue.get()
                if item is None:
                    break
                yield item
        finally:
            # Ensure producer completes
            await producer_task


class ParallelItemProcessor(ItemProcessor):
    """
    An ItemProcessor that applies two ItemProcessors in parallel to each item.

    Each input item is processed by both processors concurrently,
    and all results are yielded.
    """

    def __init__(self, first: ItemProcessor, second: ItemProcessor) -> None:
        # Use the minimum concurrency of both processors
        max_concurrency = min(first.max_concurrency, second.max_concurrency)
        super().__init__(max_concurrency=max_concurrency)
        self.first = first
        self.second = second

    async def call(self, item: StreamItem) -> AsyncIterator[StreamItem]:
        """Process item with both processors in parallel."""
        # Create tasks for both processors
        first_task = asyncio.create_task(self._collect_results(self.first, item))
        second_task = asyncio.create_task(self._collect_results(self.second, item))

        # Wait for both to complete
        first_results, second_results = await asyncio.gather(first_task, second_task)

        # Yield all results
        for result in first_results:
            yield result
        for result in second_results:
            yield result

    @staticmethod
    async def _collect_results(
        processor: ItemProcessor, item: StreamItem
    ) -> list[StreamItem]:
        """Collect all results from a processor."""
        results: list[StreamItem] = []
        async for result in processor(item):
            results.append(result)
        return results
