"""
Decorators for creating Processors and ItemProcessors from functions.
"""

from collections.abc import AsyncIterable, AsyncIterator, Callable
from functools import wraps
from typing import Any

from llm_pipelines.core import ItemProcessor, Processor, StreamItem


def processor_function(
    func: Callable[[AsyncIterable[StreamItem]], AsyncIterator[StreamItem]]
) -> Processor:
    """
    Decorator to convert an async function into a Processor.

    The decorated function should:
    - Accept AsyncIterable[StreamItem] as input
    - Return/yield AsyncIterator[StreamItem] as output

    Example:
        @processor_function
        async def uppercase_text(content: AsyncIterable[StreamItem]):
            async for item in content:
                if isinstance(item.data, str):
                    yield StreamItem(
                        data=item.data.upper(),
                        role=item.role,
                        mimetype=item.mimetype
                    )
                else:
                    yield item

    Args:
        func: Async generator function to convert

    Returns:
        A Processor instance
    """

    class FunctionProcessor(Processor):
        def __init__(self) -> None:
            self.func = func

        async def call(
            self, content: AsyncIterable[StreamItem]
        ) -> AsyncIterator[StreamItem]:
            async for item in self.func(content):
                yield item

        def __repr__(self) -> str:
            return f"Processor({func.__name__})"

    return FunctionProcessor()


def item_processor_function(
    max_concurrency: int = 10,
) -> Callable[
    [Callable[[StreamItem], AsyncIterator[StreamItem]]], ItemProcessor
]:
    """
    Decorator to convert an async function into an ItemProcessor.

    The decorated function should:
    - Accept a single StreamItem as input
    - Return/yield AsyncIterator[StreamItem] as output

    Example:
        @item_processor_function(max_concurrency=20)
        async def extract_keywords(item: StreamItem):
            if isinstance(item.data, str):
                keywords = await extract_keywords_logic(item.data)
                yield StreamItem(
                    data=keywords,
                    mimetype="application/json",
                    metadata={"original_text": item.data}
                )

    Args:
        max_concurrency: Maximum number of concurrent item processing tasks

    Returns:
        A decorator that creates an ItemProcessor
    """

    def decorator(
        func: Callable[[StreamItem], AsyncIterator[StreamItem]]
    ) -> ItemProcessor:
        class FunctionItemProcessor(ItemProcessor):
            def __init__(self) -> None:
                super().__init__(max_concurrency=max_concurrency)
                self.func = func

            async def call(self, item: StreamItem) -> AsyncIterator[StreamItem]:
                async for result in self.func(item):
                    yield result

            def __repr__(self) -> str:
                return f"ItemProcessor({func.__name__}, max_concurrency={max_concurrency})"

        return FunctionItemProcessor()

    return decorator


# Alias for simpler usage without arguments
def item_processor(
    func: Callable[[StreamItem], AsyncIterator[StreamItem]]
) -> ItemProcessor:
    """
    Simplified decorator for ItemProcessor with default max_concurrency.

    Example:
        @item_processor
        async def translate(item: StreamItem):
            translated = await translate_api(item.data)
            yield StreamItem(data=translated, role=item.role)

    Args:
        func: Async generator function to convert

    Returns:
        An ItemProcessor instance with default max_concurrency=10
    """
    return item_processor_function(max_concurrency=10)(func)
