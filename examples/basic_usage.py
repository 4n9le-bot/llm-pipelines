"""
Basic usage examples for llm-pipelines.

This demonstrates the core concepts:
- Creating Processors and ItemProcessors
- Using decorators
- Chaining with + operator
- Parallel processing with // operator
- Stream utilities
"""

import asyncio

from llm_pipelines import (
    ItemProcessor,
    Processor,
    StreamItem,
    item_processor,
    processor_function,
    stream_content,
    gather_stream,
    split,
    concat,
    merge,
)


# Example 1: Simple Processor using class
class UppercaseProcessor(Processor):
    """Converts text to uppercase."""

    async def call(self, content):
        async for item in content:
            if isinstance(item.data, str):
                yield StreamItem(
                    data=item.data.upper(),
                    role=item.role,
                    mimetype=item.mimetype,
                    metadata=item.metadata,
                )
            else:
                yield item


# Example 2: ItemProcessor with concurrency control
class TranslateItemProcessor(ItemProcessor):
    """Simulates translation of text items."""

    def __init__(self, target_lang: str, max_concurrency: int = 10):
        super().__init__(max_concurrency=max_concurrency)
        self.target_lang = target_lang

    async def call(self, item):
        # Simulate async translation API call
        await asyncio.sleep(0.01)

        translated_text = f"{item.data} (translated to {self.target_lang})"

        yield StreamItem(
            data=translated_text,
            role=item.role,
            metadata={
                **item.metadata,
                "translated_to": self.target_lang,
            },
        )


# Example 3: Using decorators
@processor_function
async def add_prefix(content):
    """Add a prefix to all text items."""
    async for item in content:
        yield StreamItem(
            data=f"[PREFIX] {item.data}",
            role=item.role,
            mimetype=item.mimetype,
        )


@item_processor
async def reverse_text(item):
    """Reverse text content."""
    if isinstance(item.data, str):
        yield StreamItem(
            data=item.data[::-1],
            role=item.role,
            metadata={**item.metadata, "reversed": True},
        )
    else:
        yield item


async def example_basic_processor():
    """Example 1: Basic processor usage."""
    print("\n=== Example 1: Basic Processor ===")

    processor = UppercaseProcessor()
    input_items = [
        StreamItem(data="hello world", role="user"),
        StreamItem(data="this is a test", role="user"),
    ]
    input_stream = stream_content(input_items)

    print("Input:")
    for item in input_items:
        print(f"  {item.data}")

    print("\nOutput:")
    async for item in processor(input_stream):
        print(f"  {item.data}")


async def example_chained_processors():
    """Example 2: Chaining processors with + operator."""
    print("\n=== Example 2: Chained Processors ===")

    # Create a pipeline: add prefix -> uppercase -> reverse
    pipeline = add_prefix + UppercaseProcessor() + reverse_text.to_processor()

    input_items = [StreamItem(data="hello", role="user")]
    input_stream = stream_content(input_items)

    print("Pipeline: add_prefix + uppercase + reverse")
    print("Input: 'hello'")

    results = await gather_stream(pipeline(input_stream))
    print(f"Output: '{results[0].data}'")


async def example_parallel_processors():
    """Example 3: Parallel processing with // operator."""
    print("\n=== Example 3: Parallel Processing ===")

    # Translate to multiple languages in parallel
    zh_translator = TranslateItemProcessor("Chinese", max_concurrency=5)
    es_translator = TranslateItemProcessor("Spanish", max_concurrency=5)

    parallel_pipeline = (zh_translator // es_translator).to_processor()

    input_items = [
        StreamItem(data="Hello", role="user"),
        StreamItem(data="World", role="user"),
    ]
    input_stream = stream_content(input_items)

    print("Translating to Chinese and Spanish in parallel:")
    async for item in parallel_pipeline(input_stream):
        lang = item.metadata.get("translated_to", "unknown")
        print(f"  [{lang}] {item.data}")


async def example_stream_utilities():
    """Example 4: Using stream utilities."""
    print("\n=== Example 4: Stream Utilities ===")

    # Create input
    input_items = [
        StreamItem(data="apple", role="user"),
        StreamItem(data="banana", role="user"),
    ]

    # Split stream
    print("\n4a. Split stream:")
    input_stream = stream_content(input_items)
    stream1, stream2 = await split(input_stream, n=2)

    # Process differently
    upper_processor = UppercaseProcessor()
    reverse_processor = reverse_text.to_processor()

    processed1 = upper_processor(stream1)
    processed2 = reverse_processor(stream2)

    # Merge results
    merged = merge([processed1, processed2])

    print("Split -> [uppercase, reverse] -> merge:")
    results = await gather_stream(merged)
    for item in results:
        print(f"  {item.data}")

    # Concat streams
    print("\n4b. Concat streams:")
    s1 = stream_content([StreamItem(data="first", role="user")])
    s2 = stream_content([StreamItem(data="second", role="user")])
    s3 = stream_content([StreamItem(data="third", role="user")])

    concatenated = concat(s1, s2, s3)
    results = await gather_stream(concatenated)

    print("Concat three streams:")
    for item in results:
        print(f"  {item.data}")


async def example_complex_pipeline():
    """Example 5: Complex pipeline combining multiple concepts."""
    print("\n=== Example 5: Complex Pipeline ===")

    @item_processor
    async def add_metadata(item):
        """Add processing metadata."""
        yield StreamItem(
            data=item.data,
            role=item.role,
            metadata={
                **item.metadata,
                "processed": True,
                "length": len(str(item.data)),
            },
        )

    @item_processor
    async def filter_short(item):
        """Filter out short strings."""
        if len(str(item.data)) > 3:
            yield item

    # Build complex pipeline
    pipeline = (
        add_metadata.to_processor()
        + filter_short.to_processor()
        + UppercaseProcessor()
    )

    input_items = [
        StreamItem(data="hi", role="user"),
        StreamItem(data="hello", role="user"),
        StreamItem(data="bye", role="user"),
        StreamItem(data="world", role="user"),
    ]

    print("Pipeline: add_metadata -> filter_short -> uppercase")
    print("Input: ['hi', 'hello', 'bye', 'world']")

    input_stream = stream_content(input_items)
    results = await gather_stream(pipeline(input_stream))

    print("\nOutput (filtered > 3 chars):")
    for item in results:
        print(f"  {item.data} (metadata: {item.metadata})")


async def main():
    """Run all examples."""
    print("=" * 60)
    print("llm-pipelines Basic Usage Examples")
    print("=" * 60)

    await example_basic_processor()
    await example_chained_processors()
    await example_parallel_processors()
    await example_stream_utilities()
    await example_complex_pipeline()

    print("\n" + "=" * 60)
    print("All examples completed!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
