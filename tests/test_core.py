"""
Tests for core module: StreamItem, Processor, ItemProcessor.
"""

import pytest

from llm_pipelines.core import (
    DEBUG_STREAM,
    MAIN_STREAM,
    STATUS_STREAM,
    ItemProcessor,
    Processor,
    StreamItem,
)
from llm_pipelines.stream_utils import gather_stream, stream_content


class TestStreamItem:
    """Tests for StreamItem dataclass."""

    def test_create_stream_item_with_defaults(self) -> None:
        """Test creating StreamItem with default values."""
        item = StreamItem(data="hello")

        assert item.data == "hello"
        assert item.mimetype == "text/plain"
        assert item.role == "user"
        assert item.metadata == {}
        assert item.substream_name == MAIN_STREAM

    def test_create_stream_item_with_custom_values(self) -> None:
        """Test creating StreamItem with custom values."""
        item = StreamItem(
            data={"key": "value"},
            mimetype="application/json",
            role="assistant",
            metadata={"source": "test"},
            substream_name=DEBUG_STREAM,
        )

        assert item.data == {"key": "value"}
        assert item.mimetype == "application/json"
        assert item.role == "assistant"
        assert item.metadata == {"source": "test"}
        assert item.substream_name == DEBUG_STREAM

    def test_stream_constants(self) -> None:
        """Test stream name constants."""
        assert MAIN_STREAM == ""
        assert DEBUG_STREAM == "debug:"
        assert STATUS_STREAM == "status:"


class TestProcessor:
    """Tests for Processor abstract class."""

    async def test_simple_processor(self) -> None:
        """Test a simple processor implementation."""

        class UppercaseProcessor(Processor):
            async def call(self, content):
                async for item in content:
                    yield StreamItem(
                        data=item.data.upper(),
                        role=item.role,
                        mimetype=item.mimetype,
                    )

        processor = UppercaseProcessor()
        input_stream = stream_content(
            [StreamItem(data="hello"), StreamItem(data="world")]
        )

        results = await gather_stream(processor(input_stream))

        assert len(results) == 2
        assert results[0].data == "HELLO"
        assert results[1].data == "WORLD"

    async def test_chained_processors(self) -> None:
        """Test chaining processors with + operator."""

        class UppercaseProcessor(Processor):
            async def call(self, content):
                async for item in content:
                    yield StreamItem(data=item.data.upper(), role=item.role)

        class ExclamationProcessor(Processor):
            async def call(self, content):
                async for item in content:
                    yield StreamItem(data=f"{item.data}!", role=item.role)

        pipeline = UppercaseProcessor() + ExclamationProcessor()
        input_stream = stream_content([StreamItem(data="hello")])

        results = await gather_stream(pipeline(input_stream))

        assert len(results) == 1
        assert results[0].data == "HELLO!"

    async def test_processor_filter(self) -> None:
        """Test processor that filters items."""

        class FilterProcessor(Processor):
            async def call(self, content):
                async for item in content:
                    if len(item.data) > 3:
                        yield item

        processor = FilterProcessor()
        input_stream = stream_content(
            [
                StreamItem(data="hi"),
                StreamItem(data="hello"),
                StreamItem(data="bye"),
                StreamItem(data="world"),
            ]
        )

        results = await gather_stream(processor(input_stream))

        assert len(results) == 2
        assert results[0].data == "hello"
        assert results[1].data == "world"


class TestItemProcessor:
    """Tests for ItemProcessor abstract class."""

    async def test_simple_item_processor(self) -> None:
        """Test a simple ItemProcessor implementation."""

        class DoubleItemProcessor(ItemProcessor):
            async def call(self, item):
                yield StreamItem(data=item.data * 2, role=item.role)

        processor = DoubleItemProcessor().to_processor()
        input_stream = stream_content(
            [StreamItem(data="a"), StreamItem(data="b"), StreamItem(data="c")]
        )

        results = await gather_stream(processor(input_stream))

        assert len(results) == 3
        assert results[0].data == "aa"
        assert results[1].data == "bb"
        assert results[2].data == "cc"

    async def test_item_processor_with_multiple_outputs(self) -> None:
        """Test ItemProcessor that yields multiple items per input."""

        class SplitItemProcessor(ItemProcessor):
            async def call(self, item):
                for char in item.data:
                    yield StreamItem(data=char, role=item.role)

        processor = SplitItemProcessor().to_processor()
        input_stream = stream_content([StreamItem(data="abc")])

        results = await gather_stream(processor(input_stream))

        assert len(results) == 3
        assert [r.data for r in results] == ["a", "b", "c"]

    async def test_parallel_item_processors(self) -> None:
        """Test parallel ItemProcessors with // operator."""

        class UpperItemProcessor(ItemProcessor):
            async def call(self, item):
                yield StreamItem(
                    data=item.data.upper(),
                    role=item.role,
                    metadata={"processor": "upper"},
                )

        class LowerItemProcessor(ItemProcessor):
            async def call(self, item):
                yield StreamItem(
                    data=item.data.lower(),
                    role=item.role,
                    metadata={"processor": "lower"},
                )

        parallel = (UpperItemProcessor() // LowerItemProcessor()).to_processor()
        input_stream = stream_content([StreamItem(data="HeLLo")])

        results = await gather_stream(parallel(input_stream))

        assert len(results) == 2
        # Results from both processors
        data_values = {r.data for r in results}
        assert data_values == {"HELLO", "hello"}

    async def test_item_processor_concurrency(self) -> None:
        """Test that ItemProcessor respects max_concurrency."""
        import asyncio

        processed_items = []
        concurrent_count = 0
        max_concurrent = 0

        class ConcurrencyTestProcessor(ItemProcessor):
            async def call(self, item):
                nonlocal concurrent_count, max_concurrent

                concurrent_count += 1
                max_concurrent = max(max_concurrent, concurrent_count)

                # Simulate some work
                await asyncio.sleep(0.01)

                processed_items.append(item.data)
                concurrent_count -= 1

                yield StreamItem(data=f"processed-{item.data}", role=item.role)

        processor = ConcurrencyTestProcessor(max_concurrency=3).to_processor()
        input_stream = stream_content(
            [StreamItem(data=f"item{i}") for i in range(10)]
        )

        results = await gather_stream(processor(input_stream))

        assert len(results) == 10
        assert max_concurrent <= 3
        # All items should be processed
        assert len(processed_items) == 10

    async def test_item_processor_error_handling(self) -> None:
        """Test that ItemProcessor handles errors gracefully."""

        class ErrorItemProcessor(ItemProcessor):
            async def call(self, item):
                if item.data == "error":
                    raise ValueError("Test error")
                yield StreamItem(data=f"ok-{item.data}", role=item.role)

        processor = ErrorItemProcessor().to_processor()
        input_stream = stream_content(
            [
                StreamItem(data="good"),
                StreamItem(data="error"),
                StreamItem(data="also_good"),
            ]
        )

        results = await gather_stream(processor(input_stream))

        # Should have 2 successful results + 1 error item
        assert len(results) == 3

        ok_results = [r for r in results if r.substream_name == MAIN_STREAM]
        error_results = [r for r in results if r.substream_name == STATUS_STREAM]

        assert len(ok_results) == 2
        assert len(error_results) == 1
        assert "Error processing item" in error_results[0].data
