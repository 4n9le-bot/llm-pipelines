"""
Tests for decorator module.
"""

import pytest

from llm_pipelines.core import StreamItem
from llm_pipelines.decorators import (
    item_processor,
    item_processor_function,
    processor_function,
)
from llm_pipelines.stream_utils import gather_stream, stream_content


class TestProcessorFunction:
    """Tests for @processor_function decorator."""

    async def test_processor_function_basic(self) -> None:
        """Test basic @processor_function usage."""

        @processor_function
        async def uppercase_processor(content):
            async for item in content:
                yield StreamItem(data=item.data.upper(), role=item.role)

        input_stream = stream_content(
            [StreamItem(data="hello"), StreamItem(data="world")]
        )
        results = await gather_stream(uppercase_processor(input_stream))

        assert len(results) == 2
        assert results[0].data == "HELLO"
        assert results[1].data == "WORLD"

    async def test_processor_function_chaining(self) -> None:
        """Test chaining decorated processors."""

        @processor_function
        async def add_prefix(content):
            async for item in content:
                yield StreamItem(data=f"prefix-{item.data}", role=item.role)

        @processor_function
        async def add_suffix(content):
            async for item in content:
                yield StreamItem(data=f"{item.data}-suffix", role=item.role)

        pipeline = add_prefix + add_suffix
        input_stream = stream_content([StreamItem(data="test")])
        results = await gather_stream(pipeline(input_stream))

        assert len(results) == 1
        assert results[0].data == "prefix-test-suffix"


class TestItemProcessorFunction:
    """Tests for @item_processor_function decorator."""

    async def test_item_processor_function_basic(self) -> None:
        """Test basic @item_processor_function usage."""

        @item_processor_function(max_concurrency=5)
        async def double_processor(item):
            yield StreamItem(data=item.data * 2, role=item.role)

        processor = double_processor.to_processor()
        input_stream = stream_content(
            [StreamItem(data="a"), StreamItem(data="b")]
        )
        results = await gather_stream(processor(input_stream))

        assert len(results) == 2
        assert results[0].data == "aa"
        assert results[1].data == "bb"

    async def test_item_processor_function_parallel(self) -> None:
        """Test parallel combination with decorated ItemProcessors."""

        @item_processor_function(max_concurrency=10)
        async def upper_processor(item):
            yield StreamItem(
                data=item.data.upper(),
                role=item.role,
                metadata={"type": "upper"},
            )

        @item_processor_function(max_concurrency=10)
        async def lower_processor(item):
            yield StreamItem(
                data=item.data.lower(),
                role=item.role,
                metadata={"type": "lower"},
            )

        parallel = (upper_processor // lower_processor).to_processor()
        input_stream = stream_content([StreamItem(data="TeSt")])
        results = await gather_stream(parallel(input_stream))

        assert len(results) == 2
        data_values = {r.data for r in results}
        assert data_values == {"TEST", "test"}

    async def test_item_processor_simple_decorator(self) -> None:
        """Test @item_processor simplified decorator."""

        @item_processor
        async def reverse_processor(item):
            if isinstance(item.data, str):
                yield StreamItem(data=item.data[::-1], role=item.role)
            else:
                yield item

        processor = reverse_processor.to_processor()
        input_stream = stream_content([StreamItem(data="hello")])
        results = await gather_stream(processor(input_stream))

        assert len(results) == 1
        assert results[0].data == "olleh"

    async def test_item_processor_multiple_yields(self) -> None:
        """Test ItemProcessor that yields multiple items."""

        @item_processor
        async def explode_processor(item):
            for char in item.data:
                yield StreamItem(data=char, role=item.role)

        processor = explode_processor.to_processor()
        input_stream = stream_content([StreamItem(data="abc")])
        results = await gather_stream(processor(input_stream))

        assert len(results) == 3
        assert [r.data for r in results] == ["a", "b", "c"]
