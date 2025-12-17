"""
Tests for stream_utils module.
"""

import asyncio

import pytest

from llm_pipelines.core import StreamItem
from llm_pipelines.stream_utils import (
    concat,
    gather_stream,
    merge,
    split,
    stream_content,
)


class TestStreamContent:
    """Tests for stream_content function."""

    async def test_stream_content_basic(self) -> None:
        """Test converting list to async stream."""
        items = [
            StreamItem(data="a"),
            StreamItem(data="b"),
            StreamItem(data="c"),
        ]

        results = await gather_stream(stream_content(items))

        assert len(results) == 3
        assert [r.data for r in results] == ["a", "b", "c"]

    async def test_stream_content_empty(self) -> None:
        """Test streaming empty list."""
        results = await gather_stream(stream_content([]))
        assert results == []


class TestGatherStream:
    """Tests for gather_stream function."""

    async def test_gather_stream_basic(self) -> None:
        """Test collecting stream to list."""
        items = [StreamItem(data=i) for i in range(5)]
        stream = stream_content(items)

        results = await gather_stream(stream)

        assert len(results) == 5
        assert [r.data for r in results] == [0, 1, 2, 3, 4]

    async def test_gather_stream_empty(self) -> None:
        """Test gathering empty stream."""
        stream = stream_content([])
        results = await gather_stream(stream)
        assert results == []


class TestSplit:
    """Tests for split function."""

    async def test_split_two_streams(self) -> None:
        """Test splitting stream into two."""
        items = [StreamItem(data=i) for i in range(3)]
        input_stream = stream_content(items)

        stream1, stream2 = await split(input_stream, n=2)

        results1 = await gather_stream(stream1)
        results2 = await gather_stream(stream2)

        assert len(results1) == 3
        assert len(results2) == 3
        assert [r.data for r in results1] == [0, 1, 2]
        assert [r.data for r in results2] == [0, 1, 2]

    async def test_split_three_streams(self) -> None:
        """Test splitting stream into three."""
        items = [StreamItem(data=x) for x in ["a", "b"]]
        input_stream = stream_content(items)

        s1, s2, s3 = await split(input_stream, n=3)

        r1 = await gather_stream(s1)
        r2 = await gather_stream(s2)
        r3 = await gather_stream(s3)

        assert [r.data for r in r1] == ["a", "b"]
        assert [r.data for r in r2] == ["a", "b"]
        assert [r.data for r in r3] == ["a", "b"]

    async def test_split_invalid_n(self) -> None:
        """Test split with invalid n value."""
        input_stream = stream_content([StreamItem(data="test")])

        with pytest.raises(ValueError, match="n must be >= 1"):
            await split(input_stream, n=0)


class TestConcat:
    """Tests for concat function."""

    async def test_concat_two_streams(self) -> None:
        """Test concatenating two streams."""
        stream1 = stream_content([StreamItem(data="a"), StreamItem(data="b")])
        stream2 = stream_content([StreamItem(data="c"), StreamItem(data="d")])

        combined = concat(stream1, stream2)
        results = await gather_stream(combined)

        assert len(results) == 4
        assert [r.data for r in results] == ["a", "b", "c", "d"]

    async def test_concat_multiple_streams(self) -> None:
        """Test concatenating multiple streams."""
        s1 = stream_content([StreamItem(data=1)])
        s2 = stream_content([StreamItem(data=2)])
        s3 = stream_content([StreamItem(data=3)])

        combined = concat(s1, s2, s3)
        results = await gather_stream(combined)

        assert [r.data for r in results] == [1, 2, 3]

    async def test_concat_empty_streams(self) -> None:
        """Test concatenating with empty streams."""
        s1 = stream_content([StreamItem(data="a")])
        s2 = stream_content([])
        s3 = stream_content([StreamItem(data="b")])

        combined = concat(s1, s2, s3)
        results = await gather_stream(combined)

        assert [r.data for r in results] == ["a", "b"]


class TestMerge:
    """Tests for merge function."""

    async def test_merge_two_streams(self) -> None:
        """Test merging two streams."""

        async def slow_stream():
            for i in [1, 2]:
                await asyncio.sleep(0.01)
                yield StreamItem(data=i)

        async def fast_stream():
            for i in [10, 20]:
                yield StreamItem(data=i)

        merged = merge([slow_stream(), fast_stream()])
        results = await gather_stream(merged)

        assert len(results) == 4
        # Fast stream items should appear first
        data_values = {r.data for r in results}
        assert data_values == {1, 2, 10, 20}

    async def test_merge_empty_list(self) -> None:
        """Test merging empty list of streams."""
        merged = merge([])
        results = await gather_stream(merged)
        assert results == []

    async def test_merge_with_stop_on_first(self) -> None:
        """Test merge with stop_on_first=True."""

        async def short_stream():
            yield StreamItem(data="short")

        async def long_stream():
            yield StreamItem(data="long1")
            await asyncio.sleep(0.1)  # Long delay
            yield StreamItem(data="long2")

        merged = merge([short_stream(), long_stream()], stop_on_first=True)
        results = await gather_stream(merged)

        # Should stop after short stream completes
        # May get long1 if it yielded before stop
        assert len(results) >= 1
        assert any(r.data == "short" for r in results)

    async def test_merge_concurrent_processing(self) -> None:
        """Test that merge processes streams concurrently."""
        start_time = asyncio.get_event_loop().time()

        async def delayed_stream(delay: float, value: str):
            await asyncio.sleep(delay)
            yield StreamItem(data=value)

        streams = [
            delayed_stream(0.02, "a"),
            delayed_stream(0.02, "b"),
            delayed_stream(0.02, "c"),
        ]

        merged = merge(streams)
        results = await gather_stream(merged)

        elapsed = asyncio.get_event_loop().time() - start_time

        assert len(results) == 3
        # Should complete in ~0.02s (concurrent), not ~0.06s (sequential)
        assert elapsed < 0.05  # Allow some margin


class TestIntegration:
    """Integration tests combining multiple utilities."""

    async def test_split_process_merge(self) -> None:
        """Test splitting, processing, and merging pattern."""
        from llm_pipelines.decorators import processor_function

        @processor_function
        async def add_prefix(content):
            async for item in content:
                yield StreamItem(data=f"prefix-{item.data}", role=item.role)

        @processor_function
        async def add_suffix(content):
            async for item in content:
                yield StreamItem(data=f"{item.data}-suffix", role=item.role)

        # Create input
        input_stream = stream_content([StreamItem(data="test")])

        # Split into two streams
        s1, s2 = await split(input_stream, n=2)

        # Process each differently
        processed1 = add_prefix(s1)
        processed2 = add_suffix(s2)

        # Merge results
        merged = merge([processed1, processed2])
        results = await gather_stream(merged)

        assert len(results) == 2
        data_values = {r.data for r in results}
        assert data_values == {"prefix-test", "test-suffix"}
