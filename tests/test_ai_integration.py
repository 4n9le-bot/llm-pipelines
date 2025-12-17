"""
Tests for AI integration module.
"""

from unittest.mock import AsyncMock, MagicMock, patch
from typing import Any

import pytest

from llm_pipelines.core import STATUS_STREAM, StreamItem
from llm_pipelines.stream_utils import gather_stream, stream_content


# Mock classes for testing without OpenAI dependency
class MockChatCompletionChoice:
    def __init__(self, message: Any, finish_reason: str = "stop"):
        self.message = message
        self.finish_reason = finish_reason


class MockChatCompletionMessage:
    def __init__(self, content: str, role: str = "assistant"):
        self.content = content
        self.role = role


class MockChatCompletionUsage:
    def __init__(
        self, prompt_tokens: int = 10, completion_tokens: int = 20, total_tokens: int = 30
    ):
        self.prompt_tokens = prompt_tokens
        self.completion_tokens = completion_tokens
        self.total_tokens = total_tokens


class MockChatCompletion:
    def __init__(self, content: str):
        self.choices = [
            MockChatCompletionChoice(
                message=MockChatCompletionMessage(content=content),
                finish_reason="stop",
            )
        ]
        self.usage = MockChatCompletionUsage()


class MockStreamChunkDelta:
    def __init__(self, content: str | None):
        self.content = content


class MockStreamChunkChoice:
    def __init__(self, delta: MockStreamChunkDelta, finish_reason: str | None = None):
        self.delta = delta
        self.finish_reason = finish_reason


class MockStreamChunk:
    def __init__(self, content: str | None, chunk_id: str = "chunk-123"):
        self.id = chunk_id
        self.choices = [MockStreamChunkChoice(delta=MockStreamChunkDelta(content))]


@pytest.fixture
def mock_openai():
    """Mock the OpenAI module."""
    with patch("llm_pipelines.ai_integration.OPENAI_AVAILABLE", True):
        with patch("llm_pipelines.ai_integration.AsyncOpenAI") as mock_client:
            yield mock_client


class TestStreamingChatProcessor:
    """Tests for StreamingChatProcessor."""

    async def test_streaming_chat_basic(self, mock_openai: Any) -> None:
        """Test basic streaming chat completion."""
        from llm_pipelines.ai_integration import StreamingChatProcessor

        # Setup mock
        mock_client_instance = AsyncMock()
        mock_openai.return_value = mock_client_instance

        # Mock streaming response
        async def mock_stream():
            chunks = ["Hello", " ", "World", "!"]
            for i, content in enumerate(chunks):
                yield MockStreamChunk(content, chunk_id=f"chunk-{i}")

        mock_client_instance.chat.completions.create = AsyncMock(
            return_value=mock_stream()
        )

        # Create processor
        processor = StreamingChatProcessor(
            api_key="test-key", base_url="https://test.com/v1", model="gpt-4"
        )

        # Test input
        input_stream = stream_content(
            [StreamItem(data="Say hello world", role="user")]
        )

        # Process
        results = await gather_stream(processor(input_stream))

        # Verify
        assert len(results) == 4
        assert results[0].data == "Hello"
        assert results[1].data == " "
        assert results[2].data == "World"
        assert results[3].data == "!"
        assert all(r.role == "assistant" for r in results)
        assert all(r.metadata.get("model") == "gpt-4" for r in results)

    async def test_streaming_chat_with_multiple_messages(self, mock_openai: Any) -> None:
        """Test streaming with multiple input messages."""
        from llm_pipelines.ai_integration import StreamingChatProcessor

        mock_client_instance = AsyncMock()
        mock_openai.return_value = mock_client_instance

        # Track messages sent to API
        sent_messages = []

        async def mock_create(**kwargs: Any):
            sent_messages.extend(kwargs["messages"])

            async def mock_stream():
                yield MockStreamChunk("Response")

            return mock_stream()

        mock_client_instance.chat.completions.create = mock_create

        processor = StreamingChatProcessor(api_key="test-key", model="gpt-4")

        input_stream = stream_content(
            [
                StreamItem(data="Hello", role="user"),
                StreamItem(data="Hi there", role="assistant"),
                StreamItem(data="How are you?", role="user"),
            ]
        )

        results = await gather_stream(processor(input_stream))

        # Verify messages were sent correctly
        assert len(sent_messages) == 3
        assert sent_messages[0] == {"role": "user", "content": "Hello"}
        assert sent_messages[1] == {"role": "assistant", "content": "Hi there"}
        assert sent_messages[2] == {"role": "user", "content": "How are you?"}

    async def test_streaming_chat_empty_input(self, mock_openai: Any) -> None:
        """Test streaming with empty input."""
        from llm_pipelines.ai_integration import StreamingChatProcessor

        mock_client_instance = AsyncMock()
        mock_openai.return_value = mock_client_instance

        processor = StreamingChatProcessor(api_key="test-key", model="gpt-4")

        input_stream = stream_content([])
        results = await gather_stream(processor(input_stream))

        # Should yield error message
        assert len(results) == 1
        assert results[0].substream_name == STATUS_STREAM
        assert "No messages" in results[0].data

    async def test_streaming_chat_api_error(self, mock_openai: Any) -> None:
        """Test streaming with API error."""
        from llm_pipelines.ai_integration import StreamingChatProcessor

        mock_client_instance = AsyncMock()
        mock_openai.return_value = mock_client_instance

        # Mock API error
        mock_client_instance.chat.completions.create = AsyncMock(
            side_effect=Exception("API Error")
        )

        processor = StreamingChatProcessor(api_key="test-key", model="gpt-4")

        input_stream = stream_content([StreamItem(data="Test", role="user")])
        results = await gather_stream(processor(input_stream))

        # Should yield error as status stream
        assert len(results) == 1
        assert results[0].substream_name == STATUS_STREAM
        assert "API Error" in results[0].data

    async def test_streaming_chat_with_config(self, mock_openai: Any) -> None:
        """Test streaming with additional configuration."""
        from llm_pipelines.ai_integration import StreamingChatProcessor

        mock_client_instance = AsyncMock()
        mock_openai.return_value = mock_client_instance

        config_used = {}

        async def mock_create(**kwargs: Any):
            config_used.update(kwargs)

            async def mock_stream():
                yield MockStreamChunk("Test")

            return mock_stream()

        mock_client_instance.chat.completions.create = mock_create

        processor = StreamingChatProcessor(
            api_key="test-key",
            model="gpt-4",
            temperature=0.7,
            max_tokens=100,
        )

        input_stream = stream_content([StreamItem(data="Test", role="user")])
        await gather_stream(processor(input_stream))

        # Verify config was passed
        assert config_used["temperature"] == 0.7
        assert config_used["max_tokens"] == 100
        assert config_used["stream"] is True


class TestChatCompletionProcessor:
    """Tests for ChatCompletionProcessor."""

    async def test_chat_completion_basic(self, mock_openai: Any) -> None:
        """Test basic chat completion."""
        from llm_pipelines.ai_integration import ChatCompletionProcessor

        mock_client_instance = AsyncMock()
        mock_openai.return_value = mock_client_instance

        # Mock response
        mock_client_instance.chat.completions.create = AsyncMock(
            return_value=MockChatCompletion("Hello, how can I help you?")
        )

        processor = ChatCompletionProcessor(api_key="test-key", model="gpt-4")

        input_stream = stream_content([StreamItem(data="Hi", role="user")])
        results = await gather_stream(processor(input_stream))

        # Verify
        assert len(results) == 1
        assert results[0].data == "Hello, how can I help you?"
        assert results[0].role == "assistant"
        assert results[0].metadata["model"] == "gpt-4"
        assert results[0].metadata["finish_reason"] == "stop"
        assert "usage" in results[0].metadata

    async def test_chat_completion_with_usage(self, mock_openai: Any) -> None:
        """Test chat completion includes usage statistics."""
        from llm_pipelines.ai_integration import ChatCompletionProcessor

        mock_client_instance = AsyncMock()
        mock_openai.return_value = mock_client_instance

        mock_client_instance.chat.completions.create = AsyncMock(
            return_value=MockChatCompletion("Response")
        )

        processor = ChatCompletionProcessor(api_key="test-key", model="gpt-4")

        input_stream = stream_content([StreamItem(data="Test", role="user")])
        results = await gather_stream(processor(input_stream))

        # Verify usage stats
        usage = results[0].metadata["usage"]
        assert usage["prompt_tokens"] == 10
        assert usage["completion_tokens"] == 20
        assert usage["total_tokens"] == 30

    async def test_chat_completion_empty_input(self, mock_openai: Any) -> None:
        """Test chat completion with empty input."""
        from llm_pipelines.ai_integration import ChatCompletionProcessor

        mock_client_instance = AsyncMock()
        mock_openai.return_value = mock_client_instance

        processor = ChatCompletionProcessor(api_key="test-key", model="gpt-4")

        input_stream = stream_content([])
        results = await gather_stream(processor(input_stream))

        # Should yield error message
        assert len(results) == 1
        assert results[0].substream_name == STATUS_STREAM
        assert "No messages" in results[0].data

    async def test_chat_completion_api_error(self, mock_openai: Any) -> None:
        """Test chat completion with API error."""
        from llm_pipelines.ai_integration import ChatCompletionProcessor

        mock_client_instance = AsyncMock()
        mock_openai.return_value = mock_client_instance

        # Mock API error
        mock_client_instance.chat.completions.create = AsyncMock(
            side_effect=Exception("Rate limit exceeded")
        )

        processor = ChatCompletionProcessor(api_key="test-key", model="gpt-4")

        input_stream = stream_content([StreamItem(data="Test", role="user")])
        results = await gather_stream(processor(input_stream))

        # Should yield error as status stream
        assert len(results) == 1
        assert results[0].substream_name == STATUS_STREAM
        assert "Rate limit exceeded" in results[0].data

    async def test_chat_completion_with_config(self, mock_openai: Any) -> None:
        """Test chat completion with additional configuration."""
        from llm_pipelines.ai_integration import ChatCompletionProcessor

        mock_client_instance = AsyncMock()
        mock_openai.return_value = mock_client_instance

        config_used = {}

        async def mock_create(**kwargs: Any):
            config_used.update(kwargs)
            return MockChatCompletion("Response")

        mock_client_instance.chat.completions.create = mock_create

        processor = ChatCompletionProcessor(
            api_key="test-key",
            model="gpt-3.5-turbo",
            temperature=0.5,
            max_tokens=200,
            top_p=0.9,
        )

        input_stream = stream_content([StreamItem(data="Test", role="user")])
        await gather_stream(processor(input_stream))

        # Verify config was passed
        assert config_used["model"] == "gpt-3.5-turbo"
        assert config_used["temperature"] == 0.5
        assert config_used["max_tokens"] == 200
        assert config_used["top_p"] == 0.9
        assert config_used["stream"] is False


class TestImportError:
    """Test behavior when OpenAI is not installed."""

    def test_import_error_streaming(self) -> None:
        """Test StreamingChatProcessor raises ImportError when OpenAI not available."""
        with patch("llm_pipelines.ai_integration.OPENAI_AVAILABLE", False):
            from llm_pipelines.ai_integration import StreamingChatProcessor

            with pytest.raises(ImportError, match="OpenAI library is required"):
                StreamingChatProcessor(api_key="test-key", model="gpt-4")

    def test_import_error_completion(self) -> None:
        """Test ChatCompletionProcessor raises ImportError when OpenAI not available."""
        with patch("llm_pipelines.ai_integration.OPENAI_AVAILABLE", False):
            from llm_pipelines.ai_integration import ChatCompletionProcessor

            with pytest.raises(ImportError, match="OpenAI library is required"):
                ChatCompletionProcessor(api_key="test-key", model="gpt-4")
