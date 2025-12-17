"""
AI service integration for llm-pipelines.

Provides processors for calling OpenAI-compatible AI APIs with streaming
and batch modes.
"""

from collections.abc import AsyncIterator
from typing import Any

from llm_pipelines.core import Processor, StreamItem, STATUS_STREAM

try:
    from openai import AsyncOpenAI
    from openai.types.chat import ChatCompletion, ChatCompletionChunk

    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    AsyncOpenAI = None  # type: ignore
    ChatCompletion = None  # type: ignore
    ChatCompletionChunk = None  # type: ignore


class StreamingChatProcessor(Processor):
    """
    Processor for streaming chat completions using OpenAI-compatible APIs.

    This processor collects input StreamItems as messages, calls the AI API
    in streaming mode, and yields response chunks as they arrive.

    Example:
        processor = StreamingChatProcessor(
            api_key="your-api-key",
            base_url="https://api.openai.com/v1",
            model="gpt-4"
        )

        input_stream = stream_content([
            StreamItem(data="Hello", role="user")
        ])

        async for chunk in processor(input_stream):
            print(chunk.data, end="", flush=True)
    """

    def __init__(
        self,
        api_key: str,
        base_url: str = "https://api.openai.com/v1",
        model: str = "gpt-4",
        **config: Any,
    ) -> None:
        """
        Initialize StreamingChatProcessor.

        Args:
            api_key: API key for authentication
            base_url: Base URL for the API endpoint
            model: Model name to use
            **config: Additional configuration passed to the API
                     (temperature, max_tokens, etc.)
        """
        if not OPENAI_AVAILABLE:
            raise ImportError(
                "OpenAI library is required for AI integration. "
                'Install it with: pip install "llm-pipelines[openai]"'
            )

        self.client = AsyncOpenAI(api_key=api_key, base_url=base_url)
        self.model = model
        self.config = config

    async def call(self, content: AsyncIterator[StreamItem]) -> AsyncIterator[StreamItem]:
        """
        Process input stream and yield streaming AI responses.

        Args:
            content: Input stream of StreamItems

        Yields:
            StreamItems containing response chunks
        """
        # Collect input messages
        messages: list[Any] = []
        async for item in content:
            # Convert StreamItem to OpenAI message format
            message = {"role": item.role, "content": item.data}
            messages.append(message)

        if not messages:
            yield StreamItem(
                data="No messages to process",
                substream_name=STATUS_STREAM,
                mimetype="text/error",
            )
            return

        try:
            # Call API in streaming mode
            stream = await self.client.chat.completions.create(
                model=self.model, messages=messages, stream=True, **self.config 
            )

            # Yield chunks as they arrive
            async for chunk in stream:
                if chunk.choices and chunk.choices[0].delta.content:
                    yield StreamItem(
                        data=chunk.choices[0].delta.content,
                        role="assistant",
                        mimetype="text/plain",
                        metadata={
                            "model": self.model,
                            "chunk_id": chunk.id,
                            "finish_reason": chunk.choices[0].finish_reason,
                        },
                    )

        except Exception as e:
            yield StreamItem(
                data=f"AI API Error: {str(e)}",
                substream_name=STATUS_STREAM,
                mimetype="text/error",
                metadata={"exception_type": type(e).__name__},
            )


class ChatCompletionProcessor(Processor):
    """
    Processor for batch chat completions using OpenAI-compatible APIs.

    This processor collects input StreamItems as messages, calls the AI API
    in batch mode, and yields a single complete response.

    Example:
        processor = ChatCompletionProcessor(
            api_key="your-api-key",
            base_url="https://api.openai.com/v1",
            model="gpt-4"
        )

        input_stream = stream_content([
            StreamItem(data="What is 2+2?", role="user")
        ])

        async for response in processor(input_stream):
            print(response.data)
    """

    def __init__(
        self,
        api_key: str,
        base_url: str = "https://api.openai.com/v1",
        model: str = "gpt-4",
        **config: Any,
    ) -> None:
        """
        Initialize ChatCompletionProcessor.

        Args:
            api_key: API key for authentication
            base_url: Base URL for the API endpoint
            model: Model name to use
            **config: Additional configuration passed to the API
                     (temperature, max_tokens, etc.)
        """
        if not OPENAI_AVAILABLE:
            raise ImportError(
                "OpenAI library is required for AI integration. "
                'Install it with: pip install "llm-pipelines[openai]"'
            )

        self.client = AsyncOpenAI(api_key=api_key, base_url=base_url)
        self.model = model
        self.config = config

    async def call(self, content: AsyncIterator[StreamItem]) -> AsyncIterator[StreamItem]:
        """
        Process input stream and yield complete AI response.

        Args:
            content: Input stream of StreamItems

        Yields:
            Single StreamItem containing complete response
        """
        # Collect input messages
        messages: list[Any] = []
        async for item in content:
            # Convert StreamItem to OpenAI message format
            message = {"role": item.role, "content": item.data}
            messages.append(message)

        if not messages:
            yield StreamItem(
                data="No messages to process",
                substream_name=STATUS_STREAM,
                mimetype="text/error",
            )
            return

        try:
            # Call API in batch mode
            response = await self.client.chat.completions.create(
                model=self.model, messages=messages, stream=False, **self.config
            )

            # Yield complete response
            yield StreamItem(
                data=response.choices[0].message.content or "",
                role="assistant",
                mimetype="text/plain",
                metadata={
                    "model": self.model,
                    "finish_reason": response.choices[0].finish_reason,
                    "usage": {
                        "prompt_tokens": response.usage.prompt_tokens
                        if response.usage
                        else 0,
                        "completion_tokens": response.usage.completion_tokens
                        if response.usage
                        else 0,
                        "total_tokens": response.usage.total_tokens
                        if response.usage
                        else 0,
                    },
                },
            )

        except Exception as e:
            yield StreamItem(
                data=f"AI API Error: {str(e)}",
                substream_name=STATUS_STREAM,
                mimetype="text/error",
                metadata={"exception_type": type(e).__name__},
            )
