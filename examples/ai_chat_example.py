"""
AI Chat Integration Example for llm-pipelines.

This example demonstrates how to use StreamingChatProcessor and
ChatCompletionProcessor with OpenAI-compatible APIs.

Note: This requires the OpenAI library to be installed:
    pip install "llm-pipelines[openai]"

You also need to set your API credentials:
    export OPENAI_API_KEY="your-api-key"
    # Or for other providers:
    export AI_API_KEY="your-api-key"
    export AI_BASE_URL="https://api.provider.com/v1"
"""

import asyncio
import os
from typing import Any

from llm_pipelines import (
    STATUS_STREAM,
    Processor,
    StreamItem,
    gather_stream,
    processor_function,
    stream_content,
)

# Import AI processors (will fail gracefully if OpenAI not installed)
try:
    from llm_pipelines.ai_integration import (
        ChatCompletionProcessor,
        StreamingChatProcessor,
    )

    AI_AVAILABLE = True
except ImportError:
    AI_AVAILABLE = False
    print("⚠️  OpenAI library not installed.")
    print('   Install with: pip install "llm-pipelines[openai]"')


# Helper function to get API credentials
def get_api_config() -> dict[str, Any]:
    """Get API configuration from environment variables."""
    api_key = os.getenv("OPENAI_API_KEY") or os.getenv("AI_API_KEY")
    base_url = os.getenv("AI_BASE_URL", "https://api.openai.com/v1")

    if not api_key:
        raise ValueError(
            "API key not found. Set OPENAI_API_KEY or AI_API_KEY environment variable."
        )

    return {
        "api_key": api_key,
        "base_url": base_url,
    }


async def example_streaming_chat():
    """Example 1: Streaming chat with real-time output."""
    print("\n=== Example 1: Streaming Chat ===")
    print("Real-time streaming response from AI\n")

    if not AI_AVAILABLE:
        print("Skipped - OpenAI library not installed")
        return

    try:
        config = get_api_config()

        # Create streaming processor
        streaming_chat = StreamingChatProcessor(
            **config,
            model="gpt-3.5-turbo",
            temperature=0.7,
        )

        # Prepare input
        input_stream = stream_content(
            [
                StreamItem(
                    data="Write a haiku about programming.",
                    role="user",
                )
            ]
        )

        # Stream response
        print("AI: ", end="", flush=True)
        async for chunk in streaming_chat(input_stream):
            if chunk.substream_name == STATUS_STREAM:
                print(f"\n[Error: {chunk.data}]")
            else:
                print(chunk.data, end="", flush=True)
        print("\n")

    except ValueError as e:
        print(f"Configuration error: {e}")
    except Exception as e:
        print(f"Error: {e}")


async def example_batch_completion():
    """Example 2: Batch completion (non-streaming)."""
    print("\n=== Example 2: Batch Completion ===")
    print("Complete response in one shot\n")

    if not AI_AVAILABLE:
        print("Skipped - OpenAI library not installed")
        return

    try:
        config = get_api_config()

        # Create batch processor
        chat_completion = ChatCompletionProcessor(
            **config,
            model="gpt-3.5-turbo",
            temperature=0.5,
        )

        # Prepare input
        input_stream = stream_content(
            [StreamItem(data="What is 2+2? Answer in one word.", role="user")]
        )

        # Get complete response
        results = await gather_stream(chat_completion(input_stream))

        for item in results:
            if item.substream_name == STATUS_STREAM:
                print(f"[Error: {item.data}]")
            else:
                print(f"AI: {item.data}")
                print(f"\nToken usage: {item.metadata.get('usage', {})}")

    except ValueError as e:
        print(f"Configuration error: {e}")
    except Exception as e:
        print(f"Error: {e}")


async def example_conversation():
    """Example 3: Multi-turn conversation."""
    print("\n=== Example 3: Multi-turn Conversation ===")
    print("Conversation with context\n")

    if not AI_AVAILABLE:
        print("Skipped - OpenAI library not installed")
        return

    try:
        config = get_api_config()

        chat = ChatCompletionProcessor(
            **config,
            model="gpt-3.5-turbo",
            temperature=0.7,
        )

        # Conversation history
        conversation = [
            StreamItem(
                data="My name is Alice and I love Python programming.",
                role="user",
            ),
            StreamItem(
                data="Nice to meet you, Alice! Python is a great language.",
                role="assistant",
            ),
            StreamItem(
                data="What's my name and what do I love?",
                role="user",
            ),
        ]

        input_stream = stream_content(conversation)
        results = await gather_stream(chat(input_stream))

        for item in results:
            if item.substream_name == STATUS_STREAM:
                print(f"[Error: {item.data}]")
            else:
                print(f"AI: {item.data}")

    except ValueError as e:
        print(f"Configuration error: {e}")
    except Exception as e:
        print(f"Error: {e}")


async def example_pipeline_with_ai():
    """Example 4: Combining AI with other processors."""
    print("\n=== Example 4: AI in Pipeline ===")
    print("Pre-processing -> AI -> Post-processing\n")

    if not AI_AVAILABLE:
        print("Skipped - OpenAI library not installed")
        return

    # Pre-processor: Add system prompt
    @processor_function
    async def add_system_prompt(content):
        """Add system instructions."""
        yield StreamItem(
            data="You are a helpful assistant. Be concise.",
            role="system",
        )
        async for item in content:
            yield item

    # Post-processor: Format output
    @processor_function
    async def format_output(content):
        """Format AI response."""
        async for item in content:
            if item.substream_name == "":  # Main stream only
                formatted = f"[AI Response]\n{item.data}\n[End]"
                yield StreamItem(data=formatted, role=item.role)
            else:
                yield item

    try:
        config = get_api_config()

        # Build pipeline
        pipeline = (
            add_system_prompt
            + ChatCompletionProcessor(**config, model="gpt-3.5-turbo")
            + format_output
        )

        # Use pipeline
        input_stream = stream_content(
            [StreamItem(data="Explain async/await in 2 sentences.", role="user")]
        )

        results = await gather_stream(pipeline(input_stream))

        for item in results:
            print(item.data)

    except ValueError as e:
        print(f"Configuration error: {e}")
    except Exception as e:
        print(f"Error: {e}")


async def example_parallel_ai_calls():
    """Example 5: Parallel AI calls for different tasks."""
    print("\n=== Example 5: Parallel AI Processing ===")
    print("Multiple AI calls in parallel\n")

    if not AI_AVAILABLE:
        print("Skipped - OpenAI library not installed")
        return

    try:
        config = get_api_config()

        # Create different processors for different tasks
        summarizer = ChatCompletionProcessor(
            **config,
            model="gpt-3.5-turbo",
            temperature=0.3,
        )

        translator = ChatCompletionProcessor(
            **config,
            model="gpt-3.5-turbo",
            temperature=0.5,
        )

        # Process text in parallel
        text = "Python is a high-level programming language."

        # Note: For true parallel processing, you'd use split() and merge()
        # Here we demonstrate sequential calls with different prompts

        print("Original:", text)

        # Summarize
        summary_input = stream_content(
            [StreamItem(data=f"Summarize in 5 words: {text}", role="user")]
        )
        summary_results = await gather_stream(summarizer(summary_input))
        if summary_results:
            print(f"Summary: {summary_results[0].data}")

        # Translate
        translate_input = stream_content(
            [StreamItem(data=f"Translate to Spanish: {text}", role="user")]
        )
        translate_results = await gather_stream(translator(translate_input))
        if translate_results:
            print(f"Spanish: {translate_results[0].data}")

    except ValueError as e:
        print(f"Configuration error: {e}")
    except Exception as e:
        print(f"Error: {e}")


async def main():
    """Run all examples."""
    print("=" * 60)
    print("llm-pipelines AI Integration Examples")
    print("=" * 60)

    if not AI_AVAILABLE:
        print("\n⚠️  OpenAI library not installed.")
        print('Install with: pip install "llm-pipelines[openai]"')
        print("\nRunning examples in demo mode...\n")

    await example_streaming_chat()
    await example_batch_completion()
    await example_conversation()
    await example_pipeline_with_ai()
    await example_parallel_ai_calls()

    print("\n" + "=" * 60)
    print("All examples completed!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
