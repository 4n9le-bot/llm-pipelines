# llm-pipelines

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A modular, async, and composable Python library for building AI pipelines with streaming support. Perfect for conversational agents, content processing, and multi-step reasoning systems.

## ✨ Key Features

- **🔄 Unified Streaming Abstraction**: Everything is `AsyncIterable[StreamItem]` - consistent input/output interface
- **🧩 Composable Architecture**: Chain processors with `+` operator, parallelize with `//` operator
- **⚡ Async-First Design**: Built on asyncio for high-performance concurrent processing
- **🤖 AI Integration**: OpenAI-compatible API support (OpenAI, DeepSeek, Qwen, etc.)
- **🎯 Context Management**: Automatic task tracking and cleanup for resource safety
- **📦 Dual Processing Modes**: Stream-level `Processor` and item-level `ItemProcessor` for flexibility
- **🔧 Simple Decorators**: Transform functions into processors with minimal boilerplate

## 📦 Installation

**Basic installation:**
```bash
pip install llm-pipelines
```

**With OpenAI support:**
```bash
pip install "llm-pipelines[openai]"
```

**For development:**
```bash
git clone https://github.com/yourusername/llm-pipelines.git
cd llm-pipelines
pip install -e ".[dev,openai]"
```

## 🚀 Quick Start

### Basic Processor

```python
from llm_pipelines import Processor, StreamItem, stream_content
import asyncio

class UppercaseProcessor(Processor):
    async def call(self, content):
        async for item in content:
            yield StreamItem(
                data=item.data.upper(),
                role=item.role,
                mimetype=item.mimetype
            )

async def main():
    processor = UppercaseProcessor()
    input_stream = stream_content([
        StreamItem(data="hello world", role="user")
    ])

    async for item in processor(input_stream):
        print(item.data)  # Output: HELLO WORLD

asyncio.run(main())
```

### Using Decorators

```python
from llm_pipelines import processor_function, StreamItem

@processor_function
async def uppercase_processor(content):
    async for item in content:
        yield StreamItem(data=item.data.upper())

@processor_function
async def exclaim_processor(content):
    async for item in content:
        yield StreamItem(data=f"{item.data}!")

# Chain processors with + operator
pipeline = uppercase_processor + exclaim_processor

# Input: "hello" → Output: "HELLO!"
```

### Item Processing with Parallelism

```python
from llm_pipelines import item_processor_function

@item_processor_function
async def process_item(item):
    # Process single items with automatic concurrency control
    processed_data = await some_async_operation(item.data)
    return StreamItem(data=processed_data)

# Automatically processes items concurrently (default: max 10 parallel)
async for result in process_item(input_stream):
    print(result.data)
```

### AI Integration

```python
from llm_pipelines import StreamItem, stream_content
from llm_pipelines.chat.streaming import StreamingChatProcessor

# Initialize with any OpenAI-compatible API
chat = StreamingChatProcessor(
    api_key="your-api-key",
    base_url="https://api.openai.com/v1",
    model="gpt-4"
)

# Stream real-time responses
input_stream = stream_content([
    StreamItem(data="Explain quantum computing", role="user")
])

async for chunk in chat(input_stream):
    print(chunk.data, end="", flush=True)
```

### Context Management

```python
from llm_pipelines import context, create_task
import asyncio

async def background_task():
    await asyncio.sleep(1)
    return "completed"

async with context():
    # All tasks created here are automatically tracked
    task1 = create_task(background_task(), name="task1")
    task2 = create_task(background_task(), name="task2")

    results = await asyncio.gather(task1, task2)
    print(results)
# Tasks are automatically cleaned up on exit
```

## 🏗️ Core Concepts

### StreamItem

The fundamental data unit in llm-pipelines:

```python
@dataclass
class StreamItem:
    data: Any                              # The actual content
    mimetype: str = "text/plain"           # Content type
    role: str = "user"                     # Message role (user/assistant/system)
    metadata: dict[str, Any] = field(...)  # Additional metadata
    substream_name: str = MAIN_STREAM      # Stream identifier
```

### Processor

Processes entire streams with full context:

```python
class Processor(ABC):
    @abstractmethod
    async def call(self, content: AsyncIterable[StreamItem]) -> AsyncIterator[StreamItem]:
        ...

    def __add__(self, other: Processor) -> Processor:
        # Chain processors: processor1 + processor2
        ...
```

### ItemProcessor

Processes individual items with automatic concurrency:

```python
class ItemProcessor(ABC):
    @abstractmethod
    async def call(self, item: StreamItem) -> StreamItem:
        ...

    def __floordiv__(self, other: ItemProcessor) -> ItemProcessor:
        # Parallel processing: processor1 // processor2
        ...
```

## 🔧 Stream Utilities

```python
from llm_pipelines import split, concat, merge, gather_stream

# Split stream into multiple independent streams
stream1, stream2 = await split(input_stream, n=2)

# Concatenate streams sequentially
combined = concat([stream1, stream2])

# Merge streams concurrently (items interleaved)
merged = merge([stream1, stream2])

# Collect all items into a list
items = await gather_stream(input_stream)
```

## 📚 Examples

Check the `examples/` directory for more comprehensive examples:

- **basic_usage.py**: Core processor patterns and composition
- **ai_chat_example.py**: AI integration with streaming and batch processing
- **context_example.py**: Context management and task tracking

## 🧪 Testing

Run tests with pytest:

```bash
# Run all tests
pytest

# Run with verbose output
pytest -v

# Run specific test file
pytest tests/test_core.py
```

## 📖 API Reference

### Core Classes

- `StreamItem`: Data container with metadata
- `Processor`: Stream-level processing abstraction
- `ItemProcessor`: Item-level processing with concurrency

### Decorators

- `@processor_function`: Convert async generator to Processor
- `@item_processor_function`: Convert async function to ItemProcessor
- `@item_processor`: Flexible decorator for both patterns

### AI Integration

- `StreamingChatProcessor`: Real-time streaming chat completions
- `ChatCompletionProcessor`: Batch chat completions

### Context Management

- `context()`: Context manager for task tracking
- `Context`: Task management class
- `current()`: Get current active context
- `create_task()`: Create tracked task in current context

### Stream Utilities

- `split(stream, n)`: Copy stream to n independent streams
- `concat(streams)`: Concatenate streams sequentially
- `merge(streams)`: Merge streams concurrently
- `gather_stream(stream)`: Collect all items
- `stream_content(items)`: Create stream from list

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

Built with modern Python async patterns and inspired by functional programming principles.

## 📞 Support

- 🐛 Issues: [GitHub Issues](https://github.com/yourusername/llm-pipelines/issues)
- 💬 Discussions: [GitHub Discussions](https://github.com/yourusername/llm-pipelines/discussions)
