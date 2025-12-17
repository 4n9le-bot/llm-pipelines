"""
llm-pipelines: A modular, async, composable AI pipeline library.
"""

from llm_pipelines.core import (
    StreamItem,
    Processor,
    ItemProcessor,
    MAIN_STREAM,
    DEBUG_STREAM,
    STATUS_STREAM,
)
from llm_pipelines.decorators import (
    processor_function,
    item_processor_function,
    item_processor,
)
from llm_pipelines.stream_utils import (
    split,
    concat,
    merge,
    gather_stream,
    stream_content,
)
from llm_pipelines.context import (
    context,
    Context,
    current,
    create_task,
)

# Optional AI integration (requires openai package)
try:
    from llm_pipelines.ai_integration import (
        StreamingChatProcessor,
        ChatCompletionProcessor,
    )

    _AI_AVAILABLE = True
except ImportError:
    _AI_AVAILABLE = False
    StreamingChatProcessor = None  # type: ignore
    ChatCompletionProcessor = None  # type: ignore

__version__ = "0.3.0"

__all__ = [
    # Core classes
    "StreamItem",
    "Processor",
    "ItemProcessor",
    # Stream names
    "MAIN_STREAM",
    "DEBUG_STREAM",
    "STATUS_STREAM",
    # Decorators
    "processor_function",
    "item_processor_function",
    "item_processor",
    # Stream utilities
    "split",
    "concat",
    "merge",
    "gather_stream",
    "stream_content",
    # Context management
    "context",
    "Context",
    "current",
    "create_task",
    # AI Integration (optional)
    "StreamingChatProcessor",
    "ChatCompletionProcessor",
]
