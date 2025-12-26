"""Core abstractions and utilities for llm-pipelines."""

from llm_pipelines.core.base import (
    DEBUG_STREAM,
    MAIN_STREAM,
    STATUS_STREAM,
    ItemProcessor,
    Processor,
    StreamItem,
)
from llm_pipelines.core.context import Context, context, create_task, current
from llm_pipelines.core.decorators import (
    item_processor,
    item_processor_function,
    processor_function,
)
from llm_pipelines.core.stream_utils import (
    concat,
    gather_stream,
    merge,
    split,
    stream_content,
)

__all__ = [
    # Constants
    "DEBUG_STREAM",
    "MAIN_STREAM",
    "STATUS_STREAM",
    # Core classes
    "Processor",
    "StreamItem",
    "ItemProcessor",
    # Decorators
    "processor_function",
    "item_processor",
    "item_processor_function",
    # Stream utils
    "stream_content",
    "gather_stream",
    "split",
    "concat",
    "merge",
    # Context
    "Context",
    "context",
    "current",
    "create_task",
]
