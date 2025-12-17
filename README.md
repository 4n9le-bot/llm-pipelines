# llm-pipelines

一个模块化、异步、可组合的 AI Pipeline 库，用于构建：
- 对话代理 (Conversational agents)
- 内容处理管道 (Content processing pipelines)
- 多步推理系统 (Multi-step reasoning)

## 核心设计原则

### 1. 统一的流式抽象 - 一切皆流

所有数据都是 `AsyncIterable[StreamItem]`，输入是流，输出也是流。

### 2. 可组合性优先 - 乐高式构建

通过运算符重载（`+` 链式，`//` 并行），Processor 像乐高积木一样自由组合。

### 3. 双层处理抽象 - 灵活与性能兼得

- **Processor**: 处理整个流，适合需要上下文的操作
- **ItemProcessor**: 处理单个 item，自动并发执行，性能最优

## 安装

基础安装：
```bash
pip install llm-pipelines
```

包含 AI 集成（OpenAI SDK）：
```bash
pip install "llm-pipelines[openai]"
```

开发安装：
```bash
pip install -e ".[dev,openai]"
```

## 快速开始

```python
from llm_pipelines import Processor, StreamItem, stream_content
import asyncio

# 创建简单的 Processor
class UppercaseProcessor(Processor):
    async def call(self, content):
        async for item in content:
            yield StreamItem(
                data=item.data.upper(),
                role=item.role,
                mimetype=item.mimetype
            )

# 使用 Processor
async def main():
    processor = UppercaseProcessor()
    input_stream = stream_content([
        StreamItem(data="hello", role="user")
    ])

    async for item in processor(input_stream):
        print(item.data)  # 输出: HELLO

asyncio.run(main())
```

## 组合 Processors

```python
# 链式组合 (+)
pipeline = processor1 + processor2 + processor3

# 并行组合 (//)
parallel_pipeline = item_processor1 // item_processor2
```

## AI 集成

支持 OpenAI 兼容的 API（OpenAI、DeepSeek、Qwen 等）：

```python
from llm_pipelines import StreamingChatProcessor, StreamItem, stream_content

# 流式 AI 对话
chat = StreamingChatProcessor(
    api_key="your-api-key",
    base_url="https://api.openai.com/v1",
    model="gpt-4"
)

input_stream = stream_content([
    StreamItem(data="你好！", role="user")
])

# 实时输出
async for chunk in chat(input_stream):
    print(chunk.data, end="", flush=True)
```

查看 `examples/ai_chat_example.py` 获取更多 AI 集成示例。

## Context 管理

统一的异步任务管理，确保资源正确清理：

```python
from llm_pipelines import context, create_task

async with context():
    # 所有在此创建的任务会被自动追踪
    task = create_task(some_coroutine(), name="my_task")
    result = await task
# 退出时自动清理所有未完成的任务
```

查看 `examples/context_example.py` 获取更多 Context 管理示例。

## 项目状态

✅ **版本 0.3.0** - Context 管理已完成

已实现:
- [x] StreamItem 数据模型
- [x] Processor 和 ItemProcessor 抽象
- [x] 组合运算符 (`+`, `//`)
- [x] Stream 工具函数
- [x] 装饰器支持
- [x] **StreamingChatProcessor** - 流式 AI 调用
- [x] **ChatCompletionProcessor** - 批量 AI 调用
- [x] OpenAI 兼容 API 支持
- [x] **Context 管理** - 统一任务管理和自动清理

## 开发

运行测试：
```bash
pytest
```

类型检查：
```bash
mypy src/llm_pipelines
```

代码格式化：
```bash
ruff check src/ tests/
```

## License

MIT
