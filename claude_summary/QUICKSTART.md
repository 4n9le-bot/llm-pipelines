# llm-pipelines 快速开始

## 安装

```bash
# 克隆或进入项目目录
cd llm-pipelines

# 创建虚拟环境
python3 -m venv .venv
source .venv/bin/activate

# 安装开发依赖
pip install pytest pytest-asyncio

# 设置 PYTHONPATH
export PYTHONPATH=$(pwd)/src:$PYTHONPATH
```

## 运行测试

```bash
# 运行所有测试
pytest tests/ -v

# 运行特定测试文件
pytest tests/test_core.py -v

# 运行单个测试
pytest tests/test_core.py::TestProcessor::test_simple_processor -v
```

## 运行示例

```bash
# 运行基础用法示例
python3 examples/basic_usage.py
```

## 基础用法

### 1. 创建简单的 Processor

```python
import asyncio
from llm_pipelines import Processor, StreamItem, stream_content, gather_stream

class UppercaseProcessor(Processor):
    """将文本转换为大写"""
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
        StreamItem(data="hello world")
    ])

    results = await gather_stream(processor(input_stream))
    print(results[0].data)  # 输出: HELLO WORLD

asyncio.run(main())
```

### 2. 使用装饰器

```python
from llm_pipelines import processor_function, item_processor

@processor_function
async def add_prefix(content):
    """添加前缀"""
    async for item in content:
        yield StreamItem(data=f"[PREFIX] {item.data}")

@item_processor
async def reverse_text(item):
    """反转文本"""
    yield StreamItem(data=item.data[::-1])
```

### 3. 链式组合 (+)

```python
# 创建处理管道：添加前缀 -> 转大写 -> 反转
pipeline = (
    add_prefix +
    UppercaseProcessor() +
    reverse_text.to_processor()
)

# 使用管道
async for item in pipeline(input_stream):
    print(item.data)
```

### 4. 并行处理 (//)

```python
from llm_pipelines import ItemProcessor

class TranslateZH(ItemProcessor):
    async def call(self, item):
        # 模拟翻译
        yield StreamItem(
            data=f"{item.data} (中文)",
            metadata={"lang": "zh"}
        )

class TranslateEN(ItemProcessor):
    async def call(self, item):
        # 模拟翻译
        yield StreamItem(
            data=f"{item.data} (English)",
            metadata={"lang": "en"}
        )

# 同时翻译成两种语言
parallel = (TranslateZH() // TranslateEN()).to_processor()

async for item in parallel(input_stream):
    lang = item.metadata.get("lang")
    print(f"[{lang}] {item.data}")
```

### 5. Stream 工具

```python
from llm_pipelines import split, concat, merge

# 分割流
stream1, stream2 = await split(input_stream, n=2)

# 连接流
combined = concat(stream1, stream2, stream3)

# 合并流（并发）
merged = merge([stream1, stream2, stream3])
```

## 完整示例

```python
import asyncio
from llm_pipelines import (
    Processor,
    ItemProcessor,
    StreamItem,
    processor_function,
    item_processor,
    stream_content,
    gather_stream,
)

# 定义处理器
@processor_function
async def validate_input(content):
    """验证输入"""
    async for item in content:
        if isinstance(item.data, str) and len(item.data) > 0:
            yield item

@item_processor
async def process_text(item):
    """处理文本"""
    processed = item.data.strip().lower()
    yield StreamItem(
        data=processed,
        metadata={"processed": True}
    )

class FormatOutput(Processor):
    """格式化输出"""
    async def call(self, content):
        async for item in content:
            formatted = f"Result: {item.data}"
            yield StreamItem(data=formatted)

# 构建管道
pipeline = (
    validate_input +
    process_text.to_processor() +
    FormatOutput()
)

# 使用管道
async def main():
    input_items = [
        StreamItem(data="  HELLO  "),
        StreamItem(data=""),  # 会被过滤掉
        StreamItem(data="WORLD  "),
    ]

    input_stream = stream_content(input_items)
    results = await gather_stream(pipeline(input_stream))

    for item in results:
        print(item.data)

    # 输出:
    # Result: hello
    # Result: world

asyncio.run(main())
```

## 调试技巧

### 1. 使用 substream 输出调试信息

```python
from llm_pipelines import DEBUG_STREAM, STATUS_STREAM

class DebugProcessor(Processor):
    async def call(self, content):
        async for item in content:
            # 输出调试信息
            yield StreamItem(
                data=f"Processing: {item.data}",
                substream_name=DEBUG_STREAM
            )

            # 处理并输出结果
            result = item.data.upper()
            yield StreamItem(data=result)

            # 输出状态信息
            yield StreamItem(
                data=f"Completed: {result}",
                substream_name=STATUS_STREAM
            )

# 分离处理不同的 substream
async for item in processor(input_stream):
    if item.substream_name == DEBUG_STREAM:
        print(f"[DEBUG] {item.data}")
    elif item.substream_name == STATUS_STREAM:
        print(f"[STATUS] {item.data}")
    else:
        print(f"[OUTPUT] {item.data}")
```

### 2. 测试单个 Processor

```python
# 创建测试输入
test_input = stream_content([
    StreamItem(data="test1"),
    StreamItem(data="test2"),
])

# 运行处理器
results = await gather_stream(processor(test_input))

# 验证结果
assert len(results) == 2
assert results[0].data == "expected_value"
```

## 常见问题

### Q: 如何控制 ItemProcessor 的并发数？

```python
# 在初始化时设置
processor = MyItemProcessor(max_concurrency=5)

# 或使用装饰器
@item_processor_function(max_concurrency=20)
async def my_processor(item):
    yield item
```

### Q: 如何处理错误？

ItemProcessor 会自动捕获异常并将其转换为 STATUS_STREAM 的 StreamItem：

```python
async for item in processor(input_stream):
    if item.substream_name == STATUS_STREAM:
        print(f"Error: {item.data}")
    else:
        # 正常处理
        pass
```

### Q: 可以在 Processor 中访问整个流的上下文吗？

是的，Processor 可以访问整个流：

```python
class ContextProcessor(Processor):
    async def call(self, content):
        items = []
        # 先收集所有 items
        async for item in content:
            items.append(item)

        # 基于全局上下文处理
        total = len(items)
        for i, item in enumerate(items):
            yield StreamItem(
                data=f"{item.data} ({i+1}/{total})",
                metadata={"index": i, "total": total}
            )
```

## 下一步

- 查看 `examples/basic_usage.py` 获取更多示例
- 阅读 `IMPLEMENTATION_SUMMARY.md` 了解实现细节
- 查看 `tests/` 目录学习如何测试

## 资源

- 项目文档: `README.md`
- 实现总结: `IMPLEMENTATION_SUMMARY.md`
- 设计文档: `claude.md` 和 `claude_docs/`
