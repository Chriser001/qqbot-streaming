# QQ Bot Streaming Plugin (qqbot-streaming)

为 Hermes Agent 的 QQ Bot 通道添加原生打字机流式输出效果。

## 原理

QQ 的流式输出和其他平台（Telegram/Discord 的 `edit_message` 模式）**完全不同**：

| 平台 | 方式 | API |
|------|------|-----|
| Telegram/Discord | 发消息 → 编辑消息（全量替换） | sendMessage + editMessageText |
| **QQ Bot** | 发消息时带 `stream` 字段，分片追加 | `/v2/users/{id}/messages` + `stream: {state, id, index, reset}` |

QQ 的 `stream` 协议：
- `state=1, reset=False`：**追加**增量文本（每个 chunk 只发新增内容）
- `state=10, reset=True`：**终结**，全量替换为完整文本
- 每个 chunk 的 `index` 递增，`msg_seq` 递增

## 安装

插件目录已在 Hermes 仓库中，只需在 `config.yaml` 中启用：

```yaml
# ~/.hermes/config.yaml
plugins:
  enabled:
    - qqbot-streaming   # 添加这一行
```

前提：QQ Bot 平台已配置（`QQ_APP_ID` + `QQ_CLIENT_SECRET`，与普通 QQ 通道共用）。

## 依赖

无额外依赖。插件直接 monkey-patch 现有的 `QQAdapter` 类，使用已有的 `aiohttp`/`httpx`。

## 不修改的内容

- **不修改** `~/.hermes/hermes-agent/` 下的任何核心代码
- **不修改** `config.yaml` 中的 QQ 平台配置段
- **不需要**额外的环境变量
- **不影响**其他平台的流式输出

插件通过运行时 monkey-patch 在 `QQAdapter` 类上添加 `edit_message()` 方法并设置 `SUPPORTS_MESSAGE_EDITING = True`，Hermes 的 gateway 检测到这个标志后自动启用流式消费。

## 架构详解

### 总体架构

```
┌─────────────────────────────────────────────────────────────┐
│                    Hermes Gateway                           │
│                                                             │
│  ┌──────────────┐    ┌────────────────┐    ┌────────────┐  │
│  │ LLM Response │───>│ StreamConsumer │───>│  QQAdapter │  │
│  │ (streaming)  │    │ (accumulates)  │    │ (patched)  │  │
│  └──────────────┘    └────────────────┘    └─────┬──────┘  │
│                                                   │         │
│  ┌──────────────┐    ┌────────────────┐          │         │
│  │ _stream_states│<───│ _send_stream_ │<─────────┘         │
│  │ (per-msg ctx) │    │   chunk()     │                    │
│  └──────────────┘    └────────────────┘                    │
│                           │                                │
│                           ▼                                │
│                    ┌──────────────┐                        │
│                    │ QQ API POST │                        │
│                    │ /v2/.../msgs│                        │
│                    └──────┬───────┘                        │
└───────────────────────────┼────────────────────────────────┘
                            │
                    ┌───────▼───────┐
                    │  QQ 开放平台  │
                    │  (websocket)  │
                    └───────┬───────┘
                            │
                    ┌───────▼───────┐
                    │  QQ 客户端    │
                    │ (打字机效果)  │
                    └───────────────┘
```

### 生命周期

一个完整的流式消息从 LLM 到 QQ 客户端经过以下阶段：

```
Phase 1: 首次发送 (cursor 触发)
────────
[LLM 思考中] → [Gateway 检测到 █ 光标] → _patched_send()
  → POST /v2/users/{openid}/messages (state=1, reset=False, no stream.id)
  ← 服务器返回 stream_msg_id
  → 记录到 _stream_states[stream_msg_id] = {chat_id, chat_type, msg_seq, delta_index, last_full}

Phase 2: 中间更新 (多次)
────────
[LLM 输出更多文本] → [StreamConsumer 触发 edit_message(finalize=False)]
  → _patched_edit_message()
  → _delta() 计算新增文本
  → POST ... (state=1, reset=False, id=stream_msg_id, index=N)
  → 更新 _stream_states: delta_index++, msg_seq++, last_full

Phase 3: 终结
────────
[LLM 输出完毕] → [StreamConsumer 触发 edit_message(finalize=True)]
  → _patched_edit_message()
  → POST ... (state=10, reset=True, id=stream_msg_id, index=1)
  → 清除 _stream_states[stream_msg_id]
```

### 核心组件

#### 1. `_stream_states` (模块级 dict)

```
{
  "<server_stream_msg_id>": {
    "chat_id": str,        # QQ 的 openid 或 group_openid
    "chat_type": str,      # "c2c" 或 "group"
    "msg_seq": int,        # 递增的消息序号
    "delta_index": int,    # 下一个 delta chunk 的 index
    "last_full": str,      # 上次发送时的完整文本（用于计算 delta）
  }
}
```

每个正在流式输出的消息有一个条目。终结后立即 `pop` 清理。

#### 2. `_send_stream_chunk()` — 底层 API 调用

直接调用 `QQAdapter._api_request("POST", path, body)`，复用已有的 token 管理和 HTTP 客户端。

构建的请求体格式：

```json
{
  "msg_type": 2,           // 2=markdown, 0=纯文本
  "markdown": {
    "content": "增量文本"   // 中间 chunk 只发增量，终结 chunk 发全量
  },
  "msg_seq": 1,
  "stream": {
    "state": 1,            // 1=进行中, 10=终结
    "id": null,            // 首次为 null，后续为服务器返回的 stream id
    "index": 0,            // chunk 序号
    "reset": false         // 中间=false, 终结=true
  }
}
```

路径根据聊天类型不同：
- 私聊：`/v2/users/{openid}/messages`
- 群聊：`/v2/groups/{group_openid}/messages`

#### 3. `_patched_send()` — 首次消息拦截

- 检查内容末尾是否有光标字符（`▉` / `█`）
- 没有光标 → 走原始的 `_original_send()`，普通消息，无流式
- 有光标 → 剥离光标 → 发送首次 stream chunk（state=1, id=None）→ 记录返回的 stream_msg_id

**为什么拦截 `send` 而不是 `edit_message`？** 因为 Hermes 的 stream consumer 流程是：
1. 先 `send()` 发第一条消息（带光标标记）
2. 后续每次更新调用 `edit_message()` 修改它

#### 4. `_patched_edit_message()` — 中间/终结消息

Hermes 的 `edit_message(chat_id, message_id, content, finalize=False)` 每次传的都是**完整累积文本**。但 QQ 的 `state=1, reset=False` 是**追加模式**（append），每次都传完整文本会导致内容重复叠加。

**核心桥接逻辑：**

```python
# Hermes 的 edit_message 传 full_text，我们只发增量
delta_text = _delta(full_text, state["last_full"])
```

- 中间（`finalize=False`）：计算 delta，只发送新增部分
- 终结（`finalize=True`）：发送全量文本 + `state=10, reset=True`，QQ 客户端用全量替换掉之前追加的内容

#### 5. `_delta()` — 增量计算

```python
def _delta(new_text: str, old_text: str) -> str:
    if not old_text:
        return new_text
    if new_text.startswith(old_text):
        return new_text[len(old_text):]
    return new_text  # fallback: LLM 重写了开头，无法安全计算 delta
```

- 正常情况：新文本以旧文本开头 → 截取新增部分
- 异常情况：LLM 中途重写了前面的内容（很少发生）→ 直接传新文本，QQ 端可能有轻微重复但不丢数据

#### 6. `_has_cursor()` / `_strip_cursor()` — 光标处理

Hermes 的 StreamConsumer 会在流式输出末尾追加一个 `▉` 光标字符（或 `█`）来标记"正在输出中"。

- `_has_cursor(content)`：检测末尾是否有光标
- `_strip_cursor(content)`：去掉光标字符，保留干净文本
- 终结时也剥离光标，确保最终显示的是完整消息

### 关键设计决策

| 决策 | 原因 |
|------|------|
| **Monkey-patch QQAdapter，不继承** | 插件系统限制：无法替换 gateway 中已实例化的 adapter 实例 |
| **`_send_stream_chunk` 用 adapter._api_request** | 复用已有的 token 刷新、HTTP 客户端和 base URL |
| **首次调用不传 `msg_id`** | QQ API 要求：第一次发 stream 时不传，服务器返回 stream id |
| **input_state 用字符串** | QQ API 严格区分类型，传整数会 500 |
| **中间 chunk 只发增量 delta** | 避免 QQ 客户端追加模式下的内容重复叠加 |
| **终结 chunk 发全量 + reset=true** | QQ 的 reset 操作会用全量替换掉所有追加内容 |
| **仅光标消息走流式** | 非光标消息（如工具调用提示）走普通发送，避免无意义地创建流 |

### 与 Hermes StreamConsumer 的交互

Hermes 的 `GatewayStreamConsumer` 在 `SUPPORTS_MESSAGE_EDITING=True` 时会：

1. 收集 LLM 的增量输出到 `_accumulated_text`
2. 以节流（throttle）频率调用 `send()` → 我们的 `_patched_send()`
3. 后续每次输出时调用 `edit_message(finalize=False)` → 我们的 `_patched_edit_message()`
4. 最终调用 `edit_message(finalize=True)` → 终结消息

非流式消息（工具调用提示、操作反馈）不经过 stream consumer，直接用原始 `send()`，不受影响。

### 模块状态管理

```
_original_send     ── 保存 QQAdapter.send 的原始引用（用于 fallback）
_stream_states     ── 模块级 dict，存活跃流的状态（终结后自动清理）
_DEFAULT_API_TIMEOUT ── 从 QQAdapter 的常量读取，复用已有配置
_CURSORS           ── 编译好的正则，匹配光标字符
```

## 验证

重启 gateway 后在 QQ 中发送消息，观察是否有打字机效果（文字逐个/逐块出现）。

```bash
hermes gateway restart
```

检查日志确认插件加载：

```bash
grep "QQ Bot streaming loaded" ~/.hermes/logs/agent.log
# 应输出: QQ Bot streaming loaded: delta-based append on messages endpoint
```

## 故障排查

如果 QQ 客户端收到重复内容（消息反复从头开始），说明 delta 计算失败，检查日志中 `QQ stream` 相关行。

如果收到 500 错误，说明 API 端点或 body 格式不对——检查 QQ 开放平台文档是否有 API 变更。

日志级别设为 DEBUG 可看到每个 chunk 的详细信息：

```yaml
# config.yaml
logging:
  level: DEBUG
```

DEBUG 日志示例：
```
QQ stream: idx=0 state=1 reset=False id=(none) len=15
QQ stream: idx=1 state=1 reset=False id=xxx len=8
QQ stream: idx=2 state=10 reset=True id=xxx len=200
QQ stream DONE: server_id=xxx chunks=3
```