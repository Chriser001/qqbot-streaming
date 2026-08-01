# QQ Bot Streaming Plugin (qqbot-streaming)

为 Hermes Agent 的 QQ Bot 通道添加原生打字机流式输出效果。

**v1.1.0 迁移说明**：插件已从**未公开的 `stream` 字段协议**（打在普通 `/messages` 接口上）迁移到**官方 `stream_messages` 接口**（`POST /v2/users/{openid}/stream_messages`，2026-07 正式公开文档化，独立 50 QPS 配额）。群聊无官方流式接口，回退普通发送。

## 背景

- 旧方案（v1.0.0）：在普通 `/v2/users/{openid}/messages` 上带私有 `stream` 字段实现流式，**官方文档从未公开**，属于无背书的私有行为。
- 新方案（v1.1.0）：官方正式发布的 `stream_messages` 接口，字段全部文档化（`input_mode` / `input_state` / `index` / `content_type` / `content_raw` / `stream_msg_id`），且**默认就是 append 语义**，与旧协议的增量追加逻辑 1:1 对应，迁移成本极低。

## 原理

QQ 的流式输出和其他平台（Telegram/Discord 的 `edit_message` 模式）**完全不同**：

| 平台 | 方式 | API |
|------|------|-----|
| Telegram/Discord | 发消息 → 编辑消息（全量替换） | sendMessage + editMessageText |
| **QQ Bot** | 官方流式接口，分片追加 | `/v2/users/{openid}/stream_messages` |

官方 `stream_messages` 协议（`input_mode="append"` 默认模式）：
- `input_state=1` + `content_raw=<增量>`：**追加**增量文本（每个 chunk 只发新增内容）
- `input_state=10`：**终结**，把剩余增量发完并标记完成
- `stream_msg_id`：首片不传（服务端生成并返回），后续分片必须携带上一分片返回的 id
- `index`：分片序号，从 0 递增
- 每个 chunk 的 `msg_seq` 递增（**msg_seq 必须严格单调递增**，否则被去重）

**群聊不支持流式**：官方没有 `/v2/groups/{group_openid}/stream_messages` 接口，群消息自动回退普通发送（无打字机效果，但消息可靠）。

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
│                    │ /v2/users/..│                        │
│                    │ /stream_msg │                        │
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
  → POST /v2/users/{openid}/stream_messages (input_state=1, index=0, 不带 stream_msg_id)
  ← 服务器返回 stream id
  → 记录到 _stream_states[server_id] = {chat_id, msg_seq, delta_index, last_full}

Phase 2: 中间更新 (多次)
────────
[LLM 输出更多文本] → [StreamConsumer 触发 edit_message(finalize=False)]
  → _patched_edit_message()
  → _delta() 计算新增文本
  → POST /v2/users/{openid}/stream_messages (input_state=1, content_raw=增量, stream_msg_id=server_id, index=N)
  → 更新 _stream_states: delta_index++, msg_seq++, last_full

Phase 3: 终结
────────
[LLM 输出完毕] → [StreamConsumer 触发 edit_message(finalize=True)]
  → _patched_edit_message()
  → POST /v2/users/{openid}/stream_messages (input_state=10, content_raw=剩余增量, stream_msg_id=server_id)
  → 清除 _stream_states[server_id]
```

### 核心组件

#### 1. `_stream_states` (模块级 dict)

```
{
  "<server_stream_msg_id>": {
    "chat_id": str,        # QQ 的 openid
    "chat_type": str,      # "c2c"
    "msg_seq": int,        # 单调递增的消息序号（插件自行维护，不依赖 adapter）
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
  "msg_seq": 1,
  "msg_id": "ROBOT1.0_xxx",  // 被动回复必带！官方示例每个分片都带，缺了返回 500
  "input_mode": "append",    // append=增量追加（默认），replace=全量替换
  "input_state": 1,          // 1=进行中, 10=终结
  "index": 0,                // chunk 序号，从 0 递增
  "content_type": "markdown",// 或 "text"
  "content_raw": "增量文本",  // append 模式下只发增量
  "stream_msg_id": null      // 首次不传，后续为服务器返回的 stream id
}
```

**坑**：`msg_id` 字段在文档里标"否"（可选），但**被动回复不带它会返回 500 internal server error**（2026-08-01 实测）。所有分片（含首片）都要带上触发消息的 msg_id。首片从 `reply_to` 取，后续分片从 `_stream_states` 里取。

路径固定为 C2C 流式接口：
- 私聊：`/v2/users/{openid}/stream_messages`

#### 3. `_patched_send()` — 首次消息拦截

- 检查内容末尾是否有光标字符（`▉` / `█`）
- 没有光标 → 走原始的 `_original_send()`，普通消息，无流式
- 群聊 → 走原始的 `_original_send()`（官方无群流式接口，回退普通发送）
- 有光标 → 剥离光标 → 发送首次 stream chunk（input_state=1, index=0, 不带 stream_msg_id）→ 记录返回的 stream id

**为什么拦截 `send` 而不是 `edit_message`？** 因为 Hermes 的 stream consumer 流程是：
1. 先 `send()` 发第一条消息（带光标标记）
2. 后续每次更新调用 `edit_message()` 修改它

#### 4. `_patched_edit_message()` — 中间/终结消息

Hermes 的 `edit_message(chat_id, message_id, content, finalize=False)` 每次传的都是**完整累积文本**。但 QQ 官方流式接口默认是 **append 模式**（追加），每次都传完整文本会导致内容重复叠加。

**核心桥接逻辑：**

```python
# Hermes 的 edit_message 传 full_text，我们只发增量
delta_text = _delta(full_text, state["last_full"])
```

- 中间（`finalize=False`）：计算 delta，只发送新增部分（`input_state=1`）
- 终结（`finalize=True`）：发送剩余 delta（`input_state=10`），服务端把增量拼完并标记流结束
- **LLM 中途重写开头**（`_delta()` 无法安全计算，返回全量）：自动切换 `input_mode="replace"` 发全量，服务端替换 Pending 内容而不是追加，不会重复（旧协议此场景会有重复）

#### 5. `_delta()` — 增量计算

```python
def _delta(new_text: str, old_text: str) -> str:
    if not old_text:
        return new_text
    if new_text.startswith(old_text):
        return new_text[len(old_text):]
    return new_text  # fallback: LLM 重写了开头 → 上层切 replace 模式发全量
```

- 正常情况：新文本以旧文本开头 → 截取新增部分
- 异常情况：LLM 中途重写了前面的内容（很少发生）→ 返回全量，上层用 `replace` 模式发送（不会重复）

#### 6. `_has_cursor()` / `_strip_cursor()` — 光标处理

Hermes 的 StreamConsumer 会在流式输出末尾追加一个 `▉` 光标字符（或 `█`）来标记"正在输出中"。

- `_has_cursor(content)`：检测末尾是否有光标
- `_strip_cursor(content)`：去掉光标字符，保留干净文本
- 终结时也剥离光标，确保最终显示的是完整消息

#### 7. `_next_plugin_seq()` — 单调 msg_seq 生成器

替代 QQAdapter 的 `_next_msg_seq()`（基于 `time XOR random`，非单调），插件自行维护单调计数器：

```python
_msg_seq_counter = 0
def _next_plugin_seq() -> int:
    global _msg_seq_counter
    _msg_seq_counter = (_msg_seq_counter + 1) % 65536
    return _msg_seq_counter
```

**原因**：adapter 的 `_next_msg_seq()` 是随机生成的 0-65535 值，在同一个秒内可能和后续的 `_send_c2c_text()` / `_build_text_body()` 调用撞车，导致 QQ 返回"消息被去重"。插件自行维护单调计数器后彻底避免此问题。

### 关键设计决策

| 决策 | 原因 |
|------|------|
| **Monkey-patch QQAdapter，不继承** | 插件系统限制：无法替换 gateway 中已实例化的 adapter 实例 |
| **`_send_stream_chunk` 用 adapter._api_request** | 复用已有的 token 刷新、HTTP 客户端和 base URL |
| **首片不传 `stream_msg_id`** | 官方协议：第一条由服务端生成并返回，后续分片携带 |
| **默认 `input_mode="append"` 发增量** | 官方默认语义，与旧协议 delta-append 一致，避免内容重复叠加 |
| **重写场景切 `replace` 模式** | LLM 重写开头时无法算 delta，replace 让服务端替换 Pending，不重复 |
| **终结片发剩余 delta + `input_state=10`** | 服务端自行维护 Pending 拼接，最后一片只补剩余增量并标记完成 |
| **仅光标消息走流式** | 非光标消息（如工具调用提示）走普通发送，避免无意义地创建流 |
| **插件自己维护 msg_seq** | 避免和 adapter 的随机 msg_seq 碰撞导致去重 |
| **群聊回退普通发送** | 官方无群聊流式接口，回退保证消息可靠 |

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
_msg_seq_counter   ── 插件级单调 msg_seq 计数器
_DEFAULT_API_TIMEOUT ── 从 QQAdapter 的常量读取，复用已有配置
_CURSORS           ── 编译好的正则，匹配光标字符
```

## 已知问题和修复

### Bug 1: msg_seq 随机碰撞 → 消息被去重

**症状**：日志中出现 `消息被去重，请检查请求msgseq`，消息被切成两段。

**根因**：首次流式 chunk 使用 adapter 的 `_next_msg_seq()` 生成 msg_seq。该函数基于 `time XOR random`，不是单调计数器。当流式消息的 finalize 完成后 `_stream_states` 被 pop，后续 StreamConsumer 对同一条消息的编辑触发 state NOT FOUND → 重新创建 state 时生成新 msg_seq → 和之前 `_send_c2c_text()` 或 `_build_text_body()` 已用掉的 msg_seq 撞车。

**修复**：插件自行维护 `_msg_seq_counter`，streaming 路径全部使用单调递增值，彻底隔离。

### Bug 2（v1.1.0 已解决）: Markdown 流式 chunk 缺少尾部 \n

**旧症状**：日志中出现 `流式消息md分片需要\n结束`，delta 发送失败。

**旧根因**：老 `stream` 私有协议要求每个 markdown chunk 以 `\n` 结尾。

**解决**：迁移到官方 `stream_messages` 接口后，官方协议无此要求，补 `\n` 逻辑已移除（append 模式下补 `\n` 反而会在文本中间插入多余换行）。

### Bug 3: 已终结流的重复编辑 → state NOT FOUND → 30s 重试风暴

**症状**：日志中 `state NOT FOUND` + 持续重试（len 从 83 涨到 442），最终 fallback 发第二条消息。

**根因**：StreamConsumer 在流终结后（input_state=10 成功，state 已 pop）仍可能发送额外的 `edit_message()` 调用。原代码在 state NOT FOUND 时尝试重建 state，重建的 msg_seq 又与已有值碰撞。

**修复**：state NOT FOUND 时直接返回 `SendResult(success=True)`，静默跳过不重试。

## 客户端兼容性

| 客户端 | 效果 | 备注 |
|--------|------|------|
| **手机 QQ (iOS/Android)** | ✅ 打字机效果正常 | 完整显示所有内容 |
| **PC QQ (旧版本)** | ⚠️ 输出可能不完整 | 客户端版本过老时截断，建议更新 QQ 到最新版 |
| **QQ NT (新版 PC QQ)** | ✅ 正常 | |

## 验证

重启 gateway 后在 QQ 中发送消息，观察是否有打字机效果（文字逐个/逐块出现）。

```bash
hermes gateway restart
```

检查日志确认插件加载：

```bash
grep "QQ Bot streaming loaded" ~/.hermes/logs/gateway.log
# 应输出: QQ Bot streaming loaded: official stream_messages API (C2C)
```

## 故障排查

| 现象 | 可能原因 | 检查 |
|------|---------|------|
| 消息被切成两段 | msg_seq 去重碰撞 | 日志中搜 `消息被去重` |
| 打字机效果中途停止 | 服务端 Pending 与本地 last_full 不一致（重写场景） | 日志中搜 `DELTA FAILED` 或 `40007` |
| 流式未启动（一次性发出） | 消息末尾无光标 `▉`/`█` | 检查 StreamConsumer 配置 |
| 群聊无打字机效果 | 官方不支持群流式，回退普通发送 | 正常行为，非故障 |
| PC QQ 显示不完整 | 客户端版本过旧 | 升级到最新版 QQ |
| finalize 相关错误 | state NOT FOUND | 日志中搜 `state NOT FOUND` |

日志级别设为 DEBUG 可看到每个 chunk 的详细信息：

```yaml
# config.yaml
logging:
  level: DEBUG
```

DEBUG 日志示例：
```
QQ stream: idx=0 state=1 mode=append id=(none) len=15
QQ stream: idx=1 state=1 mode=append id=xxx len=8
QQ stream: idx=2 state=10 mode=append id=xxx len=30
QQ stream DONE: server_id=xxx chunks=3
```
