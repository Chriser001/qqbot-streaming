"""
QQ Bot Streaming (Typewriter) Plugin for Hermes Agent.

Adds native QQ typewriter streaming to the existing QQ Bot adapter.

Uses the OFFICIAL QQ Bot streaming API (documented 2026-07, docs re-checked
2026-08):
  POST /v2/users/{user_openid}/stream_messages   (C2C only, 50 QPS)

Append-based protocol (input_mode="append" is the documented default):
  - input_state=1, content_raw=<delta>: APPENDS the new delta chunk
  - input_state=10: FINAL — send remaining delta, mark stream complete
  - stream_msg_id: first chunk omits it (server returns id), later chunks
    must carry the id returned by the previous chunk
  - index: chunk sequence number, starting from 0, strictly increasing

This is fundamentally different from Hermes's edit_message() which sends
the FULL accumulated text each time. We bridge by tracking last-sent content
and sending only the delta.

Group chat is NOT supported by the official streaming API (no
/v2/groups/.../stream_messages endpoint exists). Group messages fall back to
the normal non-streaming send path.

Migration note (v1.0.0 -> v1.1.0): the old plugin used the undocumented
`stream` field on POST /v2/users/{openid}/messages. The official endpoint
replaces that with stream_messages + input_state/index/stream_msg_id/
content_raw/content_type. Delta-append semantics are identical, so the
delta-bridging logic is unchanged.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Dict, Optional

from gateway.platforms.base import SendResult

logger = logging.getLogger(__name__)

_STREAMING = 1
_FINISH = 10

_stream_states: Dict[str, Dict[str, Any]] = {}

_original_send = None
_DEFAULT_API_TIMEOUT = 30.0

# Plugin-level monotonic msg_seq counter — avoids collisions with
# the adapter's random-based _next_msg_seq() on API-dedup checks.
_msg_seq_counter = 0

def _next_plugin_seq() -> int:
    global _msg_seq_counter
    _msg_seq_counter = (_msg_seq_counter + 1) % 65536
    return _msg_seq_counter

_CURSORS = re.compile(r"[▉█]$")


def _has_cursor(content: str) -> bool:
    return bool(content and _CURSORS.search(content.rstrip()[-3:]))


def _strip_cursor(content: str) -> str:
    text = content.rstrip()
    text = _CURSORS.sub("", text).rstrip()
    return text if text.strip() else "…"


def _delta(new_text: str, old_text: str) -> str:
    """Return only the new portion of text since last send."""
    if not old_text:
        return new_text
    if new_text.startswith(old_text):
        return new_text[len(old_text):]
    # Can't safely compute delta — text changed from the beginning.
    # This happens when the LLM rewrites earlier content mid-stream.
    # Caller switches to input_mode="replace" in this case so the server
    # swaps its pending buffer instead of appending (no duplication).
    return new_text


async def _send_stream_chunk(
    adapter,
    chat_id: str,
    content: str,
    stream_id: Optional[str],
    msg_seq: int,
    index: int,
    state: int,
    *,
    mode: str = "append",
    msg_id: Optional[str] = None,
) -> SendResult:
    use_markdown = getattr(adapter, "_markdown_support", True)

    body: Dict[str, Any] = {
        "msg_seq": msg_seq,
        "input_mode": mode,   # "append" (delta) or "replace" (full text)
        "input_state": state, # 1 = generating, 10 = generation complete
        "index": index,
        "content_type": "markdown" if use_markdown else "text",
        "content_raw": content,
    }
    if msg_id:
        # Passive reply requires the triggering message ID (or event_id).
        # Official docs show msg_id on EVERY chunk, first included.
        body["msg_id"] = msg_id
    if stream_id:
        # First chunk: omit (server generates and returns the id).
        # Subsequent chunks: must carry the id returned previously.
        body["stream_msg_id"] = stream_id

    path = f"/v2/users/{chat_id}/stream_messages"

    try:
        data = await adapter._api_request("POST", path, body, timeout=_DEFAULT_API_TIMEOUT)
        new_id = str(data.get("id", "")) if data else ""
        logger.debug(
            "QQ stream: idx=%d state=%d mode=%s id=%s len=%d",
            index, state, mode, new_id or "(none)", len(content),
        )
        return SendResult(success=True, message_id=new_id, raw_response=data)
    except Exception as exc:
        logger.error("QQ stream chunk FAILED [%s]: %s", path, exc)
        return SendResult(success=False, error=str(exc), retryable=True)


async def _patched_send(
    self, chat_id: str, content: str,
    reply_to: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> SendResult:
    if not _has_cursor(content):
        return await _original_send(self, chat_id, content, reply_to, metadata)

    chat_type = self._guess_chat_type(chat_id)
    if chat_type == "group":
        # Official streaming API is C2C-only — group chat has no
        # stream_messages endpoint. Fall back to normal send (no
        # typewriter effect), keep the message reliable.
        return await _original_send(self, chat_id, content, reply_to, metadata)

    msg_seq = _next_plugin_seq()
    displayed = _strip_cursor(content)

    logger.info("QQ stream START: chat=%s type=%s len=%d", chat_id, chat_type, len(displayed))
    result = await _send_stream_chunk(
        self, chat_id, displayed,
        stream_id=None, msg_seq=msg_seq, index=0, state=_STREAMING,
        msg_id=reply_to,
    )

    if result.success and result.message_id:
        server_id = result.message_id
        _stream_states[server_id] = {
            "chat_id": chat_id,
            "chat_type": chat_type,
            "msg_seq": msg_seq + 1,
            "delta_index": 1,        # next delta chunk index
            "last_full": displayed,  # last accumulated full text sent
            "msg_id": reply_to,      # passive-reply msg id for subsequent chunks
        }
        logger.info("QQ stream created: server_id=%s", server_id)
        return SendResult(success=True, message_id=server_id)
    else:
        logger.error("QQ stream START failed: %s", result.error)
        return await _original_send(self, chat_id, content, reply_to, metadata)


async def _patched_edit_message(
    self, chat_id: str, message_id: str,
    content: str,
    *, finalize: bool = False,
) -> SendResult:
    state = _stream_states.get(message_id)
    if state is None:
        # Stream already finalized or never created — this edit targets
        # a stale / previous message.  Silently skip; returning success
        # prevents the StreamConsumer from entering a 30s retry storm
        # with ever-growing delta content.
        return SendResult(success=True)

    full_text = _strip_cursor(content)
    delta_text = _delta(full_text, state["last_full"])
    # Delta-based append normally; if the LLM rewrote earlier content
    # _delta() falls back to the full text — in that case use replace mode
    # so the server swaps its pending buffer instead of appending a dup.
    mode = "replace" if delta_text == full_text else "append"

    if finalize:
        # Final: input_state=10, send the remaining delta (or full text
        # when rewritten) and mark the stream complete.
        result = await _send_stream_chunk(
            self,
            chat_id=state["chat_id"],
            content=delta_text,
            stream_id=message_id,
            msg_seq=state["msg_seq"],
            index=state["delta_index"],
            state=_FINISH,
            mode=mode,
            msg_id=state.get("msg_id"),
        )
        if result.success:
            _stream_states.pop(message_id, None)
            logger.info("QQ stream DONE: server_id=%s chunks=%d", message_id, state.get("delta_index", 1))
        else:
            logger.warning("QQ stream FINALIZE FAILED: msg_id=%s idx=%d state=10 — stream may be left hanging", message_id, state.get("delta_index", 1))
        return result
    else:
        if not delta_text.strip():
            return SendResult(success=True)  # nothing new to send

        idx = state["delta_index"]
        result = await _send_stream_chunk(
            self,
            chat_id=state["chat_id"],
            content=delta_text,
            stream_id=message_id,
            msg_seq=state["msg_seq"],
            index=idx,
            state=_STREAMING,
            mode=mode,
            msg_id=state.get("msg_id"),
        )
        if result.success:
            state["delta_index"] = idx + 1
            state["msg_seq"] += 1
            state["last_full"] = full_text  # track full accumulated text
        else:
            logger.warning("QQ stream DELTA FAILED: msg_id=%s idx=%d len=%d — edit_interval=%.1fs may cause retry delay",
                           message_id, idx, len(delta_text), getattr(self, '_current_edit_interval', 0.8))
        return result


# ── Patch ─────────────────────────────────────────────────────────────────

def _apply_patches() -> bool:
    global _original_send, _DEFAULT_API_TIMEOUT
    try:
        from gateway.platforms.qqbot.adapter import QQAdapter
        from gateway.platforms.qqbot.constants import DEFAULT_API_TIMEOUT
    except ImportError as exc:
        logger.warning("QQ Bot adapter unavailable: %s", exc)
        return False

    _DEFAULT_API_TIMEOUT = DEFAULT_API_TIMEOUT
    QQAdapter.SUPPORTS_MESSAGE_EDITING = True
    global _original_send
    _original_send = QQAdapter.send
    QQAdapter.send = _patched_send
    QQAdapter.edit_message = _patched_edit_message
    logger.info("QQ Bot streaming loaded: official stream_messages API (C2C)")
    return True


def check_requirements() -> bool:
    try:
        from gateway.platforms.qqbot.adapter import QQAdapter  # noqa: F401
        return True
    except ImportError:
        return False


def register(ctx):
    _apply_patches()
