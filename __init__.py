"""
QQ Bot Streaming (Typewriter) Plugin for Hermes Agent.

Adds native QQ typewriter streaming to the existing QQ Bot adapter.

QQ streaming is APPEND-based (not replace-based like Telegram):
  - state=1, reset=False: APPENDS the new delta chunk
  - state=10, reset=True: FINAL full-content replacement
  - Each chunk's markdown.content is only the NEW incremental text

This is fundamentally different from Hermes's edit_message() which sends
the FULL accumulated text each time. We bridge by tracking last-sent content
and sending only the delta.
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
    # Send as-is; QQ may show some duplication but won't lose data.
    return new_text


async def _send_stream_chunk(
    adapter,
    chat_type: str,
    chat_id: str,
    content: str,
    stream_id: Optional[str],
    msg_seq: int,
    index: int,
    state: int,
    *,
    reset: bool = False,
) -> SendResult:
    use_markdown = getattr(adapter, "_markdown_support", True)

    body: Dict[str, Any] = {
        "msg_seq": msg_seq,
        "stream": {
            "state": state,
            "id": stream_id,
            "index": index,
            "reset": reset,
        },
    }
    if use_markdown:
        body["msg_type"] = 2
        body["markdown"] = {"content": content}
    else:
        body["msg_type"] = 0
        body["content"] = content

    path = (
        f"/v2/groups/{chat_id}/messages"
        if chat_type == "group"
        else f"/v2/users/{chat_id}/messages"
    )

    try:
        data = await adapter._api_request("POST", path, body, timeout=_DEFAULT_API_TIMEOUT)
        new_id = str(data.get("id", "")) if data else ""
        logger.debug(
            "QQ stream: idx=%d state=%d reset=%s id=%s len=%d",
            index, state, reset, new_id or "(none)", len(content),
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
    msg_seq = self._next_msg_seq(chat_id)
    displayed = _strip_cursor(content)

    logger.info("QQ stream START: chat=%s type=%s len=%d", chat_id, chat_type, len(displayed))
    result = await _send_stream_chunk(
        self, chat_type, chat_id, displayed,
        stream_id=None, msg_seq=msg_seq, index=0, state=_STREAMING,
    )

    if result.success and result.message_id:
        server_id = result.message_id
        _stream_states[server_id] = {
            "chat_id": chat_id,
            "chat_type": chat_type,
            "msg_seq": msg_seq + 1,
            "delta_index": 1,        # next delta chunk index
            "last_full": displayed,   # last accumulated full text sent
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
        logger.debug("QQ edit: stream state not found for %s, bridging", message_id)
        chat_type = self._guess_chat_type(chat_id)
        state = {
            "chat_id": chat_id, "chat_type": chat_type,
            "msg_seq": self._next_msg_seq(chat_id),
            "delta_index": 0, "last_full": "",
        }
        _stream_states[message_id] = state

    full_text = _strip_cursor(content)

    if finalize:
        # Final: state=10, reset=True, index=1, FULL content replaces everything
        result = await _send_stream_chunk(
            self,
            chat_type=state["chat_type"],
            chat_id=state["chat_id"],
            content=full_text,
            stream_id=message_id,
            msg_seq=state["msg_seq"],
            index=1,
            state=_FINISH,
            reset=True,
        )
        if result.success:
            _stream_states.pop(message_id, None)
            logger.info("QQ stream DONE: server_id=%s chunks=%d", message_id, state.get("delta_index", 1))
        return result
    else:
        # Intermediate: send ONLY the new delta since last send
        delta_text = _delta(full_text, state["last_full"])
        if not delta_text.strip():
            return SendResult(success=True)  # nothing new to send

        idx = state["delta_index"]
        result = await _send_stream_chunk(
            self,
            chat_type=state["chat_type"],
            chat_id=state["chat_id"],
            content=delta_text,
            stream_id=message_id,
            msg_seq=state["msg_seq"],
            index=idx,
            state=_STREAMING,
        )
        if result.success:
            state["delta_index"] = idx + 1
            state["msg_seq"] += 1
            state["last_full"] = full_text  # track full accumulated text
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
    logger.info("QQ Bot streaming loaded: delta-based append on messages endpoint")
    return True


def check_requirements() -> bool:
    try:
        from gateway.platforms.qqbot.adapter import QQAdapter  # noqa: F401
        return True
    except ImportError:
        return False


def register(ctx):
    _apply_patches()
