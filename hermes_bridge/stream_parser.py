"""Normalize Claude Code `--output-format stream-json` lines into bridge events.

The raw stream stays in the attempt's stdout log; only compact, redacted facts
become events. Thinking blocks are never recorded and partial chunks only count
as liveness, so the event table reports observed actions rather than progress
guesses.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Optional

TEXT_LIMIT = 1000
_TOOL_INPUT_KEYS = ("command", "file_path", "path", "pattern", "url", "query", "description")


@dataclass
class Parsed:
    events: list[tuple[str, dict]] = field(default_factory=list)
    last_action: Optional[str] = None
    claude_session_id: Optional[str] = None
    result: Optional[dict] = None
    unparsable: bool = False


def _clip(value: str, limit: int = TEXT_LIMIT) -> str:
    return value if len(value) <= limit else value[:limit] + f"…[{len(value) - limit} more chars]"


def _tool_input_summary(tool_input: Any) -> dict:
    if not isinstance(tool_input, dict):
        return {}
    summary = {k: _clip(str(tool_input[k]), 300) for k in _TOOL_INPUT_KEYS if k in tool_input}
    return summary or {"keys": sorted(tool_input)[:10]}


def _tool_result_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return " ".join(b.get("text", "") for b in content if isinstance(b, dict))
    return ""


def _parse_assistant(obj: dict, out: Parsed) -> None:
    parent = obj.get("parent_tool_use_id")
    for block in (obj.get("message") or {}).get("content") or []:
        kind = block.get("type") if isinstance(block, dict) else None
        if kind == "text" and block.get("text"):
            out.events.append(("assistant.message", {"text": _clip(block["text"]), "parent_tool_use_id": parent}))
        elif kind == "tool_use":
            summary = _tool_input_summary(block.get("input"))
            out.events.append(("tool.use", {
                "tool": block.get("name"),
                "tool_use_id": block.get("id"),
                "input": summary,
                "parent_tool_use_id": parent,
            }))
            detail = next((summary[k] for k in _TOOL_INPUT_KEYS if k in summary), "")
            out.last_action = _clip(f"{block.get('name')}: {detail}".rstrip(": "), 200)


def _parse_user(obj: dict, out: Parsed) -> None:
    content = (obj.get("message") or {}).get("content")
    if not isinstance(content, list):
        return
    for block in content:
        if isinstance(block, dict) and block.get("type") == "tool_result":
            out.events.append(("tool.result", {
                "tool_use_id": block.get("tool_use_id"),
                "is_error": bool(block.get("is_error")),
                "output": _clip(_tool_result_text(block.get("content")), 400),
            }))


def parse_line(line: bytes) -> Parsed:
    out = Parsed()
    try:
        obj = json.loads(line)
    except ValueError:
        out.unparsable = True
        return out
    if not isinstance(obj, dict):
        out.unparsable = True
        return out

    if obj.get("session_id"):
        out.claude_session_id = obj["session_id"]
    kind = obj.get("type")
    if kind == "system":
        subtype = obj.get("subtype")
        if subtype == "init":
            out.events.append(("claude.init", {
                "session_id": obj.get("session_id"),
                "model": obj.get("model"),
                "permission_mode": obj.get("permissionMode"),
                "version": obj.get("claude_code_version"),
                "tool_count": len(obj.get("tools") or []),
            }))
            out.last_action = "Claude Code started"
        elif subtype not in (None, "status"):
            out.events.append(("claude.system", {"subtype": subtype}))
    elif kind == "assistant":
        _parse_assistant(obj, out)
    elif kind == "user":
        _parse_user(obj, out)
    elif kind == "result":
        out.result = {
            "subtype": obj.get("subtype"),
            "is_error": bool(obj.get("is_error")),
            "summary": obj.get("result") if isinstance(obj.get("result"), str) else None,
            "errors": obj.get("errors") or [],
            "num_turns": obj.get("num_turns"),
            "duration_ms": obj.get("duration_ms"),
            "total_cost_usd": obj.get("total_cost_usd"),
            "permission_denials": obj.get("permission_denials") or [],
            "session_id": obj.get("session_id"),
        }
        out.events.append(("claude.result", {
            "subtype": obj.get("subtype"),
            "is_error": bool(obj.get("is_error")),
            "num_turns": obj.get("num_turns"),
            "total_cost_usd": obj.get("total_cost_usd"),
        }))
        out.last_action = f"Claude Code finished: {obj.get('subtype')}"
    return out
