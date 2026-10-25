"""假 ACP agent(换行分隔 JSON-RPC over stdio):AcpRuntime 传输层与契约测试用。

作为子进程被拉起(``python tests/fake_acp_agent.py``),行为脚本:
- ``initialize`` → ``{"protocolVersion": 1, "agentCapabilities": {}}``;
  ``session/new`` → ``{"sessionId": "fake-sess-N"}``(递增);
- ``session/prompt`` → 依次发 ``session/update`` 通知(tool_call → tool_call_update →
  agent_message_chunk ×2),延迟 ``FAKE_ACP_PROMPT_DELAY`` 秒(默认 0.3,给 stop/steer
  用例留操作窗口)后回 ``{"stopReason": "end_turn"}``;
- prompt 文本含「审批」→ 先发 ``session/request_permission`` 入向请求并等客户端响应:
  outcome=selected 补「审批已通过。」块继续;rejected 只回「操作已被拒绝。」块后完成;
- ``echo``(params{text, delay})→ 延迟后回 ``{"echo": text}``(id 配对/乱序响应测试);
  ``sleep``(params{seconds})→ 沉睡后回(超时测试);``die`` → 立即退出(进程死亡测试,
  请求/通知两种形态都认);
- ``session/cancel`` 等通知接受即弃;未知 method → -32601。

线程模型:主线程读 stdin 派发;每个客户端请求独立线程处理(支持并发 prompt /
乱序响应);我方入向请求的响应经 ``_pending`` + threading.Event 回收;stdout 写加锁。

pytest 侧 import 本模块只用 ``install_fake_agent_settings``(子进程形态不触发;
stdin/stdout 重配置仅发生在 ``__main__`` 分支,不会动 pytest 的捕获流)。
"""

from __future__ import annotations

import json
import os
import sys
import threading
import time
from itertools import count

_write_lock = threading.Lock()
_session_ids = count(1)
_perm_ids = count(1)
_pending: dict = {}  # 我方入向请求 id → [threading.Event, response_frame]
_pending_lock = threading.Lock()


def _prompt_delay() -> float:
    return float(os.environ.get("FAKE_ACP_PROMPT_DELAY", "0.3"))


def _send(frame: dict) -> None:
    with _write_lock:
        sys.stdout.write(json.dumps(frame) + "\n")
        sys.stdout.flush()


def _respond(req_id, result=None, error=None) -> None:
    frame = {"jsonrpc": "2.0", "id": req_id}
    if error is not None:
        frame["error"] = error
    else:
        frame["result"] = result if result is not None else {}
    _send(frame)


def _notify_update(session_id: str, update: dict) -> None:
    _send({
        "jsonrpc": "2.0",
        "method": "session/update",
        "params": {"sessionId": session_id, "update": update},
    })


def _prompt_text(params: dict) -> str:
    return "".join(
        str(block.get("text") or "")
        for block in params.get("prompt") or []
        if isinstance(block, dict)
    )


def _handle_prompt(req_id, params: dict) -> None:
    session_id = str(params.get("sessionId") or "")
    text = _prompt_text(params)
    if "审批" in text:
        perm_id = f"perm-{next(_perm_ids)}"
        decided = threading.Event()
        with _pending_lock:
            _pending[perm_id] = [decided, {}]
        _send({
            "jsonrpc": "2.0",
            "id": perm_id,
            "method": "session/request_permission",
            "params": {
                "sessionId": session_id,
                "toolCall": {"title": "fake_dangerous_tool"},
                "options": [{"optionId": "allow_once"}, {"optionId": "reject_once"}],
            },
        })
        decided.wait(timeout=30)
        with _pending_lock:
            response = _pending.pop(perm_id, [None, {}])[1]
        outcome = ((response.get("result") or {}).get("outcome") or {}).get("outcome")
        if outcome != "selected":
            _notify_update(session_id, {
                "sessionUpdate": "agent_message_chunk",
                "content": {"type": "text", "text": "操作已被拒绝。"},
            })
            _respond(req_id, {"stopReason": "end_turn"})
            return
        _notify_update(session_id, {
            "sessionUpdate": "agent_message_chunk",
            "content": {"type": "text", "text": "审批已通过。"},
        })
    _notify_update(session_id, {
        "sessionUpdate": "tool_call",
        "toolCallId": "tc-1", "title": "fake_tool", "status": "in_progress",
    })
    _notify_update(session_id, {
        "sessionUpdate": "tool_call_update", "toolCallId": "tc-1", "status": "completed",
    })
    _notify_update(session_id, {
        "sessionUpdate": "agent_message_chunk",
        "content": {"type": "text", "text": "fake 回答:"},
    })
    _notify_update(session_id, {
        "sessionUpdate": "agent_message_chunk",
        "content": {"type": "text", "text": text[:80] or "空"},
    })
    time.sleep(_prompt_delay())
    _respond(req_id, {"stopReason": "end_turn"})


def _handle_request(frame: dict) -> None:
    req_id, method = frame.get("id"), frame.get("method")
    params = frame.get("params") or {}
    if method == "initialize":
        _respond(req_id, {"protocolVersion": 1, "agentCapabilities": {}})
    elif method == "session/new":
        # 带 pid:跨进程唯一(同机多个假 agent 并存时 sessionId 不撞车)
        _respond(req_id, {"sessionId": f"fake-sess-{os.getpid()}-{next(_session_ids)}"})
    elif method == "session/prompt":
        _handle_prompt(req_id, params)
    elif method == "echo":
        time.sleep(float(params.get("delay") or 0))
        _respond(req_id, {"echo": params.get("text")})
    elif method == "sleep":
        time.sleep(float(params.get("seconds") or 30))
        _respond(req_id, {"slept": True})
    elif method == "die":
        os._exit(0)  # 进程死亡测试:立即退出,EOF 由对端 reader 检测
    else:
        _respond(req_id, error={"code": -32601, "message": f"未知 method: {method}"})


def main() -> None:
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            frame = json.loads(line)
        except ValueError:
            continue
        if "method" in frame and "id" in frame:
            # 客户端请求 → 独立线程处理(并发 prompt / 乱序响应)
            threading.Thread(target=_handle_request, args=(frame,), daemon=True).start()
        elif "method" in frame:
            if frame.get("method") == "die":  # 通知形态的 die 也认
                os._exit(0)
            continue  # session/cancel 等通知:接受即弃
        elif "id" in frame:
            # 我方入向请求(request_permission)的响应
            with _pending_lock:
                entry = _pending.get(frame["id"])
            if entry is not None:
                entry[1].update(frame)
                entry[0].set()


# --- 测试装配助手(pytest 侧 import 用;子进程形态不触发) ------------------------------


def install_fake_agent_settings(monkeypatch, *, timeout: float = 10.0) -> None:
    """把 acp_* 配置指向本脚本(当前 venv 解释器 + posix 路径;acp_args 经 shlex 拆分)。"""
    from app.config import get_settings

    settings = get_settings()
    monkeypatch.setattr(settings, "acp_command", sys.executable)
    monkeypatch.setattr(settings, "acp_args", os.path.abspath(__file__).replace("\\", "/"))
    monkeypatch.setattr(settings, "acp_cwd", "")
    monkeypatch.setattr(settings, "acp_timeout_seconds", timeout)


if __name__ == "__main__":
    # 仅子进程形态重配置编码(pytest import 时绝不动捕获流)
    sys.stdin.reconfigure(encoding="utf-8")
    sys.stdout.reconfigure(encoding="utf-8", newline="\n")
    main()
