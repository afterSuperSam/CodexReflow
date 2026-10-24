"""连接 Codex 自带桌面工具管道；由主程序的子进程超时约束调用。"""

import json
import os
import struct
import sys
import uuid


PIPE_PREFIX = "\\\\.\\pipe\\"
MAX_FRAME_BYTES = 8 * 1024 * 1024


def discover_pipe(setting: str) -> str:
    if setting != "auto":
        if not setting.startswith(PIPE_PREFIX + "codex-app-tools-"):
            raise ValueError("invalid desktop pipe path")
        return setting
    names = {name for name in os.listdir(PIPE_PREFIX) if name.startswith("codex-app-tools-")}
    inherited = os.environ.get("CODEX_APP_TOOLS_PIPE_PATH", "")
    if inherited.startswith(PIPE_PREFIX) and inherited[len(PIPE_PREFIX):] in names:
        return inherited
    if len(names) != 1:
        raise RuntimeError("desktop pipe unavailable or ambiguous")
    return PIPE_PREFIX + names.pop()


def read_exact(connection, size: int) -> bytes:
    chunks = []
    while size:
        chunk = connection.read(size)
        if not chunk:
            raise EOFError("desktop pipe closed before response")
        chunks.append(chunk)
        size -= len(chunk)
    return b"".join(chunks)


def request(pipe: str, method: str, parameters: dict) -> dict:
    payload = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": parameters}).encode("utf-8")
    if len(payload) > MAX_FRAME_BYTES:
        raise ValueError("desktop request too large")
    with open(pipe, "r+b", buffering=0) as connection:
        frame = struct.pack("<I", len(payload)) + payload
        while frame:
            written = connection.write(frame)
            if not written:
                raise EOFError("desktop pipe closed during request")
            frame = frame[written:]
        size = struct.unpack("<I", read_exact(connection, 4))[0]
        if size > MAX_FRAME_BYTES:
            raise ValueError("desktop response too large")
        response = json.loads(read_exact(connection, size))
    if response.get("id") != 1 or response.get("jsonrpc") != "2.0":
        raise ValueError("invalid desktop response")
    if "error" in response:
        raise RuntimeError("desktop RPC error: " + response["error"].get("message", "unknown"))
    return response["result"]


def call_parameters(caller: str, tool: str, arguments: dict) -> dict:
    uuid.UUID(caller)
    return {
        "callerSource": "codex", "namespace": "codex_app", "tool": tool,
        "hostId": "local", "threadId": caller,
        "callId": "auto-continue-" + str(uuid.uuid4()),
        "turnId": "auto-continue-" + str(uuid.uuid4()), "arguments": arguments,
    }


def main() -> None:
    data = json.load(sys.stdin)
    pipe = discover_pipe(data["desktop_pipe"])
    if data["operation"] == "check":
        result = request(pipe, "tools/list", {"threadStartKind": "all"})
        available = any(tool.get("name") == "send_message_to_thread" and tool.get("namespace") == "codex_app"
                        for tool in result.get("tools", []))
        if not available:
            raise RuntimeError("desktop continuation tool unavailable")
        print(json.dumps({"success": True, "transport": "codex_app"}))
        return
    identifier = data["thread_id"]
    uuid.UUID(identifier)
    if data["operation"] == "read":
        tool = "read_thread"
        arguments = {"threadId": identifier, "hostId": "local", "turnLimit": 1,
                     "includeOutputs": False, "maxOutputCharsPerItem": 0}
    elif data["operation"] == "send":
        tool = "send_message_to_thread"
        arguments = {"threadId": identifier, "hostId": "local", "prompt": data["message"]}
    else:
        raise ValueError("unknown operation")
    result = request(pipe, "tools/call", call_parameters(data["caller_thread_id"], tool, arguments))
    print(json.dumps(result, ensure_ascii=False))
    if result.get("success") is not True:
        raise SystemExit(1)


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, RuntimeError, EOFError, KeyError) as error:
        print(json.dumps({"success": False, "error": str(error)}, ensure_ascii=False))
        raise SystemExit(1)
