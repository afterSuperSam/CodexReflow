"""监测 Codex 最终断流失败，通过桌面工具向原会话发送继续。"""

from __future__ import annotations

import argparse
import contextlib
import json
import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path
import re
import sqlite3
import subprocess
import sys
import time


VERSION = "1.0.1"
ROOT = Path(__file__).resolve().parent
UUID_PATTERN = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
TURN_PATTERN = re.compile(r'(?:turn_id|turn\.id)="?(' + UUID_PATTERN + r')')
CONFIGURATION_SCHEMA = {
    "codex_home": (str, None, None),
    "desktop_pipe": (str, None, None),
    "caller_thread_id": (str, None, None),
    "error_text": (str, None, None),
    "message": (str, None, None),
    "poll_seconds": ((int, float), 0.5, 60),
    "delay_seconds": ((int, float), 1, 600),
    "cooldown_seconds": ((int, float), 1, 3600),
    "max_continues": (int, 1, 10),
    "send_timeout_seconds": ((int, float), 1, 60),
    "confirmation_timeout_seconds": ((int, float), 5, 300),
    "excluded_threads": (list, None, None),
}
DATABASE_SCHEMA = {
    "logs_2.sqlite": {"logs": {"id", "ts", "level", "target", "thread_id", "feedback_log_body"}},
    "state_5.sqlite": {"threads": {"id", "rollout_path", "source", "thread_source", "archived"}},
    "thread_history_1.sqlite": {
        "thread_turns": {"thread_id", "turn_id", "status", "error_json", "rollout_ordinal", "completed_at", "first_user_item_id"},
        "thread_items": {"thread_id", "item_id", "item_json"},
    },
    "queue_1.sqlite": {"queued_items": {"id", "thread_id", "created_at_ms"}},
}


class MonitorError(Exception):
    pass


def load_configuration(path: Path) -> dict:
    # 配置结构、范围和会话标识校验。
    configuration = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(configuration, dict) or set(configuration) != set(CONFIGURATION_SCHEMA):
        raise MonitorError("配置字段不完整或包含未知字段，请参照 config.example.json。")
    for name, (kind, minimum, maximum) in CONFIGURATION_SCHEMA.items():
        value = configuration[name]
        if isinstance(value, bool) or not isinstance(value, kind):
            raise MonitorError(f"配置项 {name} 的类型错误。")
        if isinstance(value, str) and not value.strip():
            raise MonitorError(f"配置项 {name} 不能为空。")
        if minimum is not None and not minimum <= value <= maximum:
            raise MonitorError(f"配置项 {name} 必须在 {minimum} 到 {maximum} 之间。")
    if any(not isinstance(item, str) or not re.fullmatch(UUID_PATTERN, item)
           for item in configuration["excluded_threads"]):
        raise MonitorError("excluded_threads 必须是会话 UUID 的数组。")
    if not re.fullmatch(UUID_PATTERN, configuration["caller_thread_id"]):
        raise MonitorError("caller_thread_id 必须是授权本程序的会话 UUID。")
    return configuration


def atomic_json(path: Path, value: dict) -> None:
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding="utf-8", newline="\n") as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2, allow_nan=False)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


class CodexStore:
    # 只读访问日志、历史与队列，兼容旧会话的存储 ID。
    def __init__(self, home: Path):
        self.home = home

    def query(self, database: str, statement: str, parameters=()) -> list[dict]:
        with contextlib.closing(sqlite3.connect(
            (self.home / database).resolve().as_uri() + "?mode=ro", uri=True, timeout=1
        )) as connection:
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA query_only = ON")
            return [dict(row) for row in connection.execute(statement, parameters).fetchall()]

    def validate(self) -> None:
        for database, tables in DATABASE_SCHEMA.items():
            for table, required in tables.items():
                actual = {row["name"] for row in self.query(database, f"PRAGMA table_info({table})")}
                if not required <= actual:
                    raise MonitorError(f"Codex 数据结构不兼容：{database}/{table}；已停止发送。")

    def last_log_id(self) -> int:
        return self.query("logs_2.sqlite", "SELECT coalesce(max(id), 0) AS latest FROM logs")[0]["latest"]

    def errors(self, after: int, through: int, error_text: str) -> list[dict]:
        return self.query("logs_2.sqlite", """
            SELECT id, thread_id, feedback_log_body FROM logs
            WHERE id > ? AND id <= ? AND level IN ('WARN', 'ERROR')
              AND target IN ('codex_core::responses_retry', 'codex_core::codex')
              AND instr(feedback_log_body, ?) > 0 AND thread_id IS NOT NULL
            ORDER BY id
        """, (after, through, error_text))

    def thread(self, identifier: str) -> dict | None:
        rows = self.query("state_5.sqlite", """
            SELECT id, rollout_path, source, thread_source, archived FROM threads WHERE id = ?
        """, (identifier,))
        if not rows:
            return None
        thread = rows[0]
        if thread["archived"] or thread["source"] not in ("vscode", "cli"):
            return None
        if thread["thread_source"] not in (None, "user"):
            return None
        identifiers = re.findall(UUID_PATTERN, thread["rollout_path"].lower())
        thread["storage_id"] = identifiers[-1] if identifiers else identifier
        return thread

    def latest_turn(self, thread: dict) -> dict | None:
        rows = self.query("thread_history_1.sqlite", """
            SELECT thread_id, turn_id, status, error_json, completed_at, first_user_item_id
            FROM thread_turns WHERE thread_id IN (?, ?)
            ORDER BY rollout_ordinal DESC LIMIT 1
        """, (thread["id"], thread["storage_id"]))
        return rows[0] if rows else None

    def queued(self, thread: dict) -> list[dict]:
        return self.query("queue_1.sqlite", """
            SELECT id, created_at_ms FROM queued_items WHERE thread_id IN (?, ?)
        """, (thread["id"], thread["storage_id"]))

    def message(self, turn: dict) -> str | None:
        rows = self.query("thread_history_1.sqlite", """
            SELECT item_json FROM thread_items WHERE thread_id = ? AND item_id = ?
        """, (turn["thread_id"], turn["first_user_item_id"]))
        if not rows:
            return None
        item = json.loads(rows[0]["item_json"])
        return "".join(part.get("text", "") for part in item.get("content", []) if part.get("type") == "text").strip()


def desktop_request(configuration: dict, operation: str, identifier: str | None = None) -> dict:
    result = subprocess.run(
        [sys.executable, "-X", "utf8", str(ROOT / "desktop.py")],
        input=json.dumps({"operation": operation, "desktop_pipe": configuration["desktop_pipe"],
                          "caller_thread_id": configuration["caller_thread_id"],
                          "thread_id": identifier, "message": configuration["message"]}),
        capture_output=True, encoding="utf-8", errors="strict",
        timeout=configuration["send_timeout_seconds"],
        creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
    )
    response = json.loads(result.stdout) if result.stdout.strip() else {}
    if result.returncode or response.get("success") is not True:
        detail = response.get("error") or " ".join(
            item.get("text", "") for item in response.get("contentItems", []) if item.get("type") == "inputText"
        ) or "desktop tool rejected request"
        logging.error("desktop_request_failed operation=%s thread=%s exit=%s detail=%s", operation, identifier,
                      result.returncode, detail[-1000:])
        raise MonitorError("无法调用 Codex 桌面会话工具，请确认 Codex 已打开；详情见运行日志。")
    return response


def send_message(configuration: dict, identifier: str) -> dict:
    response = desktop_request(configuration, "send", identifier)
    logging.info("desktop_message_accepted thread=%s", identifier)
    return response


class Monitor:
    # 失败判定、发送去重、限流与送达确认。
    def __init__(self, configuration: dict, store: CodexStore, state_path: Path, sender=send_message):
        self.configuration = configuration
        self.store = store
        self.state_path = state_path
        self.sender = sender
        self.cursor = store.last_log_id()
        self.candidates: dict[tuple[str, str], dict] = {}
        self.state = {"version": 1, "threads": {}}
        if state_path.exists():
            self.state = json.loads(state_path.read_text(encoding="utf-8"))
            if self.state.get("version") != 1 or not isinstance(self.state.get("threads"), dict):
                raise MonitorError("运行状态文件损坏，请检查 runtime/state.json；已停止发送。")
        self.started_at = time.time()
        self.last_event = "正在监测新出现的断流错误。"

    def save(self) -> None:
        atomic_json(self.state_path, self.state)

    def record(self, identifier: str) -> dict:
        return self.state["threads"].setdefault(identifier, {
            "count": 0, "last_sent_at": 0, "handled": [], "pending": None, "blocked": None,
        })

    def handle(self, record: dict, turn_id: str) -> None:
        record["handled"] = (record["handled"] + [turn_id])[-100:]

    def reconcile(self, now: float) -> None:
        for identifier, record in self.state["threads"].items():
            thread = self.store.thread(identifier)
            if thread is None:
                continue
            turn = self.store.latest_turn(thread)
            if turn is None:
                continue
            pending = record["pending"]
            if pending:
                if turn["turn_id"] != pending["failed_turn"]:
                    message = self.store.message(turn)
                    if message is None:
                        logging.info("thread_progress_observed thread=%s turn=%s status=%s", identifier, turn["turn_id"], turn["status"])
                        self.last_event = "已观察到原会话开始新一轮。"
                    elif message == self.configuration["message"].strip():
                        logging.info("continuation_observed thread=%s turn=%s status=%s", identifier, turn["turn_id"], turn["status"])
                        self.last_event = "已观察到原会话收到继续并开始新一轮。"
                    else:
                        logging.info("user_progress_observed thread=%s turn=%s", identifier, turn["turn_id"])
                        self.last_event = "会话已有新的用户输入。"
                    record["pending"] = None
                    self.save()
                elif now - pending["sent_at"] >= self.configuration["confirmation_timeout_seconds"]:
                    record["blocked"] = "消息已提交，但未确认原会话开始新一轮；已暂停自动发送。"
                    record["pending"] = None
                    self.last_event = record["blocked"]
                    logging.error("continuation_unconfirmed thread=%s turn=%s", identifier, pending["failed_turn"])
                    self.save()
            if turn["status"] == "completed" and record["count"] and not record["pending"]:
                record["count"] = 0
                record["blocked"] = None
                logging.info("recovery_counter_reset thread=%s", identifier)
                self.save()

    def tick(self, now: float, stopped=lambda: False) -> None:
        self.reconcile(now)
        latest = self.store.last_log_id()
        if latest < self.cursor:
            logging.warning("log_database_replaced previous=%s current=%s", self.cursor, latest)
            self.cursor = latest
            self.candidates.clear()
            return
        through = min(latest, self.cursor + 10000)
        for event in self.store.errors(self.cursor, through, self.configuration["error_text"]):
            identifier = event["thread_id"]
            match = TURN_PATTERN.search(event["feedback_log_body"])
            if not re.fullmatch(UUID_PATTERN, identifier) or not match:
                continue
            if identifier in self.configuration["excluded_threads"]:
                continue
            key = (identifier, match.group(1))
            self.candidates.setdefault(key, {"failed_since": None})
        self.cursor = through
        for (identifier, turn_id), candidate in list(self.candidates.items()):
            key = (identifier, turn_id)
            thread = self.store.thread(identifier)
            if thread is None:
                del self.candidates[key]
                continue
            turn = self.store.latest_turn(thread)
            if turn is None or (turn["turn_id"] == turn_id and turn["status"] == "inProgress"):
                continue
            record = self.record(identifier)
            if turn["turn_id"] != turn_id or turn["status"] != "failed" or turn_id in record["handled"]:
                del self.candidates[key]
                continue
            error = json.loads(turn["error_json"] or "{}")
            if error.get("message", "").strip() != self.configuration["error_text"]:
                del self.candidates[key]
                continue
            if candidate["failed_since"] is None:
                candidate["failed_since"] = now
                logging.info("terminal_failure_detected thread=%s turn=%s", identifier, turn_id)
            if record["blocked"] or record["pending"] or record["count"] >= self.configuration["max_continues"]:
                if record["count"] >= self.configuration["max_continues"] and not record["blocked"]:
                    record["blocked"] = "连续自动继续已达到上限，等待成功完成或手动解除限制。"
                    self.last_event = record["blocked"]
                    self.save()
                continue
            if now - candidate["failed_since"] < self.configuration["delay_seconds"]:
                continue
            if record["last_sent_at"] and now - record["last_sent_at"] < self.configuration["cooldown_seconds"]:
                continue
            if self.store.queued(thread):
                self.handle(record, turn_id)
                self.save()
                logging.info("skipped_existing_queue thread=%s turn=%s", identifier, turn_id)
                del self.candidates[key]
                continue
            current = self.store.latest_turn(thread)
            if not current or current["turn_id"] != turn_id or current["status"] != "failed" or stopped():
                continue
            self.handle(record, turn_id)
            record["count"] += 1
            record["last_sent_at"] = now
            record["pending"] = {"failed_turn": turn_id, "sent_at": now}
            self.save()
            del self.candidates[key]
            try:
                self.sender(self.configuration, identifier)
            except (MonitorError, subprocess.TimeoutExpired, OSError, ValueError) as error:
                record["pending"] = None
                record["blocked"] = "发送失败或结果不明，该会话已暂停自动发送；不会重复补发。"
                self.last_event = record["blocked"]
                logging.error("dispatch_failed thread=%s kind=%s", identifier, type(error).__name__)
                self.save()
            else:
                self.last_event = "消息已提交，正在确认原会话是否继续。"
                logging.info("continuation_sent thread=%s failed_turn=%s attempt=%s", identifier, turn_id, record["count"])


class InstanceLock:
    # 单实例锁只用于自己的运行目录。
    def __init__(self, path: Path):
        self.path = path
        self.stream = None

    def __enter__(self):
        self.stream = self.path.open("a+b")
        if self.path.stat().st_size == 0:
            self.stream.write(b"0")
            self.stream.flush()
        self.stream.seek(0)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(self.stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self.stream.close()
            self.stream = None
            raise MonitorError("监测程序已经运行。") from None
        return self

    def __exit__(self, *_):
        if self.stream:
            self.stream.close()


def running(runtime: Path) -> bool:
    try:
        with InstanceLock(runtime / "monitor.lock"):
            return False
    except MonitorError:
        return True


def configure_logging(runtime: Path) -> None:
    handler = RotatingFileHandler(runtime / "monitor.log", maxBytes=1_048_576, backupCount=2, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logging.getLogger().setLevel(logging.INFO)
    logging.getLogger().addHandler(handler)


def run_monitor(configuration: dict, runtime: Path) -> None:
    configure_logging(runtime)
    stop_file = runtime / "stop.request"
    with InstanceLock(runtime / "monitor.lock"):
        stop_file.unlink(missing_ok=True)
        store = CodexStore(Path(os.path.expandvars(os.path.expanduser(configuration["codex_home"]))))
        store.validate()
        desktop_request(configuration, "check")
        monitor = Monitor(configuration, store, runtime / "state.json")
        logging.info("monitor_started version=%s pid=%s cursor=%s", VERSION, os.getpid(), monitor.cursor)
        errors = 0
        try:
            while not stop_file.exists():
                try:
                    monitor.tick(time.time(), stop_file.exists)
                    errors = 0
                except sqlite3.OperationalError:
                    errors += 1
                    logging.exception("database_read_failed attempt=%s", errors)
                    if errors >= 3:
                        raise MonitorError("连续三次无法读取 Codex 数据，已停止；请查看日志。") from None
                atomic_json(runtime / "status.json", {
                    "running": True, "pid": os.getpid(), "version": VERSION,
                    "started_at": monitor.started_at, "updated_at": time.time(),
                    "last_event": monitor.last_event, "pending_errors": len(monitor.candidates),
                    "threads": monitor.state["threads"],
                })
                deadline = time.monotonic() + configuration["poll_seconds"]
                while not stop_file.exists() and time.monotonic() < deadline:
                    time.sleep(min(0.2, max(0, deadline - time.monotonic())))
        finally:
            atomic_json(runtime / "status.json", {"running": False, "pid": os.getpid(), "updated_at": time.time()})
            stop_file.unlink(missing_ok=True)
            logging.info("monitor_stopped pid=%s", os.getpid())


def main() -> int:
    parser = argparse.ArgumentParser(description="Codex Reflow v" + VERSION)
    parser.add_argument("command", choices=("start", "stop", "status", "run", "check", "reset"))
    parser.add_argument("--config", type=Path, default=ROOT / "config.json")
    arguments = parser.parse_args()
    configuration = load_configuration(arguments.config) if arguments.command in ("start", "run", "check") else None
    runtime = ROOT / "runtime"
    runtime.mkdir(exist_ok=True)
    if arguments.command == "run":
        run_monitor(configuration, runtime)
    elif arguments.command == "check":
        store = CodexStore(Path(os.path.expandvars(os.path.expanduser(configuration["codex_home"]))))
        store.validate()
        desktop_request(configuration, "check")
        print("配置、Python、Codex 桌面发送接口及只读数据库结构检查通过。")
    elif arguments.command == "start":
        if running(runtime):
            print("监测程序已经运行。")
            return 0
        (runtime / "status.json").unlink(missing_ok=True)
        with (runtime / "launcher.log").open("a", encoding="utf-8") as output:
            process = subprocess.Popen(
                [sys.executable, "-X", "utf8", str(Path(__file__).resolve()), "run", "--config", str(arguments.config.resolve())],
                stdin=subprocess.DEVNULL, stdout=output, stderr=output,
                creationflags=(subprocess.CREATE_NO_WINDOW | subprocess.CREATE_NEW_PROCESS_GROUP) if os.name == "nt" else 0,
                start_new_session=os.name != "nt",
            )
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise MonitorError("启动失败，请查看 runtime/launcher.log。")
            if (runtime / "status.json").exists() and running(runtime):
                print("已在后台启动；双击 stop.cmd 停止。")
                return 0
            time.sleep(0.2)
        raise MonitorError("启动状态尚未确认，请运行 status 查看；不要重复启动。")
    elif arguments.command == "stop":
        if not running(runtime):
            print("监测程序没有运行。")
            return 0
        (runtime / "stop.request").touch()
        deadline = time.monotonic() + 30
        while running(runtime) and time.monotonic() < deadline:
            time.sleep(0.2)
        if running(runtime):
            raise MonitorError("已请求停止，程序仍在结束当前操作；请稍后查看 status。")
        print("已停止监测。")
    elif arguments.command == "status":
        print("状态：" + ("运行中" if running(runtime) else "已停止"))
        path = runtime / "status.json"
        if path.exists():
            status = json.loads(path.read_text(encoding="utf-8"))
            print(status.get("last_event", ""))
            for identifier, record in status.get("threads", {}).items():
                if record["count"] or record["blocked"]:
                    print(f"{identifier}：已自动继续 {record['count']} 次；{record['blocked'] or '正常'}")
        print("日志：" + str(runtime / "monitor.log"))
    elif arguments.command == "reset":
        with InstanceLock(runtime / "monitor.lock"):
            path = runtime / "state.json"
            if path.exists():
                state = json.loads(path.read_text(encoding="utf-8"))
                for record in state["threads"].values():
                    record.update(count=0, blocked=None, pending=None)
                atomic_json(path, state)
        print("已解除自动继续次数限制；历史失败轮次仍保持去重。")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (MonitorError, OSError, ValueError, sqlite3.DatabaseError) as error:
        logging.exception("monitor_failed kind=%s", type(error).__name__)
        print(str(error) if isinstance(error, MonitorError) else "运行失败，请检查配置、文件权限和日志。", file=sys.stderr)
        raise SystemExit(1)
