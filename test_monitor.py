"""用独立 SQLite 样本验证监测规则，不向真实会话发送消息。"""

import contextlib
import io
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest import mock

from desktop import call_parameters, read_exact
from monitor import CodexStore, Monitor, MonitorError, ROOT, load_configuration, main


THREAD = "11111111-1111-4111-8111-111111111111"
STORAGE = "22222222-2222-4222-8222-222222222222"
TURN = "33333333-3333-4333-8333-333333333333"
NEXT_TURN = "44444444-4444-4444-8444-444444444444"


class MonitorTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.home = Path(self.directory.name)
        example = json.loads((ROOT / "config.example.json").read_text(encoding="utf-8"))
        example.update(caller_thread_id=STORAGE, delay_seconds=10, cooldown_seconds=60, confirmation_timeout_seconds=30)
        configuration_path = self.home / "config.json"
        configuration_path.write_text(json.dumps(example), encoding="utf-8")
        self.configuration = load_configuration(configuration_path)
        schemas = {
            "logs_2.sqlite": "CREATE TABLE logs (id INTEGER PRIMARY KEY, ts INTEGER, level TEXT, target TEXT, thread_id TEXT, feedback_log_body TEXT)",
            "state_5.sqlite": "CREATE TABLE threads (id TEXT, rollout_path TEXT, source TEXT, thread_source TEXT, archived INTEGER)",
            "thread_history_1.sqlite": "CREATE TABLE thread_turns (thread_id TEXT, turn_id TEXT, status TEXT, error_json TEXT, rollout_ordinal INTEGER, completed_at INTEGER, first_user_item_id TEXT); CREATE TABLE thread_items (thread_id TEXT, item_id TEXT, item_json TEXT)",
            "queue_1.sqlite": "CREATE TABLE queued_items (id TEXT, thread_id TEXT, created_at_ms INTEGER)",
        }
        for database, statement in schemas.items():
            with contextlib.closing(sqlite3.connect(self.home / database)) as connection:
                connection.executescript(statement)
        self.execute("state_5.sqlite", "INSERT INTO threads VALUES (?, ?, 'vscode', 'user', 0)", (THREAD, f"rollout-{THREAD}_{STORAGE}.jsonl"))
        self.store = CodexStore(self.home)
        self.store.validate()
        self.sent = []
        self.monitor = Monitor(self.configuration, self.store, self.home / "state.json", self.send)

    def execute(self, database, statement, parameters=()):
        with contextlib.closing(sqlite3.connect(self.home / database)) as connection:
            connection.execute(statement, parameters)
            connection.commit()

    def send(self, configuration, identifier):
        self.sent.append((identifier, configuration["message"]))

    def warning(self, turn=TURN, target="codex_core::responses_retry"):
        body = f"turn.id={turn}: stream disconnected - retrying sampling request (1/5)... " + self.configuration["error_text"]
        self.execute("logs_2.sqlite", "INSERT INTO logs VALUES (NULL, 100, 'WARN', ?, ?, ?)", (target, THREAD, body))

    def turn(self, identifier=TURN, status="failed", ordinal=1, message=None, error=None):
        self.execute("thread_history_1.sqlite", "DELETE FROM thread_turns WHERE turn_id = ?", (identifier,))
        self.execute("thread_history_1.sqlite", "INSERT INTO thread_turns VALUES (?, ?, ?, ?, ?, 100, ?)",
                     (STORAGE, identifier, status, json.dumps({"message": error or self.configuration["error_text"]}), ordinal, identifier))
        if message is not None:
            self.execute("thread_history_1.sqlite", "INSERT INTO thread_items VALUES (?, ?, ?)",
                         (STORAGE, identifier, json.dumps({"type": "userMessage", "content": [{"type": "text", "text": message}]})))

    def trigger(self, now=100):
        self.warning()
        self.turn()
        self.monitor.tick(now)
        self.monitor.tick(now + 10)

    def test_internal_retries_do_not_send(self):
        self.warning()
        self.turn(status="inProgress")
        self.monitor.tick(100)
        self.monitor.tick(1000)
        self.assertEqual(self.sent, [])

    def test_failed_turn_waits_and_uses_original_thread_id(self):
        self.warning()
        self.turn()
        self.monitor.tick(100)
        self.monitor.tick(109)
        self.assertEqual(self.sent, [])
        self.monitor.tick(110)
        self.assertEqual(self.sent, [(THREAD, "继续")])

    def test_duplicate_warnings_and_restart_do_not_resend(self):
        self.trigger()
        self.warning()
        self.monitor.tick(112)
        restarted = Monitor(self.configuration, self.store, self.home / "state.json", self.send)
        self.warning()
        restarted.tick(113)
        restarted.tick(140)
        self.assertEqual(len(self.sent), 1)

    def test_user_continuation_cancels_waiting_message(self):
        self.warning()
        self.turn()
        self.monitor.tick(100)
        self.turn(NEXT_TURN, "inProgress", 2, "继续")
        self.monitor.tick(111)
        self.assertEqual(self.sent, [])

    def test_interrupted_successful_and_other_error_turns_are_ignored(self):
        for status, error in (("interrupted", None), ("completed", None), ("failed", "another error")):
            with self.subTest(status=status):
                self.warning()
                self.turn(status=status, error=error)
                self.monitor.tick(100)
                self.monitor.tick(200)
        self.assertEqual(self.sent, [])

    def test_old_logs_are_not_replayed(self):
        self.warning()
        self.turn()
        fresh = Monitor(self.configuration, self.store, self.home / "state.json", self.send)
        fresh.tick(200)
        fresh.tick(300)
        self.assertEqual(self.sent, [])

    def test_existing_user_queue_prevents_dispatch(self):
        self.execute("queue_1.sqlite", "INSERT INTO queued_items VALUES ('queued', ?, 100)", (STORAGE,))
        self.trigger()
        self.assertEqual(self.sent, [])

    def test_queued_is_not_reported_as_started(self):
        self.trigger()
        self.monitor.tick(141)
        self.assertIsNotNone(self.monitor.record(THREAD)["blocked"])
        self.assertEqual(len(self.sent), 1)

    def test_original_thread_message_confirms_continuation(self):
        self.trigger()
        self.turn(NEXT_TURN, "inProgress", 2, "继续")
        self.monitor.tick(112)
        self.assertIsNone(self.monitor.record(THREAD)["pending"])
        self.assertIn("新一轮", self.monitor.last_event)

    def test_cooldown_limit_and_success_reset(self):
        record = self.monitor.record(THREAD)
        record.update(count=2, last_sent_at=90)
        self.trigger(100)
        self.assertEqual(self.sent, [])
        self.monitor.tick(150)
        self.assertEqual(len(self.sent), 1)
        self.turn(NEXT_TURN, "failed", 2, "继续")
        self.warning(NEXT_TURN)
        self.monitor.tick(160)
        self.monitor.tick(300)
        self.assertEqual(len(self.sent), 1)
        self.assertIsNotNone(record["blocked"])
        self.turn(NEXT_TURN, "completed", 2)
        self.monitor.tick(301)
        self.assertEqual(record["count"], 0)
        self.assertIsNone(record["blocked"])

    def test_uncertain_dispatch_is_not_retried(self):
        def fail(*_):
            raise MonitorError("发送结果不明")
        self.monitor.sender = fail
        self.trigger()
        self.assertIsNotNone(self.monitor.record(THREAD)["blocked"])
        self.assertIn(TURN, self.monitor.record(THREAD)["handled"])
        self.monitor.sender = self.send
        self.warning()
        self.monitor.tick(200)
        self.assertEqual(self.sent, [])

    def test_archived_excluded_and_subagent_threads_are_ignored(self):
        for column, value in (("archived", 1), ("thread_source", "subagent")):
            with self.subTest(column=column):
                self.execute("state_5.sqlite", f"UPDATE threads SET {column} = ?", (value,))
                self.trigger()
                self.execute("state_5.sqlite", "UPDATE threads SET archived = 0, thread_source = 'user'")
        self.configuration["excluded_threads"] = [THREAD]
        self.trigger()
        self.assertEqual(self.sent, [])

    def test_quoted_error_in_tool_output_is_not_a_trigger(self):
        self.warning(target="codex_core::tools::context")
        self.turn()
        self.monitor.tick(100)
        self.monitor.tick(200)
        self.assertEqual(self.sent, [])

    def test_stop_prevents_dispatch(self):
        self.warning()
        self.turn()
        self.monitor.tick(100)
        self.monitor.tick(110, lambda: True)
        self.assertEqual(self.sent, [])

    def test_incompatible_database_is_rejected(self):
        self.execute("queue_1.sqlite", "DROP TABLE queued_items")
        with self.assertRaises(MonitorError):
            self.store.validate()

    def test_invalid_configuration_is_rejected(self):
        path = self.home / "config.json"
        for key, value in (("max_continues", True), ("poll_seconds", 0), ("excluded_threads", ["not-a-uuid"]), ("unexpected", 1)):
            configuration = dict(self.configuration)
            configuration[key] = value
            path.write_text(json.dumps(configuration), encoding="utf-8")
            with self.subTest(key=key), self.assertRaises(MonitorError):
                load_configuration(path)

    def test_desktop_continuation_without_user_item_confirms_new_turn(self):
        self.trigger()
        self.turn(NEXT_TURN, "inProgress", 2)
        self.execute("thread_history_1.sqlite", "UPDATE thread_turns SET first_user_item_id = NULL WHERE turn_id = ?", (NEXT_TURN,))
        self.monitor.tick(112)
        self.assertIsNone(self.monitor.record(THREAD)["pending"])
        self.assertIsNone(self.monitor.record(THREAD)["blocked"])
        self.assertIn("新一轮", self.monitor.last_event)

    def test_published_fast_defaults_are_valid_and_cool_down_for_five_seconds(self):
        example = json.loads((ROOT / "config.example.json").read_text(encoding="utf-8"))
        example["caller_thread_id"] = STORAGE
        path = self.home / "fast-config.json"
        path.write_text(json.dumps(example), encoding="utf-8")
        self.monitor.configuration = load_configuration(path)
        self.assertEqual(self.monitor.configuration["cooldown_seconds"], 5)
        record = self.monitor.record(THREAD)
        record.update(count=1, last_sent_at=100)
        self.warning()
        self.turn()
        self.monitor.tick(100)
        self.monitor.tick(104.9)
        self.assertEqual(self.sent, [])
        self.monitor.tick(105)
        self.assertEqual(self.sent, [(THREAD, "继续")])

    def test_invalid_config_does_not_prevent_stopping_or_reading_status(self):
        path = self.home / "config.json"
        path.write_text("invalid configuration", encoding="utf-8")
        for command in ("stop", "status"):
            with self.subTest(command=command), mock.patch("monitor.ROOT", self.home), \
                    mock.patch("sys.argv", ["monitor.py", command, "--config", str(path)]), \
                    contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(main(), 0)


class DesktopTransportTests(unittest.TestCase):
    def test_fragmented_utf8_frame_and_premature_close(self):
        class FragmentedStream(io.BytesIO):
            def read(self, size):
                return super().read(min(size, 1))
        payload = "继续".encode("utf-8")
        self.assertEqual(read_exact(FragmentedStream(payload), len(payload)).decode("utf-8"), "继续")
        with self.assertRaises(EOFError):
            read_exact(FragmentedStream(payload[:-1]), len(payload))

    def test_caller_and_target_are_distinct_and_settings_not_overridden(self):
        arguments = {"threadId": THREAD, "hostId": "local", "prompt": "继续"}
        request = call_parameters(STORAGE, "send_message_to_thread", arguments)
        self.assertEqual(request["threadId"], STORAGE)
        self.assertEqual(request["arguments"]["threadId"], THREAD)
        self.assertEqual(request["arguments"], arguments)


if __name__ == "__main__":
    unittest.main()
