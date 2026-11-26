"""回调代理的关键不变量测试。"""

import json
import tempfile
import unittest
from pathlib import Path

from callback_proxy import CallbackProxy, RerouteNotEligible, run_scenario


class CallbackProxyTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.state_path = Path(self.tmp.name) / "proxy-state.json"
        self.proxy = CallbackProxy(self.state_path)

    def tearDown(self):
        self.tmp.cleanup()

    def _sub(self, **overrides):
        record = {
            "sub": "s",
            "project": "p",
            "kinds": ["create", "release"],
            "cut_at": 1,
            "target": "one",
            "revision": 1,
        }
        record.update(overrides)
        return self.proxy.subscribe(record)

    def _event(self, seq=1, object_="x", kind="create", after=None):
        return self.proxy.add_event(
            {"project": "p", "seq": seq, "object": object_, "kind": kind, "after": after or []}
        )

    def test_examples_follow_context_and_historical_targets(self):
        root = Path(__file__).parent / "examples"

        first = run_scenario(root / "new-filter-needs-context.jsonl", state_path=Path(self.tmp.name) / "a.json")
        s1 = first["final_inspection"]["subscriptions"]["s"]
        self.assertEqual([(n["notification"], n["status"]) for n in s1["notifications"]], [(1, "done")])

        second = run_scenario(
            root / "old-dispatch-before-endpoint-change.jsonl",
            state_path=Path(self.tmp.name) / "b.json",
        )
        s2 = second["final_inspection"]["subscriptions"]["s"]
        self.assertEqual(
            [(n["notification"], n["status"], n["revision"], n["target"]) for n in s2["notifications"]],
            [(1, "done", 1, "old"), (2, "sent", 2, "new")],
        )

    def test_retry_keeps_notification_revision_target_and_event_identity(self):
        self._sub(kinds=["create"], target="old", revision=1)
        self._event(1, kind="create")
        first = self.proxy.poll("s")["dispatched"][0]
        self.assertEqual((first["notification"], first["revision"], first["target"]), (1, 1, "old"))
        self.assertEqual(first["event"], "p#1")

        self.proxy.receive_receipt(
            {"sub": "s", "target": "old", "notification": 1, "state": "failed"}
        )
        retry = self.proxy.poll("s")["dispatched"][0]
        self.assertEqual((retry["notification"], retry["revision"], retry["target"], retry["event"]), (1, 1, "old", "p#1"))
        self.assertTrue(retry["duplicate"])

        report = self.proxy.inspect("s")
        notification = report["notifications"][0]
        self.assertEqual(notification["attempt_count"], 2)
        self.assertEqual(notification["id"], 1)
        self.assertEqual(notification["identity"], "s:1")

    def test_accepted_old_target_blocks_migration_until_not_processed(self):
        self._sub(kinds=["create"], target="old")
        self._event(1, kind="create")
        self.proxy.poll("s")
        self.proxy.receive_receipt(
            {"sub": "s", "target": "old", "notification": 1, "state": "accepted"}
        )

        with self.assertRaises(RerouteNotEligible):
            self.proxy.reroute("s", 1, "new")

        # poll 不能把 accepted 的同一通知再发给任何端。
        poll = self.proxy.poll("s")
        self.assertEqual(poll["dispatched"], [])
        self.assertEqual(poll["skipped"][0]["reason"], "accepted_awaiting_conclusion")

        self.proxy.receive_receipt(
            {"sub": "s", "target": "old", "notification": 1, "state": "not_processed"}
        )
        rerouted = self.proxy.reroute("s", 1, "new")["dispatched"][0]
        self.assertEqual((rerouted["notification"], rerouted["revision"], rerouted["target"]), (1, 1, "new"))

        # 旧端迟到的 done 是陈旧目标回执，不能把新端正在处理的通知标成 done。
        conflict = self.proxy.receive_receipt(
            {"sub": "s", "target": "old", "notification": 1, "state": "done"}
        )
        self.assertEqual(conflict["conflict"], "stale_target_receipt_after_takeover")
        report = self.proxy.inspect("s")
        self.assertEqual(report["notifications"][0]["status"], "sent")

        self.proxy.receive_receipt(
            {"sub": "s", "target": "new", "notification": 1, "state": "done"}
        )
        self.assertEqual(self.proxy.inspect("s")["notifications"][0]["status"], "done")

    def test_duplicate_receipt_does_not_complete_another_notification(self):
        self._sub(kinds=["create"])
        self._event(1, object_="a")
        self._event(2, object_="b")
        self.proxy.poll("s")
        self.proxy.poll("s")
        notifications = self.proxy.inspect("s")["notifications"]
        self.assertEqual([n["notification"] for n in notifications], [1, 2])

        first = self.proxy.receive_receipt(
            {"sub": "s", "target": "one", "notification": 1, "state": "done"}
        )
        repeated = self.proxy.receive_receipt(
            {"sub": "s", "target": "one", "notification": 1, "state": "done"}
        )
        self.assertFalse(first["duplicate"])
        self.assertTrue(repeated["duplicate"])
        states = [(n["notification"], n["status"]) for n in self.proxy.inspect("s")["notifications"]]
        self.assertEqual(states, [(1, "done"), (2, "sent")])

    def test_missing_context_blocks_dependency_but_summary_unlocks(self):
        self._sub(kinds=["release"], cut_at=1)
        self._event(1, kind="create")
        self._event(2, kind="release", after=[1])

        poll = self.proxy.poll("s")
        self.assertEqual(poll["dispatched"], [])
        self.assertEqual(poll["blocked"][0]["code"], "missing_initial_context")
        self.assertEqual(poll["blocked"][0]["missing"], [1])

        report = self.proxy.inspect("s")
        self.assertEqual(report["blocked_object_names"], ["x"])
        self.assertTrue(report["blocked_objects"][0]["items"][0]["can_send_initial_state"])

        self.proxy.declare_summary(
            {"sub": "s", "target": "one", "object": "x", "through": 1, "body": "secret"}
        )
        summary = self.proxy.inspect("s")["summaries"][-1]
        self.assertNotIn("body", summary)
        self.assertTrue(summary["has_body"])

        poll = self.proxy.poll("s")
        self.assertEqual(len(poll["dispatched"]), 1)
        self.assertEqual(poll["dispatched"][0]["notification"], 1)

    def test_initial_state_notification_must_complete_before_dependency(self):
        self._sub(kinds=["release"], cut_at=1)
        self._event(1, kind="create")
        self._event(2, kind="release", after=[1])
        self.proxy.poll("s")

        opened = self.proxy.request_initial_state("s", "x", through=1, send=True)
        ctx_dispatch = opened["poll"]["dispatched"][0]
        self.assertEqual(ctx_dispatch["category"], "initial_state")
        self.assertEqual(ctx_dispatch["notification"], 1)

        # 仅 accepted 还不能放行语义依赖；必须有真实处理完成。
        self.proxy.receive_receipt(
            {"sub": "s", "target": "one", "notification": 1, "state": "accepted"}
        )
        self.assertEqual(self.proxy.poll("s")["dispatched"], [])
        self.proxy.receive_receipt(
            {"sub": "s", "target": "one", "notification": 1, "state": "done"}
        )
        release_poll = self.proxy.poll("s")
        self.assertEqual([d["notification"] for d in release_poll["dispatched"]], [2])
        self.assertEqual(release_poll["dispatched"][0]["event"], "p#2")

    def test_explicitly_excluded_history_is_not_sent_as_dependency(self):
        self._sub(kinds=["release"], cut_at=1)
        self._event(1, kind="create")
        self._event(2, kind="release", after=[1])
        self.proxy.poll("s")
        # 即使强制 poll，门控也不自动生成 create 事件通知。
        forced = self.proxy.poll("s", ignore_gate=True)
        # ignore_gate is an explicit test/admin escape; normal poll above proved block.
        self.assertEqual(forced["dispatched"][0]["event"], "p#2")
        kinds_sent = [n["event_kind"] for n in self.proxy.inspect("s")["notifications"]]
        self.assertNotIn("create", kinds_sent)

    def test_permanent_predecessor_blocks_only_same_object_and_termination(self):
        self._sub(kinds=["create", "release"])
        self._event(1, object_="x", kind="create")
        self._event(2, object_="x", kind="release", after=[1])
        self._event(3, object_="y", kind="create")
        self.proxy.poll("s")  # sends x create n1 and y create n2; x release blocked
        self.proxy.receive_receipt(
            {"sub": "s", "target": "one", "notification": 1, "state": "permanent_failed"}
        )

        poll = self.proxy.poll("s")
        codes = {(b["object"], b["code"]) for b in poll["blocked"]}
        self.assertIn(("x", "predecessor_permanent_failed"), codes)
        # y 的通知仍可继续；这里 n2 已在首次 poll 发出。
        self.assertEqual([d["notification"] for d in poll["dispatched"]], [])

        report = self.proxy.inspect("s")
        x_group = next(b for b in report["blocked_objects"] if b["object"] == "x")
        self.assertEqual(x_group["state"], "hard_blocked")

        terminated = self.proxy.terminate_chain("s", "x", reason="gap accepted")
        self.assertIn("p#2", terminated["terminated"])
        self.assertIn(1, terminated["already_terminal"])
        report = self.proxy.inspect("s")
        x_group = next(b for b in report["blocked_objects"] if b["object"] == "x")
        self.assertEqual(x_group["state"], "terminated")
        self.assertEqual(report["status_counts"].get("terminated"), 1)

    def test_restart_preserves_cursor_revisions_and_receipt_state(self):
        self._sub(kinds=["create"], target="old")
        self._event(1, kind="create")
        self.proxy.poll("s")
        self.proxy.receive_receipt(
            {"sub": "s", "target": "old", "notification": 1, "state": "accepted"}
        )
        self.proxy.subscribe(
            {"sub": "s", "project": "p", "kinds": ["release"], "cut_at": 2, "target": "new", "revision": 2}
        )
        self.proxy.restart()

        reloaded = CallbackProxy(self.state_path)
        report = reloaded.inspect("s")
        self.assertEqual(report["source_cursor"], 1)
        self.assertEqual(report["current_revision"], 2)
        self.assertEqual(report["notifications"][0]["status"], "accepted")
        poll = reloaded.poll("s")
        self.assertEqual(poll["dispatched"], [])
        self.assertEqual(poll["skipped"][0]["reason"], "accepted_awaiting_conclusion")

    def test_restart_retries_unacknowledged_send_but_keeps_accepted_state(self):
        self._sub(kinds=["create"], target="old")
        self._event(1, kind="create")
        self.proxy.poll("s")
        self.proxy.restart()
        report = self.proxy.inspect("s")
        self.assertEqual(report["notifications"][0]["status"], "failed")
        self.assertTrue(report["notifications"][0]["retryable"])
        retry = self.proxy.poll("s")["dispatched"][0]
        self.assertEqual((retry["notification"], retry["revision"], retry["target"]), (1, 1, "old"))
        self.assertTrue(retry["duplicate"])

    def test_cut_at_is_explicit_inclusive_boundary(self):
        self._event(1, kind="create")
        self._event(2, kind="release")
        # 后建订阅并显式从 2 开始：不能送出 seq1。
        self.proxy.subscribe(
            {"sub": "later", "project": "p", "kinds": ["create", "release"], "cut_at": 2, "target": "t", "revision": 1}
        )
        self.proxy.poll("later")
        events = [n["event"] for n in self.proxy.inspect("later")["notifications"]]
        self.assertEqual(events, ["p#2"])

    def test_inspect_keeps_per_notification_results_and_eligibility(self):
        self._sub(kinds=["create"])
        self._event(1, kind="create")
        self.proxy.poll("s")
        report = self.proxy.inspect("s")
        self.assertIn("revisions", report)
        self.assertEqual(len(report["revisions"]), 1)
        self.assertIn("reroute_candidates", report)
        self.assertFalse(report["all_done"])
        self.assertGreaterEqual(report["counts"]["sent"], 1)

    def test_state_file_is_json_serializable(self):
        self._sub()
        self._event()
        self.proxy.poll("s")
        json.dumps(self.proxy.to_dict(), ensure_ascii=False)

    def test_local_synthetic_endpoint_detects_duplicate_identity(self):
        from local_http import HttpTransport, SyntheticCallbackService

        service = SyntheticCallbackService()
        try:
            transport = HttpTransport({"one": service.target_url("one")})
            proxy = CallbackProxy(
                Path(self.tmp.name) / "http-state.json", transport=transport
            )
            proxy.subscribe(
                {
                    "sub": "s",
                    "project": "p",
                    "kinds": ["create"],
                    "cut_at": 1,
                    "target": "one",
                    "revision": 1,
                }
            )
            proxy.add_event({"project": "p", "seq": 1, "object": "x", "kind": "create", "after": []})
            proxy.poll("s")
            proxy.receive_receipt(
                {"sub": "s", "target": "one", "notification": 1, "state": "failed"}
            )
            proxy.poll("s")
            self.assertEqual(len(service.deliveries), 2)
            self.assertFalse(service.deliveries[0]["duplicate"])
            self.assertTrue(service.deliveries[1]["duplicate"])
            self.assertEqual(service.deliveries[1]["identity"], "s:1")
        finally:
            service.close()


if __name__ == "__main__":
    unittest.main()
