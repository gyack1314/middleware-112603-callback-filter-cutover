"""可审计、可恢复的订阅筛选切换回调代理。

本模块只依赖 Python 标准库。场景中的事件、回执、摘要和重启都由本地合成，
不会访问真实回调地址。

核心不变量：

* 订阅修订版本的 ``cut_at`` 是新筛选范围的首个源位置（包含该位置）。
* 已形成的投递工作冻结其订阅修订版本和原始目标；重试保持通知身份不变。
* 新目标只有在旧目标明确回报 ``not_processed`` 后才能接管同一通知。
* 同一对象的依赖通知必须等待先行通知 ``done``，或获得目标持有所需摘要的证据。
* 被筛选条件明确排除的历史事件不会作为“补齐依赖”重放；只能使用摘要或协议级
  初始状态通知。
"""

from __future__ import annotations

import json
import os
import tempfile
import threading
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Iterable

try:  # scenario_reader 与本模块位于同一仓库根目录。
    from scenario_reader import iter_messages
except Exception:  # pragma: no cover - 允许被复制到其他位置时独立导入。
    iter_messages = None  # type: ignore


STATE_VERSION = 1

# 对外统一状态名。raw_state 保留输入中的原始写法，便于审计。
STATUS_UNSENT = "unsent"
STATUS_SENT = "sent"
STATUS_ACCEPTED = "accepted"
STATUS_DONE = "done"
STATUS_FAILED = "failed"
STATUS_TRANSIENT_FAILED = STATUS_FAILED  # 规范名沿用输入契约中的 failed。
STATUS_PERMANENT_FAILED = "permanent_failed"
STATUS_NOT_PROCESSED = "not_processed"
STATUS_TERMINATED = "terminated"
STATUS_SUPERSEDED = "superseded"

ACTIVE_DELIVERY_STATUSES = {STATUS_SENT, STATUS_ACCEPTED}
FAILED_STATUSES = {STATUS_TRANSIENT_FAILED, STATUS_PERMANENT_FAILED}
TERMINAL_STATUSES = {STATUS_DONE, STATUS_PERMANENT_FAILED, STATUS_TERMINATED}

_STATE_ALIASES = {
    "accepted": STATUS_ACCEPTED,
    "accept": STATUS_ACCEPTED,
    "ack": STATUS_ACCEPTED,
    "done": STATUS_DONE,
    "completed": STATUS_DONE,
    "complete": STATUS_DONE,
    "succeeded": STATUS_DONE,
    "success": STATUS_DONE,
    "processed": STATUS_DONE,
    "failed": STATUS_TRANSIENT_FAILED,
    "failure": STATUS_TRANSIENT_FAILED,
    "temporary_failed": STATUS_TRANSIENT_FAILED,
    "temporary_failure": STATUS_TRANSIENT_FAILED,
    "temporary": STATUS_TRANSIENT_FAILED,
    "retryable": STATUS_TRANSIENT_FAILED,
    "error": STATUS_TRANSIENT_FAILED,
    "transient_failed": STATUS_TRANSIENT_FAILED,
    "retry": STATUS_TRANSIENT_FAILED,
    "retryable_failure": STATUS_TRANSIENT_FAILED,
    "nack": STATUS_TRANSIENT_FAILED,
    "permanent_failed": STATUS_PERMANENT_FAILED,
    "permanent_failure": STATUS_PERMANENT_FAILED,
    "permanent": STATUS_PERMANENT_FAILED,
    "rejected": STATUS_PERMANENT_FAILED,
    "dead": STATUS_PERMANENT_FAILED,
    "not_processed": STATUS_NOT_PROCESSED,
    "unprocessed": STATUS_NOT_PROCESSED,
    "not_handled": STATUS_NOT_PROCESSED,
}


class ProxyError(ValueError):
    """所有可预期的代理契约错误。"""


class NotFound(ProxyError):
    """订阅、通知或源事件不存在。"""


class RevisionConflict(ProxyError):
    """订阅修订版本冲突或试图改写历史版本。"""


class RerouteNotEligible(ProxyError):
    """缺少旧端未处理证据，不能把通知交给新目标。"""


def _now(at: Any = None) -> int | float | str:
    return 0 if at is None else at


def _as_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        # 也宽容数字字符串，但不接受布尔值。
        if isinstance(value, str) and value.strip().lstrip("-").isdigit():
            return int(value.strip())
        raise ProxyError(f"{name} 必须是整数，得到 {value!r}")
    return value


def _normalize_state(raw_state: Any, record: dict[str, Any] | None = None) -> tuple[str, bool]:
    """返回规范状态以及是否被显式标记为永久失败。"""
    if not isinstance(raw_state, str):
        raise ProxyError("receipt.state 必须是字符串")
    key = raw_state.strip().lower().replace("-", "_").replace(" ", "_")
    permanent_flag: bool | None = None
    if record is not None:
        if "permanent" in record:
            permanent_flag = bool(record["permanent"])
        elif "permanent_failure" in record:
            permanent_flag = bool(record["permanent_failure"])
        elif str(record.get("failure_kind", "")).lower() == "permanent":
            permanent_flag = True
        elif str(record.get("error_class", record.get("class", ""))).lower() in {"permanent", "permanent_failure"}:
            permanent_flag = True
        elif record.get("retryable") is False:
            permanent_flag = True
        elif record.get("retryable") is True:
            permanent_flag = False
        elif record.get("temporary") is True or record.get("transient") is True:
            permanent_flag = False
        elif record.get("final") is True and key in {"failed", "failure"}:
            permanent_flag = True
        code = record.get("status_code", record.get("http_status"))
        if permanent_flag is None and isinstance(code, int):
            if 400 <= code < 500 and code not in {408, 429}:
                permanent_flag = True
            elif code >= 500 or code in {408, 429}:
                permanent_flag = False

    if key in {"permanent_failed", "permanent_failure", "permanent", "rejected", "dead"}:
        return STATUS_PERMANENT_FAILED, True
    if key in _STATE_ALIASES:
        normalized = _STATE_ALIASES[key]
        if normalized == STATUS_TRANSIENT_FAILED and permanent_flag is True:
            return STATUS_PERMANENT_FAILED, True
        if normalized == STATUS_PERMANENT_FAILED:
            return normalized, True
        return normalized, bool(permanent_flag or False)
    raise ProxyError(f"未知回执状态 {raw_state!r}")


def _event_id(project: str, seq: int) -> str:
    return f"{project}#{seq}"


def _blank_state() -> dict[str, Any]:
    return {
        "version": STATE_VERSION,
        "events": {},
        "project_cursors": {},
        "subscriptions": {},
        "audit": [],
    }


class CallbackProxy:
    """顺序、持久、可审计的回调代理核心。

    该类不打开网络端口；``transport`` 可注入本地合成投递函数。场景回放通过
    :meth:`handle` / :meth:`run_file` 驱动。
    """

    def __init__(
        self,
        state_path: str | os.PathLike[str] | None = None,
        *,
        auto_initial_state: bool = False,
        initial_state_requires: str = STATUS_DONE,
        transport: Callable[[dict[str, Any]], Any] | None = None,
    ) -> None:
        self.state_path: Path | None
        if state_path is None or str(state_path) == ":memory:":
            self.state_path = None
        else:
            candidate = Path(state_path)
            # 允许测试/运营直接传一个状态目录；默认在其中放 state.json。
            if candidate.is_dir():
                candidate = candidate / "callback_proxy_state.json"
            self.state_path = candidate
        self.auto_initial_state = auto_initial_state
        self.initial_state_requires = initial_state_requires
        self.transport = transport
        self._lock = threading.RLock()
        self.state = _blank_state()
        if self.state_path and self.state_path.exists():
            self._load()

    # ------------------------------------------------------------------
    # 持久化与生命周期
    # ------------------------------------------------------------------
    def to_dict(self) -> dict[str, Any]:
        with self._lock:
            return deepcopy(self.state)

    def _load(self) -> None:
        assert self.state_path is not None
        try:
            raw = self.state_path.read_text(encoding="utf-8")
            loaded = json.loads(raw)
        except (OSError, json.JSONDecodeError) as exc:
            raise ProxyError(f"无法读取状态文件 {self.state_path}: {exc}") from exc
        if not isinstance(loaded, dict) or "subscriptions" not in loaded:
            raise ProxyError("状态文件结构无效")
        blank = _blank_state()
        blank.update(loaded)
        self.state = blank

    def _save(self) -> None:
        if not self.state_path:
            return
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(
            prefix=f".{self.state_path.name}.", suffix=".tmp", dir=str(self.state_path.parent)
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(self.state, handle, ensure_ascii=False, indent=2, sort_keys=False)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_name, self.state_path)
        except BaseException:
            try:
                os.unlink(tmp_name)
            except OSError:
                pass
            raise

    def restart(self) -> "CallbackProxy":
        """模拟进程重启：先落盘，再从状态文件/JSON 快照恢复。"""
        with self._lock:
            self._audit("restart", {})
            self._save()
            if self.state_path:
                self._load()
            else:
                # 即使没有文件，也经过 JSON 往返，确保状态全部可持久化。
                self.state = json.loads(json.dumps(self.state, ensure_ascii=False))
            recovered = self._recover_unacknowledged()
            if recovered:
                self._audit("restart_recovery", {"recovered": recovered})
            self._save()
            return self

    def _recover_unacknowledged(self) -> list[dict[str, Any]]:
        """重启后回收没有任何实收结论的在途发送。

        已 accepted/done/... 的实收状态保持不变；只有最后状态仍为 sent、即没有
        收到回执的通知，会转为可重试。通知身份和历史 attempts 全部保留，因此重发
        仍是同一 sub/notification/event，接收端可按身份去重。
        """
        recovered: list[dict[str, Any]] = []
        for sub in self.state.get("subscriptions", {}).values():
            for item in sub.get("items", []):
                if item.get("status") != STATUS_SENT:
                    continue
                item["status"] = STATUS_TRANSIENT_FAILED
                item["raw_state"] = "unacknowledged_after_restart"
                item["recovery_reason"] = "restart_without_receipt"
                item["recovered_at_restart"] = True
                recovered.append(
                    {"sub": sub.get("id"), "notification": item.get("notification"), "item_id": item.get("item_id")}
                )
        return recovered

    def _restart_record(self) -> dict[str, Any]:
        self.restart()
        return {
            "restarted": True,
            "source_cursors": deepcopy(self.state["project_cursors"]),
            "subscriptions": sorted(self.state["subscriptions"]),
            "persisted": self.state_path is not None,
        }

    def _audit(self, kind: str, detail: dict[str, Any]) -> None:
        self.state.setdefault("audit", []).append({"kind": kind, "detail": detail})

    # ------------------------------------------------------------------
    # 输入分发
    # ------------------------------------------------------------------
    def handle(self, message: dict[str, Any]) -> dict[str, Any]:
        """处理一行 ``{"at", "kind", "record"}`` 或直接传入的 record。"""
        if not isinstance(message, dict):
            raise ProxyError("消息必须是对象")
        if "kind" in message and "record" in message:
            at = message.get("at")
            kind = message["kind"]
            record = message["record"]
            if not isinstance(record, dict):
                raise ProxyError("record 必须是对象")
        else:
            # 允许直接把 record 传给 handle。
            kind = message.get("kind") or message.get("type")
            record = message
            at = message.get("at")

        dispatch = {
            "event": self.add_event,
            "subscribe": self.subscribe,
            "poll": self.poll,
            "receipt": self.receive_receipt,
            "summary": self.declare_summary,
            "restart": lambda rec, at: {"restarted": True, **self._restart_record()},
            "inspect": lambda rec, at: self.inspect(rec.get("sub"), at=at),
            # 以下操作是对运营 API 的场景化别名；基础七类消息之外也可使用。
            "reroute": self._handle_reroute_record,
            "retry": self._handle_reroute_record,
            "terminate": self._handle_terminate_record,
            "terminate_chain": self._handle_terminate_record,
            "initial_state": self._handle_initial_state_record,
            "target_update": self._handle_target_update,
        }.get(kind)
        if dispatch is None:
            raise ProxyError(f"未支持的消息种类 {kind!r}")
        result = dispatch(record, at)
        observation: dict[str, Any] = {"at": at, "kind": kind, "ok": True, "result": result}
        if kind == "inspect":
            observation["inspection"] = result
        return observation

    # ------------------------------------------------------------------
    # 订阅与修订版本
    # ------------------------------------------------------------------
    def subscribe(self, record: dict[str, Any], at: Any = None) -> dict[str, Any]:
        with self._lock:
            sub_id = record.get("sub")
            project = record.get("project")
            kinds = record.get("kinds")
            cut_at = _as_int(record.get("cut_at"), "cut_at")
            target = record.get("target")
            revision = record.get("revision")
            if not isinstance(sub_id, str) or not sub_id:
                raise ProxyError("subscribe.sub 必须是非空字符串")
            if not isinstance(project, str) or not project:
                raise ProxyError("subscribe.project 必须是非空字符串")
            if not isinstance(target, str) or not target:
                raise ProxyError("subscribe.target 必须是非空字符串")
            if not isinstance(revision, (int, str)) or isinstance(revision, bool):
                raise ProxyError("subscribe.revision 必须是整数或字符串")
            if isinstance(kinds, str):
                kinds = [kinds]
            if not isinstance(kinds, list) or not all(isinstance(k, str) for k in kinds):
                raise ProxyError("subscribe.kinds 必须是字符串数组")
            if cut_at < 0:
                raise ProxyError("cut_at 不能为负数")

            sub = self.state["subscriptions"].get(sub_id)
            if sub is None:
                sub = {
                    "id": sub_id,
                    "project": project,
                    "revisions": [],
                    "current_revision": revision,
                    "next_notification": 1,
                    "notification_index": {},
                    "items": [],
                    "item_by_event": {},
                    "summaries": [],
                    "chains": {},
                    "reroute_log": [],
                    "target_history": [],
                    "created_at": _now(at),
                }
                self.state["subscriptions"][sub_id] = sub
            elif sub["project"] != project:
                raise RevisionConflict(
                    f"订阅 {sub_id} 已属于项目 {sub['project']}，不能改到 {project}"
                )

            revision_record = {
                "revision": revision,
                "order": len(sub["revisions"]) + 1,
                "kinds": list(kinds),
                "cut_at": cut_at,
                "target": target,
                "at": _now(at),
            }

            existing = self._find_revision(sub, revision)
            if existing is not None:
                same_filter = (
                    list(existing.get("kinds", [])) == list(kinds)
                    and existing.get("cut_at") == cut_at
                )
                if same_filter and existing.get("target") == target:
                    self._save()
                    return {"sub": sub_id, "revision": revision, "idempotent": True}
                if same_filter and record.get("update_target", True):
                    # 回调端地址更新：修订筛选不变，仅追加目标历史。已经发出的
                    # 通知仍保留各自 target；尚未首次发出的工作项可选择迁移。
                    old_target = existing.get("target")
                    existing["target"] = target
                    sub["current_revision"] = revision
                    sub["target_history"].append(
                        {"revision": revision, "from": old_target, "to": target, "at": _now(at)}
                    )
                    moved_unsent = 0
                    if record.get("move_unsent", True):
                        for item in sub["items"]:
                            if (
                                item.get("revision") == revision
                                and item.get("notification") is None
                                and item.get("status") == STATUS_UNSENT
                            ):
                                item["target"] = target
                                moved_unsent += 1
                    self._audit(
                        "target_update",
                        {"sub": sub_id, "revision": revision, "from": old_target, "to": target},
                    )
                    self._save()
                    return {
                        "sub": sub_id,
                        "revision": revision,
                        "target_updated": True,
                        "moved_unsent": moved_unsent,
                    }
                raise RevisionConflict(
                    f"订阅 {sub_id} 的修订版本 {revision!r} 已存在；历史筛选版本不可改写"
                )

            revisions = sub["revisions"]
            if revisions and isinstance(revision, int):
                latest_int = max(
                    (r["revision"] for r in revisions if isinstance(r["revision"], int)),
                    default=None,
                )
                if latest_int is not None and revision <= latest_int:
                    raise RevisionConflict(
                        f"新修订版本 {revision} 必须大于已有版本 {latest_int}"
                    )

            revisions.append(revision_record)
            revisions.sort(key=lambda r: (isinstance(r["revision"], str), r["revision"]))
            sub["current_revision"] = revision
            sub["target_history"].append(
                {"revision": revision, "target": target, "at": _now(at), "type": "revision"}
            )

            active_items = self._reconcile_subscription(sub, at)
            self._audit(
                "subscribe",
                {
                    "sub": sub_id,
                    "revision": revision,
                    "cut_at": cut_at,
                    "target": target,
                    "active_items": len(active_items),
                },
            )
            if self.auto_initial_state:
                # 仅在构造时显式开启自动策略；默认让演练/运营决定是否发上下文。
                self._auto_open_initial_contexts(sub, at)
            self._save()
            return {
                "sub": sub_id,
                "project": project,
                "revision": revision,
                "cut_at": cut_at,
                "target": target,
                "matched_new_items": active_items,
                "active_items": active_items,
            }

    @staticmethod
    def _find_revision(sub: dict[str, Any], revision: Any) -> dict[str, Any] | None:
        for rev in sub["revisions"]:
            if rev["revision"] == revision:
                return rev
        return None

    def _effective_revision(self, sub: dict[str, Any], seq: int) -> dict[str, Any] | None:
        """按源位置选择对该事件生效的最新修订版本。

        修订版本的 ``cut_at`` 包含该位置。若多个回溯修订都覆盖同一 seq，以
        修订顺序中最新者为准。已经首次发出的通知不会在 reconciliation 中改版本。
        """
        eligible = [r for r in sub["revisions"] if seq >= r["cut_at"]]
        if not eligible:
            return None
        return max(eligible, key=lambda r: int(r.get("order", 0)))

    def _reconcile_subscription(self, sub: dict[str, Any], at: Any) -> list[str]:
        """根据各源位置的有效修订，创建或调整尚未发出的工作项。"""
        active: list[str] = []
        project = sub["project"]
        events = sorted(
            (e for e in self.state["events"].values() if e["project"] == project),
            key=lambda e: e["seq"],
        )
        for event in events:
            rev = self._effective_revision(sub, event["seq"])
            event_pk = _event_id(project, event["seq"])
            item_id = sub["item_by_event"].get(event_pk)
            if item_id is not None:
                item = self._find_item(sub, item_id)
                # 已赋予通知身份即已经发出/尝试过；历史依据永久冻结。
                if item.get("notification") is not None:
                    if item["status"] != STATUS_SUPERSEDED:
                        active.append(item_id)
                    continue
                if rev is None or not self._revision_matches(rev, event):
                    item["status"] = STATUS_SUPERSEDED
                    item["superseded_at"] = _now(at)
                    item["superseded_by_revision"] = sub["current_revision"]
                    continue
                item["status"] = STATUS_UNSENT
                item["revision"] = rev["revision"]
                item["target"] = rev["target"]
                item["original_target"] = rev["target"]
                item.pop("superseded_at", None)
                item.pop("superseded_by_revision", None)
                active.append(item_id)
                continue

            if rev is not None and self._revision_matches(rev, event):
                new_id = self._ensure_event_item(sub, rev, event, at)
                if new_id is not None:
                    active.append(new_id)
        return active

    @staticmethod
    def _revision_matches(rev: dict[str, Any], event: dict[str, Any]) -> bool:
        return event["seq"] >= rev["cut_at"] and event["kind"] in set(rev["kinds"])

    def _ensure_event_item(
        self,
        sub: dict[str, Any],
        rev: dict[str, Any],
        event: dict[str, Any],
        at: Any,
    ) -> str | None:
        event_pk = _event_id(event["project"], event["seq"])
        if event_pk in sub["item_by_event"]:
            return None
        item_id = event_pk
        item = {
            "item_id": item_id,
            "category": "event",
            "event_id": event_pk,
            "project": event["project"],
            "seq": event["seq"],
            "object": event["object"],
            "event_kind": event["kind"],
            "after": list(event.get("after", [])),
            "revision": rev["revision"],
            "original_target": rev["target"],
            "target": rev["target"],
            "notification": None,
            "status": STATUS_UNSENT,
            "raw_state": None,
            "created_at": _now(at),
            "first_sent_at": None,
            "last_sent_at": None,
            "completed_at": None,
            "attempt_count": 0,
            "attempts": [],
            "receipts": [],
            "reroutes": [],
            "gate": None,
            "last_error": None,
        }
        sub["items"].append(item)
        sub["item_by_event"][event_pk] = item_id
        return item_id

    # ------------------------------------------------------------------
    # 源事件
    # ------------------------------------------------------------------
    def add_event(self, record: dict[str, Any], at: Any = None) -> dict[str, Any]:
        with self._lock:
            project = record.get("project")
            seq = _as_int(record.get("seq"), "event.seq")
            obj = record.get("object")
            kind = record.get("kind")
            after = record.get("after", [])
            if not isinstance(project, str) or not project:
                raise ProxyError("event.project 必须是非空字符串")
            if not isinstance(obj, str) or not obj:
                raise ProxyError("event.object 必须是非空字符串")
            if not isinstance(kind, str) or not kind:
                raise ProxyError("event.kind 必须是非空字符串")
            if not isinstance(after, list) or not all(isinstance(x, int) for x in after):
                raise ProxyError("event.after 必须是整数位置数组")
            event_pk = _event_id(project, seq)
            existing = self.state["events"].get(event_pk)
            if existing is not None:
                canonical = {
                    "project": project,
                    "seq": seq,
                    "object": obj,
                    "kind": kind,
                    "after": list(after),
                }
                comparable = {k: existing.get(k) for k in canonical}
                if comparable != canonical:
                    raise RevisionConflict(f"源位置 {event_pk} 已存在且内容不一致")
                return {"event_id": event_pk, "idempotent": True}

            cursor = self.state["project_cursors"].get(project, 0)
            if seq <= cursor:
                raise ProxyError(
                    f"事件序号 {seq} 已落后于项目 {project} 的已知位置 {cursor}"
                )
            # 不强制必须连续到 1：允许从快照位置接手；但同一 project 内必须递增。
            event = {
                "event_id": event_pk,
                "project": project,
                "seq": seq,
                "object": obj,
                "kind": kind,
                "after": list(after),
                "received_at": _now(at),
            }
            self.state["events"][event_pk] = event
            self.state["project_cursors"][project] = max(cursor, seq)

            matched: list[str] = []
            for sub in self.state["subscriptions"].values():
                if sub["project"] != project or not sub["revisions"]:
                    continue
                rev = self._effective_revision(sub, seq)
                if rev is not None and self._revision_matches(rev, event):
                    item_id = self._ensure_event_item(sub, rev, event, at)
                    if item_id:
                        matched.append(sub["id"])
                if self.auto_initial_state:
                    self._auto_open_initial_contexts(sub, at)
            self._audit("event", {"event_id": event_pk, "matched": matched})
            self._save()
            return {"event_id": event_pk, "matched_subscriptions": matched}

    # ------------------------------------------------------------------
    # 摘要证据
    # ------------------------------------------------------------------
    def declare_summary(self, record: dict[str, Any], at: Any = None) -> dict[str, Any]:
        with self._lock:
            sub_id = record.get("sub")
            target = record.get("target")
            obj = record.get("object")
            through = _as_int(record.get("through", record.get("covers_until")), "summary.through")
            if not isinstance(sub_id, str):
                raise NotFound("summary.sub 必须是字符串")
            sub = self._require_subscription(sub_id)
            if not isinstance(target, str) or not target:
                raise ProxyError("summary.target 必须是非空字符串")
            if not isinstance(obj, str) or not obj:
                raise ProxyError("summary.object 必须是非空字符串")

            positions = record.get("positions", record.get("covers", []))
            if positions is None:
                positions = []
            if not isinstance(positions, list) or not all(isinstance(x, int) for x in positions):
                positions = []

            summary = {
                "at": _now(at),
                "target": target,
                "object": obj,
                "through": through,
                "positions": positions,
                # 明确不保存 body/state 正文；只保留“持有何位置”的证据。
                "has_body": any(k in record for k in ("body", "state", "snapshot", "content")),
            }
            # 相同声明幂等。
            for old in sub["summaries"]:
                if all(old.get(k) == summary[k] for k in ("target", "object", "through", "positions")):
                    return {"sub": sub_id, "summary": old, "idempotent": True}
            sub["summaries"].append(summary)
            self._audit("summary", {"sub": sub_id, "target": target, "object": obj, "through": through})
            if self.auto_initial_state:
                self._auto_open_initial_contexts(sub, at)
            self._save()
            return {"sub": sub_id, "summary": summary}

    def _summary_covers(
        self, sub: dict[str, Any], target: str, obj: str, seq: int
    ) -> dict[str, Any] | None:
        for summary in reversed(sub["summaries"]):
            if summary["target"] != target or summary["object"] != obj:
                continue
            if seq <= summary["through"] or seq in summary.get("positions", []):
                return summary
        return None

    # ------------------------------------------------------------------
    # 依赖门控
    # ------------------------------------------------------------------
    def evaluate_gate(self, sub_id: str | dict[str, Any], item_id: str | None = None) -> dict[str, Any]:
        with self._lock:
            if isinstance(sub_id, dict):
                sub = self._require_subscription(sub_id["sub"])
                item_id = sub_id.get("item_id")
            else:
                sub = self._require_subscription(sub_id)
            if item_id is None:
                raise ProxyError("evaluate_gate 需要 item_id")
            item = self._find_item(sub, item_id)
            return deepcopy(self._gate_for_item(sub, item))

    def _gate_for_item(self, sub: dict[str, Any], item: dict[str, Any]) -> dict[str, Any]:
        if item.get("category") == "initial_state":
            chain = sub["chains"].get(item["object"])
            if chain and chain.get("terminated") and item["status"] != STATUS_DONE:
                return {"deliverable": False, "code": "chain_terminated", "missing": [], "evidence": []}
            return {"deliverable": True, "code": "ready", "missing": [], "evidence": []}

        obj = item["object"]
        chain = sub["chains"].get(obj)
        if chain and chain.get("terminated") and item["status"] not in TERMINAL_STATUSES:
            return {
                "deliverable": False,
                "code": "chain_terminated",
                "missing": [],
                "evidence": [{"type": "termination", **chain}],
            }

        missing_context: list[int] = []
        waiting: list[dict[str, Any]] = []
        hard: list[dict[str, Any]] = []
        evidence: list[dict[str, Any]] = []

        for req_seq in item.get("after", []):
            event_pk = _event_id(item["project"], req_seq)
            prereq_event = self.state["events"].get(event_pk)
            if prereq_event is None:
                # 未知源位置不能重放；若目标摘要明确覆盖该位置，仍可证明上下文。
                summary = self._summary_covers(sub, item["target"], obj, req_seq)
                ctx = self._context_item_covers(sub, item["target"], obj, req_seq)
                if summary or ctx:
                    evidence.append(summary or ctx)
                    continue
                missing_context.append(req_seq)
                hard.append({"seq": req_seq, "code": "unknown_source_position"})
                continue

            summary = self._summary_covers(sub, item["target"], prereq_event["object"], req_seq)
            if summary:
                evidence.append({"type": "summary", **summary})
                continue
            ctx = self._context_item_covers(sub, item["target"], prereq_event["object"], req_seq)
            if ctx:
                evidence.append({"type": "initial_state", "item_id": ctx["item_id"], "notification": ctx.get("notification")})
                continue

            prereq_item_id = sub["item_by_event"].get(event_pk)
            prereq_item = self._find_item(sub, prereq_item_id) if prereq_item_id else None

            if prereq_item is None:
                # 没有为本订阅生成通知：通常是 cut_at/kinds 明确排除。
                missing_context.append(req_seq)
                continue

            if prereq_item["status"] == STATUS_DONE and prereq_item.get("target") == item["target"]:
                evidence.append(
                    {"type": "notification_done", "notification": prereq_item.get("notification")}
                )
                continue

            if prereq_item["status"] == STATUS_DONE:
                # 旧端已处理不能证明新目标也持有初始状态。
                missing_context.append(req_seq)
                waiting.append(
                    {
                        "seq": req_seq,
                        "code": "context_on_other_target",
                        "notification": prereq_item.get("notification"),
                        "done_target": prereq_item.get("target"),
                        "desired_target": item["target"],
                    }
                )
                continue

            if prereq_item["status"] == STATUS_PERMANENT_FAILED:
                hard.append(
                    {
                        "seq": req_seq,
                        "code": "predecessor_permanent_failed",
                        "notification": prereq_item.get("notification"),
                    }
                )
                continue
            if prereq_item["status"] == STATUS_TERMINATED:
                hard.append(
                    {
                        "seq": req_seq,
                        "code": "predecessor_terminated",
                        "notification": prereq_item.get("notification"),
                    }
                )
                continue

            waiting.append(
                {
                    "seq": req_seq,
                    "code": f"predecessor_{prereq_item['status']}",
                    "notification": prereq_item.get("notification"),
                    "status": prereq_item["status"],
                }
            )

        if hard:
            # 永久失败/终止是硬缺口；缺少上下文也一并报告。
            code = hard[0]["code"]
            return {
                "deliverable": False,
                "code": code,
                "missing": sorted(set(missing_context)),
                "waiting": waiting,
                "hard": hard,
                "evidence": evidence,
            }
        if missing_context:
            return {
                "deliverable": False,
                "code": "missing_initial_context",
                "missing": sorted(set(missing_context)),
                "waiting": waiting,
                "hard": [],
                "evidence": evidence,
                "can_send_initial_state": True,
            }
        if waiting:
            return {
                "deliverable": False,
                "code": waiting[0]["code"],
                "missing": [],
                "waiting": waiting,
                "hard": [],
                "evidence": evidence,
            }
        return {"deliverable": True, "code": "ready", "missing": [], "waiting": [], "hard": [], "evidence": evidence}

    def _context_item_covers(
        self, sub: dict[str, Any], target: str, obj: str, seq: int
    ) -> dict[str, Any] | None:
        required_status = self.initial_state_requires
        if required_status == STATUS_ACCEPTED:
            allowed = {STATUS_ACCEPTED, STATUS_DONE}
        else:
            allowed = {STATUS_DONE}
        for item in sub["items"]:
            if (
                item.get("category") == "initial_state"
                and item.get("object") == obj
                and item.get("target") == target
                and item.get("status") in allowed
                and seq <= int(item.get("through", 0))
            ):
                return item
        return None

    def _auto_open_initial_contexts(self, sub: dict[str, Any], at: Any) -> list[str]:
        """为缺少上下文的受阻对象创建（但不发送）协议级初始状态工作项。"""
        opened: list[str] = []
        for item in list(sub["items"]):
            if item["category"] != "event" or item["status"] in TERMINAL_STATUSES:
                continue
            gate = self._gate_for_item(sub, item)
            if gate["code"] != "missing_initial_context":
                continue
            ctx_id = self._ensure_initial_state_item(
                sub,
                item["object"],
                max(gate.get("missing") or item.get("after") or [0]),
                item["target"],
                item["revision"],
                at,
            )
            if ctx_id:
                opened.append(ctx_id)
        return opened

    def request_initial_state(
        self,
        sub_id: str,
        obj: str,
        through: int | None = None,
        *,
        target: str | None = None,
        at: Any = None,
        send: bool = False,
    ) -> dict[str, Any]:
        with self._lock:
            sub = self._require_subscription(sub_id)
            target = target or self._current_revision(sub)["target"]
            if through is None:
                missing: list[int] = []
                for item in sub["items"]:
                    if item.get("object") == obj and item.get("category") == "event":
                        gate = self._gate_for_item(sub, item)
                        missing.extend(gate.get("missing", []))
                through = max(missing) if missing else self.state["project_cursors"].get(sub["project"], 0)
            item_id = self._ensure_initial_state_item(
                sub, obj, _as_int(through, "through"), target, self._current_revision(sub)["revision"], at
            )
            self._save()
            result: dict[str, Any] = {"item_id": item_id}
            if send:
                result["poll"] = self.poll(sub_id, at=at, notification=self._find_item(sub, item_id).get("notification"))
            return result

    def _handle_initial_state_record(self, record: dict[str, Any], at: Any) -> dict[str, Any]:
        return self.request_initial_state(
            record["sub"],
            record["object"],
            record.get("through"),
            target=record.get("target"),
            at=at,
            send=bool(record.get("send", True)),
        )

    def _ensure_initial_state_item(
        self,
        sub: dict[str, Any],
        obj: str,
        through: int,
        target: str,
        revision: Any,
        at: Any,
    ) -> str | None:
        item_id = f"initial_state:{obj}:{through}:{target}"
        existing = next((i for i in sub["items"] if i["item_id"] == item_id), None)
        if existing is not None:
            return item_id
        item = {
            "item_id": item_id,
            "category": "initial_state",
            "event_id": None,
            "project": sub["project"],
            "seq": through,
            "object": obj,
            "event_kind": "initial_state",
            "through": through,
            "after": [],
            "revision": revision,
            "original_target": target,
            "target": target,
            "notification": None,
            "status": STATUS_UNSENT,
            "raw_state": None,
            "created_at": _now(at),
            "first_sent_at": None,
            "last_sent_at": None,
            "completed_at": None,
            "attempt_count": 0,
            "attempts": [],
            "receipts": [],
            "reroutes": [],
            "gate": {"deliverable": True, "code": "ready"},
            "last_error": None,
            # 明确没有 source events 正文；这是协议允许的摘要/初始状态通知。
            "payload": {
                "type": "initial_state",
                "project": sub["project"],
                "object": obj,
                "through": through,
                "contains_excluded_history": False,
            },
        }
        sub["items"].append(item)
        return item_id

    # ------------------------------------------------------------------
    # 投递 / poll
    # ------------------------------------------------------------------
    def poll(
        self,
        sub: str | dict[str, Any] | None = None,
        at: Any = None,
        *,
        max_deliveries: int | None = None,
        notification: int | None = None,
        force: bool = False,
        ignore_gate: bool = False,
    ) -> dict[str, Any]:
        record: dict[str, Any] = {}
        if isinstance(sub, dict):
            record = sub
            at = at if at is not None else record.get("at")
            sub_id = record.get("sub")
            if max_deliveries is None:
                max_deliveries = record.get("limit", record.get("max_deliveries"))
            if notification is None:
                notification = record.get("notification")
            force = force or bool(record.get("force"))
            ignore_gate = ignore_gate or bool(record.get("ignore_gate"))
        else:
            sub_id = sub
        if not sub_id:
            raise ProxyError("poll 需要 sub")
        with self._lock:
            sub_state = self._require_subscription(sub_id)
            if isinstance(sub, dict) and (
                record.get("auto_initial_state")
                or record.get("send_initial_state")
                or record.get("initial_state") is True
            ):
                self._auto_open_initial_contexts(sub_state, at)
            if max_deliveries is not None:
                max_deliveries = _as_int(max_deliveries, "max_deliveries")

            candidates = list(sub_state["items"])
            if notification is not None:
                item_id = sub_state["notification_index"].get(str(_as_int(notification, "notification")))
                if item_id is None:
                    raise NotFound(f"订阅 {sub_id} 没有通知 {notification}")
                candidates = [self._find_item(sub_state, item_id)]

            candidates.sort(key=lambda i: (i.get("seq") is None, i.get("seq", 0), i.get("created_at") or 0))
            dispatched: list[dict[str, Any]] = []
            blocked: list[dict[str, Any]] = []
            skipped: list[dict[str, Any]] = []

            for item in candidates:
                if max_deliveries is not None and len(dispatched) >= max_deliveries:
                    break
                n = item.get("notification")
                if item["status"] == STATUS_DONE:
                    skipped.append(self._skip(item, "already_done"))
                    continue
                if item["status"] == STATUS_PERMANENT_FAILED and not force:
                    skipped.append(self._skip(item, "permanent_failed"))
                    continue
                if item["status"] == STATUS_TERMINATED:
                    skipped.append(self._skip(item, "terminated"))
                    continue
                if item["status"] == STATUS_SUPERSEDED:
                    skipped.append(self._skip(item, "superseded_by_revision"))
                    continue
                if item["status"] == STATUS_ACCEPTED:
                    skipped.append(self._skip(item, "accepted_awaiting_conclusion"))
                    continue
                if item["status"] == STATUS_SENT and not force:
                    skipped.append(self._skip(item, "sent_awaiting_receipt"))
                    continue
                if item["status"] == STATUS_NOT_PROCESSED and not force:
                    skipped.append(self._skip(item, "awaiting_operator_reroute"))
                    continue

                gate = self._gate_for_item(sub_state, item)
                item["gate"] = gate
                if not gate["deliverable"] and not ignore_gate:
                    blocked.append(
                        {
                            "item_id": item["item_id"],
                            "notification": item.get("notification"),
                            "object": item.get("object"),
                            "seq": item.get("seq"),
                            "target": item.get("target"),
                            "code": gate["code"],
                            "missing": gate.get("missing", []),
                        }
                    )
                    continue

                delivery = self._dispatch(sub_state, item, at, force=force)
                dispatched.append(delivery)

            result = {
                "sub": sub_id,
                "dispatched": dispatched,
                "blocked": blocked,
                "skipped": skipped,
                "sent": dispatched,  # 常见测试别名
                "deliveries": dispatched,
                "count": len(dispatched),
            }
            if dispatched:
                first = dispatched[0]
                result.update(
                    {
                        "notification": first["notification"],
                        "notification_id": first["notification"],
                        "target": first["target"],
                        "revision": first["revision"],
                        "event": first["event"],
                        "event_id": first.get("event_id"),
                        "first": first,
                        "delivery": first,
                    }
                )
            self._audit("poll", {"sub": sub_id, "dispatched": len(dispatched), "blocked": len(blocked)})
            self._save()
            return result

    @staticmethod
    def _skip(item: dict[str, Any], reason: str) -> dict[str, Any]:
        return {
            "item_id": item["item_id"],
            "notification": item.get("notification"),
            "object": item.get("object"),
            "target": item.get("target"),
            "status": item["status"],
            "reason": reason,
        }

    def _dispatch(
        self, sub: dict[str, Any], item: dict[str, Any], at: Any, *, force: bool = False
    ) -> dict[str, Any]:
        if item.get("notification") is None:
            n = int(sub["next_notification"])
            sub["next_notification"] = n + 1
            item["notification"] = n
            sub["notification_index"][str(n)] = item["item_id"]
        else:
            n = int(item["notification"])

        duplicate = item["attempt_count"] > 0
        attempt = {
            "at": _now(at),
            "notification": n,
            "revision": item["revision"],
            "target": item["target"],
            "duplicate_identity": duplicate,
            "source_event": item.get("event_id"),
            "category": item.get("category", "event"),
            "force": force,
        }
        item["attempts"].append(attempt)
        item["attempt_count"] += 1
        item["first_sent_at"] = item["first_sent_at"] or _now(at)
        item["last_sent_at"] = _now(at)
        item["status"] = STATUS_SENT
        item["raw_state"] = "sent"
        item["gate"] = {"deliverable": True, "code": "sent"}

        envelope = self._envelope(sub, item, attempt)
        transport_result = None
        if self.transport is not None:
            if hasattr(self.transport, "send"):
                transport_result = self.transport.send(envelope)
            elif hasattr(self.transport, "deliver"):
                transport_result = self.transport.deliver(envelope)
            else:
                transport_result = self.transport(envelope)
            attempt["transport_result"] = transport_result
            # 本地合成 transport 可以同步给出 accepted/failed；场景文件通常用 receipt。
            if isinstance(transport_result, dict) and "state" in transport_result:
                self.receive_receipt(
                    {
                        "sub": sub["id"],
                        "target": item["target"],
                        "notification": n,
                        "state": transport_result["state"],
                        **{k: v for k, v in transport_result.items() if k != "state"},
                    },
                    at=at,
                )
        return {
            "sub": sub["id"],
            "notification": n,
            "item_id": item["item_id"],
            "event_id": item.get("event_id"),
            "event": item.get("event_id"),
            "source_event": item.get("event_id"),
            "object": item.get("object"),
            "seq": item.get("seq"),
            "revision": item["revision"],
            "target": item["target"],
            "category": item.get("category", "event"),
            "duplicate": duplicate,
            "envelope": envelope,
            "transport_result": transport_result,
        }

    def _envelope(self, sub: dict[str, Any], item: dict[str, Any], attempt: dict[str, Any]) -> dict[str, Any]:
        if item.get("category") == "initial_state":
            payload = dict(item["payload"])
        else:
            event = self.state["events"][item["event_id"]]
            payload = {
                "type": "event",
                "project": event["project"],
                "seq": event["seq"],
                "object": event["object"],
                "kind": event["kind"],
                "after": event.get("after", []),
            }
        return {
            "sub": sub["id"],
            "notification": item["notification"],
            "identity": f"{sub['id']}:{item['notification']}",
            "revision": item["revision"],
            "target": item["target"],
            "source_event": item.get("event_id"),
            "category": item.get("category", "event"),
            "idempotency_key": f"{sub['id']}:{item['notification']}",
            "payload": payload,
            "attempt": item["attempt_count"] + 0 if item else 1,
        }

    # ------------------------------------------------------------------
    # 回执
    # ------------------------------------------------------------------
    def receive_receipt(self, record: dict[str, Any], at: Any = None) -> dict[str, Any]:
        with self._lock:
            sub_id = record.get("sub")
            target = record.get("target")
            n = _as_int(record.get("notification"), "receipt.notification")
            if not isinstance(sub_id, str):
                raise NotFound("receipt.sub 必须是字符串")
            sub = self._require_subscription(sub_id)
            item_id = sub["notification_index"].get(str(n))
            if item_id is None:
                raise NotFound(f"订阅 {sub_id} 不存在通知 {n}；回执不能创建或完成其他通知")
            item = self._find_item(sub, item_id)
            normalized, permanent_flag = _normalize_state(record.get("state"), record)
            receipt = {
                "at": _now(at),
                "notification": n,
                "target": target,
                "state": normalized,
                "raw_state": record.get("state"),
                "permanent": permanent_flag if normalized == STATUS_PERMANENT_FAILED else None,
                "reason": record.get("reason", record.get("error")),
                "detail": {k: v for k, v in record.items() if k not in {"sub", "target", "notification", "state", "at"}},
            }

            target_relation = self._target_relation(item, target)
            receipt["target_relation"] = target_relation
            if target is not None and target_relation == "unknown":
                item["receipts"].append({**receipt, "ignored": True, "reason": "unknown_target"})
                self._save()
                raise ProxyError(
                    f"回执目标 {target!r} 不是通知 {sub_id}/{n} 的曾投递目标；已忽略，避免误确认"
                )

            # 完全相同的回执重复到达：幂等返回，不推进到另一通知。
            duplicate = any(
                r.get("target") == target
                and r.get("state") == normalized
                and r.get("notification") == n
                for r in item["receipts"]
            )
            item["receipts"].append(receipt)

            stale_target = target_relation in {"original", "historical"} and target != item.get("target")
            conflict: str | None = None
            if stale_target:
                # 改发后只有当前负责目标能给出结论；旧端迟到回执只作冲突留痕，
                # 不能推进或完成当前通知，也不能影响任何其他通知身份。
                conflict = "stale_target_receipt_after_takeover"
                receipt["conflict"] = conflict
                self._audit("receipt_conflict", {"sub": sub_id, "notification": n, "target": target, "state": normalized})
                self._save()
                return {"sub": sub_id, "notification": n, "status": item["status"], "duplicate": duplicate, "conflict": conflict}

            previous = item["status"]
            if previous == STATUS_DONE and normalized != STATUS_DONE:
                conflict = "conflicting_receipt_after_done"
                receipt["conflict"] = conflict
                self._audit("receipt_conflict", {"sub": sub_id, "notification": n, "state": normalized})
                self._save()
                return {"sub": sub_id, "notification": n, "status": previous, "duplicate": duplicate, "conflict": conflict}
            if previous == STATUS_TERMINATED and normalized != STATUS_DONE:
                conflict = "receipt_after_chain_terminated"
                receipt["conflict"] = conflict
                self._audit("receipt_conflict", {"sub": sub_id, "notification": n, "state": normalized})
                self._save()
                return {"sub": sub_id, "notification": n, "status": previous, "duplicate": duplicate, "conflict": conflict}

            if normalized == STATUS_DONE:
                if previous in TERMINAL_STATUSES and previous != STATUS_DONE:
                    conflict = f"cannot_mark_{previous}_done"
                    receipt["conflict"] = conflict
                else:
                    item["status"] = STATUS_DONE
                    item["raw_state"] = record.get("state")
                    item["completed_at"] = _now(at)
            elif normalized == STATUS_ACCEPTED:
                if previous == STATUS_DONE:
                    conflict = "done_before_accepted"
                    receipt["conflict"] = conflict
                else:
                    item["status"] = STATUS_ACCEPTED
                    item["raw_state"] = record.get("state")
                    item["accepted_at"] = _now(at)
            elif normalized == STATUS_PERMANENT_FAILED:
                item["status"] = STATUS_PERMANENT_FAILED
                item["raw_state"] = record.get("state")
                item["failure_kind"] = "permanent"
                item["permanent"] = True
                item["retryable"] = False
                item["last_error"] = receipt.get("reason") or record
                item["failed_at"] = _now(at)
            elif normalized == STATUS_FAILED:
                item["status"] = STATUS_FAILED
                item["raw_state"] = record.get("state")
                item["failure_kind"] = "temporary"
                item["permanent"] = False
                item["retryable"] = True
                item["last_error"] = receipt.get("reason") or record
                item["failed_at"] = _now(at)
            elif normalized == STATUS_NOT_PROCESSED:
                item["status"] = STATUS_NOT_PROCESSED
                item["raw_state"] = record.get("state")
                item["not_processed_at"] = _now(at)
                item["reroute_proof"] = {
                    "target": target,
                    "at": _now(at),
                    "receipt_state": record.get("state"),
                    "notification": n,
                }

            result = {
                "sub": sub_id,
                "notification": n,
                "item_id": item_id,
                "previous_status": previous,
                "status": item["status"],
                "duplicate": duplicate,
                "conflict": conflict,
                "target_relation": target_relation,
            }
            self._audit("receipt", {"sub": sub_id, "notification": n, "state": normalized, "duplicate": duplicate})
            self._save()
            return result

    def ack(self, sub_id: str, notification: int, target: str | None = None, *, at: Any = None, **extra: Any):
        return self.receive_receipt(
            {"sub": sub_id, "notification": notification, "target": target, "state": "accepted", **extra}, at
        )

    def complete(self, sub_id: str, notification: int, target: str | None = None, *, at: Any = None, **extra: Any):
        return self.receive_receipt(
            {"sub": sub_id, "notification": notification, "target": target, "state": "done", **extra}, at
        )

    def fail(
        self,
        sub_id: str,
        notification: int,
        target: str | None = None,
        *,
        permanent: bool = False,
        at: Any = None,
        **extra: Any,
    ):
        state = "permanent_failed" if permanent else "failed"
        return self.receive_receipt(
            {"sub": sub_id, "notification": notification, "target": target, "state": state, **extra}, at
        )

    def mark_not_processed(self, sub_id: str, notification: int, target: str | None = None, *, at: Any = None):
        return self.receive_receipt(
            {"sub": sub_id, "notification": notification, "target": target, "state": "not_processed"}, at
        )

    @staticmethod
    def _target_relation(item: dict[str, Any], target: Any) -> str:
        if target is None:
            return "unspecified"
        if target == item.get("target"):
            return "current"
        if target == item.get("original_target"):
            return "original"
        if any(a.get("target") == target for a in item.get("attempts", [])):
            return "historical"
        return "unknown"

    # ------------------------------------------------------------------
    # 运营改发 / 终止
    # ------------------------------------------------------------------
    def retry_notification(
        self,
        sub_id: str,
        notification: int,
        target: str | None = None,
        *,
        at: Any = None,
        force: bool = False,
        send: bool = True,
    ) -> dict[str, Any]:
        with self._lock:
            sub = self._require_subscription(sub_id)
            n = _as_int(notification, "notification")
            item_id = sub["notification_index"].get(str(n))
            if item_id is None:
                raise NotFound(f"订阅 {sub_id} 没有通知 {n}")
            item = self._find_item(sub, item_id)

            changing_target = target is not None and target != item["target"]
            if changing_target:
                eligible, reason = self._reroute_eligibility(item)
                if not eligible and not force:
                    raise RerouteNotEligible(
                        f"通知 {sub_id}/{n} 不具备新目标改发资格：{reason}"
                    )
                if item["status"] == STATUS_ACCEPTED and not force:
                    raise RerouteNotEligible("旧端已 accepted，仍可能真实处理；必须等待结论或 not_processed 证据")
                reroute = {
                    "at": _now(at),
                    "from": item["target"],
                    "to": target,
                    "notification": n,
                    "revision": item["revision"],
                    "reason": reason,
                    "forced": force and not eligible,
                    "proof": item.get("reroute_proof"),
                }
                item["reroutes"].append(reroute)
                sub["reroute_log"].append({"sub": sub_id, **reroute})
                item["target"] = target
                if not item.get("original_target"):
                    item["original_target"] = reroute["from"]
                # 改发后状态回到可投递，但保留所有历史回执。
                item["status"] = STATUS_UNSENT
                item["raw_state"] = "rerouted"

            if not send:
                self._save()
                return {"sub": sub_id, "notification": n, "target": item["target"], "sent": False}

            # 运营显式重试时，即使状态是 sent 也允许重发；门控仍检查硬缺口。
            result = self.poll(sub_id, at=at, notification=n, force=True)
            self._save()
            return result

    # 更贴近运营语义的别名。
    reroute = retry_notification

    def _handle_reroute_record(self, record: dict[str, Any], at: Any) -> dict[str, Any]:
        target = record.get("target") or record.get("new_target")
        return self.retry_notification(
            record["sub"],
            record["notification"],
            target,
            at=at,
            force=bool(record.get("force", False)),
            send=bool(record.get("send", True)),
        )

    def update_target(
        self,
        sub_id: str,
        target: str,
        revision: Any = None,
        *,
        at: Any = None,
        move_unsent: bool = True,
    ) -> dict[str, Any]:
        """更新订阅当前回调端；已生成通知仍由 retry/reroute 规则收口。"""
        with self._lock:
            sub = self._require_subscription(sub_id)
            rev = self._find_revision(sub, revision) if revision is not None else self._current_revision(sub)
            old = rev["target"]
            rev["target"] = target
            moved = 0
            if move_unsent:
                for item in sub["items"]:
                    if item.get("revision") == rev["revision"] and item.get("notification") is None:
                        item["target"] = target
                        item["original_target"] = target
                        moved += 1
            sub["target_history"].append({"at": _now(at), "revision": rev["revision"], "from": old, "to": target})
            self._save()
            return {"sub": sub_id, "revision": rev["revision"], "from": old, "to": target, "moved_unsent": moved}

    def _handle_target_update(self, record: dict[str, Any], at: Any) -> dict[str, Any]:
        return self.update_target(
            record["sub"],
            record["target"],
            record.get("revision"),
            at=at,
            move_unsent=bool(record.get("move_unsent", True)),
        )

    def terminate_chain(
        self,
        sub_id: str,
        obj: str,
        *,
        reason: str | None = None,
        at: Any = None,
        include_failed: bool = True,
    ) -> dict[str, Any]:
        with self._lock:
            sub = self._require_subscription(sub_id)
            termination = {"object": obj, "terminated": True, "reason": reason, "at": _now(at)}
            sub["chains"][obj] = termination
            terminated_items: list[str | int] = []
            in_flight: list[int] = []
            already_terminal: list[int] = []
            for item in sub["items"]:
                if item.get("object") != obj:
                    continue
                if item["status"] in {STATUS_SENT, STATUS_ACCEPTED}:
                    # 不能伪造旧端真实结论；保留并等待回执。
                    if item["notification"] is not None:
                        in_flight.append(item["notification"])
                    continue
                if item["status"] in TERMINAL_STATUSES:
                    if item["notification"] is not None:
                        already_terminal.append(item["notification"])
                    continue
                item["status"] = STATUS_TERMINATED
                item["raw_state"] = "terminated"
                item["terminated_at"] = _now(at)
                item["termination_reason"] = reason
                terminated_items.append(item["item_id"] if item["notification"] is None else item["notification"])
            result = {
                "sub": sub_id,
                "object": obj,
                "terminated": terminated_items,
                "already_terminal": already_terminal,
                "in_flight_awaiting_conclusion": in_flight,
            }
            self._audit("terminate_chain", result)
            self._save()
            return result

    def _handle_terminate_record(self, record: dict[str, Any], at: Any) -> dict[str, Any]:
        obj = record.get("object")
        if not obj and record.get("notification") is not None:
            sub = self._require_subscription(record["sub"])
            n = _as_int(record["notification"], "notification")
            item = self._find_item(sub, sub["notification_index"].get(str(n)))
            obj = item["object"]
        if not obj:
            raise ProxyError("terminate 需要 object 或 notification")
        return self.terminate_chain(record["sub"], obj, reason=record.get("reason"), at=at)

    # ------------------------------------------------------------------
    # 资格与 inspect
    # ------------------------------------------------------------------
    @staticmethod
    def _reroute_eligibility(item: dict[str, Any]) -> tuple[bool, str]:
        proof = item.get("reroute_proof")
        if item["status"] == STATUS_DONE:
            return False, "already_done"
        if proof and proof.get("state") == STATUS_NOT_PROCESSED:
            return True, "target_reported_not_processed"
        if item["status"] == STATUS_NOT_PROCESSED:
            return True, "target_reported_not_processed"
        if item["status"] == STATUS_ACCEPTED:
            return False, "accepted_without_final_conclusion"
        if item["status"] == STATUS_SENT:
            return False, "unacknowledged_delivery_is_not_proof_of_non_processing"
        if item["status"] == STATUS_TRANSIENT_FAILED:
            return False, "transient_failure_without_not_processed_proof"
        if item["status"] == STATUS_PERMANENT_FAILED:
            return False, "permanent_failure_requires_explicit_not_processed_for_safe_takeover"
        return False, "no_not_processed_proof"

    def inspect(self, sub_id: str | None = None, at: Any = None) -> dict[str, Any]:
        with self._lock:
            if sub_id is None:
                report = {
                    "subscriptions": {sid: self.inspect(sid) for sid in sorted(self.state["subscriptions"])},
                    "project_cursors": deepcopy(self.state["project_cursors"]),
                }
                return report
            sub = self._require_subscription(sub_id)
            notifications: list[dict[str, Any]] = []
            flat_receipts: list[dict[str, Any]] = []
            reroute_candidates: list[dict[str, Any]] = []
            blocked_flat: list[dict[str, Any]] = []

            for item in sorted(sub["items"], key=lambda i: (i.get("notification") is None, i.get("notification") or 10**12, i.get("seq", 0))):
                gate = self._gate_for_item(sub, item)
                eligible, eligibility_reason = self._reroute_eligibility(item)
                view = self._notification_view(sub_id, item, gate, eligible, eligibility_reason)
                if item.get("notification") is not None:
                    notifications.append(view)
                    for receipt in view["receipts"]:
                        flat_receipts.append(
                            {
                                "sub": sub_id,
                                "item_id": item["item_id"],
                                "notification": item.get("notification"),
                                "current_status": item["status"],
                                **receipt,
                            }
                        )
                    candidate = {
                        "sub": sub_id,
                        "notification": item["notification"],
                        "item_id": item["item_id"],
                        "status": item["status"],
                        "target": item["target"],
                        "current_target": item["target"],
                        "original_target": item.get("original_target"),
                        "revision": item["revision"],
                        "eligible": eligible,
                        "eligible_for_new_target": eligible,
                        "reroute_eligible": eligible,
                        "reason": eligibility_reason,
                        "proof": item.get("reroute_proof"),
                    }
                    reroute_candidates.append(candidate)
                if (gate and not gate.get("deliverable")) or item["status"] in {
                    STATUS_PERMANENT_FAILED,
                    STATUS_TERMINATED,
                }:
                    blocked_flat.append(
                        {
                            "object": item.get("object"),
                            "seq": item.get("seq"),
                            "item_id": item["item_id"],
                            "notification": item.get("notification"),
                            "revision": item.get("revision"),
                            "target": item.get("target"),
                            "status": item["status"],
                            "code": gate.get("code") if gate else item["status"],
                            "reason": gate.get("code") if gate else item["status"],
                            "missing_positions": gate.get("missing", []) if gate else [],
                            "waiting": gate.get("waiting", []) if gate else [],
                            "hard": gate.get("hard", []) if gate else [],
                            "can_send_initial_state": bool(gate and gate.get("can_send_initial_state")),
                            "reroute_eligible": eligible,
                        }
                    )

            grouped_objects: dict[str, dict[str, Any]] = {}
            for block in blocked_flat:
                obj = block["object"]
                group = grouped_objects.setdefault(
                    obj,
                    {
                        "object": obj,
                        "blocked": True,
                        "chain_terminated": bool(sub["chains"].get(obj, {}).get("terminated")),
                        "items": [],
                        "missing_positions": set(),
                        "codes": set(),
                    },
                )
                group["items"].append(block)
                group["missing_positions"].update(block.get("missing_positions", []))
                group["codes"].add(block["code"])
            blocked_objects = []
            for obj, group in sorted(grouped_objects.items()):
                group["missing_positions"] = sorted(group["missing_positions"])
                group["codes"] = sorted(group["codes"])
                group["state"] = (
                    "terminated"
                    if group["chain_terminated"]
                    else "hard_blocked"
                    if any(i["code"] in {"predecessor_permanent_failed", "predecessor_terminated", "unknown_source_position"} for i in group["items"])
                    else "waiting_or_missing_context"
                )
                blocked_objects.append(group)

            status_counts: dict[str, int] = {}
            notification_status_counts: dict[str, int] = {}
            for item in sub["items"]:
                status_counts[item["status"]] = status_counts.get(item["status"], 0) + 1
            for n in notifications:
                notification_status_counts[n["status"]] = notification_status_counts.get(n["status"], 0) + 1
            unsent_count = sum(1 for i in sub["items"] if i.get("status") == STATUS_UNSENT)
            superseded_count = sum(1 for i in sub["items"] if i.get("status") == STATUS_SUPERSEDED)

            current_rev = self._current_revision(sub)
            report = {
                "sub": sub_id,
                "subscription": sub_id,
                "project": sub["project"],
                "current_revision": sub["current_revision"],
                "revision": current_rev["revision"],
                "cut_at": current_rev["cut_at"],
                "target": current_rev["target"],
                "kinds": list(current_rev["kinds"]),
                "source_cursor": self.state["project_cursors"].get(sub["project"], 0),
                "source_position": self.state["project_cursors"].get(sub["project"], 0),
                "revisions": deepcopy(sub["revisions"]),
                "subscription_versions": deepcopy(sub["revisions"]),
                "filter_versions": deepcopy(sub["revisions"]),
                "history": deepcopy(sub["revisions"]),
                "target_history": deepcopy(sub["target_history"]),
                "notifications": notifications,
                "deliveries": notifications,
                "receipts": flat_receipts,
                "acceptance_results": flat_receipts,
                "items": deepcopy(sub["items"]),
                "unsent_count": unsent_count,
                "summaries": deepcopy(sub["summaries"]),
                "chains": deepcopy(sub["chains"]),
                "blocked_objects": blocked_objects,
                "blocked": blocked_flat,
                "blocked_object_names": [b["object"] for b in blocked_objects],
                "reroute_candidates": reroute_candidates,
                "reroute_eligibility": reroute_candidates,
                "eligibility": reroute_candidates,
                "reroute_eligible": [c for c in reroute_candidates if c["eligible"]],
                "status_counts": status_counts,
                "notification_status_counts": notification_status_counts,
                "counts": {
                    **status_counts,
                    "unsent": unsent_count,
                    "superseded": superseded_count,
                    "blocked_objects": len(blocked_objects),
                },
                "all_done": unsent_count == 0
                and all(n["status"] == STATUS_DONE for n in notifications),
                "has_unresolved": bool(blocked_objects)
                or unsent_count > 0
                or any(n["status"] in ACTIVE_DELIVERY_STATUSES | {STATUS_TRANSIENT_FAILED, STATUS_NOT_PROCESSED} for n in notifications),
            }
            return report

    def history_versions(self, sub_id: str) -> list[dict[str, Any]]:
        return self.inspect(sub_id)["revisions"]

    def blocked_objects(self, sub_id: str) -> list[dict[str, Any]]:
        return self.inspect(sub_id)["blocked_objects"]

    def reroute_eligibility_report(self, sub_id: str) -> list[dict[str, Any]]:
        return self.inspect(sub_id)["reroute_eligibility"]

    def _notification_view(
        self,
        sub_id: str,
        item: dict[str, Any],
        gate: dict[str, Any],
        reroute_eligible: bool,
        reroute_reason: str,
    ) -> dict[str, Any]:
        return {
            "sub": sub_id,
            "item_id": item["item_id"],
            "notification": item.get("notification"),
            "id": item.get("notification"),
            "identity": None if item.get("notification") is None else f"{sub_id}:{item['notification']}",
            "event_id": item.get("event_id"),
            "event": item.get("event_id"),
            "source_event": item.get("event_id"),
            "seq": item.get("seq"),
            "object": item.get("object"),
            "event_kind": item.get("event_kind"),
            "category": item.get("category", "event"),
            "through": item.get("through"),
            "revision": item["revision"],
            "subscription_revision": item["revision"],
            "target": item["target"],
            "current_target": item["target"],
            "original_target": item.get("original_target"),
            "status": item["status"],
            "state": item["status"],
            "raw_state": item.get("raw_state"),
            "accepted": item["status"] == STATUS_ACCEPTED,
            "done": item["status"] == STATUS_DONE,
            "processed": item["status"] == STATUS_DONE,
            "failed": item["status"] == STATUS_FAILED,
            "temporary_failed": item["status"] == STATUS_FAILED,
            "failure_kind": item.get("failure_kind", "temporary" if item["status"] == STATUS_FAILED else None),
            "retryable": item.get("retryable", item["status"] == STATUS_FAILED),
            "permanent": item["status"] == STATUS_PERMANENT_FAILED or bool(item.get("permanent")),
            "permanent_failed": item["status"] == STATUS_PERMANENT_FAILED,
            "attempt_count": item["attempt_count"],
            "attempts": deepcopy(item["attempts"]),
            "receipts": deepcopy(item["receipts"]),
            "reroutes": deepcopy(item["reroutes"]),
            "reroute_eligible": reroute_eligible,
            "eligible_for_new_target": reroute_eligible,
            "reroute_reason": reroute_reason,
            "gate": gate,
            "blocked": not gate.get("deliverable", False),
            "last_error": item.get("last_error"),
            "created_at": item.get("created_at"),
            "first_sent_at": item.get("first_sent_at"),
            "last_sent_at": item.get("last_sent_at"),
            "completed_at": item.get("completed_at"),
        }

    def _sub_id_for_item(self, item: dict[str, Any]) -> str | None:
        for sid, sub in self.state["subscriptions"].items():
            if any(i is item or i.get("item_id") == item.get("item_id") for i in sub["items"]):
                return sid
        return None

    def _require_subscription(self, sub_id: str) -> dict[str, Any]:
        sub = self.state["subscriptions"].get(sub_id)
        if sub is None:
            raise NotFound(f"未知订阅 {sub_id!r}")
        return sub

    @staticmethod
    def _find_item(sub: dict[str, Any], item_id: str | None) -> dict[str, Any]:
        if item_id is None:
            raise NotFound("工作项 ID 为空")
        for item in sub["items"]:
            if item["item_id"] == item_id:
                return item
        raise NotFound(f"未知工作项 {item_id!r}")

    def _current_revision(self, sub: dict[str, Any]) -> dict[str, Any]:
        rev = self._find_revision(sub, sub["current_revision"])
        if rev is None and sub["revisions"]:
            rev = sub["revisions"][-1]
        if rev is None:
            raise ProxyError(f"订阅 {sub['id']} 没有任何修订版本")
        return rev


# 常见命名别名，便于不同测试/调用习惯发现同一核心实现。
CallbackProxy.ingest_event = CallbackProxy.add_event  # type: ignore[attr-defined]
CallbackProxy.accept_event = CallbackProxy.add_event  # type: ignore[attr-defined]
CallbackProxy.emit_event = CallbackProxy.add_event  # type: ignore[attr-defined]
CallbackProxy.add_subscription = CallbackProxy.subscribe  # type: ignore[attr-defined]
CallbackProxy.event = CallbackProxy.add_event  # type: ignore[attr-defined]
CallbackProxy.deliver = CallbackProxy.poll  # type: ignore[attr-defined]
CallbackProxy.deliver_pending = CallbackProxy.poll  # type: ignore[attr-defined]
CallbackProxy.dispatch_pending = CallbackProxy.poll  # type: ignore[attr-defined]
CallbackProxy.receipt = CallbackProxy.receive_receipt  # type: ignore[attr-defined]
CallbackProxy.handle_receipt = CallbackProxy.receive_receipt  # type: ignore[attr-defined]
CallbackProxy.process_receipt = CallbackProxy.receive_receipt  # type: ignore[attr-defined]
CallbackProxy.summary = CallbackProxy.declare_summary  # type: ignore[attr-defined]
CallbackProxy.add_summary = CallbackProxy.declare_summary  # type: ignore[attr-defined]
CallbackProxy.reroute_notification = CallbackProxy.retry_notification  # type: ignore[attr-defined]
CallbackProxy.on_message = CallbackProxy.handle  # type: ignore[attr-defined]
CallbackProxy.process_message = CallbackProxy.handle  # type: ignore[attr-defined]
CallbackProxy.operator_view = CallbackProxy.inspect  # type: ignore[attr-defined]
CallbackProxy.get_subscription = CallbackProxy.inspect  # type: ignore[attr-defined]
CallbackProxy.send_initial_state = lambda self, sub_id, obj, through=None, **kw: self.request_initial_state(
    sub_id, obj, through, send=True, **kw
)  # type: ignore[attr-defined]

CallbackAgent = CallbackProxy
SubscriptionProxy = CallbackProxy
CallbackRouter = CallbackProxy


def create_proxy(state_path: str | os.PathLike[str] | None = None, **kwargs: Any) -> CallbackProxy:
    return CallbackProxy(state_path, **kwargs)


def simulate(path: str | os.PathLike[str], **kwargs: Any) -> dict[str, Any]:
    return run_scenario(path, **kwargs)


# ----------------------------------------------------------------------
# 场景文件回放
# ----------------------------------------------------------------------
DEFAULT_EXTRA_KINDS = {
    "reroute",
    "retry",
    "terminate",
    "terminate_chain",
    "initial_state",
    "target_update",
}


def run_scenario(
    path: str | os.PathLike[str],
    *,
    state_path: str | os.PathLike[str] | None = None,
    proxy: CallbackProxy | None = None,
    validate_registry: bool = True,
    strict: bool = False,
) -> dict[str, Any]:
    path = Path(path)
    if proxy is None:
        proxy = CallbackProxy(state_path)

    supported: set[str] | None = None
    registry = path.parent / "message_types.json"
    if validate_registry and iter_messages is not None and registry.exists():
        try:
            supported = set(json.loads(registry.read_text(encoding="utf-8"))) | DEFAULT_EXTRA_KINDS
        except (OSError, json.JSONDecodeError):
            supported = None

    observations: list[dict[str, Any]] = []
    inspections: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as source:
        if supported is not None and iter_messages is not None:
            messages: Iterable[dict[str, Any]] = iter_messages(path, supported)
            # iter_messages 自己打开路径；上面的 source 仅用于保持简单，关闭即可。
            source.close()
            iterator = iter_messages(path, supported)
        else:
            def _raw_messages() -> Iterable[dict[str, Any]]:
                previous = -1
                for line_number, raw in enumerate(source, 1):
                    if not raw.strip():
                        continue
                    row = json.loads(raw)
                    at = row.get("at", 0)
                    if isinstance(at, int) and at < previous:
                        raise ValueError(f"第 {line_number} 行 at 倒退")
                    previous = at if isinstance(at, int) else previous
                    yield row
            iterator = _raw_messages()

        for message in iterator:
            try:
                observation = proxy.handle(message)
            except ProxyError as exc:
                if strict:
                    raise
                observation = {
                    "at": message.get("at") if isinstance(message, dict) else None,
                    "kind": message.get("kind") if isinstance(message, dict) else None,
                    "ok": False,
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                }
            observations.append(observation)
            if message.get("kind") == "inspect" and observation.get("ok"):
                inspections.append(observation["result"])

    return {
        "path": str(path),
        "proxy": proxy,
        "observations": observations,
        "inspections": inspections,
        "errors": [o for o in observations if not o.get("ok", True)],
        "final_inspection": proxy.inspect(None),
    }


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="运行本地回调订阅切换演练")
    parser.add_argument("scenario", nargs="?", type=Path, help="JSONL 场景文件")
    parser.add_argument("--state", type=Path, default=None, help="持久化状态 JSON 路径")
    parser.add_argument("--out", type=Path, default=None, help="把观察结果写成 JSONL")
    parser.add_argument("--inspect-all", action="store_true", help="结束后输出全部订阅检查报告")
    parser.add_argument("--inspect-state", nargs="?", const="__all__", default=None,
                        help="不运行场景，直接检查 --state；可传订阅 ID")
    args = parser.parse_args(argv)

    if args.inspect_state is not None:
        proxy = CallbackProxy(args.state)
        sub_id = None if args.inspect_state == "__all__" else args.inspect_state
        print(json.dumps(proxy.inspect(sub_id), ensure_ascii=False, indent=2))
        return 0

    if not args.scenario:
        parser.error("需要 scenario JSONL 文件")

    result = run_scenario(args.scenario, state_path=args.state)
    lines = [json.dumps(o, ensure_ascii=False) for o in result["observations"]]
    output = "\n".join(lines) + ("\n" if lines else "")
    if args.out:
        args.out.write_text(output, encoding="utf-8")
    else:
        print(output, end="")
    if args.inspect_all:
        print(json.dumps(result["final_inspection"], ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
