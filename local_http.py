"""仅绑定 127.0.0.1 的合成回调端与可选 HTTP 代理 API。

场景 JSONL 已经能完整模拟回执；本模块用于需要真实本地 HTTP 回路的演练。
不访问任何外网地址。
"""

from __future__ import annotations

import json
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Iterable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable

from callback_proxy import CallbackProxy


def _read_json(handler: BaseHTTPRequestHandler) -> dict[str, Any]:
    length = int(handler.headers.get("Content-Length", "0") or "0")
    raw = handler.rfile.read(length) if length else b"{}"
    try:
        value = json.loads(raw.decode("utf-8") or "{}")
    except json.JSONDecodeError as exc:
        raise ValueError(f"请求体不是 JSON: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError("请求体必须是 JSON 对象")
    return value


def _write_json(handler: BaseHTTPRequestHandler, status: int, payload: Any) -> None:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


class HttpTransport:
    """把代理 envelope POST 到本地合成端。"""

    def __init__(
        self,
        target_urls: dict[str, str] | None = None,
        *,
        timeout: float = 2.0,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.target_urls = dict(target_urls or {})
        self.timeout = timeout
        self.sleep = sleep
        self.calls: list[dict[str, Any]] = []

    def url_for(self, target: str) -> str:
        return self.target_urls.get(target, target)

    def __call__(self, envelope: dict[str, Any]) -> dict[str, Any]:
        target = envelope["target"]
        url = self.url_for(target)
        data = json.dumps(envelope, ensure_ascii=False).encode("utf-8")
        request = urllib.request.Request(
            url,
            data=data,
            method="POST",
            headers={"Content-Type": "application/json", "Idempotency-Key": envelope["identity"]},
        )
        self.calls.append({"target": target, "url": url, "identity": envelope["identity"]})
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                raw = response.read().decode("utf-8")
                result = json.loads(raw) if raw else {}
                result.setdefault("http_status", response.status)
                return result
        except urllib.error.HTTPError as exc:
            raw = exc.read().decode("utf-8", "replace")
            try:
                result = json.loads(raw) if raw else {}
            except json.JSONDecodeError:
                result = {"state": "failed", "reason": raw[:200]}
            result["http_status"] = exc.code
            # 4xx 默认会被 _normalize_state 的状态码规则识别为永久失败；
            # 408/429 与 5xx 为暂时失败。
            result.setdefault("state", "failed")
            return result
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            return {"state": "failed", "retryable": True, "reason": f"local transport error: {exc}"}


class SyntheticCallbackService:
    """按 target 记录投递并按通知身份去重的本地合成回调服务。"""

    def __init__(self, host: str = "127.0.0.1", port: int = 0):
        self.host = host
        self.server = ThreadingHTTPServer((host, port), self._make_handler())
        self.base_url = f"http://{host}:{self.server.server_address[1]}"
        self.deliveries: list[dict[str, Any]] = []
        self._seen: dict[str, set[str]] = {}
        self._lock = threading.RLock()
        # behavior(target, envelope, duplicate) -> 同步响应 dict。
        self.behavior: Callable[[str, dict[str, Any], bool], dict[str, Any]] | None = None
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def target_url(self, target: str) -> str:
        return f"{self.base_url}/callback/{target}"

    def target_urls(self, targets: Iterable[str] | None = None) -> dict[str, str]:
        if targets is None:
            return {}
        return {target: self.target_url(target) for target in targets}

    def _make_handler(self):
        service = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, fmt: str, *args: Any) -> None:  # 保持测试输出安静。
                return

            def do_GET(self) -> None:  # noqa: N802
                if self.path.startswith("/healthz"):
                    _write_json(self, 200, {"ok": True})
                elif self.path.startswith("/deliveries"):
                    target = None
                    if "target=" in self.path:
                        target = self.path.split("target=", 1)[1].split("&", 1)[0]
                    with service._lock:
                        rows = [d for d in service.deliveries if target is None or d["target"] == target]
                    _write_json(self, 200, {"deliveries": rows})
                elif self.path.startswith("/identities"):
                    with service._lock:
                        _write_json(self, 200, {k: sorted(v) for k, v in service._seen.items()})
                else:
                    _write_json(self, 404, {"error": "not_found"})

            def do_POST(self) -> None:  # noqa: N802
                if not self.path.startswith("/callback/"):
                    _write_json(self, 404, {"error": "not_found"})
                    return
                target = self.path.rsplit("/", 1)[-1]
                try:
                    envelope = _read_json(self)
                except ValueError as exc:
                    _write_json(self, 400, {"error": str(exc)})
                    return
                identity = str(
                    envelope.get("idempotency_key")
                    or envelope.get("identity")
                    or f"{envelope.get('sub')}:{envelope.get('notification')}"
                )
                with service._lock:
                    seen = service._seen.setdefault(target, set())
                    duplicate = identity in seen
                    seen.add(identity)
                    record = {
                        "target": target,
                        "identity": identity,
                        "notification": envelope.get("notification"),
                        "sub": envelope.get("sub"),
                        "revision": envelope.get("revision"),
                        "source_event": envelope.get("source_event"),
                        "duplicate": duplicate,
                        "envelope": envelope,
                    }
                    service.deliveries.append(record)
                    behavior = service.behavior
                if behavior is not None:
                    response = behavior(target, envelope, duplicate)
                else:
                    response = {"state": "accepted", "duplicate": duplicate}
                response = dict(response)
                response["duplicate"] = duplicate
                _write_json(self, 200, response)

        return Handler


class ProxyHttpService:
    """暴露 CallbackProxy 运营 API 的本地 HTTP 服务。"""

    def __init__(
        self,
        proxy: CallbackProxy | None = None,
        *,
        state_path: str | Path | None = None,
        host: str = "127.0.0.1",
        port: int = 0,
    ) -> None:
        self.proxy = proxy or CallbackProxy(state_path)
        self.host = host
        self.server = ThreadingHTTPServer((host, port), self._make_handler())
        self.base_url = f"http://{host}:{self.server.server_address[1]}"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def _make_handler(self):
        service = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, fmt: str, *args: Any) -> None:
                return

            def do_GET(self) -> None:  # noqa: N802
                path = self.path.split("?", 1)[0]
                try:
                    if path == "/healthz":
                        _write_json(self, 200, {"ok": True})
                    elif path in {"/v1/inspect", "/inspect", "/v1/subscriptions", "/subscriptions"}:
                        sub = None
                        if "sub=" in self.path:
                            sub = self.path.split("sub=", 1)[1].split("&", 1)[0]
                        _write_json(self, 200, service.proxy.inspect(sub))
                    elif path.startswith(("/v1/inspect/", "/inspect/", "/v1/subscriptions/", "/subscriptions/")):
                        sub = path.rsplit("/", 1)[-1]
                        _write_json(self, 200, service.proxy.inspect(sub))
                    else:
                        _write_json(self, 404, {"error": "not_found"})
                except Exception as exc:  # noqa: BLE001 - 本地 API 返回结构化错误。
                    _write_json(self, 400, {"ok": False, "error": str(exc)})

            def do_POST(self) -> None:  # noqa: N802
                path = self.path.split("?", 1)[0]
                routes = {
                    "/v1/events": service.proxy.add_event,
                    "/events": service.proxy.add_event,
                    "/v1/subscriptions": service.proxy.subscribe,
                    "/subscriptions": service.proxy.subscribe,
                    "/subscribe": service.proxy.subscribe,
                    "/v1/poll": service.proxy.poll,
                    "/poll": service.proxy.poll,
                    "/v1/receipts": service.proxy.receive_receipt,
                    "/receipts": service.proxy.receive_receipt,
                    "/receipt": service.proxy.receive_receipt,
                    "/v1/summaries": service.proxy.declare_summary,
                    "/summaries": service.proxy.declare_summary,
                    "/summary": service.proxy.declare_summary,
                    "/v1/reroute": service._reroute,
                    "/reroute": service._reroute,
                    "/v1/retry": service._reroute,
                    "/retry": service._reroute,
                    "/v1/terminate": service._terminate,
                    "/terminate": service._terminate,
                    "/v1/restart": lambda body: service._restart(),
                    "/restart": lambda body: service._restart(),
                }
                func = routes.get(path)
                if func is None:
                    _write_json(self, 404, {"ok": False, "error": "not_found"})
                    return
                try:
                    body = _read_json(self)
                    result = func(body)
                    _write_json(self, 200, {"ok": True, "result": result})
                except Exception as exc:  # noqa: BLE001
                    _write_json(self, 400, {"ok": False, "error": str(exc)})

        return Handler

    def _restart(self) -> dict[str, Any]:
        self.proxy.restart()
        return {"restarted": True, "subscriptions": sorted(self.proxy.state["subscriptions"])}

    def _reroute(self, body: dict[str, Any]):
        return self.proxy.retry_notification(
            body["sub"],
            int(body["notification"]),
            body.get("target") or body.get("new_target"),
            force=bool(body.get("force", False)),
        )

    def _terminate(self, body: dict[str, Any]):
        obj = body.get("object")
        if not obj and body.get("notification") is not None:
            report = self.proxy.inspect(body["sub"])
            n = int(body["notification"])
            match = next(x for x in report["notifications"] if x["notification"] == n)
            obj = match["object"]
        return self.proxy.terminate_chain(body["sub"], obj, reason=body.get("reason"))


class CallbackProxyServer(ProxyHttpService):
    """语义化服务器别名。"""


def serve_demo(state_path: str | Path | None = None, port: int = 8765) -> None:
    """启动代理 API 和合成回调端的简单演示入口。"""
    synthetic = SyntheticCallbackService(port=0)
    transport = HttpTransport()
    proxy = CallbackProxy(state_path, transport=transport)
    for sub in proxy.state.get("subscriptions", {}).values():
        for rev in sub.get("revisions", []):
            target = rev.get("target", "")
            if target and not target.startswith(("http://", "https://")):
                transport.target_urls[target] = synthetic.target_url(target)

    original_subscribe = proxy.subscribe

    def subscribe_with_urls(record, at=None):
        target = record["target"]
        if not target.startswith("http://") and not target.startswith("https://"):
            transport.target_urls[target] = synthetic.target_url(target)
        return original_subscribe(record, at)

    proxy.subscribe = subscribe_with_urls  # type: ignore[method-assign]
    api = ProxyHttpService(proxy, port=port)
    print(f"proxy api: {api.base_url}")
    print(f"synthetic callbacks: {synthetic.base_url}/callback/<target>")
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        pass
    finally:
        api.close()
        synthetic.close()


if __name__ == "__main__":  # pragma: no cover
    import argparse

    parser = argparse.ArgumentParser(description="启动本地合成回调代理服务")
    parser.add_argument("--state", type=Path, default=None)
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()
    serve_demo(args.state, args.port)
