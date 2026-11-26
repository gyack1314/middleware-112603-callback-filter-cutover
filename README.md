# 多系统回调订阅的筛选切换与顺序保全

event 以项目 publication 内连续 seq 标识。topic 依赖在同一 object 实体内解释，view 以订阅编号及修订识别。

## 输入消息

每个 JSONL 文件是一段独立输入记录。每行包括整数秒点 at、消息名称 kind 与对象 record。at 代表中间件收到消息的时刻，同一秒内按行序提交。业务时间和对应回执引用写在 record 中。

- **event**　提供 project、seq、object、kind。after 列本对象需要的先行源位置。
- **subscribe**　sub 订阅项目，kinds、cut_at、target、revision。
- **poll**　指定 sub 向当前可发目标尝试投递。
- **receipt**　目标确认 sub/notification，状态 accepted、failed、done 或 not_processed。合成通知一律按发出的先后从 1 编号。
- **summary**　目标声明当前持有的 object 初始状态及覆盖位置，可查询但不透出正文。
- **restart**　重启。
- **inspect**　查询订阅。

## 示例

- new-filter-needs-context.jsonl
- old-dispatch-before-endpoint-change.jsonl

## 读取演练资料

运行环境为 Python 3.11，只使用标准库。在根目录运行以下命令可核对文件格式及读取接口。

    python3 scenario_reader.py examples
    python3 -m unittest discover -v

资料读取器会按文件统计各类参与方消息的数量。演练记录只描述各参与方交给系统的输入。

## 回调代理实现

核心实现在 `callback_proxy.py`，只使用 Python 标准库；`proxy.py` / `main.py` 是导入和命令行别名。

运行单个演练并输出每条输入的处理结果：

```bash
python3 callback_proxy.py examples/new-filter-needs-context.jsonl
python3 callback_proxy.py examples/old-dispatch-before-endpoint-change.jsonl \
  --state /tmp/callback-proxy-state.json --inspect-all
```

编程接口：

```python
from callback_proxy import CallbackProxy

proxy = CallbackProxy("state.json")
proxy.add_event({"project": "p", "seq": 1, "object": "x",
                 "kind": "create", "after": []})
proxy.subscribe({
    "sub": "s", "project": "p", "kinds": ["release"],
    "cut_at": 2, "target": "new", "revision": 2,
})
proxy.add_event({"project": "p", "seq": 2, "object": "x",
                 "kind": "release", "after": [1]})
proxy.declare_summary({"sub": "s", "target": "new",
                       "object": "x", "through": 1})
proxy.poll("s")
proxy.receive_receipt({"sub": "s", "target": "new",
                       "notification": 1, "state": "done"})
report = proxy.inspect("s")
```

### 状态合并规则

- `cut_at` 是新修订范围的**首个包含位置**。每个源位置按覆盖它的最新修订解释；新订阅不会从流首全量重扫。
- 通知在第一次实际发出时按该订阅的发出顺序获得数字身份。首次发出后，通知的修订版本、原始目标、源事件和 `sub:notification` 身份冻结；临时失败后的轮询重发保持这些字段不变，并标记重复提交身份。
- 已 `accepted` 的通知仍等待 `done`、失败或 `not_processed` 真实结论。只有旧目标明确回报 `not_processed`（或调用方显式 `force=True` 承担运营责任）后，`retry_notification(..., target="new-target")` 才会把同一通知身份改发到新目标。
- 依赖位置必须由以下任一证据满足：同一目标上的先行通知已 `done`；该目标的 `summary` 明确覆盖对应 `object/through`；或协议级 `initial_state` 通知已经 `done`。被 `kinds`/`cut_at` 排除的源事件不会被自动补放。
- 先行通知永久失败或对象链被终止后，只阻塞同一 `object` 的后续项；其他对象继续投递。
- `inspect(sub)` 同时给出修订历史、逐项通知/回执、受阻对象、缺失位置、初始状态补齐资格和改发资格；没有用单一全组状态掩盖未送达。
- 每个变更操作都会原子写入状态 JSON。`restart()` 后源位置、修订、通知身份和实收回执均保留。

额外的运营记录种类 `reroute`/`retry`、`terminate`、`initial_state` 和 `target_update` 可直接交给 `proxy.handle(...)` 或写入 JSONL；基础七类消息仍保持 `message_types.json` 登记不变。

## 本地 HTTP 合成服务

`local_http.py` 提供仅绑定 `127.0.0.1` 的合成回调端和可选代理 HTTP API：

```bash
python3 local_http.py --state /tmp/callback-proxy-state.json --port 8765
```

`SyntheticCallbackService` 会按 `(target, sub:notification)` / `Idempotency-Key` 自行发现重复投递；`HttpTransport` 只向本地合成 URL 发 POST。
