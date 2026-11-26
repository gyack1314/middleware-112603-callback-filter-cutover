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
