# 顺序脚本 HTTP 替身（ordered-http-stub）

一个可以**手动控制响应顺序**的 HTTP 测试替身：通过 HTTP 上传 1～20 步脚本，
客户端再直接调用本服务。每步声明预期的方法、路径、JSON 正文，以及命中后返回的
状态码和 JSON 正文；某一步可以标记 `defer: true`，命中后**只挂住那个请求的响应**，
等控制端拿到放行标识（token）调用放行接口后，才把脚本响应返回给客户端。

* Python 3.11 + FastAPI，单进程内存实现，**重启即清空**，无数据库、无磁盘文件。
* 只匹配"当前步"：方法/路径/正文任一不符、或脚本已耗尽，一律返回 **409 且不推进**。
* 匹配成功时"占用该步 + 指针推进"在同一把锁内原子完成，并发请求不可能占用同一步，
  查询中也不会出现"已占用却未推进"的半次操作。
* `defer` 不阻塞后续步骤匹配，也不阻塞任何控制接口（查询/放行/重置）。

## 一条命令启动

```bash
docker compose up --build
```

服务监听 **8000 端口**：

* 替身业务地址：`http://localhost:8000/<脚本里的任意路径>`
* 控制接口前缀：`http://localhost:8000/__control`
* 健康检查：`curl http://localhost:8000/__control/health` → `{"ok":true}`
* 交互式 API 文档：`http://localhost:8000/docs`

> 状态只存在单个进程内存中，Compose 固定 `replicas: 1`、uvicorn 固定 `--workers 1`，
> 切勿水平扩容，否则各副本状态互不可见。

不用 Docker 时：

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 1
```

## 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/__control/runs` | 创建运行（上传 1～20 步脚本），返回 201 和完整状态。重复调用会替换脚本：未放行的暂缓请求全部以 409 结束，旧 token 作废 |
| GET | `/__control/state` | 查询完整一致状态（见下） |
| POST | `/__control/release/{token}` | 按放行标识放行一个暂缓请求，**该请求随即收到脚本响应**（放行接口自己也返回同样的脚本响应）。标识无效/属于旧轮次 → 409 |
| POST | `/__control/reset` | 重置：未放行请求以 409 结束、占用记录清空、轮次 +1、从第 1 步重跑 |
| * | `/{path}` | 业务 catch-all：与脚本当前步精确比对方法 + 路径 + JSON 正文 |

### 脚本格式

```json
{
  "steps": [
    {
      "method": "POST",
      "path": "/orders",
      "requestBody": {"sku": "A", "qty": 1},
      "statusCode": 201,
      "responseBody": {"id": 1, "accepted": true},
      "defer": true
    }
  ]
}
```

| 字段 | 必填 | 含义 |
|---|---|---|
| `method` | 是 | 预期 HTTP 方法（大小写不敏感，内部归一为大写） |
| `path` | 是 | 预期路径，必须 `/` 开头，区分大小写，不含查询串（查询串不参与匹配） |
| `requestBody` | 否 | 预期 JSON 正文；省略或 `null` 表示要求**空请求体** |
| `statusCode` | 否 | 命中后返回状态码，默认 200（100～599） |
| `responseBody` | 否 | 返回的 JSON 正文；省略/`null` 或状态码 204 时返回空响应体 |
| `defer` | 否 | `true` 时命中后挂起响应，等待放行；默认 `false` 立即响应 |

步数必须在 **1～20** 之间；`/__control...` 为保留路径，脚本不得使用（校验返回 422）。

### GET /__control/state 返回

```json
{
  "epoch": 1,
  "phase": "running",
  "current_step": 3,
  "last_occupied_step": 2,
  "remaining": 1,
  "total_steps": 3,
  "steps": [ {"step": 1, "method": "POST", "path": "/orders", "defer": true, "status_code": 201} ],
  "occupations": [
    {
      "step": 1, "method": "POST", "path": "/orders",
      "request_body": {"sku": "A", "qty": 1},
      "status_code": 201, "response_body": {"id": 1, "accepted": true},
      "deferred": true, "occupied_at": 1728100000.1,
      "release_token": "e1-xxxxxxxx",
      "state": "held"
    }
  ],
  "waiting": [ {"token": "e1-xxxxxxxx", "step": 1, "method": "POST", "path": "/orders",
                "request_body": {"sku": "A", "qty": 1}, "state": "waiting",
                "held_since": 1728100000.1, "released_at": null} ],
  "release_tokens": ["e1-xxxxxxxx"],
  "mismatch_count": 0,
  "created_at": 1728100000.0,
  "started_at": 1728100000.0
}
```

* `current_step`：下一个待匹配步（1 基）；全部走完后为 `null`，`phase` 变 `finished`。
* `occupations[].state`：`responded`（已立即返回）/ `held`（暂缓等待中）/
  `released`（已放行）/ `client_disconnected`（客户端中途断开，该步仍占用）。
* `release_tokens`：当前所有暂缓请求的放行标识，按步号排序。
* `epoch`：轮次号，每次创建/重置 +1；token 内含轮次前缀（如 `e2-...`）。

## 示例：双请求暂缓、放行、重置（无任何固定延时）

关键做法：不靠 `sleep` 猜时机，而是**轮询 `/__control/state` 等 token 出现**。

### 1. 创建三步脚本

```bash
curl -s -X POST http://localhost:8000/__control/runs \
  -H 'content-type: application/json' -d '{
  "steps": [
    {"method":"POST","path":"/orders","requestBody":{"sku":"A","qty":1},
     "statusCode":201,"responseBody":{"id":1,"accepted":true},"defer":true},
    {"method":"GET","path":"/orders/1",
     "statusCode":200,"responseBody":{"id":1,"state":"PAID"},"defer":true},
    {"method":"DELETE","path":"/orders/1","statusCode":204,"responseBody":null}
  ]}'
```

### 2. 两个客户端请求并发打出，各自挂起

终端 A：

```bash
curl -s -X POST http://localhost:8000/orders \
  -H 'content-type: application/json' -d '{"sku":"A","qty":1}' &   # 命中第 1 步，挂起
curl -s http://localhost:8000/orders/1 &                           # 命中第 2 步，挂起
wait
```

如果此时打错一步（例如再来一个相同 POST），会立即收到 409
（`第 3 步方法不符`），指针不动。

### 3. 状态驱动地取出两个放行标识（终端 B）

```bash
# 轮询直到出现 2 个暂缓请求——不使用任何固定等待时间
while [ "$(curl -s http://localhost:8000/__control/state \
        | python3 -c 'import sys,json;print(len(json.load(sys.stdin)["release_tokens"]))')" -lt 2 ]
do :; done

eval set -- $(curl -s http://localhost:8000/__control/state \
  | python3 -c 'import sys,json;print(" ".join(json.load(sys.stdin)["release_tokens"]))')
T1=$1; T2=$2
```

此时查询会看到 `occupations` 已含第 1、2 步（即"占用即推进"），
`current_step` 已经是 3。

### 4. 放行（可任意顺序，这里逆序）

```bash
curl -s -X POST http://localhost:8000/__control/release/$T2
# {"id":1,"state":"PAID"}  —— 终端 A 的 GET /orders/1 同时解除挂起并收到同样内容
curl -s -X POST http://localhost:8000/__control/release/$T1
# {"id":1,"accepted":true} —— 终端 A 的 POST /orders 解除挂起

curl -s -i -X DELETE http://localhost:8000/orders/1   # HTTP/1.1 204，空响应体
```

### 5. 重置：未放行请求 409 结束、清空记录、旧 token 作废

```bash
curl -s -X POST http://localhost:8000/__control/reset >/dev/null   # 进入新轮次

# 新轮次第 1 步，命中后挂起
curl -s -X POST http://localhost:8000/orders \
  -H 'content-type: application/json' -d '{"sku":"A","qty":1}' &
while [ -z "$OLD" ]; do
  OLD=$(curl -s http://localhost:8000/__control/state \
    | python3 -c 'import sys,json;t=json.load(sys.stdin)["release_tokens"];print(t[0] if t else "")')
done

curl -s -X POST http://localhost:8000/__control/reset
# 后台那个 curl 立刻结束并打印 409：
# {"error":"conflict","message":"第 1 步的暂缓请求因重置/替换脚本以 409 结束","step":1}

curl -s -i -X POST http://localhost:8000/__control/release/$OLD
# HTTP/1.1 409 —— 旧轮次 token 永远不能放行新轮次请求
```

### Python 版完整脚本

上述全流程（含逆序放行、重置中止、旧 token 409、新轮重跑）也有一份可直接运行的
asyncio 版本，同样只轮询状态、不写死任何延时：

```bash
.venv/bin/pip install -r requirements-dev.txt   # 仅跑示例需要 httpx
python3 examples/defer_release_reset_demo.py
```

## 运行测试

```bash
.venv/bin/pip install -r requirements-dev.txt
.venv/bin/python -m pytest -q
```

测试覆盖：顺序匹配与 409 不推进、脚本耗尽、并发不可重复占用同一步、
双请求暂缓/逆序放行、重置中止与 token 失效、替换脚本、空正文/非法 JSON、参数校验。
