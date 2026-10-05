# Sequential HTTP Stand-in（顺序受控的 HTTP 替身）

一个单进程、纯内存的 HTTP 替身服务：上传一段 1～20 步的脚本，客户端按顺序调用；
只有**当前步**被严格匹配（方法 + 路径 + JSON 正文），匹配后该步立即被占用并推进。
可把某一步标记为**暂缓响应（hold）**，请求会被挂起，直到通过管理接口按放行标识放行，
才返回该步脚本里定义的状态码和 JSON 正文。

- 不符当前步 / 脚本耗尽：返回 **409**，且**不推进**
- 并发请求在锁上排队，**不可能占用同一步**；匹配与推进在同一把锁内完成，查询不会看到“已占用却未推进”的半次操作
- 暂缓只阻塞该请求本身：后续步骤的请求、查询、放行、重置均可照常执行
- 重置：所有未放行的挂起请求立即以 **409** 结束，清空占用记录，从首步重跑；
  **旧放行标识一律返回 409**，不能放行新一轮请求
- 数据仅存于单进程内存，**重启清空**

## 启动

需要 Docker + Compose v2。一条命令：

```bash
docker compose up --build
```

访问端口：**http://localhost:8080**

> 不使用 Docker 时：`pip install -r requirements.txt && uvicorn app.main:app --port 8080`

## API

### 管理接口（保留前缀 `/__admin`，脚本里不能使用该前缀的路径）

| 方法 | 路径 | 说明 |
| ---- | ---- | ---- |
| POST | `/__admin/runs` | 创建一轮运行，上传 1～20 步脚本，返回 `run_id` |
| GET  | `/__admin/runs` | 列出全部 run |
| GET  | `/__admin/runs/{run_id}` | 查询完整一致状态：步号、全部占用记录、暂缓放行标识 |
| POST | `/__admin/runs/{run_id}/release` | 按 `release_token` 放行，请求随后返回脚本响应 |
| POST | `/__admin/runs/{run_id}/reset` | 重置：挂起请求 409 结束、清空记录、回到首步 |

### 脚本步格式

```json
{
  "steps": [
    {
      "method": "POST",
      "path": "/orders",
      "json_body": {"item": "book", "qty": 2},
      "status_code": 201,
      "response_json": {"id": "order-1"},
      "hold": true
    }
  ]
}
```

- `method`：预期 HTTP 方法（大小写不敏感）
- `path`：预期路径，必须以 `/` 开头，不含 query（客户端请求的 query 不参与匹配）
- `json_body`：预期 JSON 正文。**省略该字段**表示不校验正文；显式写 `null` 表示正文必须是 JSON `null`
- `status_code`：放行/匹配后返回的状态码（100–599，默认 200）
- `response_json`：返回的 JSON 正文（默认 `null`）
- `hold`：`true` 时暂缓响应，直到放行（默认 `false`）

### 客户端调用替身

普通请求直接打到对应路径即可。当存在多个 run 时必须带请求头 `X-Run-ID: <run_id>`；
只有一个 run 时可省略。任何与当前步不符的请求、或脚本耗尽后的请求，得到：

```json
HTTP 409
{"error": "conflict", "detail": "……", "current_step_number": 2}
```

## 示例：双请求暂缓 → 查询 → 放行 → 重置

下面示例**没有任何固定延时（sleep）**：用轮询查询接口来确认“请求已占用并暂缓”，
查询返回完整一致状态，拿到标识后立刻放行。

### 1) 创建脚本（两步都暂缓）

```bash
BASE=http://localhost:8080

curl -s -X POST "$BASE/__admin/runs" \
  -H 'Content-Type: application/json' \
  -d '{
    "steps": [
      {"method": "POST", "path": "/orders",
       "json_body": {"item": "book"}, "status_code": 201,
       "response_json": {"id": "order-1"}, "hold": true},
      {"method": "GET", "path": "/orders/order-1", "status_code": 200,
       "response_json": {"id": "order-1", "state": "paid"}, "hold": true}
    ]
  }'
# => {"run_id":"ab12...","total_steps":2}

RID=ab12...   # 换成上面返回的 run_id
```

### 2) 并发发出两个客户端请求（都会挂起等待放行）

```bash
# 终端 A
curl -s -X POST "$BASE/orders" -H "X-Run-ID: $RID" \
  -H 'Content-Type: application/json' -d '{"item":"book"}'

# 终端 B
curl -s "$BASE/orders/order-1" -H "X-Run-ID: $RID"
```

后台方式 + 自动取标识（无 sleep，轮询查询直到两步均被占用）：

```bash
curl -s -X POST "$BASE/orders" -H "X-Run-ID: $RID" \
  -H 'Content-Type: application/json' -d '{"item":"book"}' > resp1.json &
curl -s "$BASE/orders/order-1" -H "X-Run-ID: $RID" > resp2.json &

# 轮询（无固定延时）：等到两步都已占用
for i in $(seq 1 1000); do
  Q=$(curl -s "$BASE/__admin/runs/$RID")
  [ "$(printf '%s' "$Q" | python3 -c 'import sys,json;print(len(json.load(sys.stdin)["occupancies"]))')" = "2" ] && break
done
printf '%s\n' "$Q" | python3 -m json.tool
```

查询结果包含当前步号（此时脚本耗尽，`current_step_number: null`、`exhausted: true`）、
每步的占用记录，以及暂缓请求的一次性放行标识：

```json
{
  "run_id": "ab12...",
  "epoch": 0,
  "current_step_number": null,
  "exhausted": true,
  "total_steps": 2,
  "steps": [ ... ],
  "occupancies": [
    {"seq": 1, "step_number": 1, "method": "POST", "path": "/orders",
     "held": true, "released": false, "cancelled": false,
     "release_token": "ep0-aaaa……", ...},
    {"seq": 2, "step_number": 2, "method": "GET", "path": "/orders/order-1",
     "held": true, "released": false, "cancelled": false,
     "release_token": "ep0-bbbb……", ...}
  ],
  "held": [
    {"seq": 1, "step_number": 1, "release_token": "ep0-aaaa……", "released": false},
    {"seq": 2, "step_number": 2, "release_token": "ep0-bbbb……", "released": false}
  ]
}
```

### 3) 按标识放行（顺序可自选，放行后请求才返回脚本响应）

```bash
# 放行第 2 步（终端 B 的请求先返回）
T2=$(curl -s "$BASE/__admin/runs/$RID" | python3 -c 'import sys,json;print(json.load(sys.stdin)["held"][1]["release_token"])')
curl -s -X POST "$BASE/__admin/runs/$RID/release" \
  -H 'Content-Type: application/json' \
  -d "{\"release_token\": \"$T2\"}"
# 终端 B 收到：HTTP 200 {"id":"order-1","state":"paid"}

# 放行第 1 步
T1=$(curl -s "$BASE/__admin/runs/$RID" | python3 -c 'import sys,json;print([h for h in json.load(sys.stdin)["held"] if h["step_number"]==1][0]["release_token"])')
curl -s -X POST "$BASE/__admin/runs/$RID/release" \
  -H 'Content-Type: application/json' \
  -d "{\"release_token\": \"$T1\"}"
# 终端 A 收到：HTTP 201 {"id":"order-1"}

wait
cat resp1.json   # {"id":"order-1"}
cat resp2.json   # {"id":"order-1","state":"paid"}
```

同一标识放行两次，第二次返回 **409**。

### 4) 重置：未放行请求以 409 结束，从首步重跑，旧标识失效

上面两步都放行后脚本已耗尽，先重置回到首步：

```bash
curl -s -X POST "$BASE/__admin/runs/$RID/reset" | python3 -m json.tool
# => state.current_step_number=1, occupancies=[], held=[], epoch=1

# 重新占用首步并被挂起（无 sleep：轮询到 held 出现即继续）
curl -s -X POST "$BASE/orders" -H "X-Run-ID: $RID" \
  -H 'Content-Type: application/json' -d '{"item":"book"}' > resp3.json &

for i in $(seq 1 1000); do
  Q=$(curl -s "$BASE/__admin/runs/$RID")
  OLD=$(printf '%s' "$Q" | python3 -c 'import sys,json
h=json.load(sys.stdin)["held"]
print(h[0]["release_token"] if h else "")')
  [ -n "$OLD" ] && break
done
echo "旧轮（epoch 0 之后又重置，当前 epoch 1）的标识: $OLD"

# 再次重置：挂起请求立刻以 409 结束，记录清空，回到首步，epoch 变为 2
curl -s -X POST "$BASE/__admin/runs/$RID/reset" | python3 -m json.tool
wait
cat resp3.json
# => HTTP 409 {"error":"conflict","detail":"该响应已被重置取消，未放行请求以 409 结束", ...}

# 旧标识放行新一轮请求 -> 一律 409
curl -s -i -X POST "$BASE/__admin/runs/$RID/release" \
  -H 'Content-Type: application/json' \
  -d "{\"release_token\": \"$OLD\"}"
# => HTTP 409 {"error":"conflict","detail":"无效或已失效的放行标识（可能已重置、已使用或不存在）", ...}

# 状态已清空，回到首步
curl -s "$BASE/__admin/runs/$RID" | python3 -m json.tool
# current_step_number=1, occupancies=[], held=[]
```

## 不匹配与耗尽

```bash
# 当前是第 1 步 POST /orders，发 GET /orders -> 409，步号不变
curl -s -i "$BASE/orders" -H "X-Run-ID: $RID"

# 正文不符 -> 409，步号不变
curl -s -i -X POST "$BASE/orders" -H "X-Run-ID: $RID" \
  -H 'Content-Type: application/json' -d '{"item":"pen"}'

# 走完所有步后再发任何请求 -> 409 {"detail":"脚本已耗尽"}
```

## 并发与一致性说明

- 每个 run 一把 `asyncio.Lock`：匹配判定、占用记录写入、当前步推进是同一个临界区，
  因此并发请求只会串行占用不同的步，不可能两个请求拿到同一步。
- 暂缓请求在**锁外**等待 `asyncio.Event`：放行/重置/后续请求不会被它堵住。
- 查询在锁内生成整体快照（`current_step_number` + 全部 `occupancies` + `held` 标识），
  不会出现“已占用却未推进”的半次操作。
- 重置使 `epoch` 加一；放行标识带轮次前缀，事件唤醒后还会核对 `cancelled/released/epoch`，
  所以旧标识无法放行新轮请求。
