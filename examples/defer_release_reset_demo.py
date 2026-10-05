#!/usr/bin/env python3
"""
无需固定延时的双请求「暂缓 → 放行 → 重置」示例。

用法（本地 uvicorn 已在 8000 端口运行时）：
    python examples/defer_release_reset_demo.py
也可指定地址： BASE_URL=http://127.0.0.1:8000 python examples/defer_release_reset_demo.py

要点：
* 两个被调请求用 asyncio 后台并发发出，命中 defer 步骤后挂起；
* 控制端不 sleep，而是轮询 GET /__control/state 等 release_tokens 出现（状态驱动同步）；
* 可以逆序放行；重置后旧请求收到 409，旧 token 再用也是 409。
"""

import asyncio
import os

import httpx

BASE_URL = os.environ.get("BASE_URL", "http://127.0.0.1:8000")
CONTROL = f"{BASE_URL}/__control"

SCRIPT = {
    "steps": [
        {  # 第 1 步：下单，暂缓
            "method": "POST",
            "path": "/orders",
            "requestBody": {"sku": "A", "qty": 1},
            "statusCode": 201,
            "responseBody": {"id": 1, "accepted": True},
            "defer": True,
        },
        {  # 第 2 步：查单，暂缓
            "method": "GET",
            "path": "/orders/1",
            "statusCode": 200,
            "responseBody": {"id": 1, "state": "PAID"},
            "defer": True,
        },
        {  # 第 3 步：撤单，立即响应
            "method": "DELETE",
            "path": "/orders/1",
            "statusCode": 204,
            "responseBody": None,
        },
    ]
}


async def wait_tokens(client: httpx.AsyncClient, count: int) -> list[str]:
    """轮询状态直到出现 count 个放行标识——不依赖任何固定延时。"""
    while True:
        state = (await client.get(f"{CONTROL}/state")).json()
        if len(state["release_tokens"]) >= count:
            return state["release_tokens"]
        await asyncio.sleep(0.02)


async def main() -> None:
    async with httpx.AsyncClient(timeout=30) as c:
        # 1) 创建运行（1～20 步脚本）
        state = (await c.post(f"{CONTROL}/runs", json=SCRIPT)).json()
        print(f"[创建] epoch={state['epoch']} 总步数={state['total_steps']} 当前步={state['current_step']}")

        # 2) 并发发出前两个请求，各自命中 defer 步骤后挂起
        t_order = asyncio.create_task(c.post(f"{BASE_URL}/orders", json={"sku": "A", "qty": 1}))
        t_query = asyncio.create_task(c.get(f"{BASE_URL}/orders/1"))

        # 3) 状态驱动等待两个放行标识出现（无 sleep 猜时）
        token1, token2 = await wait_tokens(c, 2)
        state = (await c.get(f"{CONTROL}/state")).json()
        print(f"[占用] 已占用步骤={[o['step'] for o in state['occupations']]} "
              f"当前步={state['current_step']} 等待中={[w['step'] for w in state['waiting']]}")

        # 4) 逆序放行：先放第 2 步，再放第 1 步
        rel2 = await c.post(f"{CONTROL}/release/{token2}")
        print(f"[放行2] 状态={rel2.status_code} 正文={rel2.text}")
        r_query = await t_query
        print(f"[客户端收2] 状态={r_query.status_code} 正文={r_query.text}")

        rel1 = await c.post(f"{CONTROL}/release/{token1}")
        print(f"[放行1] 状态={rel1.status_code} 正文={rel1.text}")
        r_order = await t_order
        print(f"[客户端收1] 状态={r_order.status_code} 正文={r_order.text}")

        # 5) 第 3 步非暂缓，随时可调用并立即返回
        r_del = await c.delete(f"{BASE_URL}/orders/1")
        print(f"[第3步] 状态={r_del.status_code}（204 空体，脚本全部走完）")

        # 6) 重置后从第 1 步重跑：制造一个未放行的暂缓请求，再重置一次
        await c.post(f"{CONTROL}/reset")
        t_again = asyncio.create_task(c.post(f"{BASE_URL}/orders", json={"sku": "A", "qty": 1}))
        old_token = (await wait_tokens(c, 1))[0]

        state = (await c.post(f"{CONTROL}/reset")).json()
        print(f"[重置] epoch={state['epoch']} 当前步={state['current_step']} "
              f"占用记录数={len(state['occupations'])} 等待中={len(state['waiting'])}")
        r_again = await t_again
        print(f"[未放行请求结束] 状态={r_again.status_code} 正文={r_again.text}")

        # 7) 旧 token 一律 409，不能放行新一轮请求
        stale = await c.post(f"{CONTROL}/release/{old_token}")
        print(f"[旧token放行] 状态={stale.status_code} 正文={stale.text}")

        # 8) 新轮次从第 1 步重跑，新 token 正常放行
        t_fresh = asyncio.create_task(c.post(f"{BASE_URL}/orders", json={"sku": "A", "qty": 1}))
        new_token = (await wait_tokens(c, 1))[0]
        assert new_token != old_token
        await c.post(f"{CONTROL}/release/{new_token}")
        r_fresh = await t_fresh
        print(f"[新一轮放行] 状态={r_fresh.status_code} 正文={r_fresh.text}")


if __name__ == "__main__":
    asyncio.run(main())
