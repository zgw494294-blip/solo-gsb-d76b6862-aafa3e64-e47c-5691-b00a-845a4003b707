"""顺序脚本 HTTP 替身的语义测试。不使用 pytest-asyncio：每个用例 asyncio.run 驱动。"""

import asyncio

import httpx
import pytest

from app.main import create_app


def run(coro):
    return asyncio.run(coro)


async def _make_client():
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=create_app()), base_url="http://stub"
    )


SCRIPT = {
    "steps": [
        {"method": "POST", "path": "/orders", "requestBody": {"sku": "A"}, "statusCode": 201,
         "responseBody": {"id": 1}, "defer": True},
        {"method": "GET", "path": "/orders/1", "statusCode": 200,
         "responseBody": {"id": 1, "state": "held"}, "defer": True},
        {"method": "DELETE", "path": "/orders/1", "statusCode": 204, "responseBody": None},
    ]
}


async def _make_client():
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app()), base_url="http://stub")


def test_create_and_basic_match():
    async def case():
        c = await _make_client()
        r = await c.post("/__control/runs", json=SCRIPT)
        assert r.status_code == 201
        assert r.json()["current_step"] == 1
        assert r.json()["total_steps"] == 3

        # 不匹配：409 不推进
        bad = await c.post("/orders", json={"sku": "B"})
        assert bad.status_code == 409
        assert bad.json()["step"] == 1

        state = (await c.get("/__control/state")).json()
        assert state["current_step"] == 1
        assert state["mismatch_count"] == 1
        assert state["occupations"] == []  # 未占用
        await c.aclose()

    run(case())


def test_method_path_mismatch_and_exhaustion():
    async def case():
        c = await _make_client()
        await c.post("/__control/runs", json=SCRIPT)

        r = await c.get("/orders")
        assert r.status_code == 409 and r.json()["message"].startswith("第 1 步方法不符")
        r = await c.post("/orders/1")
        assert r.status_code == 409 and r.json()["message"].startswith("第 1 步路径不符")

        # 正常走三步
        t1 = asyncio.ensure_future(c.post("/orders", json={"sku": "A"}))  # defer
        token = (await _wait_pending(c, 1))["release_tokens"][0]
        rel = await c.post(f"/__control/release/{token}")
        assert rel.status_code == 201 and rel.json() == {"id": 1}
        r1 = await t1
        assert r1.status_code == 201 and r1.json() == {"id": 1}

        held = asyncio.ensure_future(c.get("/orders/1"))
        await _wait_pending(c)
        token2 = (await c.get("/__control/state")).json()["release_tokens"][0]
        await c.post(f"/__control/release/{token2}")
        r2 = await held
        assert r2.status_code == 200

        r3 = await c.delete("/orders/1")
        assert r3.status_code == 204 and r3.content in (b"null", b"")

        # 耗尽
        extra = await c.get("/orders/1")
        assert extra.status_code == 409 and "耗尽" in extra.json()["message"]
        st = (await c.get("/__control/state")).json()
        assert st["phase"] == "finished" and st["current_step"] is None
        await c.aclose()

    run(case())


async def _wait_pending(c, count=1, min_step=1):
    """轮询直到出现 count 个步号 >= min_step 的暂缓请求（替代固定 sleep）。"""
    for _ in range(500):
        st = (await c.get("/__control/state")).json()
        if sum(1 for w in st["waiting"] if w["step"] >= min_step) >= count:
            return st
        await asyncio.sleep(0.01)
    raise AssertionError("暂缓请求未在预期时间内出现")


def test_defer_does_not_block_later_steps_and_release_in_reverse():
    async def case():
        c = await _make_client()
        await c.post("/__control/runs", json=SCRIPT)

        # 两个暂缓请求顺序占住第 1、2 步
        t1 = asyncio.ensure_future(c.post("/orders", json={"sku": "A"}))
        st = await _wait_pending(c, 1)
        assert st["current_step"] == 2  # 占用即推进，没有半步状态

        t2 = asyncio.ensure_future(c.get("/orders/1"))
        st = await _wait_pending(c, 2)
        assert len(st["waiting"]) == 2
        assert st["phase"] == "running"
        assert st["current_step"] == 3

        # 等待期间非暂缓的后续控制操作与第 3 步匹配照常
        tokens = st["release_tokens"]
        assert len(tokens) == 2

        # 逆序放行：先放第 2 步
        rel2 = await c.post(f"/__control/release/{tokens[1]}")
        assert rel2.status_code == 200 and rel2.json() == {"id": 1, "state": "held"}
        r2 = await t2
        assert r2.status_code == 200

        # 第 1 步仍挂着
        st = (await c.get("/__control/state")).json()
        assert [w["step"] for w in st["waiting"]] == [1]
        assert not t1.done()

        rel1 = await c.post(f"/__control/release/{tokens[0]}")
        assert rel1.status_code == 201
        r1 = await t1
        assert r1.json() == {"id": 1}

        # 第 3 步（非暂缓）在整个过程之后仍可正常匹配
        r3 = await c.delete("/orders/1")
        assert r3.status_code == 204
        await c.aclose()

    run(case())


def test_reset_aborts_held_and_invalidates_tokens():
    async def case():
        c = await _make_client()
        await c.post("/__control/runs", json=SCRIPT)
        t1 = asyncio.ensure_future(c.post("/orders", json={"sku": "A"}))
        st = await _wait_pending(c, 1)
        old_token = st["release_tokens"][0]
        epoch = st["epoch"]

        t_next = asyncio.ensure_future(c.get("/orders/1"))
        await _wait_pending(c, 2)

        # 重置：两个未放行请求都以 409 结束
        r = await c.post("/__control/reset")
        assert r.status_code == 200
        assert r.json()["epoch"] == epoch + 1
        assert r.json()["current_step"] == 1
        assert r.json()["occupations"] == []
        assert r.json()["waiting"] == []

        ex1, ex2 = await asyncio.gather(t1, t_next, return_exceptions=True)
        # ASGI 下 JSONResponse 409 正常返回
        for resp in (ex1, ex2):
            assert resp.status_code == 409
            assert "重置" in resp.json()["message"]

        # 旧 token 放行 -> 409
        old = await c.post(f"/__control/release/{old_token}")
        assert old.status_code == 409

        # 新一轮：第 1 步重新可匹配，新 token 与旧 token 不同
        t_new = asyncio.ensure_future(c.post("/orders", json={"sku": "A"}))
        st = await _wait_pending(c, 1)
        new_token = st["release_tokens"][0]
        assert new_token != old_token
        # 旧 token 依旧无效，不能放行新轮请求
        old2 = await c.post(f"/__control/release/{old_token}")
        assert old2.status_code == 409
        await c.post(f"/__control/release/{new_token}")
        rn = await t_new
        assert rn.status_code == 201
        await c.aclose()

    run(case())


def test_concurrent_requests_cannot_grab_same_step():
    async def case():
        c = await _make_client()
        await c.post("/__control/runs", json=SCRIPT)
        t_held = asyncio.ensure_future(c.post("/orders", json={"sku": "A"}))
        t_loser = asyncio.ensure_future(c.post("/orders", json={"sku": "A"}))
        loser = await t_loser  # 立即得到 409
        assert loser.status_code == 409
        assert loser.json()["message"].startswith("第 2 步")  # 第 1 步已占用并推进

        st = (await c.get("/__control/state")).json()
        assert len(st["waiting"]) == 1
        occ_steps = [o["step"] for o in st["occupations"]]
        assert occ_steps == [1]  # 同一步绝不会被占用两次
        assert st["current_step"] == 2  # 没有"已占用却未推进"

        # 清理：重置会让挂起请求 409 结束
        await c.post("/__control/reset")
        held_resp = await t_held
        assert held_resp.status_code == 409
        await c.aclose()

    run(case())


def test_replace_run_aborts_old_held_and_old_token_dead():
    async def case():
        c = await _make_client()
        await c.post("/__control/runs", json=SCRIPT)
        t = asyncio.ensure_future(c.post("/orders", json={"sku": "A"}))
        st = await _wait_pending(c, 1)
        old_token = st["release_tokens"][0]

        new_script = {"steps": [{"method": "GET", "path": "/ping", "statusCode": 200,
                                 "responseBody": {"pong": True}}]}
        r = await c.post("/__control/runs", json=new_script)
        assert r.status_code == 201 and r.json()["total_steps"] == 1
        old_resp = await t
        assert old_resp.status_code == 409

        assert (await c.post(f"/__control/release/{old_token}")).status_code == 409
        ping = await c.get("/ping")
        assert ping.status_code == 200 and ping.json() == {"pong": True}
        await c.aclose()

    run(case())


def test_empty_body_semantics_and_invalid_json():
    async def case():
        c = await _make_client()
        script = {"steps": [
            {"method": "POST", "path": "/echo", "requestBody": None, "statusCode": 200,
             "responseBody": {"ok": True}},
        ]}
        await c.post("/__control/runs", json=script)
        # 有正文 vs 预期无正文 -> 409
        r = await c.post("/echo", json={"x": 1})
        assert r.status_code == 409
        # 非法 JSON
        r = await c.post("/echo", content=b"{bad", headers={"content-type": "application/json"})
        assert r.status_code == 409 and "合法 JSON" in r.json()["message"]
        # 空体 -> 匹配
        r = await c.post("/echo")
        assert r.status_code == 200 and r.json() == {"ok": True}
        await c.aclose()

    run(case())


def test_client_disconnect_leaves_step_occupied():
    async def case():
        c = await _make_client()
        await c.post("/__control/runs", json=SCRIPT)

        # 同一 client（同一 app/Store）发起暂缓请求，然后直接取消（模拟客户端断开）
        t = asyncio.ensure_future(c.post("/orders", json={"sku": "A"}))
        await _wait_pending(c, 1)
        t.cancel()
        with pytest.raises(asyncio.CancelledError):
            await t

        # 该步仍占用（不回收），记录断开状态，待放行表中已摘除
        for _ in range(50):
            st = (await c.get("/__control/state")).json()
            if not st["waiting"]:
                break
            await asyncio.sleep(0.01)
        assert st["waiting"] == []
        assert st["current_step"] == 2
        assert st["occupations"][0]["state"] == "client_disconnected"

        # 后续步骤照常匹配
        t2 = asyncio.ensure_future(c.get("/orders/1"))
        await _wait_pending(c, 1, min_step=2)
        await c.post("/__control/reset")
        r2 = await t2
        assert r2.status_code == 409
        await c.aclose()

    run(case())


def test_validation_errors():
    async def case():
        c = await _make_client()
        # 0 步
        r = await c.post("/__control/runs", json={"steps": []})
        assert r.status_code == 422
        # 21 步
        steps = [{"method": "GET", "path": f"/p{i}"} for i in range(21)]
        r = await c.post("/__control/runs", json={"steps": steps})
        assert r.status_code == 422
        # 保留路径
        r = await c.post("/__control/runs", json={"steps": [{"method": "GET", "path": "/__control/state"}]})
        assert r.status_code == 422
        # reset 无 run
        assert (await c.post("/__control/reset")).status_code == 409
        # 无脚本时业务请求
        assert (await c.get("/anything")).status_code == 409
        # 未知控制路径
        assert (await c.get("/__control/nope")).status_code == 404
        await c.aclose()

    run(case())


def test_high_concurrency_single_occupation_per_step():
    """20 个同形态请求同时打到第 1 步：只有 1 个占用，其余 409。"""

    async def case():
        c = await _make_client()
        await c.post("/__control/runs", json=SCRIPT)
        winner = asyncio.ensure_future(c.post("/orders", json={"sku": "A"}))
        losers = await asyncio.gather(
            *[c.post("/orders", json={"sku": "A"}) for _ in range(19)]
        )
        assert all(r.status_code == 409 for r in losers)
        st = (await c.get("/__control/state")).json()
        assert [o["step"] for o in st["occupations"]] == [1]
        assert st["current_step"] == 2
        await c.post("/__control/reset")
        assert (await winner).status_code == 409
        await c.aclose()

    run(case())
