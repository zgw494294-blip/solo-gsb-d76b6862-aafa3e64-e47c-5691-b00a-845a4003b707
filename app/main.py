"""
顺序脚本 HTTP 替身（单进程内存实现）。

控制面（创建脚本/查询/放行/重置）挂在 ``/__control`` 前缀下；
业务面由 catch-all 路由 ``mock_entry`` 按脚本当前步精确匹配方法/路径/JSON 正文。

核心语义：
* 只匹配当前步：不匹配或脚本耗尽 -> 409，步骤指针不推进；
* 匹配成功 -> 立即占用当前步并推进（同一把锁内完成，查询中绝不会出现
  "已占用但未推进"的半次操作）；
* ``defer`` 只挂起发起该步的那个 HTTP 响应，不阻塞后续步骤的匹配，也不阻塞控制操作；
* 暂缓请求的放行标识（token）随该步占用立即产生，可通过 GET /__control/state 查询；
* 重置/替换脚本 -> 所有未放行请求以 409 结束，占用记录清空，从第 1 步重跑；
  旧轮次 token 一律 409，无法放行新一轮请求。
"""

from __future__ import annotations

import asyncio
import copy
import json
import time
import uuid
from contextlib import asynccontextmanager
from typing import Any, Optional

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, Field, field_validator

CONTROL_PREFIX = "/__control"
MAX_STEPS = 20


class StepSpec(BaseModel):
    """单步脚本定义。request_body 省略/为 null 时要求客户端请求也没有 JSON 正文。"""

    method: str = Field(description="预期 HTTP 方法，如 GET / POST")
    path: str = Field(description="预期路径，必须以 / 开头，区分大小写，不含查询串")
    request_body: Optional[Any] = Field(
        default=None, alias="requestBody", description="预期 JSON 正文（省略或 null 表示无正文）"
    )
    status_code: int = Field(
        default=200, alias="statusCode", ge=100, le=599, description="匹配后返回的 HTTP 状态码"
    )
    response_body: Optional[Any] = Field(
        default=None, alias="responseBody", description="返回的 JSON 正文，省略或 null 表示空正文"
    )
    defer: bool = Field(default=False, description="true 时匹配后挂起响应，等待放行标识放行")

    model_config = {"populate_by_name": True}

    @field_validator("method")
    @classmethod
    def _normalize_method(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("method 不能为空")
        return v.strip().upper()

    @field_validator("path")
    @classmethod
    def _check_path(cls, v: str) -> str:
        if not v.startswith("/"):
            raise ValueError("path 必须以 / 开头")
        if v == CONTROL_PREFIX or v.startswith(CONTROL_PREFIX + "/"):
            raise ValueError(f"路径 {CONTROL_PREFIX} 为控制接口保留，脚本不得占用")
        return v


class RunSpec(BaseModel):
    steps: list[StepSpec] = Field(description="1～20 步的有序脚本")

    @field_validator("steps")
    @classmethod
    def _check_count(cls, v: list[StepSpec]) -> list[StepSpec]:
        if not 1 <= len(v) <= MAX_STEPS:
            raise ValueError(f"脚本步数必须在 1～{MAX_STEPS} 之间")
        return v


def conflict(message: str, **extra: Any) -> JSONResponse:
    body: dict[str, Any] = {"error": "conflict", "message": message}
    body.update(extra)
    return JSONResponse(status_code=409, content=body)


def script_response(status_code: int, payload: Any) -> Response:
    """按脚本构造响应；responseBody 省略/null 或 204 -> 空体，其余 -> JSON。"""
    if payload is None or status_code == 204:
        return Response(status_code=status_code)
    return JSONResponse(status_code=status_code, content=payload)


class Pending:
    """一个已占用、响应暂缓（等待放行）的请求。"""

    def __init__(self, token: str, step: int, method: str, path: str, request_body: Any):
        self.token = token
        self.step = step
        self.method = method
        self.path = path
        self.request_body = request_body
        self.event = asyncio.Event()
        self.released = False
        self.aborted = False
        self.disconnected = False
        self.held_since = time.time()
        self.released_at: Optional[float] = None
        # 放行时从脚本快照写入，避免放行后脚本被替换导致取错响应
        self.response_status: int = 0
        self.response_payload: Any = None

    def view(self) -> dict[str, Any]:
        if self.released:
            state = "released"
        elif self.aborted:
            state = "aborted"
        elif self.disconnected:
            state = "client_disconnected"
        else:
            state = "waiting"
        return {
            "token": self.token,
            "step": self.step,
            "method": self.method,
            "path": self.path,
            "request_body": self.request_body,
            "state": state,
            "held_since": self.held_since,
            "released_at": self.released_at,
        }


class Store:
    """全局单 run 状态。所有状态读写都在 ``lock`` 内完成，保证查询完整一致。"""

    def __init__(self) -> None:
        self.lock = asyncio.Lock()
        self.epoch = 0  # 轮次：创建/重置时递增，旧 token 凭它失效
        self.steps: list[StepSpec] = []
        self.cursor = 0  # 下一个待匹配步的 0 基下标
        self.occupations: list[dict[str, Any]] = []
        self.pending: dict[str, Pending] = {}
        self.created_at: Optional[float] = None
        self.started_at: Optional[float] = None
        self.mismatch_count = 0

    def _abend_pending_locked(self) -> None:
        """中止所有未放行的暂缓请求（reset / 替换脚本时调用，须持锁）。"""
        for p in self.pending.values():
            p.aborted = True
            p.event.set()
        self.pending.clear()

    def install(self, steps: list[StepSpec]) -> None:
        self._abend_pending_locked()
        self.epoch += 1
        self.steps = steps
        self.cursor = 0
        self.occupations = []
        self.pending = {}
        now = time.time()
        self.created_at = now
        self.started_at = now
        self.mismatch_count = 0

    def reset(self) -> None:
        self._abend_pending_locked()
        self.epoch += 1
        self.cursor = 0
        self.occupations = []
        self.pending = {}
        self.started_at = time.time()
        self.mismatch_count = 0

    def _set_occupation_state_locked(self, token: str, state: str) -> None:
        for rec in self.occupations:
            if rec.get("release_token") == token:
                rec["state"] = state
                break

    def snapshot(self) -> dict[str, Any]:
        """持锁调用；深拷贝出一份完整一致的状态视图。"""
        total = len(self.steps)
        if not total:
            phase = "empty"
        elif self.cursor >= total:
            phase = "finished"
        else:
            phase = "running"
        ordered = sorted(self.pending.values(), key=lambda x: x.step)
        return {
            "epoch": self.epoch,
            "phase": phase,
            "current_step": self.cursor + 1 if total and self.cursor < total else None,
            "last_occupied_step": self.cursor if total and self.cursor > 0 else None,
            "remaining": max(0, total - self.cursor),
            "total_steps": total,
            "steps": [
                {
                    "step": i + 1,
                    "method": s.method,
                    "path": s.path,
                    "defer": s.defer,
                    "status_code": s.status_code,
                }
                for i, s in enumerate(self.steps)
            ],
            "occupations": copy.deepcopy(self.occupations),
            "waiting": [p.view() for p in ordered],
            "release_tokens": [p.token for p in ordered],
            "mismatch_count": self.mismatch_count,
            "created_at": self.created_at,
            "started_at": self.started_at,
        }


def create_app() -> FastAPI:
    store = Store()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.store = store
        yield

    app = FastAPI(
        title="顺序脚本 HTTP 替身",
        version="1.0.0",
        lifespan=lifespan,
        description=(
            "上传 1～20 步脚本后，客户端按顺序直接调用本服务；"
            f"控制接口位于 {CONTROL_PREFIX} 前缀下。单进程内存，重启清空。"
        ),
    )

    async def read_request_json(request: Request) -> tuple[Any, Optional[str], Optional[str]]:
        """返回 (解析后的 JSON, 原始文本, 解析错误)。空体 -> (None, None, None)。"""
        raw_bytes = await request.body()
        if not raw_bytes.strip():
            return None, None, None
        raw_text = raw_bytes.decode("utf-8", errors="replace")
        try:
            return json.loads(raw_text), raw_text, None
        except json.JSONDecodeError as exc:
            return None, raw_text, f"请求体不是合法 JSON: {exc.msg}"

    # ---------------------------------------------------------------- 控制面

    @app.get(f"{CONTROL_PREFIX}/health")
    async def health() -> dict[str, Any]:
        return {"ok": True}

    @app.get(f"{CONTROL_PREFIX}/state")
    async def get_state() -> dict[str, Any]:
        async with store.lock:
            return store.snapshot()

    @app.post(f"{CONTROL_PREFIX}/runs", status_code=201)
    async def create_run(spec: RunSpec) -> dict[str, Any]:
        async with store.lock:
            store.install(list(spec.steps))
            return store.snapshot()

    @app.post(f"{CONTROL_PREFIX}/reset")
    async def reset_run():
        async with store.lock:
            if not store.steps:
                return conflict("尚未创建脚本运行，无可重置的轮次")
            store.reset()
            return store.snapshot()

    @app.post(CONTROL_PREFIX + "/release/{token}")
    async def release(token: str) -> JSONResponse:
        async with store.lock:
            p = store.pending.get(token)
            if p is None:
                # 未放行就重置/替换脚本后，旧 token 走这里 -> 409
                return conflict(f"放行标识无效或已失效（可能属于已结束的旧轮次）: {token}")
            spec = store.steps[p.step - 1]
            p.response_status = spec.status_code
            p.response_payload = copy.deepcopy(spec.response_body)
            p.released = True
            p.released_at = time.time()
            p.event.set()
            store.pending.pop(token, None)
            store._set_occupation_state_locked(token, "released")
            status_code = p.response_status
            payload = copy.deepcopy(p.response_payload)
        return script_response(status_code, payload)

    # ---------------------------------------------------------------- 业务面

    @app.api_route(
        "/{path:path}",
        methods=["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"],
    )
    async def mock_entry(request: Request) -> Response:
        # 未登记的控制路径不参与脚本匹配
        if request.url.path == CONTROL_PREFIX or request.url.path.startswith(CONTROL_PREFIX + "/"):
            return JSONResponse(
                status_code=404,
                content={"error": "not_found", "message": f"未知控制接口: {request.url.path}"},
            )

        body, raw_text, parse_error = await read_request_json(request)

        async with store.lock:
            # 1) 没有脚本
            if not store.steps:
                store.mismatch_count += 1
                return conflict("尚未创建脚本运行（POST /__control/runs 上传脚本）")

            # 2) 脚本耗尽：409，不推进
            if store.cursor >= len(store.steps):
                store.mismatch_count += 1
                return conflict(
                    f"脚本已耗尽：共 {len(store.steps)} 步，全部占用",
                    step=None,
                    expected=None,
                    actual={"method": request.method, "path": request.url.path},
                )

            spec = store.steps[store.cursor]
            step_no = store.cursor + 1
            expected_view = {"method": spec.method, "path": spec.path}
            actual_view = {"method": request.method, "path": request.url.path}

            # 3) 方法 / 4) 路径不符：409，不推进
            if request.method != spec.method or request.url.path != spec.path:
                store.mismatch_count += 1
                reason = "方法" if request.method != spec.method else "路径"
                return conflict(
                    f"第 {step_no} 步{reason}不符",
                    step=step_no,
                    expected=expected_view,
                    actual=actual_view,
                )

            # 5) JSON 正文不符：409，不推进
            if parse_error is not None or body != spec.request_body:
                store.mismatch_count += 1
                return conflict(
                    f"第 {step_no} 步正文不符" if parse_error is None else f"第 {step_no} 步正文不符：{parse_error}",
                    step=step_no,
                    expected_body=spec.request_body,
                    actual_body=raw_text if parse_error is not None else body,
                )

            # 6) 匹配成功：占用 + 推进在同一把锁内原子完成，不存在"占用未推进"的中间态
            record: dict[str, Any] = {
                "step": step_no,
                "method": spec.method,
                "path": spec.path,
                "request_body": copy.deepcopy(body),
                "status_code": spec.status_code,
                "response_body": copy.deepcopy(spec.response_body),
                "deferred": spec.defer,
                "occupied_at": time.time(),
                "release_token": None,
                "state": "responded",
            }
            store.occupations.append(record)
            store.cursor += 1

            if not spec.defer:
                status_code = spec.status_code
                payload = spec.response_body
            else:
                token = f"e{store.epoch}-{uuid.uuid4().hex}"
                p = Pending(token, step_no, spec.method, spec.path, copy.deepcopy(body))
                store.pending[token] = p
                record["release_token"] = token
                record["state"] = "held"
                event = p.event

        # 7) 非暂缓：立即返回脚本响应
        if not spec.defer:
            return script_response(status_code, payload)

        # 8) 暂缓：只等待本请求的放行事件；等待期间后续步骤匹配与全部控制操作均可进行
        try:
            await event.wait()
        except asyncio.CancelledError:
            # 客户端断开：该步仍算占用（不回收）；若仍在待放行表中则摘除并留痕，
            # 已放行/已中止的记录保留原状态
            async with store.lock:
                if store.pending.pop(token, None) is not None:
                    p.disconnected = True
                    store._set_occupation_state_locked(token, "client_disconnected")
            raise

        async with store.lock:
            aborted = p.aborted
            resp_status = p.response_status
            resp_payload = copy.deepcopy(p.response_payload)

        if aborted:
            return conflict(
                f"第 {step_no} 步的暂缓请求因重置/替换脚本以 409 结束",
                step=step_no,
            )
        return script_response(resp_status, resp_payload)

    return app


app = create_app()
