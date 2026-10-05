"""手动控制响应顺序的 HTTP 替身（single-process, in-memory）。

- POST /__admin/runs             创建一轮脚本（1~20 步）
- GET  /__admin/runs             列出所有 run
- GET  /__admin/runs/{rid}       查询完整一致状态
- POST /__admin/runs/{rid}/reset 重置：未放行请求以 409 结束，清空记录，从首步重跑
- POST /__admin/runs/{rid}/release  按放行标识放行暂缓响应

其余路径（除保留前缀 /__admin 外）均视为客户端对替身的调用，按当前步严格匹配。
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from typing import Any

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator

app = FastAPI(title="Sequential HTTP Stand-in", version="1.0.0")

ADMIN_PREFIX = "/__admin"
MAX_STEPS = 20
STATUS_OK = 200
STATUS_CONFLICT = 409
STATUS_NOT_FOUND = 404
STATUS_BAD_REQUEST = 400


def now_ms() -> int:
    return int(time.time() * 1000)


def conflict(detail: str, **extra: Any) -> JSONResponse:
    body: dict[str, Any] = {"error": "conflict", "detail": detail}
    body.update(extra)
    return JSONResponse(status_code=STATUS_CONFLICT, content=body)


class StepIn(BaseModel):
    method: str = Field(description="预期 HTTP 方法，大小写不敏感")
    path: str = Field(description="预期路径（不含 query），必须以 / 开头")
    json_body: Any | None = Field(
        default=None,
        description="预期 JSON 正文；显式给 null 表示正文必须是 JSON null；"
        "不给该字段表示不校验正文",
    )
    body_present: bool = Field(
        default=False,
        description="内部标记：请求是否显式提供了 json_body 字段",
        exclude=True,
    )
    status_code: int = Field(default=200, description="返回状态码 100~599")
    response_json: Any | None = Field(default=None, description="返回的 JSON 正文")
    hold: bool = Field(default=False, description="是否暂缓响应，等待放行标识")

    model_config = {"extra": "forbid"}

    @field_validator("method")
    @classmethod
    def _method(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("method 不能为空")
        return v.upper()

    @field_validator("path")
    @classmethod
    def _path(cls, v: str) -> str:
        if not v.startswith("/"):
            raise ValueError("path 必须以 / 开头")
        return v

    @field_validator("status_code")
    @classmethod
    def _status(cls, v: int) -> int:
        if not 100 <= v <= 599:
            raise ValueError("status_code 必须在 100~599 之间")
        return v


class Occupancy(BaseModel):
    seq: int
    epoch: int
    step_index: int  # 0-based
    step_number: int  # 1-based
    method: str
    path: str
    request_json: Any | None
    held: bool
    release_token: str | None
    released: bool
    cancelled: bool
    occupied_at: int
    released_at: int | None
    finished_at: int | None


class Run:
    def __init__(self, run_id: str, steps: list[StepIn]) -> None:
        self.id = run_id
        self.steps = steps
        self.lock = asyncio.Lock()
        self.current = 0  # 当前待匹配步（0-based）；等于步数表示脚本耗尽
        self.epoch = 0  # 重置轮次；旧 token / 旧 waiter 以此识别
        self.occupancies: list[Occupancy] = []
        self.seq = 0  # 占用记录流水号
        self.active_tokens: set[str] = set()  # 当前轮仍可放行的标识
        self.waiters: dict[str, asyncio.Event] = {}  # token -> 放行事件

    def snapshot(self) -> dict[str, Any]:
        """构造完整一致状态；必须在持锁时调用。"""
        return {
            "run_id": self.id,
            "epoch": self.epoch,
            "total_steps": len(self.steps),
            "current_step_number": self.current + 1 if self.current < len(self.steps) else None,
            "exhausted": self.current >= len(self.steps),
            "steps": [
                {
                    "number": i + 1,
                    "method": s.method,
                    "path": s.path,
                    "json_body": s.json_body if s.body_present else None,
                    "body_checked": s.body_present,
                    "status_code": s.status_code,
                    "hold": s.hold,
                }
                for i, s in enumerate(self.steps)
            ],
            "occupancies": [o.model_dump(mode="json") for o in self.occupancies],
            "held": [
                {
                    "seq": o.seq,
                    "step_number": o.step_number,
                    "release_token": o.release_token,
                    "released": o.released,
                }
                for o in self.occupancies
                # held 只列“仍在等待放行”的占用；已放行的留在 occupancies 里
                if o.held and not o.cancelled and not o.released
            ],
        }


# 单进程内存存储，重启清空
runs: dict[str, Run] = {}
runs_lock = asyncio.Lock()


def resolve_run(request: Request) -> Run | None:
    """选择 run：显式 X-Run-ID 时严格按其查找（不回退）；未指定且恰好一个 run 时自动使用。"""
    rid = request.headers.get("x-run-id")
    if rid:
        return runs.get(rid)
    if len(runs) == 1:
        return next(iter(runs.values()))
    return None


@app.get("/")
async def health() -> dict[str, str]:
    return {"service": "sequential-http-standin", "status": "ok"}


@app.post(f"{ADMIN_PREFIX}/runs", status_code=STATUS_OK)
async def create_run(request: Request) -> JSONResponse:
    """创建 run：直接解析原始 JSON，以区分 json_body 缺省与显式 null。"""
    try:
        raw = await request.json()
    except Exception:
        return JSONResponse(
            status_code=STATUS_BAD_REQUEST,
            content={"error": "bad_request", "detail": "请求体必须是合法 JSON"},
        )
    if not isinstance(raw, dict) or not isinstance(raw.get("steps"), list):
        return JSONResponse(
            status_code=STATUS_BAD_REQUEST,
            content={"error": "bad_request", "detail": "需要 {\"steps\": [...]}"},
        )
    raw_steps = raw["steps"]
    if not 1 <= len(raw_steps) <= MAX_STEPS:
        return JSONResponse(
            status_code=STATUS_BAD_REQUEST,
            content={
                "error": "bad_request",
                "detail": f"steps 数量必须在 1~{MAX_STEPS} 之间",
            },
        )

    steps: list[StepIn] = []
    try:
        for i, rs in enumerate(raw_steps):
            if not isinstance(rs, dict):
                raise ValueError(f"第 {i + 1} 步必须是对象")
            body_present = "json_body" in rs
            step = StepIn.model_validate(rs)
            step.body_present = body_present
            steps.append(step)
    except Exception as exc:
        return JSONResponse(
            status_code=STATUS_BAD_REQUEST,
            content={"error": "bad_request", "detail": str(exc)},
        )

    run_id = uuid.uuid4().hex
    run = Run(run_id, steps)
    async with runs_lock:
        runs[run_id] = run
    return JSONResponse(
        status_code=STATUS_OK,
        content={"run_id": run_id, "total_steps": len(steps)},
    )


@app.get(f"{ADMIN_PREFIX}/runs")
async def list_runs() -> dict[str, Any]:
    async with runs_lock:
        return {
            "run_ids": list(runs.keys()),
            "count": len(runs),
            "runs": [
                {
                    "run_id": r.id,
                    "epoch": r.epoch,
                    "current_step_number": r.current + 1
                    if r.current < len(r.steps)
                    else None,
                    "exhausted": r.current >= len(r.steps),
                    "total_steps": len(r.steps),
                }
                for r in runs.values()
            ],
        }


@app.get(f"{ADMIN_PREFIX}/runs/{{run_id}}")
async def query_run(run_id: str) -> JSONResponse:
    async with runs_lock:
        run = runs.get(run_id)
        if run is None:
            return JSONResponse(
                status_code=STATUS_NOT_FOUND,
                content={"error": "not_found", "detail": f"run {run_id} 不存在"},
            )
        async with run.lock:
            return JSONResponse(status_code=STATUS_OK, content=run.snapshot())


@app.post(f"{ADMIN_PREFIX}/runs/{{run_id}}/reset")
async def reset_run(run_id: str) -> JSONResponse:
    async with runs_lock:
        run = runs.get(run_id)
        if run is None:
            return JSONResponse(
                status_code=STATUS_NOT_FOUND,
                content={"error": "not_found", "detail": f"run {run_id} 不存在"},
            )

    async with run.lock:
        # 1) 所有未放行的暂缓请求立即以 409 结束（唤醒等待中的协程）
        for token, event in list(run.waiters.items()):
            event.set()
        run.waiters.clear()
        for o in run.occupancies:
            if o.held and not o.released and not o.cancelled:
                o.cancelled = True
                o.finished_at = now_ms()
        # 2) 清空占用记录与放行标识
        run.occupancies.clear()
        run.active_tokens.clear()
        # 3) 从首步重跑，轮次 +1（旧 token 永久失效）
        run.current = 0
        run.epoch += 1
        snapshot = run.snapshot()
    return JSONResponse(status_code=STATUS_OK, content={"reset": True, "state": snapshot})


@app.post(f"{ADMIN_PREFIX}/runs/{{run_id}}/release")
async def release_run(run_id: str, request: Request) -> JSONResponse:
    async with runs_lock:
        run = runs.get(run_id)
        if run is None:
            return JSONResponse(
                status_code=STATUS_NOT_FOUND,
                content={"error": "not_found", "detail": f"run {run_id} 不存在"},
            )

    try:
        payload = await request.json()
    except Exception:
        payload = {}
    token = None
    if isinstance(payload, dict):
        token = payload.get("release_token")
    if not token:
        token = request.headers.get("x-release-token")
    if not token:
        return JSONResponse(
            status_code=STATUS_BAD_REQUEST,
            content={"error": "bad_request", "detail": "需要 release_token"},
        )

    async with run.lock:
        # 旧轮标识 / 已用过或不存在的标识：409，不能放行新轮请求
        if token not in run.active_tokens:
            return conflict(
                "无效或已失效的放行标识（可能已重置、已使用或不存在）",
                release_token=token,
                epoch=run.epoch,
            )
        event = run.waiters.pop(token, None)
        if event is None:
            # 标识有效但等待者已不在（理论上不会发生），按冲突处理
            run.active_tokens.discard(token)
            return conflict("没有等待放行的请求", release_token=token)

        occ = next(
            (
                o
                for o in run.occupancies
                if o.release_token == token and o.held and not o.cancelled
            ),
            None,
        )
        if occ is None:
            run.active_tokens.discard(token)
            return conflict("放行标识没有对应的暂缓占用", release_token=token)

        run.active_tokens.discard(token)  # 一次性标识
        occ.released = True
        occ.released_at = now_ms()
        occ.finished_at = now_ms()
        event.set()  # 在锁内先改状态再唤醒，等待者醒来看到最终态
        snapshot = run.snapshot()
    return JSONResponse(
        status_code=STATUS_OK,
        content={"released": True, "release_token": token, "state": snapshot},
    )


@app.api_route(
    "/{full_path:path}",
    methods=["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"],
)
async def catch_all(request: Request, full_path: str) -> Response:
    path = "/" + full_path

    # 保留前缀不参与脚本匹配
    if path == ADMIN_PREFIX or path.startswith(ADMIN_PREFIX + "/"):
        return JSONResponse(
            status_code=STATUS_NOT_FOUND,
            content={"error": "not_found", "detail": "未知管理接口"},
        )

    run = resolve_run(request)
    if run is None:
        return conflict(
            "没有可用的 run：请先用 X-Run-ID 指定，或创建一个 run",
            path=path,
        )

    async with run.lock:
        # 脚本耗尽：409 且不推进
        if run.current >= len(run.steps):
            return conflict(
                "脚本已耗尽",
                run_id=run.id,
                current_step_number=None,
            )

        step = run.steps[run.current]
        index = run.current

        # 严格匹配当前步：方法 / 路径 / JSON 正文
        if request.method.upper() != step.method:
            return conflict(
                "方法与当前步不匹配",
                run_id=run.id,
                current_step_number=index + 1,
                expected=step.method,
                actual=request.method.upper(),
            )
        if path != step.path:
            return conflict(
                "路径与当前步不匹配",
                run_id=run.id,
                current_step_number=index + 1,
                expected=step.path,
                actual=path,
            )

        raw = await request.body()
        parsed: Any = None
        body_present_in_request = len(raw) > 0
        body_json_ok = False
        if body_present_in_request:
            try:
                parsed = json.loads(raw)
                body_json_ok = True
            except Exception:
                body_json_ok = False

        if step.body_present:
            # 显式约定了正文（含 null）：空请求体也属于不匹配
            if not body_present_in_request or not body_json_ok:
                return conflict(
                    "正文缺失或不是合法 JSON，无法匹配当前步",
                    run_id=run.id,
                    current_step_number=index + 1,
                )
            if parsed != step.json_body:
                return conflict(
                    "JSON 正文与当前步不匹配",
                    run_id=run.id,
                    current_step_number=index + 1,
                    expected=step.json_body,
                    actual=parsed,
                )
            request_json = parsed
        else:
            # 脚本不校验正文时，仍记录客户端发来的内容（非 JSON 时截断记录）
            if body_present_in_request and not body_json_ok:
                request_json = {"<raw-non-json>": raw.decode("utf-8", "replace")[:500]}
            else:
                request_json = parsed if body_present_in_request else None

        # 匹配成功：在同一把锁内立即占用并推进，不存在“已占用却未推进”的中间态
        run.seq += 1
        token: str | None = None
        event: asyncio.Event | None = None
        if step.hold:
            token = f"ep{run.epoch}-{uuid.uuid4().hex}"
            run.active_tokens.add(token)
            event = asyncio.Event()
            run.waiters[token] = event

        occ = Occupancy(
            seq=run.seq,
            epoch=run.epoch,
            step_index=index,
            step_number=index + 1,
            method=request.method.upper(),
            path=path,
            request_json=request_json,
            held=step.hold,
            release_token=token,
            released=False,
            cancelled=False,
            occupied_at=now_ms(),
            released_at=None,
            finished_at=None if step.hold else now_ms(),
        )
        run.occupancies.append(occ)
        run.current += 1  # 并发请求在锁上排队，不可能占用同一步

        epoch = run.epoch
        status_code = step.status_code
        response_body = step.response_json

    # 锁外等待放行；暂缓只阻塞当前请求，后续请求与管理操作照常进行
    if step.hold and event is not None:
        await event.wait()
        # 醒来后必须重新取锁读取最终状态，保证查询与放行的一致语义
        async with run.lock:
            if occ.cancelled or occ.epoch != epoch or not occ.released:
                return conflict(
                    "该响应已被重置取消，未放行请求以 409 结束",
                    run_id=run.id,
                    release_token=token,
                    epoch_at_occupy=epoch,
                    current_epoch=run.epoch,
                )
        return JSONResponse(status_code=status_code, content=response_body)

    return JSONResponse(status_code=status_code, content=response_body)
