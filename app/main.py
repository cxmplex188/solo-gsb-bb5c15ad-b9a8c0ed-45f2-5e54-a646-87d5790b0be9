"""并行任务协调收尾屏障 API。

路由：
  POST   /barriers                 创建屏障（参与者名单、达标人数、截止时间）
  GET    /barriers/{bid}           查询当前状态（不阻塞）
  POST   /barriers/{bid}/checkin   参与者幂等报到
  GET    /barriers/{bid}/wait      阻塞等待达标 / 截止
  GET    /barriers/{bid}/events    报到审计事件（cursor/limit 分页）

交互文档：GET /docs（Swagger UI）
"""

from __future__ import annotations

import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from redis.asyncio import Redis
from redis.exceptions import RedisError

from .barrier_store import (
    BarrierExpired,
    BarrierNotFound,
    BarrierStore,
    NotAParticipant,
)
from .config import load_settings
from .models import (
    BarrierEventsPage,
    BarrierView,
    CheckinEvent,
    CheckinRequest,
    CheckinResponse,
    CreateBarrierRequest,
)
from .service import _dt, build_view, wait_for_settle

settings = load_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    app.state.store = BarrierStore(redis)
    yield
    await app.state.store.aclose()


app = FastAPI(
    title="任务收尾屏障 API",
    description="并行任务协调收尾：创建屏障、幂等报到、阻塞等待达标或截止。",
    version="1.0.0",
    lifespan=lifespan,
)


def store(request: Request) -> BarrierStore:
    return request.app.state.store


@app.exception_handler(RedisError)
async def redis_error_handler(request: Request, exc: RedisError) -> JSONResponse:
    return JSONResponse(
        status_code=503, content={"detail": f"Redis 暂不可用: {exc}"}
    )


@app.get("/", tags=["meta"])
async def root() -> dict[str, object]:
    return {
        "service": "task-barrier",
        "docs": "/docs",
        "health": "/health",
        "endpoints": {
            "create": "POST /barriers",
            "get": "GET /barriers/{barrier_id}",
            "checkin": "POST /barriers/{barrier_id}/checkin",
            "wait": "GET /barriers/{barrier_id}/wait",
            "events": "GET /barriers/{barrier_id}/events",
        },
    }


@app.get("/health", tags=["meta"])
async def health(request: Request) -> dict[str, str]:
    await store(request).ping()
    return {"status": "ok"}


@app.post("/barriers", response_model=BarrierView, status_code=201, tags=["barriers"])
async def create_barrier(payload: CreateBarrierRequest, request: Request) -> BarrierView:
    """创建屏障。截止时间过后 BARRIER_TTL_BUFFER_MS 毫秒内数据可查询，之后自动回收。"""
    created_at = time.time()
    deadline_ts = payload.deadline_ts()

    if deadline_ts <= created_at:
        raise HTTPException(status_code=422, detail="deadline 必须是未来时间")

    threshold = (
        payload.threshold
        if payload.threshold is not None
        else len(payload.participants)
    )
    ttl_ms = int((deadline_ts - created_at) * 1000) + settings.ttl_buffer_ms

    meta = await store(request).create(
        participants=payload.participants,
        threshold=threshold,
        deadline_ts=deadline_ts,
        created_at=created_at,
        ttl_ms=ttl_ms,
        creator_id=payload.creator_id,
    )
    return await build_view(store(request), meta, created_at)


async def _load_meta(request: Request, barrier_id: str):
    meta = await store(request).get_meta(barrier_id)
    if meta is None:
        raise HTTPException(status_code=404, detail="屏障不存在或已过期回收")
    return meta


@app.get("/barriers/{barrier_id}", response_model=BarrierView, tags=["barriers"])
async def get_barrier(barrier_id: str, request: Request) -> BarrierView:
    """非阻塞查询屏障当前状态。"""
    meta = await _load_meta(request, barrier_id)
    return await build_view(store(request), meta)


@app.post(
    "/barriers/{barrier_id}/checkin",
    response_model=CheckinResponse,
    tags=["barriers"],
)
async def checkin(
    barrier_id: str, payload: CheckinRequest, request: Request
) -> CheckinResponse:
    """参与者幂等报到。

    - 重复报到返回 already_checked_in / late，不改变任何结果；
    - 并发报到首次达到达标人数时，达标成员在 Redis Lua 脚本内被原子冻结；
    - 冻结之后的报到只会记录为迟到，quorum 与 frozen_at 永不改写。
    """
    meta = await _load_meta(request, barrier_id)
    svc = store(request)
    try:
        result = await svc.checkin(barrier_id, payload.participant_id)
    except NotAParticipant:
        raise HTTPException(
            status_code=403,
            detail=f"参与者 {payload.participant_id!r} 不在该屏障名单内",
        )
    except BarrierExpired:
        raise HTTPException(status_code=409, detail="屏障已过截止时间，报到被拒绝")
    except BarrierNotFound:
        raise HTTPException(status_code=404, detail="屏障不存在或已过期回收")

    view = await build_view(svc, meta)

    return CheckinResponse(
        barrier_id=barrier_id,
        status=result.status,
        triggered=result.triggered,
        quorum=result.quorum,
        frozen_at=_dt(result.frozen_at) if result.frozen_at is not None else None,
        view=view,
    )


@app.get("/barriers/{barrier_id}/wait", response_model=BarrierView, tags=["barriers"])
async def wait_barrier(barrier_id: str, request: Request) -> BarrierView:
    """阻塞等待直到屏障达标（立即返回冻结快照）或截止（返回报到成员与缺席名单）。

    多个等待者同时挂起：达标时通过 Redis Pub/Sub 一并唤醒，轮询为兜底。
    最长挂起时间受 BARRIER_MAX_WAIT_S 限制。
    """
    meta = await _load_meta(request, barrier_id)
    return await wait_for_settle(
        store(request),
        meta,
        poll_interval_s=settings.poll_interval_s,
        max_wait_s=settings.max_wait_s,
    )


@app.get(
    "/barriers/{barrier_id}/events",
    response_model=BarrierEventsPage,
    tags=["barriers"],
)
async def list_events(
    barrier_id: str,
    request: Request,
    cursor: int = Query(
        default=0, ge=0, description="已读到的最大事件序号；0 表示从头开始"
    ),
    limit: int = Query(
        default=100, ge=1, le=1000, description="单页最大事件数（1-1000）"
    ),
) -> BarrierEventsPage:
    """查询报到审计事件（按 seq 升序，cursor/limit 分页）。

    每次名单成员报到（首次、重复、达标、迟到）都会在与状态变更同一 Redis
    原子操作中追加一条事件；非名单成员、截止后报到等错误请求不产生事件。
    事件只增不改，游标分页在并发报到下稳定不重不漏；屏障回收后事件一并消失。
    """
    await _load_meta(request, barrier_id)
    events, next_cursor = await store(request).list_events(barrier_id, cursor, limit)
    return BarrierEventsPage(
        barrier_id=barrier_id,
        events=[
            CheckinEvent(
                seq=e["seq"],
                participant_id=e["participant_id"],
                result=e["result"],
                ts=_dt(e["ts"]),
            )
            for e in events
        ],
        next_cursor=next_cursor,
    )
