"""屏障视图组装、事件分页与阻塞等待逻辑。"""

from __future__ import annotations

import base64
import binascii
import time
from datetime import datetime, timezone

from .barrier_store import BarrierMeta, BarrierStore
from .models import BarrierView, CheckinEvent, EventPage

# 游标负载前缀：拒绝格式不符或来自其他版本的游标。
_CURSOR_PREFIX = "b1:"
DEFAULT_EVENTS_LIMIT = 50
MAX_EVENTS_LIMIT = 200


class InvalidCursor(ValueError):
    """分页游标无法解析。"""


def encode_cursor(offset: int) -> str:
    raw = f"{_CURSOR_PREFIX}{offset}".encode("ascii")
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def decode_cursor(cursor: str) -> int:
    """解码事件分页游标；任何损坏 / 越界输入抛 InvalidCursor。"""
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        raw = base64.urlsafe_b64decode(padded.encode("ascii"))
        text = raw.decode("ascii")
    except (binascii.Error, ValueError, UnicodeDecodeError) as exc:
        raise InvalidCursor(cursor) from exc
    if not text.startswith(_CURSOR_PREFIX):
        raise InvalidCursor(cursor)
    try:
        offset = int(text[len(_CURSOR_PREFIX) :])
    except ValueError as exc:
        raise InvalidCursor(cursor) from exc
    if offset < 0:
        raise InvalidCursor(cursor)
    return offset


def _dt(ts: float) -> datetime:
    return datetime.fromtimestamp(ts, tz=timezone.utc)


async def build_view(store: BarrierStore, meta: BarrierMeta, now: float | None = None) -> BarrierView:
    """读取当前屏障状态并组装视图。

    状态机：
      reached  —— 存在冻结快照（达标成员已被原子固定）
      expired  —— 无冻结快照且 now >= deadline
      waiting  —— 其余情况
    """
    now = now if now is not None else time.time()
    frozen = await store.get_frozen(meta.barrier_id)
    checked = await store.checked_members(meta.barrier_id)

    if frozen is not None:
        quorum = frozen["quorum"]
        quorum_set = set(quorum)
        late = sorted(checked - quorum_set)
        return BarrierView(
            barrier_id=meta.barrier_id,
            status="reached",
            participants=meta.participants,
            threshold=meta.threshold,
            deadline=_dt(meta.deadline),
            created_at=_dt(meta.created_at),
            checked_in=sorted(checked),
            checked_count=len(quorum),
            quorum=quorum,
            frozen_at=_dt(frozen["frozen_at"]),
            late=late or None,
            creator_id=meta.creator_id,
        )

    if now >= meta.deadline:
        roster_set = set(meta.participants)
        checked_sorted = sorted(checked)
        missing = sorted(roster_set - checked)
        return BarrierView(
            barrier_id=meta.barrier_id,
            status="expired",
            participants=meta.participants,
            threshold=meta.threshold,
            deadline=_dt(meta.deadline),
            created_at=_dt(meta.created_at),
            checked_in=checked_sorted,
            checked_count=len(checked_sorted),
            missing=missing,
            creator_id=meta.creator_id,
        )

    checked_sorted = sorted(checked)
    return BarrierView(
        barrier_id=meta.barrier_id,
        status="waiting",
        participants=meta.participants,
        threshold=meta.threshold,
        deadline=_dt(meta.deadline),
        created_at=_dt(meta.created_at),
        checked_in=checked_sorted,
        checked_count=len(checked_sorted),
        creator_id=meta.creator_id,
    )


async def wait_for_settle(
    store: BarrierStore,
    meta: BarrierMeta,
    poll_interval_s: float,
    max_wait_s: float,
) -> BarrierView:
    """阻塞直到屏障达标或截止。

    主通道为 Pub/Sub：先订阅，再检查一次状态（消除“先检查后订阅”的丢消息窗口）；
    轮询作为兜底，覆盖发布丢失 / 截止时间到达（截止不发布消息）两种情况。
    多个等待者各自订阅同一频道，达标发布时全部被唤醒。
    """
    deadline_d = meta.deadline

    # 已经有结果则立即返回（达标快照或截止结果）
    first = await build_view(store, meta)
    if first.status in ("reached", "expired"):
        return first

    pubsub = store.redis.pubsub()
    await pubsub.subscribe(store.channel(meta.barrier_id))
    try:
        # 订阅成功后再次检查：达标发布可能发生在第一次检查与订阅之间
        second = await build_view(store, meta)
        if second.status in ("reached", "expired"):
            return second

        start = time.time()
        while True:
            now = time.time()
            remaining_budget = max_wait_s - (now - start)
            if remaining_budget <= 0:
                return await build_view(store, meta, now)

            # get_message 阻塞至多 timeout；Pub/Sub 消息与轮询共用这一等待
            timeout = min(
                poll_interval_s,
                max(0.0, deadline_d - now),
                remaining_budget,
            )
            if timeout > 0:
                await pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=timeout
                )

            # 无论被消息唤醒还是轮询到期，都回读真实状态：
            # 达标则立即返回冻结快照；到达截止则返回报到名单与缺席名单。
            view = await build_view(store, meta)
            if view.status in ("reached", "expired"):
                return view
    finally:
        try:
            await pubsub.unsubscribe(store.channel(meta.barrier_id))
        finally:
            await pubsub.aclose()


async def build_event_page(
    store: BarrierStore,
    meta: BarrierMeta,
    offset: int,
    limit: int,
) -> EventPage:
    """读取审计事件页并计算 next_cursor。

    审计流是只追加的列表，offset 是稳定下标。满页（取到 limit 条）才返回
    next_cursor，指向其后第一个下标；短页（含空页）即流末尾，返回 null。
    并发新报到只会在尾部追加：旧下标永不移位，客户端稍后复用同一个
    next_cursor 轮询即可拿到新事件，全程不重不漏。
    """
    events_raw = await store.list_events(meta.barrier_id, offset=offset, limit=limit)
    events = [
        CheckinEvent(
            seq=ev["seq"],
            participant_id=ev["participant_id"],
            result=ev["result"],
            ts=_dt(ev["ts"]),
        )
        for ev in events_raw
    ]
    next_cursor = encode_cursor(offset + len(events)) if len(events) == limit else None
    return EventPage(
        barrier_id=meta.barrier_id,
        events=events,
        next_cursor=next_cursor,
        limit=limit,
    )
