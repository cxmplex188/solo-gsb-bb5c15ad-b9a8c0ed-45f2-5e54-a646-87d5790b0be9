"""Redis 持久层：屏障元数据、原子报到（Lua）、冻结快照、报到审计事件与事件通知。

每个屏障使用如下键（``bid`` 为屏障 id）：

``barrier:{bid}``          hash  元数据（threshold、deadline、ttl_ms 等）
``barrier:{bid}:roster``   set   创建时声明的参与者名单
``barrier:{bid}:checked``  set   所有有效报到（含达标后的迟到报到）
``barrier:{bid}:frozen``   hash  达标冻结快照，仅在达标后存在（quorum/frozen_at）
``barrier:{bid}:events``   list  报到审计事件（JSON，seq 从 1 递增，只追加）

频道 ``barrier:{bid}:events`` 在达标冻结后发布 ``reached`` 消息。
"""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass
from typing import Any

from redis.asyncio import Redis

# 报到脚本。返回数组：{状态, 是否新加入(0/1), quorum JSON, frozen_at 字符串}
#
# 状态取值：
#   unknown             屏障不存在
#   not_participant     报到者不在名单内
#   expired             已过截止时间
#   reached             本次报到使屏障达标，已原子写入冻结快照
#   checked_in          报到成功，屏障仍在等待
#   already_checked_in  幂等重复报到（未达标前）
#   late                屏障达标后报到，快照不变
#
# 四种成功状态（reached/checked_in/already_checked_in/late）都会在同一脚本内
# 向 events 列表追加一条审计事件（seq 递增）；错误路径不产生任何事件。
CHECKIN_SCRIPT = """
local meta_key    = KEYS[1]
local roster_key  = KEYS[2]
local checked_key = KEYS[3]
local frozen_key  = KEYS[4]
local events_key  = KEYS[5]
local pid         = ARGV[1]
local now         = tonumber(ARGV[2])

local function refresh_ttl()
    local ttl_ms = tonumber(redis.call('HGET', meta_key, 'ttl_ms'))
    if ttl_ms then
        redis.call('PEXPIRE', meta_key, ttl_ms)
        redis.call('PEXPIRE', roster_key, ttl_ms)
        redis.call('PEXPIRE', checked_key, ttl_ms)
        redis.call('PEXPIRE', frozen_key, ttl_ms)
        redis.call('PEXPIRE', events_key, ttl_ms)
    end
end

-- 审计事件：与状态变更处于同一原子脚本，seq 自 1 递增（下标 = seq - 1）。
local function record_event(result)
    local seq = redis.call('LLEN', events_key) + 1
    redis.call('RPUSH', events_key, cjson.encode({
        seq = seq,
        participant_id = pid,
        result = result,
        ts = now
    }))
end

if redis.call('HEXISTS', meta_key, 'threshold') == 0 then
    return {'unknown', '0', '', '0'}
end

-- 名单校验优先于一切状态：非名单成员永远无法报到，也不能获得冻结快照。
if redis.call('SISMEMBER', roster_key, pid) == 0 then
    return {'not_participant', '0', '', '0'}
end

-- 达标冻结之后：冻结键已存在，任何后续报到都不得改写快照。
local quorum = redis.call('HGET', frozen_key, 'quorum')
if quorum then
    -- 仅记录迟到事实，quorum/frozen_at 保持不变。
    redis.call('SADD', checked_key, pid)
    record_event('late')
    refresh_ttl()
    return {'late', '0', quorum, redis.call('HGET', frozen_key, 'frozen_at')}
end

local deadline = tonumber(redis.call('HGET', meta_key, 'deadline'))
if now >= deadline then
    return {'expired', '0', '', '0'}
end

local newly_added = redis.call('SADD', checked_key, pid)
local count = redis.call('SCARD', checked_key)
local threshold = tonumber(redis.call('HGET', meta_key, 'threshold'))

-- 首次达到达标人数：在同一脚本内冻结成员名单，杜绝并发覆盖。
if count >= threshold then
    local members = redis.call('SMEMBERS', checked_key)
    table.sort(members)
    local payload = cjson.encode(members)
    redis.call('HSET', frozen_key, 'quorum', payload, 'frozen_at', tostring(now))
    record_event('reached')
    refresh_ttl()
    return {'reached', tostring(newly_added), payload, tostring(now)}
end

if newly_added == 1 then
    record_event('checked_in')
    refresh_ttl()
    return {'checked_in', '0', '', '0'}
end
record_event('already_checked_in')
refresh_ttl()
return {'already_checked_in', '0', '', '0'}
"""


class BarrierNotFound(Exception):
    """屏障不存在或已过期清理。"""


class NotAParticipant(Exception):
    """报到者不在屏障参与者名单内。"""


class BarrierExpired(Exception):
    """屏障已过截止时间且未达标。"""


@dataclass(frozen=True)
class BarrierMeta:
    barrier_id: str
    participants: list[str]
    threshold: int
    deadline: float  # Unix 秒
    created_at: float  # Unix 秒
    ttl_ms: int
    creator_id: str | None


@dataclass(frozen=True)
class CheckinResult:
    status: str
    newly_added: bool
    quorum: list[str] | None
    frozen_at: float | None

    @property
    def triggered(self) -> bool:
        return self.status == "reached"

    @property
    def reached(self) -> bool:
        """屏障是否处于已达标状态（含本次触发与达标后报到）。"""
        return self.status in ("reached", "late")


class BarrierStore:
    def __init__(self, redis: Redis) -> None:
        self.redis = redis
        self._checkin = redis.register_script(CHECKIN_SCRIPT)

    @staticmethod
    def channel(barrier_id: str) -> str:
        return f"barrier:{barrier_id}:events"

    def _keys(self, barrier_id: str) -> tuple[str, str, str, str, str]:
        return (
            f"barrier:{barrier_id}",
            f"barrier:{barrier_id}:roster",
            f"barrier:{barrier_id}:checked",
            f"barrier:{barrier_id}:frozen",
            f"barrier:{barrier_id}:events",
        )

    async def create(
        self,
        participants: list[str],
        threshold: int,
        deadline_ts: float,
        created_at: float,
        ttl_ms: int,
        creator_id: str | None = None,
    ) -> BarrierMeta:
        barrier_id = uuid.uuid4().hex
        meta_key, roster_key, _, _, _ = self._keys(barrier_id)
        mapping: dict[str, Any] = {
            "threshold": threshold,
            "deadline": f"{deadline_ts:.3f}",
            "created_at": f"{created_at:.3f}",
            "ttl_ms": ttl_ms,
        }
        if creator_id:
            mapping["creator_id"] = creator_id

        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.hset(meta_key, mapping=mapping)
            pipe.sadd(roster_key, *participants)
            pipe.pexpire(meta_key, ttl_ms)
            pipe.pexpire(roster_key, ttl_ms)
            await pipe.execute()

        return BarrierMeta(
            barrier_id=barrier_id,
            participants=sorted(participants),
            threshold=threshold,
            deadline=deadline_ts,
            created_at=created_at,
            ttl_ms=ttl_ms,
            creator_id=creator_id,
        )

    async def get_meta(self, barrier_id: str) -> BarrierMeta | None:
        meta_key, roster_key, _, _, _ = self._keys(barrier_id)
        data = await self.redis.hgetall(meta_key)
        if not data:
            return None
        participants = sorted(await self.redis.smembers(roster_key))
        return BarrierMeta(
            barrier_id=barrier_id,
            participants=participants,
            threshold=int(data["threshold"]),
            deadline=float(data["deadline"]),
            created_at=float(data["created_at"]),
            ttl_ms=int(data["ttl_ms"]),
            creator_id=data.get("creator_id"),
        )

    async def checkin(self, barrier_id: str, participant_id: str) -> CheckinResult:
        """参与者幂等报到；达标判定与冻结在 Lua 中原子完成。"""
        keys = self._keys(barrier_id)
        raw = await self._checkin(
            keys=list(keys), args=[participant_id, f"{time.time():.3f}"]
        )
        status, newly, quorum_json, frozen_at = raw

        if status == "unknown":
            raise BarrierNotFound(barrier_id)
        if status == "not_participant":
            raise NotAParticipant(participant_id)
        if status == "expired":
            raise BarrierExpired(barrier_id)

        result = CheckinResult(
            status=status,
            newly_added=newly == "1",
            quorum=json.loads(quorum_json) if quorum_json else None,
            frozen_at=float(frozen_at) if frozen_at not in ("", "0") else None,
        )

        # 冻结发生在 Lua 内部；脚本返回后立刻发布唤醒事件。
        # 即使发布丢失，等待方也有轮询兜底，不会睡死。
        if status == "reached":
            await self.redis.publish(self.channel(barrier_id), "reached")

        return result

    async def get_frozen(self, barrier_id: str) -> dict[str, Any] | None:
        _, _, _, frozen_key, _ = self._keys(barrier_id)
        data = await self.redis.hgetall(frozen_key)
        if not data:
            return None
        return {
            "quorum": json.loads(data["quorum"]),
            "frozen_at": float(data["frozen_at"]),
        }

    async def checked_members(self, barrier_id: str) -> set[str]:
        _, _, checked_key, _, _ = self._keys(barrier_id)
        return await self.redis.smembers(checked_key)

    async def list_events(
        self, barrier_id: str, cursor: int, limit: int
    ) -> tuple[list[dict[str, Any]], int]:
        """按游标读取报到审计事件：返回 seq > cursor 的至多 limit 条及新游标。

        事件只追加且 seq 与列表下标一一对应（下标 = seq - 1），并发报到
        只会让游标右侧变长，已读区间稳定，因此游标分页不重不漏。
        """
        *_, events_key = self._keys(barrier_id)
        raw = await self.redis.lrange(events_key, cursor, cursor + limit - 1)
        events = [json.loads(item) for item in raw]
        next_cursor = events[-1]["seq"] if events else cursor
        return events, next_cursor

    async def ping(self) -> None:
        await self.redis.ping()

    async def aclose(self) -> None:
        await self.redis.aclose()
