"""端到端集成测试：需要真实 Redis（REDIS_URL，默认 redis://localhost:6399）。

运行：
    REDIS_URL=redis://localhost:6399/0 \
    BARRIER_POLL_INTERVAL_S=0.05 python3 -m pytest -s tests/
"""

from __future__ import annotations

import asyncio
import os
import time
from datetime import datetime, timedelta, timezone

import pytest
from asgi_lifespan import LifespanManager
from httpx import ASGITransport, AsyncClient

# 在导入 app 前固定配置
os.environ.setdefault("REDIS_URL", "redis://localhost:6399/15")
os.environ.setdefault("BARRIER_POLL_INTERVAL_S", "0.05")

from app.main import app  # noqa: E402

API = "http://test"


@pytest.fixture(autouse=True)
async def _lifespan():
    async with LifespanManager(app):
        yield


async def _client() -> AsyncClient:
    return AsyncClient(transport=ASGITransport(app=app), base_url=API)


async def create_barrier(client: AsyncClient, participants, threshold=None, ttl_s=5):
    deadline = (datetime.now(timezone.utc) + timedelta(seconds=ttl_s)).isoformat()
    r = await client.post(
        "/barriers",
        json={
            "participants": participants,
            "threshold": threshold,
            "deadline": deadline,
        },
    )
    assert r.status_code == 201, r.text
    return r.json()


async def checkin(client: AsyncClient, bid: str, pid: str):
    return await client.post(f"/barriers/{bid}/checkin", json={"participant_id": pid})


@pytest.mark.asyncio
async def test_health_and_root():
    async with await _client() as c:
        r = await c.get("/health")
        assert r.status_code == 200 and r.json()["status"] == "ok"
        r = await c.get("/")
        assert r.status_code == 200 and "endpoints" in r.json()


@pytest.mark.asyncio
async def test_create_validations():
    async with await _client() as c:
        # threshold 大于人数
        r = await c.post(
            "/barriers",
            json={
                "participants": ["a"],
                "threshold": 2,
                "deadline": "2030-01-01T00:00:00Z",
            },
        )
        assert r.status_code == 422
        # deadline 已过去
        r = await c.post(
            "/barriers",
            json={
                "participants": ["a"],
                "deadline": "2000-01-01T00:00:00Z",
            },
        )
        assert r.status_code == 422
        # 名单去重
        r = await c.post(
            "/barriers",
            json={
                "participants": ["a", "a", " b ", "b"],
                "deadline": "2030-01-01T00:00:00Z",
            },
        )
        assert r.status_code == 201
        body = r.json()
        assert body["participants"] == ["a", "b"]
        assert body["threshold"] == 2  # 默认全员


@pytest.mark.asyncio
async def test_idempotent_checkin_and_non_member():
    async with await _client() as c:
        b = await create_barrier(c, ["a", "b", "c"], threshold=3, ttl_s=10)
        bid = b["barrier_id"]

        r1 = await checkin(c, bid, "a")
        assert r1.status_code == 200 and r1.json()["status"] == "checked_in"
        r2 = await checkin(c, bid, "a")
        assert r2.status_code == 200
        assert r2.json()["status"] == "already_checked_in"

        g = await c.get(f"/barriers/{bid}")
        assert g.json()["checked_in"] == ["a"]
        assert g.json()["checked_count"] == 1

        r3 = await checkin(c, bid, "outsider")
        assert r3.status_code == 403
        r4 = await c.get("/barriers/deadbeefdeadbeef/wait")
        assert r4.status_code == 404


@pytest.mark.asyncio
async def test_concurrent_reach_freezes_snapshot_exactly_once():
    """6 人屏障，达标人数 4：6 个并发报到必须且只能有一个触发 reached，
    冻结 quorum 唯一确定，其余为 late。"""
    async with await _client() as c:
        members = [f"m{i}" for i in range(6)]
        b = await create_barrier(c, members, threshold=4, ttl_s=10)
        bid = b["barrier_id"]

        results = await asyncio.gather(*(checkin(c, bid, m) for m in members))
        assert all(r.status_code == 200 for r in results), results

        bodies = [r.json() for r in results]
        triggered = [x for x in bodies if x["triggered"]]
        assert len(triggered) == 1, f"应有且仅有一个 triggered，实际 {triggered}"

        reachers = [x for x in bodies if x["status"] == "reached"]
        assert len(reachers) == 1
        quorum = reachers[0]["quorum"]
        assert len(quorum) == 4
        frozen_at = reachers[0]["frozen_at"]

        # 冻结后任何视图、任何后续报到都不能改写 quorum / frozen_at
        for _ in range(3):
            r = await checkin(c, bid, "m5")
            assert r.json()["status"] == "late"
            assert r.json()["triggered"] is False

        # 达标后非名单成员依然被拒绝，且不泄露冻结快照
        r = await checkin(c, bid, "stranger")
        assert r.status_code == 403

        g = await c.get(f"/barriers/{bid}")
        view = g.json()
        assert view["status"] == "reached"
        assert view["quorum"] == quorum
        assert view["frozen_at"] == frozen_at
        assert set(view["checked_in"]) == set(members)
        late = set(view["late"])
        assert late == set(members) - set(quorum)


@pytest.mark.asyncio
async def test_multiple_waiters_all_woken_on_reach():
    """多个等待者并发挂起，达标时必须全部立即拿到同一份冻结快照。"""
    async with await _client() as c:
        members = ["w1", "w2", "w3", "w4"]
        b = await create_barrier(c, members, threshold=2, ttl_s=10)
        bid = b["barrier_id"]

        async def wait_view():
            async with await _client() as c2:
                started = time.time()
                r = await c2.get(f"/barriers/{bid}/wait")
                return time.time() - started, r

        waiters = [asyncio.create_task(wait_view()) for _ in range(4)]
        await asyncio.sleep(0.4)  # 确保都已进入订阅+等待
        await checkin(c, bid, "w1")
        await asyncio.sleep(0.2)
        await checkin(c, bid, "w2")  # 达标

        outcomes = await asyncio.gather(*waiters)
        for elapsed, r in outcomes:
            assert r.status_code == 200, r.text
            body = r.json()
            assert body["status"] == "reached"
            assert sorted(body["quorum"]) == ["w1", "w2"]
            assert elapsed < 5, f"等待者未被及时唤醒: {elapsed:.2f}s"


@pytest.mark.asyncio
async def test_wait_returns_immmediately_when_already_settled():
    async with await _client() as c:
        b = await create_barrier(c, ["x", "y"], threshold=1, ttl_s=10)
        bid = b["barrier_id"]
        await checkin(c, bid, "x")
        t0 = time.time()
        r = await c.get(f"/barriers/{bid}/wait")
        assert r.status_code == 200
        assert r.json()["status"] == "reached"
        assert time.time() - t0 < 2


@pytest.mark.asyncio
async def test_deadline_returns_checked_and_missing_and_rejects_checkin():
    async with await _client() as c:
        members = ["d1", "d2", "d3"]
        b = await create_barrier(c, members, threshold=3, ttl_s=1)
        bid = b["barrier_id"]
        await checkin(c, bid, "d1")

        t0 = time.time()
        r = await c.get(f"/barriers/{bid}/wait")
        elapsed = time.time() - t0
        assert r.status_code == 200
        body = r.json()
        assert body["status"] == "expired"
        assert body["checked_in"] == ["d1"]
        assert body["missing"] == ["d2", "d3"]
        assert 0.8 < elapsed < 5, f"应在截止时返回，实际 {elapsed:.2f}s"

        # 截止后报到被拒绝，结果不变
        r2 = await checkin(c, bid, "d2")
        assert r2.status_code == 409
        g = await c.get(f"/barriers/{bid}")
        assert g.json()["status"] == "expired"
        assert g.json()["checked_in"] == ["d1"]
        assert g.json()["missing"] == ["d2", "d3"]


@pytest.mark.asyncio
async def test_waiter_joining_after_reach_gets_snapshot():
    """晚加入的等待者：到达时屏障已达标，应立即得到快照（二次检查路径）。"""
    async with await _client() as c:
        b = await create_barrier(c, ["p", "q"], threshold=1, ttl_s=10)
        bid = b["barrier_id"]
        await checkin(c, bid, "p")
        await asyncio.sleep(0.1)
        r = await c.get(f"/barriers/{bid}/wait")
        assert r.json()["status"] == "reached"
        assert r.json()["quorum"] == ["p"]


async def get_events(client: AsyncClient, bid: str, **params):
    return await client.get(f"/barriers/{bid}/events", params=params)


@pytest.mark.asyncio
async def test_checkin_writes_ordered_audit_events():
    """首次 / 达标前重复 / 触发达标 / 达标后迟到，每次报到都有一条事件，
    序号从 0 连续递增，时间为数字时间戳格式的 ISO 字符串。"""
    from datetime import datetime

    async with await _client() as c:
        b = await create_barrier(c, ["a", "b", "c"], threshold=2, ttl_s=10)
        bid = b["barrier_id"]

        cases = [
            ("a", "checked_in"),
            ("a", "already_checked_in"),
            ("b", "reached"),
            ("c", "late"),
            ("c", "late"),
        ]
        for i, (pid, expected_status) in enumerate(cases):
            r = await checkin(c, bid, pid)
            assert r.status_code == 200, r.text
            body = r.json()
            assert body["status"] == expected_status
            assert body["seq"] == i  # 报到响应直接回传本次事件序号

        page = await get_events(c, bid, limit=50)
        assert page.status_code == 200, page.text
        data = page.json()
        assert data["barrier_id"] == bid
        assert data["limit"] == 50
        events = data["events"]
        assert [(e["seq"], e["participant_id"], e["result"]) for e in events] == [
            (0, "a", "checked_in"),
            (1, "a", "already_checked_in"),
            (2, "b", "reached"),
            (3, "c", "late"),
            (4, "c", "late"),
        ]
        assert data["next_cursor"] is None  # 一页取完 -> 流末尾
        # 时间字段可解析为带时区的 ISO-8601，且单调不减
        tss = [datetime.fromisoformat(e["ts"]) for e in events]
        assert all(t.tzinfo is not None for t in tss)
        assert tss == sorted(tss)


@pytest.mark.asyncio
async def test_error_requests_write_no_events():
    """403/409/404/422 等错误请求都不得产生审计事件。"""
    async with await _client() as c:
        b = await create_barrier(c, ["a", "b"], threshold=2, ttl_s=1)
        bid = b["barrier_id"]

        assert (await checkin(c, bid, "stranger")).status_code == 403
        assert (await checkin(c, bid, "")).status_code == 422  # pydantic 校验
        assert (await checkin(c, "deadbeefdeadbeef", "a")).status_code == 404
        assert (await get_events(c, "deadbeefdeadbeef")).status_code == 404

        # 等截止后再报到 -> 409
        await asyncio.sleep(1.1)
        assert (await checkin(c, bid, "a")).status_code == 409

        page = await get_events(c, bid)
        assert page.json()["events"] == []
        assert page.json()["next_cursor"] is None


@pytest.mark.asyncio
async def test_events_cursor_pagination_no_dup_no_gap():
    """limit 分页 + cursor 串起所有页：序号连续、不重不漏。"""
    async with await _client() as c:
        members = [f"p{i}" for i in range(7)]
        b = await create_barrier(c, members, threshold=7, ttl_s=10)
        bid = b["barrier_id"]
        for m in members:
            await checkin(c, bid, m)

        seen: list[int] = []
        cursor = None
        pages = 0
        for _ in range(10):
            params = {"limit": 3}
            if cursor is not None:
                params["cursor"] = cursor
            page = await get_events(c, bid, **params)
            assert page.status_code == 200, page.text
            body = page.json()
            seqs = [e["seq"] for e in body["events"]]
            seen.extend(seqs)
            pages += 1
            cursor = body["next_cursor"]
            if cursor is None:
                break

        assert pages == 3  # 3 + 3 + 1
        assert seen == list(range(7))

        # 末尾游标再轮询：空页 + null，不报错
        # 倒数第二页 next_cursor 指向最后一条，应仍能取到
        tail = await get_events(c, bid, cursor=encode_test_cursor(6), limit=10)
        assert [e["seq"] for e in tail.json()["events"]] == [6]
        end = await get_events(c, bid, cursor=encode_test_cursor(7), limit=10)
        assert end.json()["events"] == []
        assert end.json()["next_cursor"] is None

        # limit 越界 -> 422；坏游标 -> 400
        assert (await get_events(c, bid, limit=0)).status_code == 422
        assert (await get_events(c, bid, limit=201)).status_code == 422
        assert (await get_events(c, bid, cursor="%%%not-base64")).status_code == 400
        forged = _b64_cursor("b2:5")  # 版本前缀不符
        assert (await get_events(c, bid, cursor=forged)).status_code == 400


def _b64_cursor(payload: str) -> str:
    import base64

    return base64.urlsafe_b64encode(payload.encode()).rstrip(b"=").decode()


def encode_test_cursor(offset: int) -> str:
    return _b64_cursor(f"b1:{offset}")


@pytest.mark.asyncio
async def test_cursor_stable_across_concurrent_checkins():
    """并发报到期间持续翻页：最终通过游标必定恰好收集到全部事件一次。"""
    async with await _client() as c:
        members = [f"c{i}" for i in range(20)]
        b = await create_barrier(c, members, threshold=20, ttl_s=10)
        bid = b["barrier_id"]

        async def produce():
            await asyncio.gather(
                *(checkin(c, bid, m) for m in members)
            )

        async def drain():
            collected: list[int] = []
            cursor = None
            for _ in range(500):
                params = {"limit": 4}
                if cursor is not None:
                    params["cursor"] = cursor
                body = (await get_events(c, bid, **params)).json()
                seqs = [e["seq"] for e in body["events"]]
                collected.extend(seqs)
                cursor = body["next_cursor"]
                if cursor is None:
                    # 短页：此刻已读到流末尾
                    if produce_task.done() and len(collected) == 20:
                        return collected
                    await asyncio.sleep(0.01)  # 等待并发新事件，复用同一游标
                else:
                    await asyncio.sleep(0)  # 让出，制造交错
            return collected

        produce_task = asyncio.create_task(produce())
        collected = await drain()
        await produce_task

        assert sorted(collected) == list(range(20))
        assert len(collected) == len(set(collected)) == 20  # 不重不漏

        # threshold=20：前 19 条 checked_in，最后 1 条 reached
        page = await get_events(c, bid, limit=50)
        results = {(e["participant_id"], e["result"]) for e in page.json()["events"]}
        assert len(results) == 20
        reached_participants = {
            e["participant_id"] for e in page.json()["events"]
            if e["result"] == "reached"
        }
        assert len(reached_participants) == 1
        assert all(
            r in ("checked_in", "reached") for _, r in results
        )


@pytest.mark.asyncio
async def test_concurrent_checkin_events_exactly_one_reached():
    """并发达标的审计流：恰好一个 reached，序号连续，且每人有事件。"""
    async with await _client() as c:
        members = [f"e{i}" for i in range(6)]
        b = await create_barrier(c, members, threshold=4, ttl_s=10)
        bid = b["barrier_id"]

        responses = await asyncio.gather(*(checkin(c, bid, m) for m in members))
        seqs = [r.json()["seq"] for r in responses]
        assert sorted(seqs) == list(range(6))  # 每个响应拿到唯一连续序号

        page = await get_events(c, bid, limit=50)
        events = page.json()["events"]
        assert [e["seq"] for e in events] == list(range(6))
        results = [e["result"] for e in events]
        assert results.count("reached") == 1
        assert sorted(p["participant_id"] for p in events) == sorted(members)
        # reached 事件序号即视图中第 4 个报到（序号 3）
        reached_events = [e for e in events if e["result"] == "reached"]
        assert reached_events[0]["seq"] == 3


@pytest.mark.asyncio
async def test_events_share_barrier_lifecycle():
    """审计键与屏障同生命周期：屏障在则事件在；屏障回收后事件同样 404。"""
    from app.main import app as _app

    redis = _app.state.store.redis

    async with await _client() as c:
        b = await create_barrier(c, ["t1", "t2"], threshold=2, ttl_s=2)
        bid = b["barrier_id"]
        await checkin(c, bid, "t1")

        assert (await get_events(c, bid)).status_code == 200
        audit_ttl = await redis.pttl(f"barrier:{bid}:audit")
        meta_ttl = await redis.pttl(f"barrier:{bid}")
        # 报到脚本统一刷新全部键 TTL，两者同量级（差仅毫秒）
        assert audit_ttl > 0 and meta_ttl > 0
        assert abs(audit_ttl - meta_ttl) < 5000

        # 模拟 Redis 回收：将该屏障所有键 TTL 压缩到 100ms
        async for key in redis.scan_iter(match=f"barrier:{bid}*"):
            await redis.pexpire(key, 100)
        await asyncio.sleep(0.5)
        assert (await c.get(f"/barriers/{bid}")).status_code == 404
        assert (await get_events(c, bid)).status_code == 404
