"""Unit tests for ``StreamBus`` + ``Subscription`` (TAP-762).

Pure asyncio — no FastAPI, no backend deps.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from agentforge_stream.stream_bus import (
    BackpressureError,
    StreamBus,
    Subscription,
)


async def test_publish_delivers_to_one_subscriber() -> None:
    bus = StreamBus()
    received: list[dict[str, Any]] = []

    async def _cb(ev: dict[str, Any]) -> None:
        received.append(ev)

    sub = bus.subscribe("s1", max_queue_size=10)
    sub.start(_cb)

    result = bus.publish({"n": 1})
    await asyncio.sleep(0)

    assert result == {"delivered": ["s1"], "backpressure": []}
    assert received == [{"n": 1}]
    await sub.disconnect()


async def test_n_ticks_all_processed() -> None:
    bus = StreamBus()
    received: list[int] = []

    async def _cb(ev: dict[str, Any]) -> None:
        received.append(ev["n"])

    sub = bus.subscribe("s", max_queue_size=20)
    sub.start(_cb)

    for i in range(10):
        bus.publish({"n": i})

    # Give the consumer a chance to drain.
    for _ in range(5):
        if len(received) == 10:
            break
        await asyncio.sleep(0.005)

    assert received == list(range(10))
    await sub.disconnect()


async def test_fan_out_to_multiple_subscribers() -> None:
    bus = StreamBus()
    counts = {"a": 0, "b": 0}

    async def _a(_ev: dict[str, Any]) -> None:
        counts["a"] += 1

    async def _b(_ev: dict[str, Any]) -> None:
        counts["b"] += 1

    sa = bus.subscribe("a", max_queue_size=5)
    sb = bus.subscribe("b", max_queue_size=5)
    sa.start(_a)
    sb.start(_b)

    for _ in range(3):
        bus.publish({"x": 1})

    for _ in range(5):
        if counts["a"] == 3 and counts["b"] == 3:
            break
        await asyncio.sleep(0.005)

    assert counts == {"a": 3, "b": 3}
    await sa.disconnect()
    await sb.disconnect()


async def test_backpressure_raises_when_queue_full() -> None:
    bus = StreamBus()

    # No start() → consumer never drains → queue fills immediately.
    sub = bus.subscribe("stuck", max_queue_size=2)
    # First two publishes fit:
    r1 = bus.publish({"n": 1})
    r2 = bus.publish({"n": 2})
    assert r1["delivered"] == ["stuck"]
    assert r2["delivered"] == ["stuck"]

    # Third triggers backpressure for the stuck subscriber:
    r3 = bus.publish({"n": 3})
    assert r3["delivered"] == []
    assert r3["backpressure"] == ["stuck"]

    # Direct subscription.publish_nowait raises so callers can detect it.
    with pytest.raises(BackpressureError):
        sub.publish_nowait({"n": 4})


async def test_backpressured_one_does_not_block_others() -> None:
    bus = StreamBus()
    fast_received: list[int] = []

    async def _fast(ev: dict[str, Any]) -> None:
        fast_received.append(ev["n"])

    # stuck: no consumer, queue=1 → fills on first publish
    bus.subscribe("stuck", max_queue_size=1)
    fast = bus.subscribe("fast", max_queue_size=10)
    fast.start(_fast)

    r1 = bus.publish({"n": 1})  # stuck queue now full
    r2 = bus.publish({"n": 2})  # stuck backpressure, fast still delivered

    assert "stuck" in r1["delivered"]
    assert "stuck" in r2["backpressure"]
    assert "fast" in r2["delivered"]

    for _ in range(5):
        if fast_received == [1, 2]:
            break
        await asyncio.sleep(0.005)
    assert fast_received == [1, 2]
    await fast.disconnect()


async def test_disconnect_removes_subscription() -> None:
    bus = StreamBus()
    sub = bus.subscribe("s1", max_queue_size=5)

    async def _cb(_ev: dict[str, Any]) -> None:
        pass

    sub.start(_cb)
    assert bus.subscription_count() == 1

    removed = await bus.disconnect("s1")
    assert removed is True
    assert bus.subscription_count() == 0

    # Second disconnect is a no-op.
    assert (await bus.disconnect("s1")) is False


async def test_disconnected_subscription_drops_events() -> None:
    bus = StreamBus()
    sub = bus.subscribe("s", max_queue_size=5)
    assert isinstance(sub, Subscription)
    await bus.disconnect("s")
    # publishing after disconnect is a no-op — no raise, no crash
    result = bus.publish({"n": 1})
    assert result == {"delivered": [], "backpressure": []}


async def test_reap_removes_finished_subscriptions() -> None:
    bus = StreamBus()

    async def _cb(_ev: dict[str, Any]) -> None:
        pass

    live = bus.subscribe("live", max_queue_size=5)
    live.start(_cb)

    # Manually cancel one subscription's task to simulate an unexpected death.
    dead = bus.subscribe("dead", max_queue_size=5)
    dead.start(_cb)
    assert dead._task is not None
    dead._task.cancel()
    try:
        await dead._task
    except asyncio.CancelledError:
        pass

    reaped = await bus.reap()
    assert reaped == 1
    assert bus.subscription_count() == 1
    await live.disconnect()


async def test_duplicate_subscription_id_raises() -> None:
    bus = StreamBus()
    bus.subscribe("dup", max_queue_size=5)
    with pytest.raises(ValueError, match="already exists"):
        bus.subscribe("dup", max_queue_size=5)


async def test_subscription_callback_exception_does_not_kill_loop() -> None:
    bus = StreamBus()
    hits: list[int] = []

    async def _cb(ev: dict[str, Any]) -> None:
        hits.append(ev["n"])
        if ev["n"] == 0:
            raise RuntimeError("first event boom")

    sub = bus.subscribe("s", max_queue_size=5)
    sub.start(_cb)

    for i in range(3):
        bus.publish({"n": i})

    for _ in range(5):
        if len(hits) == 3:
            break
        await asyncio.sleep(0.005)

    assert hits == [0, 1, 2]
    await sub.disconnect()
