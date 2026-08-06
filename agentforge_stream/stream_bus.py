"""In-process stream bus + subscription for the stream-demo rig (TAP-762).

A dumb single-topic pub/sub with a bounded queue per subscription. No network,
no threads — only ``asyncio``. Replaces what a real WebSocket server would do
so the smoke tests stay hermetic (AC: "in-process fake server, no Docker").

Backpressure: when a subscription's queue reaches ``max_queue_size``,
``StreamBus.publish`` surfaces a :class:`BackpressureError` for that
subscription. The publish call continues for *other* subscriptions — one slow
consumer can't block the rest. Callers decide whether the error is terminal
or retryable; the plugin route turns it into an HTTP 429.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

AgentCallback = Callable[[dict[str, Any]], Awaitable[None]]


class BackpressureError(RuntimeError):
    """Raised when a subscription's queue is full."""

    def __init__(self, subscription_id: str, max_size: int) -> None:
        super().__init__(
            f"subscription {subscription_id!r} queue full (max={max_size})"
        )
        self.subscription_id = subscription_id
        self.max_size = max_size


@dataclass
class Subscription:
    id: str
    max_size: int
    queue: asyncio.Queue[dict[str, Any]] = field(init=False)
    _task: asyncio.Task[None] | None = field(default=None, init=False, repr=False)
    _connected: bool = field(default=True, init=False)

    def __post_init__(self) -> None:
        self.queue = asyncio.Queue(maxsize=self.max_size)

    @property
    def connected(self) -> bool:
        return self._connected

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    def publish_nowait(self, event: dict[str, Any]) -> None:
        if not self._connected:
            # Silently drop — the bus treats this subscription as gone.
            return
        try:
            self.queue.put_nowait(event)
        except asyncio.QueueFull as exc:
            raise BackpressureError(self.id, self.max_size) from exc

    def start(self, callback: AgentCallback) -> None:
        if self.running:
            return
        self._task = asyncio.create_task(self._loop(callback))

    async def _loop(self, callback: AgentCallback) -> None:
        while self._connected:
            event = await self.queue.get()
            try:
                await callback(event)
            except Exception:  # noqa: BLE001 — one bad callback can't kill the subscription
                logger.warning(
                    "stream subscription %s callback raised", self.id, exc_info=True
                )

    async def disconnect(self) -> None:
        self._connected = False
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None


class StreamBus:
    """Minimal fan-out pub/sub for one implicit topic."""

    def __init__(self) -> None:
        self._subscriptions: dict[str, Subscription] = {}

    def subscribe(
        self, subscription_id: str, *, max_queue_size: int = 100
    ) -> Subscription:
        if subscription_id in self._subscriptions:
            raise ValueError(f"subscription id {subscription_id!r} already exists")
        sub = Subscription(id=subscription_id, max_size=max_queue_size)
        self._subscriptions[subscription_id] = sub
        return sub

    def publish(self, event: dict[str, Any]) -> dict[str, Any]:
        """Publish to every connected subscriber.

        Returns a summary dict:
        ``{"delivered": [ids], "backpressure": [ids]}``. A backpressured
        subscriber is left in the map so the caller can retry later; the
        subscription is not auto-disconnected.
        """
        delivered: list[str] = []
        backpressure: list[str] = []
        for sub in list(self._subscriptions.values()):
            if not sub.connected:
                continue
            try:
                sub.publish_nowait(event)
                delivered.append(sub.id)
            except BackpressureError:
                backpressure.append(sub.id)
        return {"delivered": delivered, "backpressure": backpressure}

    async def disconnect(self, subscription_id: str) -> bool:
        sub = self._subscriptions.pop(subscription_id, None)
        if sub is None:
            return False
        await sub.disconnect()
        return True

    async def reap(self) -> int:
        """Drop any subscription whose consumer task has finished / been cancelled."""
        dead: list[str] = [
            sid for sid, sub in self._subscriptions.items() if not sub.running
        ]
        for sid in dead:
            await self.disconnect(sid)
        return len(dead)

    def subscription_count(self) -> int:
        return len(self._subscriptions)
