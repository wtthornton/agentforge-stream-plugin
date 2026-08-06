"""Stream plugin HTTP routes (TAP-762).

- ``GET /api/stream-test/status``           — health / version
- ``GET /api/stream-test/events``           — hard-coded 3-event SSE stream (legacy)
- ``POST /api/stream-demo/subscribe``       — register an in-process subscription
- ``POST /api/stream-demo/emit``            — publish an event to all subscribers
- ``GET /api/stream-demo/counter``          — how many events each agent processed
- ``GET /api/stream-demo/stream``           — SSE variant: N events streamed over one response
- ``DELETE /api/stream-demo/subscriptions/{id}`` — disconnect one subscription (reaps it)
"""

from __future__ import annotations

import asyncio
from typing import Any

from fastapi import APIRouter, Request, Response
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from agentforge_stream import __version__
from agentforge_stream.stream_bus import StreamBus

router = APIRouter(prefix="/api/stream-test", tags=["stream-test"])
demo_router = APIRouter(prefix="/api/stream-demo", tags=["stream-demo"])


@router.get("/status")
async def status() -> dict[str, Any]:
    return {"status": "ok", "plugin": "stream-test", "version": __version__}


@router.get("/events")
async def stream_events(request: Request) -> StreamingResponse:
    events = [
        'data: {"type": "start", "seq": 1}\n\n',
        'data: {"type": "progress", "seq": 2}\n\n',
        'data: {"type": "done", "seq": 3}\n\n',
    ]

    async def generator():
        for event in events:
            yield event
            await asyncio.sleep(0)

    return StreamingResponse(generator(), media_type="text/event-stream")


# ---------------------------------------------------------------------------
# Stream demo (in-process bus + subscriptions + counter)
# ---------------------------------------------------------------------------


class _DemoState:
    def __init__(self) -> None:
        self.bus = StreamBus()
        self.counters: dict[str, int] = {}


def _state(request: Request) -> _DemoState:
    s = getattr(request.app.state, "_stream_demo_state", None)
    if s is None:
        s = _DemoState()
        request.app.state._stream_demo_state = s
    return s


class SubscribeRequest(BaseModel):
    subscription_id: str
    max_queue_size: int = Field(default=100, ge=1, le=10_000)


@demo_router.post("/subscribe")
async def subscribe(body: SubscribeRequest, request: Request) -> dict[str, Any]:
    state = _state(request)
    if body.subscription_id in state.counters:
        return {"subscribed": False, "reason": "duplicate-id"}

    sub = state.bus.subscribe(body.subscription_id, max_queue_size=body.max_queue_size)
    state.counters[body.subscription_id] = 0

    async def _on_event(_event: dict[str, Any]) -> None:
        state.counters[body.subscription_id] += 1

    sub.start(_on_event)
    return {
        "subscribed": True,
        "subscription_id": body.subscription_id,
        "max_queue_size": body.max_queue_size,
    }


class EmitRequest(BaseModel):
    payload: dict[str, Any] = Field(default_factory=dict)
    count: int = Field(default=1, ge=1, le=10_000)


@demo_router.post("/emit")
async def emit(
    body: EmitRequest, request: Request, response: Response
) -> dict[str, Any]:
    state = _state(request)
    delivered_total: list[str] = []
    backpressure_total: dict[str, int] = {}

    for _ in range(body.count):
        result = state.bus.publish(body.payload)
        delivered_total.extend(result["delivered"])
        for sid in result["backpressure"]:
            backpressure_total[sid] = backpressure_total.get(sid, 0) + 1

    if backpressure_total:
        response.status_code = 429
        return {
            "published": body.count,
            "delivered": len(delivered_total),
            "backpressure": backpressure_total,
            "error": "queue-full",
            "detail": (
                "one or more subscriptions exceeded max_queue_size; "
                "increase max_queue_size or drain the queue"
            ),
        }

    return {
        "published": body.count,
        "delivered": len(delivered_total),
        "backpressure": {},
    }


@demo_router.get("/counter")
async def counter(request: Request) -> dict[str, Any]:
    state = _state(request)
    return {
        "counters": dict(state.counters),
        "subscription_count": state.bus.subscription_count(),
    }


@demo_router.delete("/subscriptions/{subscription_id}")
async def disconnect(subscription_id: str, request: Request) -> dict[str, Any]:
    state = _state(request)
    removed = await state.bus.disconnect(subscription_id)
    state.counters.pop(subscription_id, None)
    return {"disconnected": removed, "subscription_id": subscription_id}


@demo_router.post("/reap")
async def reap(request: Request) -> dict[str, Any]:
    state = _state(request)
    reaped = await state.bus.reap()
    return {"reaped": reaped, "remaining": state.bus.subscription_count()}


@demo_router.get("/stream")
async def sse_stream(count: int = 5) -> StreamingResponse:
    """SSE variant: emit ``count`` synthetic events then close."""
    count = max(1, min(count, 1000))

    async def generator():
        for n in range(count):
            yield f'data: {{"seq": {n}}}\n\n'
            await asyncio.sleep(0)

    return StreamingResponse(generator(), media_type="text/event-stream")
