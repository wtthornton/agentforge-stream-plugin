"""Stream test plugin HTTP routes — GET /api/stream-test/status, GET /api/stream-test/events."""

from __future__ import annotations

import asyncio

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from agentforge_stream import __version__

router = APIRouter(prefix="/api/stream-test", tags=["stream-test"])


@router.get("/status")
async def status() -> dict:
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
            await asyncio.sleep(0)  # yield control

    return StreamingResponse(generator(), media_type="text/event-stream")
