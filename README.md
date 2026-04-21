# agentforge-stream-plugin

SSE streaming test rig for [AgentForge](https://github.com/nlt-labs/AgentForge). Provides a deterministic, zero-external-dependency plugin that emits exactly 3 hard-coded SSE events — the pre-release smoke gate for the streaming pipeline (TAP-762).

## Routes

- `GET /api/stream-test/status` — health check, returns `{"status": "ok", "plugin": "stream-test", "version": "1.0.0"}`
- `GET /api/stream-test/events` — SSE stream that emits `start`, `progress`, and `done` events then closes

## Agent

Registers `stream-test-agent` under namespace `project.stream-test.stream-test-agent`.
`StreamRunner.run()` always returns `"stream:ok"` — no LLM, no network.

## Development

```bash
uv sync --group dev
uv run pytest
```

## Install into AgentForge

```bash
uv pip install -e /path/to/agentforge-stream-plugin
```

Then register via `POST /api/plugins/register` with `{"package_name": "agentforge_stream"}`.
