"""AgentForge plugin entry point for agentforge-stream-plugin.

`register(app)` is called by `PluginRegistry.register_plugin()`:
1. Mounts the stream-test router onto the host FastAPI app.
2. Loads the stream-test-agent into the host's AgentLoader (if present), using an
   absolute path derived from this file's location so load never depends on
   CWD at registration time.
"""

from __future__ import annotations

import logging
from pathlib import Path

from fastapi import FastAPI

logger = logging.getLogger(__name__)

_AGENT_DIR = Path(__file__).parent / "agents" / "stream_test_agent"
_NAMESPACE = "project.stream-test"


def register(app: FastAPI) -> None:
    from agentforge_stream.routes import router

    app.include_router(router)

    agent_loader = getattr(app.state, "agent_loader", None)
    if agent_loader is None:
        logger.debug(
            "stream plugin: no agent_loader on app.state — skipping agent load"
        )
        return

    try:
        newly_loaded = agent_loader.load_external(_AGENT_DIR.parent, _NAMESPACE)
        logger.info(
            "stream plugin: loaded %d agent(s) from %s",
            len(newly_loaded),
            _AGENT_DIR,
        )
    except Exception:
        logger.exception("stream plugin: agent load failed")
