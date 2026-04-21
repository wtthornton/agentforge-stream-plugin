"""Deterministic runner for stream-test-agent. Pure function, no side effects."""

from __future__ import annotations


class StreamRunner:
    def run(self, input_text: str) -> str:  # noqa: ARG002
        return "stream:ok"
