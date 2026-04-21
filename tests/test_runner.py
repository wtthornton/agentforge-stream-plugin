"""Tests for StreamRunner."""

from agentforge_stream.agents.stream_test_agent import StreamRunner


def test_runner_returns_stream_ok():
    assert StreamRunner().run("anything") == "stream:ok"


def test_runner_ignores_input():
    r = StreamRunner()
    assert r.run("hello") == r.run("") == r.run("world") == "stream:ok"


def test_runner_is_deterministic():
    r = StreamRunner()
    assert r.run("x") == r.run("x") == "stream:ok"
