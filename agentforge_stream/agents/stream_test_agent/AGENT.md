---
name: stream-test-agent
namespace: project.stream-test.stream-test-agent
description: Test agent for AgentForge streaming rig.
keywords: [stream, sse, test]
runner: agentforge_stream.agents.stream_test_agent.runner:StreamRunner
---

# Stream Test Agent

Deterministic test fixture for the AgentForge SSE streaming pipeline.

Returns `"stream:ok"` unconditionally. Exists to exercise `AgentLoader.load_external()`
and namespace registration under `project.stream-test` without pulling in any LLM
provider or external service.
