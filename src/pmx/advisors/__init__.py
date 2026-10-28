"""Advisors: slow or non-deterministic reasoning — AI agents, outside model services — running
outside the engine loop in a worker process. They read a lagging, read-only view of state and send
bounded, expiring `Advice`; they never place orders (invariant 14)."""
