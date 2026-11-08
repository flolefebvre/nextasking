import { expect, vi } from "vitest";

import { integrationTest } from "../../tests/integration-test.js";
import { openEngine, startInProcessWorker } from "./prototype-harness.js";

const BACKOFF = 600;

integrationTest(
  "prototype: a failing attempt is backing off with its due time in the run record and the index alike, then retries at that time",
  { timeout: 20_000 },
  async ({ redisUrl, keyPrefix }) => {
    const engine = openEngine(redisUrl, keyPrefix);
    const { worker, events } = startInProcessWorker({
      redisUrl,
      keyPrefix,
      backoffInMilliseconds: (attempt) => BACKOFF * attempt,
      handlers: {
        flaky: async ({ attempt }) => {
          if (attempt < 3) {
            throw new Error(`upstream unavailable on attempt ${String(attempt)}`);
          }
          return "done";
        },
      },
    });
    try {
      await engine.enqueue({ runId: "run-1", task: "flaky", maxAttempts: 3 });
      await vi.waitUntil(async () => (await engine.run("run-1")).state === "delayed", { timeout: 5000, interval: 10 });
      const backingOff = await engine.run("run-1");
      const delayedScore = await engine.redis.zscore(engine.key("delayed"), "run-1");
      await vi.waitUntil(async () => (await engine.run("run-1")).state === "completed", { timeout: 10_000 });
      const run = await engine.run("run-1");
      const started = events.filter((event) => event.type === "started");

      expect(backingOff).toMatchObject({ state: "delayed", waitReason: "backoff", attempt: "2" });
      expect(delayedScore).toBe(backingOff.dueAt);
      expect(started.map((event) => event.attempt)).toEqual([1, 2, 3]);
      expect(started[1]!.at).toBeGreaterThanOrEqual(Number(backingOff.dueAt));
      expect(started[1]!.at - Number(backingOff.dueAt)).toBeLessThan(250);
      expect(started[2]!.at - started[1]!.at).toBeGreaterThanOrEqual(2 * BACKOFF);
      expect(run).toMatchObject({ state: "completed", attempt: "3" });
    } finally {
      await worker.close();
      engine.redis.disconnect();
    }
  },
);

integrationTest(
  "prototype: the last failing attempt fails the run with its error",
  { timeout: 20_000 },
  async ({ redisUrl, keyPrefix }) => {
    const engine = openEngine(redisUrl, keyPrefix);
    const { worker } = startInProcessWorker({
      redisUrl,
      keyPrefix,
      backoffInMilliseconds: () => 50,
      handlers: {
        broken: async () => {
          throw new Error("always broken");
        },
      },
    });
    try {
      await engine.enqueue({ runId: "run-1", task: "broken", maxAttempts: 2 });
      await vi.waitUntil(async () => (await engine.run("run-1")).state === "failed", { timeout: 5000 });

      expect(await engine.run("run-1")).toMatchObject({ state: "failed", reason: "always broken", attempt: "2" });
      expect(await engine.redis.zscore(engine.key("failed"), "run-1")).not.toBeNull();
    } finally {
      await worker.close();
      engine.redis.disconnect();
    }
  },
);
