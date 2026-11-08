import { expect, vi } from "vitest";

import { integrationTest } from "../../tests/integration-test.js";
import { openEngine, startInProcessWorker } from "./prototype-harness.js";

integrationTest(
  "prototype: a run suspended until a time holds one wake time, holds no lease, and resumes once at that time",
  { timeout: 20_000 },
  async ({ redisUrl, keyPrefix }) => {
    const engine = openEngine(redisUrl, keyPrefix);
    let beforeSleepExecutions = 0;
    const resumedAt: number[] = [];
    const { worker, events } = startInProcessWorker({
      redisUrl,
      keyPrefix,
      handlers: {
        sleeper: async ({ payload, step, sleepUntil }) => {
          const { until } = payload as { until: number };
          await step("before", async () => {
            beforeSleepExecutions += 1;
          });
          await sleepUntil("nap", until);
          resumedAt.push(Date.now());
          return "woke";
        },
      },
    });
    try {
      const until = Date.now() + 800;
      await engine.enqueue({ runId: "run-1", task: "sleeper", payload: { until }, concurrencyScope: "s", concurrencyLimit: 1 });
      await vi.waitUntil(async () => (await engine.run("run-1")).state === "delayed", { timeout: 5000, interval: 10 });
      const asleep = await engine.run("run-1");
      const delayedScore = await engine.redis.zscore(engine.key("delayed"), "run-1");
      const slotsWhileAsleep = await engine.redis.scard(engine.key("slots:s"));
      await vi.waitUntil(async () => (await engine.run("run-1")).state === "completed", { timeout: 5000 });
      const run = await engine.run("run-1");

      expect(asleep).toMatchObject({ state: "delayed", waitReason: "sleeping", dueAt: String(until) });
      expect(asleep.leaseUntil).toBeUndefined();
      expect(Object.keys(asleep).filter((field) => /at$|until$/i.test(field) && field !== "createdAt")).toEqual([
        "dueAt",
      ]);
      expect(delayedScore).toBe(String(until));
      expect(slotsWhileAsleep).toBe(0);
      expect(resumedAt).toHaveLength(1);
      expect(resumedAt[0]).toBeGreaterThanOrEqual(until);
      expect(resumedAt[0]! - until).toBeLessThan(250);
      expect(beforeSleepExecutions).toBe(1);
      expect(run).toMatchObject({ state: "completed", attempt: "1", result: '"woke"' });
      expect(events.filter((event) => event.type === "started")).toHaveLength(2);
    } finally {
      await worker.close();
      engine.redis.disconnect();
    }
  },
);
