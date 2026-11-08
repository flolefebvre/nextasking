import { expect, vi } from "vitest";

import { integrationTest } from "../../tests/integration-test.js";
import { openEngine, startPrototypeWorkerProcess } from "./prototype-harness.js";
import type { PrototypeWorkerProcess } from "./prototype-harness.js";
import type { RunEngine } from "./prototype-run-engine.js";

const SHORT_LEASE = 150;

async function stallOnce(engine: RunEngine, runId: string): Promise<void> {
  const lease = await engine.fetch(`token-${String(Math.random())}`, SHORT_LEASE, "test");
  expect(lease?.runId).toBe(runId);
  await vi.waitUntil(async () => (await engine.promoteDue()).reclaimed === 1, { timeout: 2000, interval: 20 });
}

integrationTest(
  "prototype: three stalls with no step saved in between fail the run stalled",
  { timeout: 10_000 },
  async ({ redisUrl, keyPrefix }) => {
    const engine = openEngine(redisUrl, keyPrefix);
    try {
      await engine.enqueue({ runId: "run-1", task: "noop" });

      await stallOnce(engine, "run-1");
      await stallOnce(engine, "run-1");
      const afterTwo = await engine.run("run-1");
      await stallOnce(engine, "run-1");
      const afterThree = await engine.run("run-1");

      expect(afterTwo).toMatchObject({ state: "pending", stalls: "2" });
      expect(afterThree).toMatchObject({ state: "failed", reason: "stalled", stalls: "3" });
      expect(await engine.redis.zscore(engine.key("failed"), "run-1")).not.toBeNull();
      expect(await engine.fetch("token-after", SHORT_LEASE, "test")).toBeUndefined();
    } finally {
      engine.redis.disconnect();
    }
  },
);

integrationTest(
  "prototype: a step saved between stalls resets the stall count",
  { timeout: 10_000 },
  async ({ redisUrl, keyPrefix }) => {
    const engine = openEngine(redisUrl, keyPrefix);
    try {
      await engine.enqueue({ runId: "run-1", task: "noop" });

      await stallOnce(engine, "run-1");
      await stallOnce(engine, "run-1");
      const lease = await engine.fetch("token-step", SHORT_LEASE, "test");
      const saved = await engine.saveStep("run-1", lease!.epoch, "charge", '"ok"');
      await vi.waitUntil(async () => (await engine.promoteDue()).reclaimed === 1, { timeout: 2000, interval: 20 });
      await stallOnce(engine, "run-1");
      const afterReset = await engine.run("run-1");

      expect(saved).toBe(true);
      expect(afterReset).toMatchObject({ state: "pending", stalls: "2" });
    } finally {
      engine.redis.disconnect();
    }
  },
);

integrationTest(
  "prototype: a run whose worker is SIGKILLed three times fails stalled",
  { timeout: 40_000 },
  async ({ redisUrl, keyPrefix }) => {
    const engine = openEngine(redisUrl, keyPrefix);
    const workers: PrototypeWorkerProcess[] = [];
    try {
      await engine.enqueue({
        runId: "run-1",
        task: "limited",
        payload: { scope: "", workInMilliseconds: 60_000 },
      });

      for (let kill = 1; kill <= 3; kill += 1) {
        const worker = await startPrototypeWorkerProcess({
          redisUrl,
          keyPrefix,
          concurrency: 1,
          leaseInMilliseconds: 1000,
        });
        workers.push(worker);
        await vi.waitUntil(() => worker.events.some((event) => event.type === "started"), { timeout: 10_000 });
        worker.kill("SIGKILL");
        await worker.exited;
        await vi.waitUntil(async () => (await engine.promoteDue()).reclaimed === 1, { timeout: 5000, interval: 50 });
      }
      const run = await engine.run("run-1");

      expect(run).toMatchObject({ state: "failed", reason: "stalled", stalls: "3", epoch: "3" });
    } finally {
      await Promise.all(workers.map((worker) => worker.stop()));
      engine.redis.disconnect();
    }
  },
);
