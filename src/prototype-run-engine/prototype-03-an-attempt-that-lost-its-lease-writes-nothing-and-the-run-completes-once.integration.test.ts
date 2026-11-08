import { expect, vi } from "vitest";

import { integrationTest } from "../../tests/integration-test.js";
import { executionRecordsOf, openEngine, startPrototypeWorkerProcess } from "./prototype-harness.js";
import type { PrototypeWorkerProcess } from "./prototype-harness.js";

integrationTest(
  "prototype: attempt A blocks past its lease, attempt B takes over with a new epoch, A's step save and settle are refused, the run completes once",
  { timeout: 40_000 },
  async ({ redisUrl, keyPrefix }) => {
    const engine = openEngine(redisUrl, keyPrefix);
    const workers: PrototypeWorkerProcess[] = [];
    const config = { redisUrl, keyPrefix, concurrency: 1, leaseInMilliseconds: 1000 };
    try {
      const workerA = await startPrototypeWorkerProcess(config);
      workers.push(workerA);
      await engine.enqueue({ runId: "run-1", task: "blocks-first-epoch", payload: { blockInMilliseconds: 6000 } });
      await vi.waitUntil(() => workerA.events.some((event) => event.type === "started"), { timeout: 10_000 });

      const workerB = await startPrototypeWorkerProcess(config);
      workers.push(workerB);
      await vi.waitUntil(async () => (await engine.run("run-1")).state === "completed", { timeout: 20_000 });
      await vi.waitUntil(() => workerA.events.some((event) => event.type === "lease-lost"), { timeout: 20_000 });
      const lateSettle = await engine.settle("run-1", "1", { kind: "complete", result: '"late"' });
      const lateStep = await engine.saveStep("run-1", "1", "after-block", '"1"');
      const run = await engine.run("run-1");
      const records = await executionRecordsOf(engine, keyPrefix);
      const settled = [...workerA.events, ...workerB.events].filter((event) => event.type === "settled");

      expect(run).toMatchObject({ state: "completed", result: '"2"', epoch: "2", "ended:1": "stalled" });
      expect(await engine.savedStep("run-1", "after-block")).toBe('"2"');
      expect(settled).toEqual([expect.objectContaining({ epoch: "2", kind: "complete", outcome: "ok" })]);
      expect(records.filter((record) => record.event === "ended").map((record) => record.epoch)).toEqual(["2"]);
      expect(lateSettle).toBe("stale");
      expect(lateStep).toBe(false);
    } finally {
      await Promise.all(workers.map((worker) => worker.stop()));
      engine.redis.disconnect();
    }
  },
);
