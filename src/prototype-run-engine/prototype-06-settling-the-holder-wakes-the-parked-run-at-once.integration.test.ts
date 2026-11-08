import { expect, vi } from "vitest";

import { integrationTest } from "../../tests/integration-test.js";
import { openEngine, startInProcessWorker } from "./prototype-harness.js";

integrationTest(
  "prototype: with limit 1, settling the holder wakes the parked run in the same script, which starts within 100 ms",
  { timeout: 20_000 },
  async ({ redisUrl, keyPrefix }) => {
    const engine = openEngine(redisUrl, keyPrefix);
    const releases = new Map<string, () => void>();
    const { worker, events } = startInProcessWorker({
      redisUrl,
      keyPrefix,
      concurrency: 4,
      leaseInMilliseconds: 1000,
      handlers: {
        hold: ({ runId }) => new Promise<void>((resolve) => releases.set(runId, resolve)),
      },
    });
    try {
      const limited = { task: "hold", concurrencyScope: "hold:account-1", concurrencyLimit: 1 };
      await engine.enqueue({ runId: "run-a", ...limited });
      await vi.waitUntil(() => releases.has("run-a"), { timeout: 5000 });
      await engine.enqueue({ runId: "run-b", ...limited });
      await vi.waitUntil(async () => (await engine.run("run-b")).state === "parked", { timeout: 5000 });
      await new Promise((resolve) => setTimeout(resolve, 1500));
      const parkedAfterARenewed = await engine.run("run-b");

      const releasedAt = Date.now();
      releases.get("run-a")!();
      await vi.waitUntil(() => releases.has("run-b"), { timeout: 5000, interval: 2 });
      const bStarted = events.find((event) => event.type === "started" && event.runId === "run-b")!;
      releases.get("run-b")!();
      await vi.waitUntil(async () => (await engine.run("run-b")).state === "completed", { timeout: 5000 });

      expect(parkedAfterARenewed.state).toBe("parked");
      expect(bStarted.at - releasedAt).toBeLessThan(100);
      expect(events.filter((event) => event.type === "started").map((event) => event.runId)).toEqual([
        "run-a",
        "run-b",
      ]);
    } finally {
      for (const release of releases.values()) {
        release();
      }
      await worker.close();
      engine.redis.disconnect();
    }
  },
);
