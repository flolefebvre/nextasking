import { expect, vi } from "vitest";

import { integrationTest } from "../../tests/integration-test.js";
import { startTcpProxy } from "../../tests/tcp-proxy.js";
import { openEngine, startInProcessWorker } from "./prototype-harness.js";

for (const kind of ["complete", "retry"] as const) {
  integrationTest(
    `prototype: a connection cut after a ${kind} settle is applied, before its reply, is absorbed by the resent settle`,
    { timeout: 20_000 },
    async ({ redisUrl, keyPrefix }) => {
      const proxy = await startTcpProxy(redisUrl);
      const engine = openEngine(redisUrl, keyPrefix);
      let settleReplyCut: Promise<void> | undefined;
      const { worker, events } = startInProcessWorker({
        redisUrl: proxy.url,
        keyPrefix,
        concurrency: 2,
        backoffInMilliseconds: () => 100,
        handlers: {
          flaky: async ({ runId, attempt }) => {
            if (attempt === 1) {
              settleReplyCut ??= proxy.cutBeforeReplyTo(
                (request) => request.includes(runId) && request.includes(kind),
              );
              if (kind === "retry") {
                throw new Error("upstream unavailable");
              }
            }
            return "done";
          },
        },
      });
      try {
        await engine.loadScripts();
        await engine.enqueue({ runId: "run-1", task: "flaky" });
        await vi.waitUntil(() => settleReplyCut !== undefined, { timeout: 5000 });
        await settleReplyCut;
        await proxy.restore();
        await vi.waitUntil(() => events.some((event) => event.type === "settled" && event.epoch === "1"), {
          timeout: 10_000,
        });
        await vi.waitUntil(async () => (await engine.run("run-1")).state === "completed", { timeout: 10_000 });
        const run = await engine.run("run-1");

        expect(events.filter((event) => event.type === "worker-error" || event.type === "lease-lost")).toEqual([]);
        expect(events.filter((event) => event.type === "settled" && event.epoch === "1")).toEqual([
          expect.objectContaining({ kind, outcome: "already" }),
        ]);
        expect(run).toMatchObject({ state: "completed", attempt: kind === "retry" ? "2" : "1" });
        expect(events.filter((event) => event.type === "started")).toHaveLength(kind === "retry" ? 2 : 1);
      } finally {
        await worker.close();
        await proxy.stop();
        engine.redis.disconnect();
      }
    },
  );
}
