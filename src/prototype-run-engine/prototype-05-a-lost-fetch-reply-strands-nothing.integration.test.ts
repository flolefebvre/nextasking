import { Redis } from "ioredis";
import { expect, vi } from "vitest";

import { integrationTest } from "../../tests/integration-test.js";
import { startTcpProxy } from "../../tests/tcp-proxy.js";
import { openEngine, startInProcessWorker } from "./prototype-harness.js";
import { RunEngine } from "./prototype-run-engine.js";
import { PrototypeWorker } from "./prototype-worker.js";
import type { WorkerEvent } from "./prototype-worker.js";

integrationTest(
  "prototype: a lost fetch reply is answered by the resent fetch with the same lease, so the run starts once right after the reconnect",
  { timeout: 20_000 },
  async ({ redisUrl, keyPrefix }) => {
    const proxy = await startTcpProxy(redisUrl);
    const engine = openEngine(redisUrl, keyPrefix);
    const events: WorkerEvent[] = [];
    const worker = new PrototypeWorker({
      redisUrl: proxy.url,
      keyPrefix,
      concurrency: 2,
      leaseInMilliseconds: 3000,
      handlers: { noop: async () => "done" },
      onEvent: (event) => events.push(event),
    });
    try {
      await engine.loadScripts();
      await engine.enqueue({ runId: "run-1", task: "noop" });
      const fetchReplyCut = proxy.cutBeforeReplyTo((request) => request.includes(worker.workerId));
      worker.start();
      await fetchReplyCut;
      const leasedBeforeReconnect = await engine.run("run-1");
      const reconnectAt = Date.now();
      await proxy.restore();
      await vi.waitUntil(async () => (await engine.run("run-1")).state === "completed", { timeout: 10_000 });
      const run = await engine.run("run-1");
      const started = events.filter((event) => event.type === "started");

      expect(leasedBeforeReconnect).toMatchObject({ state: "executing", epoch: "1" });
      expect(started).toEqual([expect.objectContaining({ epoch: "1" })]);
      expect(started[0]!.at - reconnectAt).toBeLessThan(1500);
      expect(run).toMatchObject({ state: "completed", epoch: "1", stalls: "0" });
      expect(events.filter((event) => event.type === "worker-error")).toEqual([]);
    } finally {
      await worker.close();
      await proxy.stop();
      engine.redis.disconnect();
    }
  },
);

integrationTest(
  "prototype: a run leased by a fetch that is never resent comes back after its lease expires and completes once",
  { timeout: 20_000 },
  async ({ redisUrl, keyPrefix }) => {
    const proxy = await startTcpProxy(redisUrl);
    const engine = openEngine(redisUrl, keyPrefix);
    const lostClient = new Redis(proxy.url, { maxRetriesPerRequest: 0, autoResendUnfulfilledCommands: false });
    lostClient.on("error", () => undefined);
    const lostEngine = new RunEngine(lostClient, keyPrefix);
    let worker: PrototypeWorker | undefined;
    try {
      await engine.loadScripts();
      await engine.enqueue({ runId: "run-1", task: "noop" });
      const fetchReplyCut = proxy.cutBeforeReplyTo((request) => request.includes("lost-worker"));
      const lostFetch = lostEngine.fetch("lost-token", 800, "lost-worker").catch((error: unknown) => error);
      await fetchReplyCut;
      lostClient.disconnect();
      await proxy.restore();
      const leased = await engine.run("run-1");
      const started = startInProcessWorker({
        redisUrl,
        keyPrefix,
        leaseInMilliseconds: 800,
        handlers: { noop: async () => "done" },
      });
      worker = started.worker;
      await vi.waitUntil(async () => (await engine.run("run-1")).state === "completed", { timeout: 10_000 });
      const run = await engine.run("run-1");

      expect(await lostFetch).toBeInstanceOf(Error);
      expect(leased).toMatchObject({ state: "executing", epoch: "1" });
      expect(started.events.filter((event) => event.type === "started")).toEqual([
        expect.objectContaining({ epoch: "2" }),
      ]);
      expect(run).toMatchObject({ state: "completed", "ended:1": "stalled", stalls: "1" });
    } finally {
      await worker?.close();
      await proxy.stop();
      engine.redis.disconnect();
    }
  },
);
