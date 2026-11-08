import { randomUUID } from "node:crypto";

import { openEngine } from "./prototype-harness.js";
import { PrototypeWorker } from "./prototype-worker.js";

const redisUrl = process.argv[2] ?? "redis://127.0.0.1:6390";
const runCount = Number(process.argv[3] ?? 10_000);
const keyPrefix = `throughput:${randomUUID()}:`;
const engine = openEngine(redisUrl, keyPrefix);
await engine.loadScripts();

const enqueueStartedAt = Date.now();
for (let start = 0; start < runCount; start += 1000) {
  await Promise.all(
    Array.from({ length: Math.min(1000, runCount - start) }, (_, offset) =>
      engine.enqueue({ runId: `run-${String(start + offset)}`, task: "noop" }),
    ),
  );
}
const enqueuedIn = Date.now() - enqueueStartedAt;

const worker = new PrototypeWorker({ redisUrl, keyPrefix, concurrency: 50, handlers: { noop: async () => undefined } });
const startedAt = Date.now();
worker.start();
while ((await engine.redis.zcard(engine.key("completed"))) < runCount) {
  await new Promise((resolve) => setTimeout(resolve, 20));
}
const executedIn = Date.now() - startedAt;
await worker.close();
engine.redis.disconnect();
console.log(
  JSON.stringify({
    runCount,
    enqueuePerSecond: Math.round((runCount / enqueuedIn) * 1000),
    executePerSecond: Math.round((runCount / executedIn) * 1000),
  }),
);
