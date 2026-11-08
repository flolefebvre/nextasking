import { Redis } from "ioredis";

import { prototypeHandlers } from "./prototype-handlers.js";
import { PrototypeWorker } from "./prototype-worker.js";

const config = JSON.parse(process.env.PROTOTYPE_WORKER_CONFIG ?? "{}") as {
  redisUrl: string;
  keyPrefix: string;
  concurrency: number;
  leaseInMilliseconds: number;
};

const recordsRedis = new Redis(config.redisUrl, { maxRetriesPerRequest: null });
recordsRedis.on("error", () => undefined);

const worker = new PrototypeWorker({
  ...config,
  handlers: prototypeHandlers(recordsRedis, config.keyPrefix),
  onEvent: (event) => {
    process.stdout.write(`${JSON.stringify(event)}\n`);
  },
});
await worker.redis.ping();
worker.start();
process.send?.("ready");
