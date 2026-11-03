import { Redis } from "ioredis";
import { expect, vi } from "vitest";
import { z } from "zod";

import { createRedisConnections } from "../src/connection.js";
import { createNextasking, defineTask, NonRetriableError } from "../src/index.js";
import { closeNextasking } from "../src/registry.js";
import { createRunQueue } from "../src/run-queue.js";
import { integrationTest } from "./integration-test.js";
import { createRecordingLogger, waitForLogMessage } from "./recording-logger.js";
import { runQueueKeyPrefix } from "./run-job-key.js";

integrationTest(
  "a Lua error the worker meets while it fails a run is logged once as a worker error, and the worker goes on",
  async ({ redisUrl, keyPrefix }) => {
    const redis = new Redis(redisUrl, { lazyConnect: true });
    redis.on("error", () => undefined);
    const { logger, lines } = createRecordingLogger();
    const completedOrderIds: string[] = [];
    const nextasking = createNextasking({
      connection: { url: redisUrl },
      prefix: keyPrefix,
      logger,
      tasks: [
        defineTask("charge-card", {
          schema: z.object({ cardId: z.string() }),
          handler: async () => {
            throw new NonRetriableError("card declined");
          },
        }),
        defineTask("ship-order", {
          schema: z.object({ orderId: z.string() }),
          handler: async ({ payload }) => {
            completedOrderIds.push(payload.orderId);
          },
        }),
      ],
    });

    try {
      await redis.connect();
      await nextasking.startWorker();
      await redis.set(`${runQueueKeyPrefix(keyPrefix)}failed`, "not a sorted set");

      await nextasking.dispatch("charge-card", { cardId: "card-1" });
      await waitForLogMessage(lines, "worker error");
      await nextasking.dispatch("ship-order", { orderId: "order-1" });
      await vi.waitFor(() => expect(completedOrderIds).toEqual(["order-1"]), { timeout: 10_000 });

      expect(lines.filter((line) => line.message.endsWith(" error"))).toEqual([
        {
          level: "error",
          fields: { err: expect.objectContaining({ message: expect.stringContaining("WRONGTYPE") }) },
          message: "worker error",
        },
      ]);
    } finally {
      await closeNextasking(nextasking);
      redis.disconnect();
    }
  },
);

integrationTest(
  "a queue error is logged once as a queue error unless it is a connection error",
  async ({ redisUrl, keyPrefix }) => {
    const { logger, lines } = createRecordingLogger();
    const runQueue = createRunQueue(createRedisConnections({ url: redisUrl }, logger), "instance", keyPrefix, logger);
    const scriptError = new Error("ERR Error running script: user_script:1: oops");
    const connectionError = Object.assign(new Error("connect ECONNREFUSED 127.0.0.1:6379"), { code: "ECONNREFUSED" });

    try {
      runQueue.bullmqQueue.emit("error", scriptError);
      runQueue.bullmqQueue.emit("error", connectionError);

      expect(lines).toEqual([{ level: "error", fields: { err: scriptError }, message: "queue error" }]);
    } finally {
      await runQueue.close();
    }
  },
);
