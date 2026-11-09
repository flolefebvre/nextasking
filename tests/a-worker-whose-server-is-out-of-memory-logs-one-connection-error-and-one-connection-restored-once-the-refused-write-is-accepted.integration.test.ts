import { setTimeout as delay } from "node:timers/promises";

import { expect, vi } from "vitest";
import { z } from "zod";

import { createNextasking, defineTask } from "../src/index.js";
import { closeNextaskingAndCheckItsDuration } from "./close-nextasking-and-check-its-duration.js";
import { createHandlerGate, gatedBetweenTwoSteps } from "./handler-gate.js";
import { integrationTest } from "./integration-test.js";
import { STORED_KEYS_READ_BOUND_IN_MILLISECONDS } from "./invariant-violations.js";
import { SERVER_CONTAINER_START_BOUND_IN_MILLISECONDS, startOwnServer } from "./own-server.js";
import {
  createRecordingLogger,
  expectOneConnectionErrorAndOneConnectionRestored,
  waitForLogMessage,
} from "./recording-logger.js";
import { providedRedisUrl } from "./valkey-container.js";
import { withRedis } from "./with-redis.js";

const OUT_OF_MEMORY_REPLY_PATTERN = /OOM command not allowed/;

const MAXMEMORY_BELOW_THE_USED_MEMORY = "1";

const OUT_OF_MEMORY_STRETCH_IN_MILLISECONDS = 5000;

integrationTest.skipIf(providedRedisUrl() !== undefined)(
  "a worker whose server is out of memory while a step save is refused across several retries, its ticks answered meanwhile, logs one connection error naming the server's reply and one connection restored once memory is back, and the run completes",
  {
    timeout:
      SERVER_CONTAINER_START_BOUND_IN_MILLISECONDS +
      10_000 +
      OUT_OF_MEMORY_STRETCH_IN_MILLISECONDS +
      15_000 +
      STORED_KEYS_READ_BOUND_IN_MILLISECONDS +
      5_000,
  },
  async ({ keyPrefix }) => {
    const ownServer = await startOwnServer({ serverArguments: ["--maxmemory-policy", "noeviction"] });
    const { logger, lines } = createRecordingLogger();
    const handlerGate = createHandlerGate();
    const nextasking = createNextasking({
      connection: { url: ownServer.url, giveUpAfter: "1s" },
      prefix: keyPrefix,
      logger,
      tasks: [defineTask("charge-card", { schema: z.object({}), handler: gatedBetweenTwoSteps(handlerGate) })],
    });
    try {
      await withRedis(ownServer.url, async (admin) => {
        await nextasking.startWorker();
        await nextasking.dispatch("charge-card", {}, { runId: "charge-ada" });
        await vi.waitFor(() => expect(handlerGate.inFlightCount()).toBe(1), { timeout: 10_000 });
        await admin.sendCommand(["CONFIG", "SET", "maxmemory", MAXMEMORY_BELOW_THE_USED_MEMORY]);

        handlerGate.release(1);
        await delay(OUT_OF_MEMORY_STRETCH_IN_MILLISECONDS);
        const connectionLinesWhileOutOfMemory = lines
          .filter(({ message }) => message.startsWith("connection "))
          .map(({ message }) => message);
        await admin.sendCommand(["CONFIG", "SET", "maxmemory", "0"]);
        await waitForLogMessage(lines, "run completed", { timeout: 15_000 });

        expect(connectionLinesWhileOutOfMemory).toEqual(["connection error"]);
        expectOneConnectionErrorAndOneConnectionRestored(lines);
        expect(lines.find(({ message }) => message === "connection error")?.fields).toEqual({
          err: expect.objectContaining({ message: expect.stringMatching(OUT_OF_MEMORY_REPLY_PATTERN) }),
        });
      });
    } finally {
      await closeNextaskingAndCheckItsDuration(nextasking);
      await ownServer.stop();
    }
  },
);
