import { setTimeout as sleep } from "node:timers/promises";

import { expect, vi } from "vitest";
import { z } from "zod";

import { createNextasking, defineTask } from "../src/index.js";
import { closeNextaskingAndCheckItsDuration } from "./close-nextasking-and-check-its-duration.js";
import { startCommandMonitor } from "./command-monitor.js";
import { createHandlerGate } from "./handler-gate.js";
import { integrationTest, TEST_TIMEOUT_PAST_ITS_WAITS_IN_MILLISECONDS } from "./integration-test.js";
import { CLOSE_BOUND_PAST_THE_SHUTDOWN_TIMEOUT_IN_MILLISECONDS } from "./invariant-violations.js";
import { STORED_KEYS_READ_BOUND_IN_MILLISECONDS } from "./invariant-violations.js";
import { SERVER_CONTAINER_START_BOUND_IN_MILLISECONDS, startOwnServer } from "./own-server.js";
import { createRecordingLogger, waitForLogMessage } from "./recording-logger.js";
import { providedRedisUrl } from "./valkey-container.js";

const SERVER_QUIET_CLIENT_TIMEOUT_IN_SECONDS = 15;

const QUIET_CHECK_INTERVAL_IN_MILLISECONDS = 10_000;

const FREE_SLOT_WINDOW_IN_MILLISECONDS = QUIET_CHECK_INTERVAL_IN_MILLISECONDS + 2_000;

const EVERY_SLOT_TAKEN_WINDOW_IN_MILLISECONDS = 3 * SERVER_QUIET_CLIENT_TIMEOUT_IN_SECONDS * 1000 + 2_000;

const WAIT_TIMEOUT_IN_MILLISECONDS = 10_000;

const SHUTDOWN_TIMEOUT_IN_MILLISECONDS = 5_000;

integrationTest.skipIf(providedRedisUrl() !== undefined)(
  "a worker on a server closing clients quiet for 15 s sends no check while it has a free slot, and writes no connection error across three of those periods with every slot taken",
  {
    timeout:
      SERVER_CONTAINER_START_BOUND_IN_MILLISECONDS +
      FREE_SLOT_WINDOW_IN_MILLISECONDS +
      3 * WAIT_TIMEOUT_IN_MILLISECONDS +
      EVERY_SLOT_TAKEN_WINDOW_IN_MILLISECONDS +
      SHUTDOWN_TIMEOUT_IN_MILLISECONDS +
      CLOSE_BOUND_PAST_THE_SHUTDOWN_TIMEOUT_IN_MILLISECONDS +
      STORED_KEYS_READ_BOUND_IN_MILLISECONDS +
      TEST_TIMEOUT_PAST_ITS_WAITS_IN_MILLISECONDS,
  },
  async ({ keyPrefix }) => {
    const ownServer = await startOwnServer({
      serverArguments: ["--timeout", String(SERVER_QUIET_CLIENT_TIMEOUT_IN_SECONDS)],
    });
    const { logger, lines } = createRecordingLogger();
    const handlerGate = createHandlerGate();
    const nextasking = createNextasking({
      connection: { url: ownServer.proxy.url },
      prefix: keyPrefix,
      logger,
      tasks: [
        defineTask("export-orders", {
          schema: z.object({ accountId: z.string() }),
          handler: () => handlerGate.enter(),
        }),
      ],
    });
    try {
      await nextasking.startWorker({ concurrency: 1, shutdownTimeout: "5s" });
      await waitForLogMessage(lines, "worker started", { timeout: WAIT_TIMEOUT_IN_MILLISECONDS });
      const commandMonitor = await startCommandMonitor(ownServer.url);
      let pingsSentWithAFreeSlot: string[][];
      try {
        await sleep(FREE_SLOT_WINDOW_IN_MILLISECONDS);
        pingsSentWithAFreeSlot = commandMonitor
          .monitoredCommands()
          .map(({ args }) => args)
          .filter(([commandName]) => commandName?.toUpperCase() === "PING");
      } finally {
        commandMonitor.stop();
      }

      await nextasking.dispatch("export-orders", { accountId: "acme" });
      await vi.waitFor(() => expect(handlerGate.inFlightCount()).toBe(1), { timeout: WAIT_TIMEOUT_IN_MILLISECONDS });
      const acceptedConnectionCountWithEverySlotTaken = ownServer.proxy.acceptedConnectionCount();
      await sleep(EVERY_SLOT_TAKEN_WINDOW_IN_MILLISECONDS);
      const connectionLinesWithEverySlotTaken = lines.filter(({ message }) => message.startsWith("connection "));
      const acceptedConnectionCountAfterTheWindow = ownServer.proxy.acceptedConnectionCount();
      handlerGate.release(1);
      await waitForLogMessage(lines, "run completed", { timeout: WAIT_TIMEOUT_IN_MILLISECONDS });

      expect(pingsSentWithAFreeSlot, "with a free slot, the worker sent a check").toEqual([]);
      expect(connectionLinesWithEverySlotTaken, "with every slot taken, the worker wrote a connection line").toEqual(
        [],
      );
      expect(
        acceptedConnectionCountAfterTheWindow,
        "with every slot taken, the server closed a connection of the worker",
      ).toBe(acceptedConnectionCountWithEverySlotTaken);
    } finally {
      handlerGate.release(1);
      await closeNextaskingAndCheckItsDuration(nextasking, { shutdownTimeout: "5s" });
      await ownServer.stop();
    }
  },
);
