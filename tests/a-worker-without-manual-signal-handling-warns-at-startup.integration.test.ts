import { expect, vi } from "vitest";
import { z } from "zod";

import { createNextasking, defineTask } from "../src/index.js";
import { closeNextasking } from "../src/registry.js";
import { integrationTest } from "./integration-test.js";
import { createRecordingLogger } from "./recording-logger.js";

const STARTUP_WARNING =
  "NEXT_MANUAL_SIG_HANDLE is not set: on SIGTERM Next exits at once and cuts the attempts running in this worker";

async function startupLinesOfAWorker(redisUrl: string, keyPrefix: string) {
  const { logger, lines } = createRecordingLogger();
  const nextasking = createNextasking({
    logger,
    connection: { url: redisUrl },
    prefix: keyPrefix,
    tasks: [defineTask("purge-sessions", { schema: z.object({}), handler: async () => undefined })],
  });
  const shutdownSignalListenerCounts = () => [process.listenerCount("SIGTERM"), process.listenerCount("SIGINT")];
  const listenerCountsBefore = shutdownSignalListenerCounts();
  try {
    await nextasking.startWorker();
    return {
      lines: [...lines],
      addedListenerCounts: shutdownSignalListenerCounts().map(
        (count, index) => count - (listenerCountsBefore[index] ?? 0),
      ),
    };
  } finally {
    await closeNextasking(nextasking);
  }
}

integrationTest(
  "a worker started without NEXT_MANUAL_SIG_HANDLE warns right after it starts, and leaves the signals to Next",
  async ({ redisUrl, keyPrefix }) => {
    vi.stubEnv("NEXT_MANUAL_SIG_HANDLE", undefined);

    try {
      const { lines, addedListenerCounts } = await startupLinesOfAWorker(redisUrl, keyPrefix);

      expect(lines).toEqual([
        { level: "info", fields: {}, message: "worker started" },
        { level: "warn", fields: {}, message: STARTUP_WARNING },
      ]);
      expect(addedListenerCounts).toEqual([0, 0]);
    } finally {
      vi.unstubAllEnvs();
    }
  },
);

integrationTest(
  "a worker started under NODE_ENV=development does not warn about NEXT_MANUAL_SIG_HANDLE",
  async ({ redisUrl, keyPrefix }) => {
    vi.stubEnv("NEXT_MANUAL_SIG_HANDLE", undefined);
    vi.stubEnv("NODE_ENV", "development");

    try {
      const { lines } = await startupLinesOfAWorker(redisUrl, keyPrefix);

      expect(lines).toEqual([{ level: "info", fields: {}, message: "worker started" }]);
    } finally {
      vi.unstubAllEnvs();
    }
  },
);
