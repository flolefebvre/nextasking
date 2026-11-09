import { z } from "zod";

import { NonRetriableError, createNextasking, defineTask } from "../src/index.js";
import { closeNextaskingAndCheckItsDuration } from "./close-nextasking-and-check-its-duration.js";
import { createRecordingLogger } from "./recording-logger.js";
import type { RecordedLogLine } from "./recording-logger.js";

function runStateTasks(importReleased: Promise<void>) {
  return [
    defineTask("send-reminder", {
      schema: z.object({ email: z.string() }),
      handler: async () => undefined,
    }),
    defineTask("import-contacts", {
      schema: z.object({ fileName: z.string() }),
      concurrency: { limit: 1 },
      handler: async () => {
        await importReleased;
      },
    }),
    defineTask("sync-calendar", {
      schema: z.object({ calendarId: z.string() }),
      handler: async ({ step }) => {
        await step.run("fetch-events", () => ({ count: 3 }));
        await step.sleep("wait-for-next-sync", "1h");
      },
    }),
    defineTask("charge-card", {
      schema: z.object({ cardId: z.string() }),
      backoff: "1m",
      handler: async () => {
        throw new Error("card declined");
      },
    }),
    defineTask("close-account", {
      schema: z.object({ accountId: z.string() }),
      handler: async () => {
        throw new NonRetriableError("account has open invoices");
      },
    }),
    defineTask("archive-invoices", {
      schema: z.object({ year: z.number() }),
      handler: async () => undefined,
    }),
  ] as const;
}

function createReader(redisUrl: string, keyPrefix: string, importReleased: Promise<void>) {
  return createNextasking({ connection: { url: redisUrl }, prefix: keyPrefix, tasks: runStateTasks(importReleased) });
}

export type ReaderAndWorker = {
  reader: ReturnType<typeof createReader>;
  startWorker: () => Promise<void>;
  workerLines: RecordedLogLine[];
};

export async function withReaderAndWorker(
  redisUrl: string,
  keyPrefix: string,
  use: (readerAndWorker: ReaderAndWorker) => Promise<void>,
): Promise<void> {
  const { logger, lines } = createRecordingLogger();
  let releaseImport = () => undefined as void;
  const importReleased = new Promise<void>((resolve) => {
    releaseImport = resolve;
  });
  const reader = createReader(redisUrl, keyPrefix, importReleased);
  const worker = createNextasking({
    connection: { url: redisUrl },
    prefix: keyPrefix,
    logger,
    tasks: runStateTasks(importReleased).filter((task) => task.name !== "archive-invoices"),
  });
  try {
    await use({ reader, startWorker: () => worker.startWorker(), workerLines: lines });
  } finally {
    releaseImport();
    await closeNextaskingAndCheckItsDuration(worker);
    await closeNextaskingAndCheckItsDuration(reader);
  }
}
