import { expect } from "vitest";
import { z } from "zod";

import { integrationTest } from "../tests/integration-test.js";
import { listenForLibraryNotifications } from "./bin.js";
import { defineTask } from "./definitions.js";
import { closeNextasking, createNextasking } from "./registry.js";

const purgeSessions = defineTask("purge-sessions", {
  schema: z.object({}),
  handler: async () => undefined,
});

integrationTest(
  "startWorker refuses a second worker in the same process, and the worker bin hears the first resolve and no rejection with no worker while it runs",
  async ({ redisUrl, keyPrefix }) => {
    const nextasking = createNextasking({ connection: { url: redisUrl }, prefix: keyPrefix, tasks: [purgeSessions] });
    const otherNextasking = createNextasking({
      connection: { url: redisUrl },
      prefix: keyPrefix,
      tasks: [purgeSessions],
    });
    const notifications: unknown[] = [];
    const stopListening = listenForLibraryNotifications((notification) => notifications.push(notification));

    try {
      await nextasking.startWorker();
      await expect(otherNextasking.startWorker()).rejects.toThrow(
        "The nextasking worker is already started in this process",
      );
      await expect(otherNextasking.startWorker({ concurrency: 0 })).rejects.toThrow("Invalid startWorker concurrency");

      expect(notifications).toEqual([
        "startWorkerCalled",
        "startWorkerResolved",
        "startWorkerCalled",
        "startWorkerCalled",
      ]);
    } finally {
      stopListening();
      await closeNextasking(otherNextasking);
      await closeNextasking(nextasking);
    }
  },
);

integrationTest("a closed worker lets the process start another one", async ({ redisUrl, keyPrefix }) => {
  const nextasking = createNextasking({ connection: { url: redisUrl }, prefix: keyPrefix, tasks: [purgeSessions] });
  await nextasking.startWorker();
  await closeNextasking(nextasking);
  const nextaskingAfterClose = createNextasking({
    connection: { url: redisUrl },
    prefix: keyPrefix,
    tasks: [purgeSessions],
  });

  try {
    await expect(nextaskingAfterClose.startWorker()).resolves.toBeUndefined();
  } finally {
    await closeNextasking(nextaskingAfterClose);
  }
});
