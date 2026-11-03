import { expect } from "vitest";
import { z } from "zod";

import { createNextasking, defineEvent, defineListener, defineTask } from "../src/index.js";
import { closeNextasking } from "../src/registry.js";
import { createNextaskingControl } from "../src/testing.js";
import { integrationTest } from "./integration-test.js";
import { createRecordingLogger, waitForLogMessage } from "./recording-logger.js";

integrationTest(
  "waitForIdle waits out the runs pending or executing, ignores delayed runs, and names what is busy at its timeout",
  async ({ redisUrl, keyPrefix }) => {
    const { logger, lines } = createRecordingLogger();
    let releaseImport = () => undefined as void;
    const importReleased = new Promise<void>((resolve) => {
      releaseImport = resolve;
    });
    const completedImports: string[] = [];
    const importContacts = defineTask("import-contacts", {
      schema: z.object({ fileName: z.string() }),
      handler: async ({ payload }) => {
        await importReleased;
        completedImports.push(payload.fileName);
      },
    });
    const sendDigest = defineTask("send-digest", {
      schema: z.object({ userId: z.string() }),
      handler: async () => undefined,
    });
    const nextasking = createNextasking({
      connection: { url: redisUrl },
      prefix: keyPrefix,
      logger,
      tasks: [importContacts, sendDigest],
    });
    await using control = createNextaskingControl({ connection: { url: redisUrl }, prefix: keyPrefix });

    try {
      await nextasking.startWorker();
      await nextasking.dispatch("send-digest", { userId: "user-1" }, { delay: "1h" });
      await nextasking.dispatch("import-contacts", { fileName: "contacts.csv" });
      await waitForLogMessage(lines, "attempt started");
      const busyWaitError = await control.waitForIdle({ timeout: "1s" }).catch((error: unknown) => error);
      releaseImport();
      await control.waitForIdle({ timeout: "10s" });

      expect(busyWaitError).toEqual(
        new Error(
          "The queue is not idle after 1s: 0 runs pending, 1 executing, 0 waiting for a concurrency slot, 0 fan-outs left, 0 firings starting a run",
        ),
      );
      expect(completedImports).toEqual(["contacts.csv"]);
      expect((await nextasking.countRuns())["send-digest"].delayed).toBe(1);
    } finally {
      releaseImport();
      await closeNextasking(nextasking);
    }
  },
);

integrationTest(
  "waitForIdle waits until an emitted event's fan-out and its listener runs are done",
  async ({ redisUrl, keyPrefix }) => {
    const welcomedUserIds: string[] = [];
    const userSignedUp = defineEvent("user.signed-up", {
      schema: z.object({ userId: z.string() }),
    });
    const sendWelcomeEmail = defineListener("send-welcome-email", {
      on: userSignedUp,
      handler: async ({ payload }) => {
        welcomedUserIds.push(payload.userId);
      },
    });
    const nextasking = createNextasking({
      connection: { url: redisUrl },
      prefix: keyPrefix,
      events: [userSignedUp],
      tasks: [sendWelcomeEmail],
    });
    await using control = createNextaskingControl({ connection: { url: redisUrl }, prefix: keyPrefix });

    try {
      await nextasking.emit("user.signed-up", { userId: "user-1" });
      await nextasking.startWorker();
      await control.waitForIdle({ timeout: "10s" });

      expect(welcomedUserIds).toEqual(["user-1"]);
    } finally {
      await closeNextasking(nextasking);
    }
  },
);
