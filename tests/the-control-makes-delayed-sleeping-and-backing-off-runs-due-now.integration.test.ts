import { expect, vi } from "vitest";
import { z } from "zod";

import { createNextasking, defineTask } from "../src/index.js";
import { closeNextasking } from "../src/registry.js";
import { createNextaskingControl } from "../src/testing.js";
import { integrationTest } from "./integration-test.js";
import { createRecordingLogger } from "./recording-logger.js";

type FinishedRun = { task: string; attempt: number };

integrationTest(
  "makeRunsDueNow promotes delayed, suspended and backing-off runs, of one task when asked, and a woken sleeper resumes in its attempt",
  async ({ redisUrl, keyPrefix }) => {
    const { logger, lines } = createRecordingLogger();
    const finishedRuns: FinishedRun[] = [];
    const finish = (task: string, attempt: number) => {
      finishedRuns.push({ task, attempt });
    };
    const syncCalendar = defineTask("sync-calendar", {
      schema: z.object({ calendarId: z.string() }),
      handler: async ({ step, attempt }) => {
        await step.sleep("wait-for-next-sync", "1h");
        finish("sync-calendar", attempt);
      },
    });
    const chargeCard = defineTask("charge-card", {
      schema: z.object({ cardId: z.string() }),
      backoff: "1h",
      handler: async ({ attempt }) => {
        if (attempt === 1) {
          throw new Error("card declined");
        }
        finish("charge-card", attempt);
      },
    });
    const sendDigest = defineTask("send-digest", {
      schema: z.object({ userId: z.string() }),
      handler: async ({ attempt }) => {
        finish("send-digest", attempt);
      },
    });
    const sendReminder = defineTask("send-reminder", {
      schema: z.object({ userId: z.string() }),
      handler: async ({ attempt }) => {
        finish("send-reminder", attempt);
      },
    });
    const nextasking = createNextasking({
      connection: { url: redisUrl },
      prefix: keyPrefix,
      logger,
      tasks: [syncCalendar, chargeCard, sendDigest, sendReminder],
    });
    await using control = createNextaskingControl({ connection: { url: redisUrl }, prefix: keyPrefix });

    try {
      await nextasking.startWorker();
      const sleeper = await nextasking.dispatch("sync-calendar", { calendarId: "calendar-1" });
      const charge = await nextasking.dispatch("charge-card", { cardId: "card-1" });
      const digest = await nextasking.dispatch("send-digest", { userId: "user-1" }, { delay: "1h" });
      const reminder = await nextasking.dispatch("send-reminder", { userId: "user-1" }, { delay: "1h" });
      await vi.waitFor(
        async () => {
          const runCounts = await nextasking.countRuns();
          expect([runCounts["sync-calendar"].suspended, runCounts["charge-card"].backingOff]).toEqual([1, 1]);
        },
        { timeout: 10_000 },
      );
      const remindersMadeDue = await control.makeRunsDueNow({ task: "send-reminder" });
      await control.waitForIdle({ timeout: "10s" });
      const finishedAfterTheReminder = [...finishedRuns];
      const runsMadeDue = await control.makeRunsDueNow();
      await control.waitForIdle({ timeout: "10s" });
      const runsLeftToMakeDue = await control.makeRunsDueNow();

      expect(remindersMadeDue).toEqual([{ task: "send-reminder", runId: reminder.runId }]);
      expect(finishedAfterTheReminder).toEqual([{ task: "send-reminder", attempt: 1 }]);
      expect(runsMadeDue).toHaveLength(3);
      expect(runsMadeDue).toEqual(
        expect.arrayContaining([
          { task: "sync-calendar", runId: sleeper.runId },
          { task: "charge-card", runId: charge.runId },
          { task: "send-digest", runId: digest.runId },
        ]),
      );
      expect(finishedRuns).toHaveLength(4);
      expect(finishedRuns).toEqual(
        expect.arrayContaining([
          { task: "sync-calendar", attempt: 1 },
          { task: "charge-card", attempt: 2 },
          { task: "send-digest", attempt: 1 },
        ]),
      );
      expect(
        lines
          .filter((line) => line.fields.task === "sync-calendar" && line.level === "debug")
          .map((line) => line.message),
      ).toEqual(["run dispatched", "attempt started", "run suspended", "run resumed"]);
      expect(runsLeftToMakeDue).toEqual([]);
    } finally {
      await closeNextasking(nextasking);
    }
  },
);
