import { setTimeout as sleep } from "node:timers/promises";

import * as v from "valibot";
import { expect, vi } from "vitest";
import { z } from "zod";

import { defineTask } from "../src/definitions.js";
import { createNextasking } from "../src/index.js";
import { closeNextasking } from "../src/registry.js";
import { createNextaskingControl } from "../src/testing.js";
import type { AnyTaskDefinition } from "../src/definitions.js";
import { engineKeysOf } from "../src/engine/engine-keys.js";
import { createEngine } from "../src/engine/engine.js";
import { createEngineControl } from "../src/engine-control.js";
import type { HandlerSender } from "../src/step-engine.js";
import { startWorkerLoop } from "../src/worker-loop.js";
import { DEFAULT_WORKER_TIMING } from "../src/worker-timing.js";
import { integrationTest } from "./integration-test.js";
import { expectNoInvariantViolations } from "./invariant-violations.js";
import { createRecordingLogger } from "./recording-logger.js";
import { createSimulatedRedisOutage } from "./simulated-redis-outage.js";
import { withRedis } from "./with-redis.js";
import { workerDefinitionsOf } from "./worker-definitions.js";

const TRY_COUNT = 50;

const HANDLER_WORK_IN_MILLISECONDS = 5;

const NO_SENDER: HandlerSender = {
  dispatch: () => Promise.reject(new Error("this scenario sends nothing")),
  emit: () => Promise.reject(new Error("this scenario sends nothing")),
};

const sendEmail = defineTask("send-email", {
  schema: v.object({}),
  handler: async () => {
    await sleep(HANDLER_WORK_IN_MILLISECONDS);
  },
}) as AnyTaskDefinition;

const sendReminder = defineTask("send-reminder", {
  schema: v.object({}),
  handler: async ({ step }) => {
    await step.sleep("wait-a-day", "1d");
    await sleep(HANDLER_WORK_IN_MILLISECONDS);
  },
}) as AnyTaskDefinition;

integrationTest(
  `makeRunsDueNow then waitForIdle returns only once the delayed and the sleeping runs made due have ended, in ${String(TRY_COUNT)} tries`,
  { timeout: 120_000 },
  async ({ redisUrl, keyPrefix }) => {
    await withRedis(redisUrl, async (redis) => {
      const keys = engineKeysOf(keyPrefix);
      const engine = createEngine(redis, keyPrefix);
      const { logger, lines } = createRecordingLogger();
      const loop = startWorkerLoop({
        engine,
        definitions: workerDefinitionsOf([sendEmail, sendReminder]),
        timing: { ...DEFAULT_WORKER_TIMING, tickIntervalInMilliseconds: 100 },
        logger,
        concurrency: 10,
        sender: NO_SENDER,
        waitForWakeUp: (timeoutInMilliseconds) => sleep(Math.min(timeoutInMilliseconds, 10)),
        redisOutage: createSimulatedRedisOutage(logger).redisOutage,
      });
      await using control = createEngineControl({ connection: { url: redisUrl }, prefix: keyPrefix });
      const statesSeenAtIdleByTry: string[] = [];
      try {
        for (let tryNumber = 1; tryNumber <= TRY_COUNT; tryNumber += 1) {
          const [delayedRunId, sleepingRunId] = [`delayed-${String(tryNumber)}`, `sleeping-${String(tryNumber)}`];
          await engine.enqueue({
            runs: [
              { runId: delayedRunId, task: "send-email", payload: "{}", retries: 0, delayInMilliseconds: 3_600_000 },
              { runId: sleepingRunId, task: "send-reminder", payload: "{}", retries: 0 },
            ],
          });
          await control.waitForIdle({ timeout: "10s" });

          await control.makeRunsDueNow();
          await control.waitForIdle({ timeout: "10s" });

          const states = await Promise.all(
            [
              { task: "send-email", runId: delayedRunId },
              { task: "send-reminder", runId: sleepingRunId },
            ].map(({ task, runId }) => redis.hget(keys.runRecord(task, runId), "state")),
          );
          statesSeenAtIdleByTry.push(`try ${String(tryNumber)}: ${states.join(", ")}`);
        }
      } finally {
        await loop.stopFetching();
        await loop.stopTicking();
        await loop.waitForAttemptsInFlight();
      }

      expect(statesSeenAtIdleByTry.filter((states) => !states.endsWith(": completed, completed"))).toEqual([]);
      expect(statesSeenAtIdleByTry).toHaveLength(TRY_COUNT);
      expect(lines.filter(({ level }) => level === "error")).toEqual([]);
      await expectNoInvariantViolations(redis, keyPrefix);
    });
  },
);

type FinishedRun = { task: string; attempt: number };

integrationTest(
  "through the instance's worker, makeRunsDueNow then waitForIdle ends delayed, suspended and backing-off runs, of one task when asked, and a woken sleeper resumes in its attempt",
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
      const syncCalendarDebugMessages = lines
        .filter((line) => line.fields.task === "sync-calendar" && line.level === "debug")
        .map((line) => line.message);
      expect(syncCalendarDebugMessages.filter((message) => message !== "run dispatched")).toEqual([
        "attempt started",
        "run suspended",
        "run resumed",
      ]);
      expect(syncCalendarDebugMessages.filter((message) => message === "run dispatched")).toHaveLength(1);
      expect(runsLeftToMakeDue).toEqual([]);
    } finally {
      await closeNextasking(nextasking);
    }
  },
);
