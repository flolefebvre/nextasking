import { setTimeout as sleep } from "node:timers/promises";

import * as v from "valibot";
import { expect } from "vitest";

import { defineTask } from "../src/definitions.js";
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
