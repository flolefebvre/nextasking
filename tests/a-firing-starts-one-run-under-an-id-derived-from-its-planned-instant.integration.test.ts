import { expect, vi } from "vitest";

import { createEngine } from "../src/engine/engine.js";
import { createNextasking, defineScheduledTask } from "../src/index.js";
import { deriveFiringRunId } from "../src/ids.js";
import { closeNextaskingAndCheckItsDuration } from "./close-nextasking-and-check-its-duration.js";
import { integrationTest } from "./integration-test.js";
import { createRecordingLogger } from "./recording-logger.js";
import { storedRunIdentities } from "./stored-run-identities.js";
import { storedRun } from "./stored-run.js";
import { moveNextFiringTo } from "./stored-schedules.js";
import { redisClientOf, withRedis } from "./with-redis.js";

type RecordedFiringRun = { runId: string; scheduledAt: Date | undefined; attempt: number };

integrationTest(
  "a firing starts one run whose id and scheduledAt come from the planned instant, not the time it was processed, and a repeat of it starts no other run",
  async ({ redisUrl, keyPrefix }) => {
    const recordedRuns: RecordedFiringRun[] = [];
    const purgeExpiredSessions = defineScheduledTask("purge-expired-sessions", {
      schedule: { cron: "0 0 1 1 *", timezone: "Europe/Paris" },
      handler: async ({ runId, scheduledAt, attempt }) => {
        recordedRuns.push({ runId, scheduledAt, attempt });
      },
    });
    const nextasking = createNextasking({
      connection: { url: redisUrl },
      prefix: keyPrefix,
      logger: createRecordingLogger().logger,
      tasks: [purgeExpiredSessions],
    });
    const plannedInstant = Date.parse("2025-12-31T23:00:00.000Z");
    const firingRunId = deriveFiringRunId("purge-expired-sessions", plannedInstant);

    try {
      await nextasking.startWorker();
      await moveNextFiringTo(redisUrl, keyPrefix, "purge-expired-sessions", plannedInstant);
      await vi.waitFor(
        () => {
          expect(recordedRuns).toHaveLength(1);
        },
        { timeout: 10_000 },
      );
      const repeatedFiring = await withRedis(redisUrl, (redis) =>
        createEngine(redisClientOf(redis), keyPrefix).fire({
          task: "purge-expired-sessions",
          instant: plannedInstant,
          instantAfter: Date.parse("2026-12-31T23:00:00.000Z"),
          runId: firingRunId,
          retries: 5,
        }),
      );

      expect(plannedInstant).toBeLessThan(Date.now());
      expect(recordedRuns).toEqual([{ runId: firingRunId, scheduledAt: new Date(plannedInstant), attempt: 1 }]);
      expect(await storedRun(redisUrl, keyPrefix, "purge-expired-sessions", firingRunId)).toMatchObject({
        scheduledAt: String(plannedInstant),
      });
      expect(repeatedFiring.kind).not.toBe("fired");
      expect(await storedRunIdentities(redisUrl, keyPrefix)).toEqual([
        { task: "purge-expired-sessions", runId: firingRunId },
      ]);
    } finally {
      await closeNextaskingAndCheckItsDuration(nextasking);
    }
  },
);
