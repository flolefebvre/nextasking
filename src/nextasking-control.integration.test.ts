import type { Redis } from "ioredis";
import { expect, vi } from "vitest";

import { engineTestContextOf, fetchedLockedRunsOf } from "../tests/engine-test.js";
import type { EngineTestContext } from "../tests/engine-test.js";
import { integrationTest } from "../tests/integration-test.js";
import { serverInstantInMillisecondsOf } from "../tests/server-instant.js";
import { withRedis } from "../tests/with-redis.js";
import { engineKeysOf, hashTaggedPrefixOf } from "./engine/engine-keys.js";
import { RECORD_VERSION } from "./engine/engine.js";
import type { RunToCreate } from "./engine/engine.js";
import { createNextaskingControl } from "./nextasking-control.js";
import { deriveFiringRunId } from "./ids.js";
import type { NextaskingControl } from "./nextasking-control.js";

type ControlTestContext = EngineTestContext & { control: NextaskingControl };

const ONE_HOUR_IN_MILLISECONDS = 3_600_000;

function controlTest(name: string, body: (context: ControlTestContext) => Promise<void>): void {
  integrationTest(name, async ({ redisUrl, keyPrefix }) => {
    await withRedis(redisUrl, async (redis, client) => {
      await using control = createNextaskingControl({ connection: { url: redisUrl }, prefix: keyPrefix });
      await body({ ...engineTestContextOf(redis, client, keyPrefix), control });
    });
  });
}

function runOf(runId: string, task: string, extraFields: Partial<RunToCreate> = {}): RunToCreate {
  return { runId, task, payload: "{}", retries: 3, ...extraFields };
}

function delayedRunsOf(task: string, count: number): RunToCreate[] {
  return Array.from({ length: count }, (_, position) =>
    runOf(`${task}-${String(position)}`, task, { delayInMilliseconds: ONE_HOUR_IN_MILLISECONDS }),
  );
}

async function storedBuildReportOf({ engine, redis, keys }: ControlTestContext): Promise<void> {
  await engine.syncSchedules({
    schedulesToSync: [
      { task: "build-report", cron: "0 3 * * *", timezone: "UTC", nextFiringAt: Date.UTC(2030, 0, 1, 3), retries: 4 },
    ],
  });
  await redis.hset(keys.schedule("build-report"), "previousFiringAt", Date.UTC(2029, 11, 31, 3));
}

async function storedKeysUnder(redis: Redis, keyPrefix: string): Promise<string[]> {
  return (await redis.keys(`${hashTaggedPrefixOf(keyPrefix).replace(/[*?[\]\\]/g, "\\$&")}:*`)).sort();
}

controlTest("waitForIdle resolves on a queue whose only run waits for a set time", async (context) => {
  const { engine, control } = context;
  await engine.enqueue({ runs: delayedRunsOf("send-email", 1) });

  await control.waitForIdle({ timeout: "1s" });
});

async function idleTimeoutErrorWhenOneRunIsDueOf({ engine, control }: ControlTestContext): Promise<unknown> {
  await engine.enqueue({ runs: [runOf("due-run", "send-email", { delayInMilliseconds: 1 })] });
  await vi.waitUntil(async () => (await engine.busyRunCounts()).dueRunCount === 1, { timeout: 1000 });
  return control.waitForIdle({ timeout: "1s" }).then(
    () => undefined,
    (error: unknown) => error,
  );
}

controlTest("waitForIdle times out naming the four counts still busy", async (context) => {
  const { engine } = context;
  await engine.enqueue({ runs: ["sms-1", "sms-2", "sms-3"].map((runId) => runOf(runId, "send-sms")) });
  await fetchedLockedRunsOf(context, 3, { "send-sms": 1 });
  await engine.enqueue({ runs: [runOf("pending-run", "send-email"), runOf("other-pending-run", "build-report")] });

  const idleTimeoutError = await idleTimeoutErrorWhenOneRunIsDueOf(context);

  expect(idleTimeoutError).toEqual(
    new Error(
      "The queue is not idle after 1s: 2 runs pending, 1 executing, 2 waiting for a concurrency slot, 1 due and not yet promoted",
    ),
  );
});

controlTest(
  "waitForIdle times out on a run whose due instant passed with no worker to promote it, under the due count",
  async (context) => {
    const idleTimeoutError = await idleTimeoutErrorWhenOneRunIsDueOf(context);

    expect(idleTimeoutError).toEqual(
      new Error(
        "The queue is not idle after 1s: 0 runs pending, 0 executing, 0 waiting for a concurrency slot, 1 due and not yet promoted",
      ),
    );
  },
);

controlTest(
  "makeRunsDueNow given a task makes only that task's runs pending, past the script's cap, and names each",
  async (context) => {
    const { engine, redis, keys, control } = context;
    await engine.enqueue({ runs: delayedRunsOf("send-email", 150) });
    await engine.enqueue({ runs: delayedRunsOf("build-report", 150) });

    const madeDueRuns = await control.makeRunsDueNow({ task: "build-report" });

    expect(madeDueRuns).toHaveLength(150);
    expect(madeDueRuns.every(({ task }) => task === "build-report")).toBe(true);
    expect(new Set(madeDueRuns.map(({ runId }) => runId))).toEqual(
      new Set(delayedRunsOf("build-report", 150).map(({ runId }) => runId)),
    );
    expect(await redis.zcard(keys.runIndex("build-report", "pending"))).toBe(150);
    expect(await redis.zcard(keys.runIndex("send-email", "delayed"))).toBe(150);
  },
);

controlTest("makeRunsDueNow without a task makes every timed run pending", async (context) => {
  const { engine, redis, keys, control } = context;
  await engine.enqueue({ runs: [...delayedRunsOf("send-email", 120), ...delayedRunsOf("build-report", 1)] });

  const madeDueRuns = await control.makeRunsDueNow();

  expect(madeDueRuns).toHaveLength(121);
  expect(await redis.zcard(keys.due)).toBe(0);
});

controlTest(
  "fireSchedule starts a run under the id derived from the server's instant, with scheduledAt and the stored retries",
  async (context) => {
    const { redis, keys, control } = context;
    await storedBuildReportOf(context);
    const scheduleBefore = await redis.hgetall(keys.schedule("build-report"));
    const instantBefore = await serverInstantInMillisecondsOf(redis);

    const { runId } = await control.fireSchedule("build-report");

    const [retries, scheduledAt] = await redis.hmget(keys.runRecord("build-report", runId), "retries", "scheduledAt");
    expect(retries).toBe("4");
    expect(runId).toBe(deriveFiringRunId("build-report", Number(scheduledAt)));
    expect(Number(scheduledAt)).toBeGreaterThanOrEqual(instantBefore);
    expect(Number(scheduledAt)).toBeLessThanOrEqual(await serverInstantInMillisecondsOf(redis));
    expect(await redis.hgetall(keys.schedule("build-report"))).toEqual(scheduleBefore);
  },
);

controlTest("fireSchedule of a task with no stored schedule throws and starts nothing", async (context) => {
  const { redis, keys, control } = context;

  const fireSchedule = control.fireSchedule("send-digest");

  await expect(fireSchedule).rejects.toThrow(
    'No schedule is stored for task "send-digest": fire a scheduled task whose worker has started once',
  );
  expect(await redis.exists(keys.tasks)).toBe(0);
});

controlTest("fireSchedule of a schedule record of a newer version throws naming the version", async (context) => {
  const { redis, keys, control } = context;
  await storedBuildReportOf(context);
  await redis.hset(keys.schedule("build-report"), "version", RECORD_VERSION + 1);

  const fireSchedule = control.fireSchedule("build-report");

  await expect(fireSchedule).rejects.toThrow(
    `The firing of task "build-report" was refused: a newer release wrote a schedule record of version ${String(RECORD_VERSION + 1)} this release cannot read, so no run was started.`,
  );
});

controlTest(
  "reset leaves exactly the schedule records and the schedules set under the prefix, and nothing of another prefix goes",
  async (context) => {
    const { engine, redis, keys, keyPrefix, control } = context;
    const neighbourKey = engineKeysOf(`${keyPrefix}neighbour`).tasks;
    await redis.set(neighbourKey, "kept");
    try {
      await storedBuildReportOf(context);
      await engine.enqueue({ runs: [runOf("sleeping-run", "send-email"), ...delayedRunsOf("build-report", 2)] });
      const [lockedRun] = await fetchedLockedRunsOf(context, 1);
      await engine.saveStep({
        ...lockedRun!,
        stepId: "nap",
        step: { kind: "sleepUntil", resumesAt: Date.UTC(2030, 0, 1) },
      });
      await engine.settle({ ...lockedRun!, outcome: { kind: "suspend", resumesAt: Date.UTC(2030, 0, 1) } });
      await engine.enqueue({ runs: [runOf("completed-run", "send-email")] });
      await engine.settle({ ...(await fetchedLockedRunsOf(context, 1))[0]!, outcome: { kind: "complete" } });
      const scheduleBefore = await redis.hgetall(keys.schedule("build-report"));

      await control.reset();

      expect(await storedKeysUnder(redis, keyPrefix)).toEqual([keys.schedule("build-report"), keys.schedules].sort());
      expect(await redis.hgetall(keys.schedule("build-report"))).toEqual(scheduleBefore);
      expect(await redis.get(neighbourKey)).toBe("kept");
    } finally {
      await redis.del(neighbourKey);
    }
  },
);

controlTest("reset waits for the attempts still executing and times out naming how many", async (context) => {
  const { engine, redis, keys, control } = context;
  await engine.enqueue({ runs: [runOf("executing-run", "send-email")] });
  await fetchedLockedRunsOf(context, 1);

  const reset = control.reset({ timeout: "1s" });

  await expect(reset).rejects.toThrow("reset() found attempts still executing after 1s: 1 executing");
  expect(await redis.exists(keys.runRecord("send-email", "executing-run"))).toBe(1);
});

controlTest("reset refuses under NODE_ENV=production before touching anything", async (context) => {
  const { engine, redis, keys, control } = context;
  await engine.enqueue({ runs: [runOf("pending-run", "send-email")] });
  vi.stubEnv("NODE_ENV", "production");
  try {
    const reset = control.reset();

    await expect(reset).rejects.toThrow(
      "reset() removes every key under the prefix, so it refuses to run in a process whose NODE_ENV is production",
    );
    expect(await redis.exists(keys.runRecord("send-email", "pending-run"))).toBe(1);
  } finally {
    vi.unstubAllEnvs();
  }
});
