import { expect } from "vitest";

import { defineScheduledTask } from "../src/definitions.js";
import type { AnyTaskDefinition } from "../src/definitions.js";
import { engineKeysOf, storedRunIdOf } from "../src/engine/engine-keys.js";
import type { RunIdentity } from "../src/engine/engine-keys.js";
import { createEngine } from "../src/engine/engine.js";
import { createReadApi } from "../src/read-api.js";
import type { ReadApi } from "../src/read-api.js";
import type { AnyListedRun } from "../src/read-api.js";
import { endRuns } from "./ended-runs.js";
import { engineTest } from "./engine-test.js";
import type { EngineTestContext } from "./engine-test.js";
import { integrationTest } from "./integration-test.js";
import { storedKeyViolationsOf } from "./stored-key-violations.js";
import { withRedis } from "./with-redis.js";

const SHARED_RUN_IDENTITIES: RunIdentity[] = [
  { task: "send-receipt", runId: "order-1" },
  { task: "update-stock", runId: "order-1" },
];

const SHARED_FINISHED_AT = Date.UTC(2030, 0, 1);

function readerOf({ redis, keyPrefix }: Pick<EngineTestContext, "redis" | "keyPrefix">): ReadApi {
  const tasksByName = new Map<string, AnyTaskDefinition>(
    SHARED_RUN_IDENTITIES.map(({ task }) => [
      task,
      defineScheduledTask(task, { schedule: { cron: "0 * * * *" }, handler: async () => undefined }),
    ]),
  );
  return createReadApi({
    client: redis,
    prefix: keyPrefix,
    tasksByName,
    listenerNamesByEventName: new Map(),
    declaredSchedules: [],
  });
}

async function completedSharedRunsFinishedTogether({ engine, redis, keys }: EngineTestContext): Promise<void> {
  for (const { task, runId } of SHARED_RUN_IDENTITIES) {
    await endRuns(engine, task, [runId], "complete");
    await redis.hset(keys.runRecord(task, runId), "finishedAt", SHARED_FINISHED_AT);
    await redis.zadd(keys.runIndex(task, "completed"), SHARED_FINISHED_AT, storedRunIdOf(task, runId));
  }
}

async function everyListedRunOf(reader: ReadApi, task?: string): Promise<AnyListedRun[]> {
  const listedRuns: AnyListedRun[] = [];
  let cursor: string | undefined;
  do {
    const page = await reader.listRuns({
      state: "completed",
      limit: 1,
      ...(task === undefined ? {} : { task }),
      ...(cursor === undefined ? {} : { cursor }),
    });
    listedRuns.push(...page.runs);
    cursor = page.nextCursor;
  } while (cursor !== undefined);
  return listedRuns;
}

engineTest("getRun reads each of two runs sharing a run id under its own task", async (context) => {
  await context.engine.enqueue({
    runs: SHARED_RUN_IDENTITIES.map(({ task, runId }) => ({ task, runId, payload: `{"of":"${task}"}`, retries: 0 })),
  });
  const reader = readerOf(context);

  const runs = await Promise.all(SHARED_RUN_IDENTITIES.map(({ task, runId }) => reader.getRun(task, runId)));

  expect(runs.map((run) => [run?.task, run?.runId, run?.payload])).toEqual([
    ["send-receipt", "order-1", { of: "send-receipt" }],
    ["update-stock", "order-1", { of: "update-stock" }],
  ]);
});

engineTest(
  "listRuns pages across two runs sharing a run id and a score, each once with the consumer's run id",
  async (context) => {
    await completedSharedRunsFinishedTogether(context);
    const reader = readerOf(context);

    const listedRuns = await everyListedRunOf(reader);
    const listedRunsOfOneTask = await everyListedRunOf(reader, "update-stock");

    expect(listedRuns.map(({ task, runId }) => ({ task, runId }))).toEqual([
      SHARED_RUN_IDENTITIES[1],
      SHARED_RUN_IDENTITIES[0],
    ]);
    expect(listedRunsOfOneTask.map(({ task, runId }) => ({ task, runId }))).toEqual([SHARED_RUN_IDENTITIES[1]]);
  },
);

engineTest("countRuns counts both runs sharing a run id, one under each task", async (context) => {
  await completedSharedRunsFinishedTogether(context);

  const runCounts = await readerOf(context).countRuns();

  expect([runCounts["send-receipt"]?.completed, runCounts["update-stock"]?.completed]).toEqual([1, 1]);
});

integrationTest(
  "listRuns throws naming the index and the member when a run index holds a member that is not a stored run id",
  async ({ redisUrl, keyPrefix, expectInvariantViolation }) => {
    await withRedis(redisUrl, async (redis) => {
      const keys = engineKeysOf(keyPrefix);
      const engine = createEngine(redis, keyPrefix);
      await endRuns(engine, "send-receipt", ["order-1"], "complete");
      const indexKey = keys.runIndex("send-receipt", "completed");
      expectInvariantViolation("stored keys", `run "order-2" sits in ${indexKey} but has no record with a known state`);
      await redis.zadd(indexKey, 0, "order-2");

      const listing = readerOf({ redis, keyPrefix }).listRuns({ state: "completed" });

      await expect(listing).rejects.toThrow(
        `The run index ${indexKey} holds a member that is not a stored run id: "order-2"`,
      );
      expect(await storedKeyViolationsOf(redis, keyPrefix)).toEqual([
        `run "order-2" sits in ${indexKey} but has no record with a known state`,
      ]);
    });
  },
);
