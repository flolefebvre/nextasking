import { expect } from "vitest";

import { engineKeysOf, storedRunIdOf } from "../src/engine/engine-keys.js";
import type { EngineKeys } from "../src/engine/engine-keys.js";
import { integrationTest } from "./integration-test.js";
import { serverInstantInMillisecondsOf } from "./server-instant.js";
import { runsLeftExecutingIn, storedKeysSnapshotOf, storedKeyViolationsOf } from "./stored-key-violations.js";
import { withRedis } from "./with-redis.js";
import type { TestClient } from "./with-redis.js";

const PENDING_RUN = storedRunIdOf("send-email", "pending-run");
const DELAYED_RUN = storedRunIdOf("send-email", "delayed-run");
const EXECUTING_RUN = storedRunIdOf("rebuild-index", "executing-run");
const PARKED_RUN = storedRunIdOf("rebuild-index", "parked-run");
const COMPLETED_RUN = storedRunIdOf("send-email", "completed-run");
const GHOST_RUN = storedRunIdOf("send-email", "ghost-run");

const LAPSED_LOCK_INSTANT = 9000;

async function writeConsistentRuns(
  redis: TestClient,
  keys: EngineKeys,
  { lockedUntil = LAPSED_LOCK_INSTANT }: { lockedUntil?: number } = {},
): Promise<void> {
  await redis
    .multi()
    .sAdd(keys.tasks, ["send-email", "rebuild-index"])
    .hSet(keys.runRecord("send-email", "pending-run"), { state: "pending", task: "send-email", order: 1 })
    .zAdd(keys.pending, { score: 1, value: PENDING_RUN })
    .zAdd(keys.runIndex("send-email", "pending"), { score: 1, value: PENDING_RUN })
    .hSet(keys.runRecord("send-email", "delayed-run"), { state: "delayed", task: "send-email", startsAt: 5000 })
    .zAdd(keys.due, { score: 5000, value: DELAYED_RUN })
    .zAdd(keys.runIndex("send-email", "delayed"), { score: 5000, value: DELAYED_RUN })
    .hSet(keys.runRecord("rebuild-index", "executing-run"), {
      state: "executing",
      task: "rebuild-index",
      concurrencyKey: "account-7",
      startedAt: 3000,
      lockedUntil,
    })
    .zAdd(keys.locks, { score: lockedUntil, value: EXECUTING_RUN })
    .zAdd(keys.runIndex("rebuild-index", "executing"), { score: 3000, value: EXECUTING_RUN })
    .sAdd(keys.slotSet("rebuild-index", "account-7"), EXECUTING_RUN)
    .hSet(keys.runRecord("rebuild-index", "parked-run"), {
      state: "waitingForConcurrencySlot",
      task: "rebuild-index",
      concurrencyKey: "account-7",
      order: 2,
    })
    .zAdd(keys.waitingList("rebuild-index", "account-7"), { score: 2, value: PARKED_RUN })
    .zAdd(keys.runIndex("rebuild-index", "waitingForConcurrencySlot"), { score: 2, value: PARKED_RUN })
    .hSet(keys.runRecord("send-email", "completed-run"), { state: "completed", task: "send-email", finishedAt: 4000 })
    .zAdd(keys.runIndex("send-email", "completed"), { score: 4000, value: COMPLETED_RUN })
    .hSet(keys.schedule("rebuild-index"), { nextFiringAt: 60000 })
    .zAdd(keys.schedules, { score: 60000, value: "rebuild-index" })
    .exec();
}

integrationTest("records and indexes that agree pass the invariant checker", async ({ redisUrl, keyPrefix }) => {
  await withRedis(redisUrl, async (redis) => {
    await writeConsistentRuns(redis, engineKeysOf(keyPrefix));

    const violations = await storedKeyViolationsOf(redis, keyPrefix);

    expect(violations).toEqual([]);
  });
});

const CORRUPTIONS: {
  name: string;
  corrupt: (redis: TestClient, keys: EngineKeys) => Promise<unknown>;
  expectedViolationsOf: (keys: EngineKeys) => string[];
}[] = [
  {
    name: "an index member without a record",
    corrupt: (redis, keys) => redis.zAdd(keys.pending, { score: 7, value: GHOST_RUN }),
    expectedViolationsOf: (keys) => [`run "${GHOST_RUN}" sits in ${keys.pending} but has no record with a known state`],
  },
  {
    name: "a read index score that disagrees with the record",
    corrupt: (redis, keys) => redis.zAdd(keys.runIndex("send-email", "pending"), { score: 99, value: PENDING_RUN }),
    expectedViolationsOf: (keys) => [
      `run "${PENDING_RUN}" has score 99 in ${keys.runIndex("send-email", "pending")} but 1 in its record`,
    ],
  },
  {
    name: "an engine structure score that disagrees with the record",
    corrupt: (redis, keys) => redis.zAdd(keys.locks, { score: 1, value: EXECUTING_RUN }),
    expectedViolationsOf: (keys) => [`run "${EXECUTING_RUN}" has score 1 in ${keys.locks} but 9000 in its record`],
  },
  {
    name: "a record left in the index of its former state",
    corrupt: (redis, keys) => redis.zAdd(keys.runIndex("send-email", "delayed"), { score: 4000, value: COMPLETED_RUN }),
    expectedViolationsOf: (keys) => [
      `run "${COMPLETED_RUN}" sits in ${keys.runIndex("send-email", "delayed")} though its record is completed`,
    ],
  },
  {
    name: "a record missing from the read index of its state",
    corrupt: (redis, keys) => redis.zRem(keys.runIndex("send-email", "delayed"), DELAYED_RUN),
    expectedViolationsOf: (keys) => [
      `run "${DELAYED_RUN}" is delayed but missing from ${keys.runIndex("send-email", "delayed")}`,
    ],
  },
  {
    name: "a record missing from the engine structure of its state",
    corrupt: (redis, keys) => redis.zRem(keys.pending, PENDING_RUN),
    expectedViolationsOf: (keys) => [`run "${PENDING_RUN}" is pending but missing from ${keys.pending}`],
  },
  {
    name: "a parked run in the waiting list of another key",
    corrupt: (redis, keys) =>
      redis.zAdd(keys.waitingList("rebuild-index", "account-8"), { score: 2, value: PARKED_RUN }),
    expectedViolationsOf: (keys) => [
      `run "${PARKED_RUN}" sits in ${keys.waitingList("rebuild-index", "account-8")} though its record is waitingForConcurrencySlot`,
    ],
  },
  {
    name: "a slot held by a run that is not executing",
    corrupt: (redis, keys) => redis.sAdd(keys.slotSet("rebuild-index", "account-7"), PARKED_RUN),
    expectedViolationsOf: (keys) => [
      `run "${PARKED_RUN}" holds a slot in ${keys.slotSet("rebuild-index", "account-7")} but is not an executing run of that key`,
    ],
  },
  {
    name: "a slot held in the slot set of another key",
    corrupt: (redis, keys) => redis.sAdd(keys.slotSet("rebuild-index", undefined), EXECUTING_RUN),
    expectedViolationsOf: (keys) => [
      `run "${EXECUTING_RUN}" holds a slot in ${keys.slotSet("rebuild-index", undefined)} but is not an executing run of that key`,
    ],
  },
  {
    name: "an executing run missing from the slot set of its key",
    corrupt: (redis, keys) => redis.sRem(keys.slotSet("rebuild-index", "account-7"), EXECUTING_RUN),
    expectedViolationsOf: (keys) => [
      `run "${EXECUTING_RUN}" is executing but holds no slot in ${keys.slotSet("rebuild-index", "account-7")}`,
    ],
  },
  {
    name: "a record of a task missing from the tasks set",
    corrupt: (redis, keys) => redis.sRem(keys.tasks, "send-email"),
    expectedViolationsOf: (keys) =>
      [PENDING_RUN, DELAYED_RUN, COMPLETED_RUN].map(
        (storedRunId) => `run "${storedRunId}" is of task "send-email", missing from ${keys.tasks}`,
      ),
  },
  {
    name: "a payload left without its record",
    corrupt: (redis, keys) => redis.set(keys.runPayload("send-email", "ghost-run"), "{}"),
    expectedViolationsOf: () => [`run "${GHOST_RUN}" has its payload key but no record`],
  },
  {
    name: "errors left without their record",
    corrupt: (redis, keys) => redis.rPush(keys.runErrors("send-email", "ghost-run"), "{}"),
    expectedViolationsOf: () => [`run "${GHOST_RUN}" has its errors key but no record`],
  },
  {
    name: "steps left without their record",
    corrupt: (redis, keys) => redis.hSet(keys.runSteps("send-email", "ghost-run"), "charge", "1"),
    expectedViolationsOf: () => [`run "${GHOST_RUN}" has its steps key but no record`],
  },
  {
    name: "a schedule whose next firing disagrees with the schedules set",
    corrupt: (redis, keys) => redis.zAdd(keys.schedules, { score: 1, value: "rebuild-index" }),
    expectedViolationsOf: (keys) => [
      `task "rebuild-index" has score 1 in ${keys.schedules} but next firing 60000 in ${keys.schedule("rebuild-index")}`,
    ],
  },
  {
    name: "a schedules member without a schedule record",
    corrupt: (redis, keys) => redis.zAdd(keys.schedules, { score: 1, value: "send-email" }),
    expectedViolationsOf: (keys) => [
      `task "send-email" has score 1 in ${keys.schedules} but next firing null in ${keys.schedule("send-email")}`,
    ],
  },
  {
    name: "a schedule record missing from the schedules set",
    corrupt: (redis, keys) => redis.zRem(keys.schedules, "rebuild-index"),
    expectedViolationsOf: (keys) => [`${keys.schedule("rebuild-index")} is missing from ${keys.schedules}`],
  },
  {
    name: "an index key holding another type",
    corrupt: (redis, keys) => redis.set(keys.runIndex("send-email", "completed"), "not a sorted set"),
    expectedViolationsOf: (keys) => [
      `${keys.runIndex("send-email", "completed")} is a string, not a zset`,
      `run "${COMPLETED_RUN}" is completed but missing from ${keys.runIndex("send-email", "completed")}`,
    ],
  },
];

for (const { name, corrupt, expectedViolationsOf } of CORRUPTIONS) {
  integrationTest(
    `the invariant checker reports ${name}`,
    async ({ redisUrl, keyPrefix, expectInvariantViolation }) => {
      await withRedis(redisUrl, async (redis) => {
        const keys = engineKeysOf(keyPrefix);
        for (const expectedViolation of expectedViolationsOf(keys)) {
          expectInvariantViolation("stored keys", expectedViolation);
        }
        await writeConsistentRuns(redis, keys);
        await corrupt(redis, keys);

        const violations = await storedKeyViolationsOf(redis, keyPrefix);

        expect([...violations].sort()).toEqual([...expectedViolationsOf(keys)].sort());
      });
    },
  );
}

integrationTest(
  "a run executing under a lock that has not lapsed is a run left executing, and one whose lock lapsed is not",
  async ({ redisUrl, keyPrefix }) => {
    await withRedis(redisUrl, async (redis) => {
      const keys = engineKeysOf(keyPrefix);
      await writeConsistentRuns(redis, keys, {
        lockedUntil: (await serverInstantInMillisecondsOf(redis)) + 60_000,
      });

      const runsLeftExecuting = runsLeftExecutingIn(await storedKeysSnapshotOf(redis, keyPrefix), keyPrefix);
      await redis.del(keys.runRecord("rebuild-index", "executing-run"));
      await writeConsistentRuns(redis, keys);
      const runsLeftExecutingOnceLapsed = runsLeftExecutingIn(await storedKeysSnapshotOf(redis, keyPrefix), keyPrefix);

      expect(runsLeftExecuting).toEqual([
        'run "rebuild-index:executing-run" is still executing under a lock that has not lapsed',
      ]);
      expect(runsLeftExecutingOnceLapsed).toEqual([]);
    });
  },
);
