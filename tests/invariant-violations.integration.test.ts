import type { Redis } from "ioredis";
import { expect } from "vitest";

import { engineKeysOf, storedRunIdOf } from "../src/engine/engine-keys.js";
import type { EngineKeys } from "../src/engine/engine-keys.js";
import { integrationTest } from "./integration-test.js";
import { expectNoInvariantViolations, invariantViolationsOf } from "./invariant-violations.js";
import { withRedis } from "./with-redis.js";

const PENDING_RUN = storedRunIdOf("send-email", "pending-run");
const DELAYED_RUN = storedRunIdOf("send-email", "delayed-run");
const EXECUTING_RUN = storedRunIdOf("rebuild-index", "executing-run");
const PARKED_RUN = storedRunIdOf("rebuild-index", "parked-run");
const COMPLETED_RUN = storedRunIdOf("send-email", "completed-run");
const GHOST_RUN = storedRunIdOf("send-email", "ghost-run");

async function writeConsistentRuns(redis: Redis, keys: EngineKeys): Promise<void> {
  await redis
    .multi()
    .sadd(keys.tasks, "send-email", "rebuild-index")
    .hset(keys.runRecord("send-email", "pending-run"), { state: "pending", task: "send-email", order: 1 })
    .zadd(keys.pending, 1, PENDING_RUN)
    .zadd(keys.runIndex("send-email", "pending"), 1, PENDING_RUN)
    .hset(keys.runRecord("send-email", "delayed-run"), { state: "delayed", task: "send-email", startsAt: 5000 })
    .zadd(keys.due, 5000, DELAYED_RUN)
    .zadd(keys.runIndex("send-email", "delayed"), 5000, DELAYED_RUN)
    .hset(keys.runRecord("rebuild-index", "executing-run"), {
      state: "executing",
      task: "rebuild-index",
      concurrencyKey: "account-7",
      startedAt: 3000,
      lockedUntil: 9000,
    })
    .zadd(keys.locks, 9000, EXECUTING_RUN)
    .zadd(keys.runIndex("rebuild-index", "executing"), 3000, EXECUTING_RUN)
    .sadd(keys.slotSet("rebuild-index", "account-7"), EXECUTING_RUN)
    .hset(keys.runRecord("rebuild-index", "parked-run"), {
      state: "waitingForConcurrencySlot",
      task: "rebuild-index",
      concurrencyKey: "account-7",
      order: 2,
    })
    .zadd(keys.waitingList("rebuild-index", "account-7"), 2, PARKED_RUN)
    .zadd(keys.runIndex("rebuild-index", "waitingForConcurrencySlot"), 2, PARKED_RUN)
    .hset(keys.runRecord("send-email", "completed-run"), { state: "completed", task: "send-email", finishedAt: 4000 })
    .zadd(keys.runIndex("send-email", "completed"), 4000, COMPLETED_RUN)
    .hset(keys.schedule("rebuild-index"), { nextFiringAt: 60000 })
    .zadd(keys.schedules, 60000, "rebuild-index")
    .exec();
}

integrationTest("records and indexes that agree pass the invariant checker", async ({ redisUrl, keyPrefix }) => {
  await withRedis(redisUrl, async (redis) => {
    await writeConsistentRuns(redis, engineKeysOf(keyPrefix));

    const violations = await invariantViolationsOf(redis, keyPrefix);

    expect(violations).toEqual([]);
  });
});

const CORRUPTIONS = [
  {
    name: "an index member without a record",
    corrupt: (redis: Redis, keys: EngineKeys) => redis.zadd(keys.pending, 7, GHOST_RUN),
    expectedViolation: `run "${GHOST_RUN}" sits in`,
  },
  {
    name: "a read index score that disagrees with the record",
    corrupt: (redis: Redis, keys: EngineKeys) => redis.zadd(keys.runIndex("send-email", "pending"), 99, PENDING_RUN),
    expectedViolation: `run "${PENDING_RUN}" has score 99`,
  },
  {
    name: "an engine structure score that disagrees with the record",
    corrupt: (redis: Redis, keys: EngineKeys) => redis.zadd(keys.locks, 1, EXECUTING_RUN),
    expectedViolation: `run "${EXECUTING_RUN}" has score 1`,
  },
  {
    name: "a record left in the index of its former state",
    corrupt: (redis: Redis, keys: EngineKeys) =>
      redis.zadd(keys.runIndex("send-email", "delayed"), 4000, COMPLETED_RUN),
    expectedViolation: `run "${COMPLETED_RUN}" sits in`,
  },
  {
    name: "a record missing from the read index of its state",
    corrupt: (redis: Redis, keys: EngineKeys) => redis.zrem(keys.runIndex("send-email", "delayed"), DELAYED_RUN),
    expectedViolation: `run "${DELAYED_RUN}" is delayed but missing from`,
  },
  {
    name: "a record missing from the engine structure of its state",
    corrupt: (redis: Redis, keys: EngineKeys) => redis.zrem(keys.pending, PENDING_RUN),
    expectedViolation: `run "${PENDING_RUN}" is pending but missing from`,
  },
  {
    name: "a parked run in the waiting list of another key",
    corrupt: (redis: Redis, keys: EngineKeys) =>
      redis.zadd(keys.waitingList("rebuild-index", "account-8"), 2, PARKED_RUN),
    expectedViolation: `run "${PARKED_RUN}" sits in`,
  },
  {
    name: "a slot held by a run that is not executing",
    corrupt: (redis: Redis, keys: EngineKeys) => redis.sadd(keys.slotSet("rebuild-index", "account-7"), PARKED_RUN),
    expectedViolation: `run "${PARKED_RUN}" holds a slot`,
  },
  {
    name: "a slot held in the slot set of another key",
    corrupt: (redis: Redis, keys: EngineKeys) => redis.sadd(keys.slotSet("rebuild-index", undefined), EXECUTING_RUN),
    expectedViolation: `run "${EXECUTING_RUN}" holds a slot`,
  },
  {
    name: "an executing run missing from the slot set of its key",
    corrupt: (redis: Redis, keys: EngineKeys) => redis.srem(keys.slotSet("rebuild-index", "account-7"), EXECUTING_RUN),
    expectedViolation: `run "${EXECUTING_RUN}" is executing but holds no slot in`,
  },
  {
    name: "a record of a task missing from the tasks set",
    corrupt: (redis: Redis, keys: EngineKeys) => redis.srem(keys.tasks, "send-email"),
    expectedViolation: `is of task "send-email", missing from`,
  },
  {
    name: "a payload left without its record",
    corrupt: (redis: Redis, keys: EngineKeys) => redis.set(keys.runPayload("send-email", "ghost-run"), "{}"),
    expectedViolation: `run "${GHOST_RUN}" has its payload key but no record`,
  },
  {
    name: "errors left without their record",
    corrupt: (redis: Redis, keys: EngineKeys) => redis.rpush(keys.runErrors("send-email", "ghost-run"), "{}"),
    expectedViolation: `run "${GHOST_RUN}" has its errors key but no record`,
  },
  {
    name: "steps left without their record",
    corrupt: (redis: Redis, keys: EngineKeys) => redis.hset(keys.runSteps("send-email", "ghost-run"), "charge", "1"),
    expectedViolation: `run "${GHOST_RUN}" has its steps key but no record`,
  },
  {
    name: "a schedule whose next firing disagrees with the schedules set",
    corrupt: (redis: Redis, keys: EngineKeys) => redis.zadd(keys.schedules, 1, "rebuild-index"),
    expectedViolation: `task "rebuild-index" has score 1`,
  },
  {
    name: "a schedules member without a schedule record",
    corrupt: (redis: Redis, keys: EngineKeys) => redis.zadd(keys.schedules, 1, "send-email"),
    expectedViolation: `task "send-email" has score 1`,
  },
  {
    name: "a schedule record missing from the schedules set",
    corrupt: (redis: Redis, keys: EngineKeys) => redis.zrem(keys.schedules, "rebuild-index"),
    expectedViolation: "is missing from",
  },
];

for (const { name, corrupt, expectedViolation } of CORRUPTIONS) {
  integrationTest(`the invariant checker reports ${name}`, async ({ redisUrl, keyPrefix }) => {
    await withRedis(redisUrl, async (redis) => {
      const keys = engineKeysOf(keyPrefix);
      await writeConsistentRuns(redis, keys);
      await corrupt(redis, keys);

      const violations = await invariantViolationsOf(redis, keyPrefix);

      expect(violations).not.toEqual([]);
      expect(violations.filter((violation) => !violation.includes(expectedViolation))).toEqual([]);
    });
  });
}

integrationTest(
  "expecting no invariant violation fails the test with the list of violations",
  async ({ redisUrl, keyPrefix }) => {
    await withRedis(redisUrl, async (redis) => {
      const keys = engineKeysOf(keyPrefix);
      await writeConsistentRuns(redis, keys);
      await redis.zadd(keys.pending, 7, GHOST_RUN);

      const expectation = expectNoInvariantViolations(redis, keyPrefix);

      await expect(expectation).rejects.toThrow(`run "${GHOST_RUN}" sits in ${keys.pending}`);
    });
  },
);
