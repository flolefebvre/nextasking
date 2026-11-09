import { onTestFinished } from "vitest";

import { engineKeysOf, storedRunIdOf } from "../src/engine/engine-keys.js";
import { integrationTest } from "./integration-test.js";
import { freshLockTokensOf } from "./lock-tokens.js";
import { createRecordingLogger } from "./recording-logger.js";
import { serverInstantInMillisecondsOf } from "./server-instant.js";
import { testFailingWith } from "./test-failing-with.js";
import { withRedis } from "./with-redis.js";

const UNDECLARED = "The test ended with invariant violations it did not declare:";

const NEVER_HAPPENED = "The test declared invariant violations that never happened:";

const GHOST_RUN = storedRunIdOf("send-email", "ghost-run");

function ghostRunViolationOf(keyPrefix: string): string {
  return `run "send-email:ghost-run" sits in {${keyPrefix}}:pending but has no record with a known state`;
}

async function writeGhostRun(redisUrl: string, keyPrefix: string): Promise<void> {
  await withRedis(redisUrl, (redis) => redis.zadd(engineKeysOf(keyPrefix).pending, 7, GHOST_RUN));
}

function runLeftExecutingViolation(): string {
  return 'run "rebuild-index:executing-run" is still executing under a lock that has not lapsed';
}

async function writeRunExecutingUnderALiveLock(
  redisUrl: string,
  keyPrefix: string,
  lockToken = "token-of-a-worker",
): Promise<void> {
  const keys = engineKeysOf(keyPrefix);
  const executingRun = storedRunIdOf("rebuild-index", "executing-run");
  await withRedis(redisUrl, async (redis) => {
    const lockedUntil = (await serverInstantInMillisecondsOf(redis)) + 60_000;
    await redis
      .multi()
      .sadd(keys.tasks, "rebuild-index")
      .hset(keys.runRecord("rebuild-index", "executing-run"), {
        state: "executing",
        task: "rebuild-index",
        startedAt: 3000,
        lockedUntil,
        lockToken,
      })
      .zadd(keys.locks, lockedUntil, executingRun)
      .zadd(keys.runIndex("rebuild-index", "executing"), 3000, executingRun)
      .sadd(keys.slotSet("rebuild-index", undefined), executingRun)
      .exec();
  });
}

integrationTest(
  "the invariants are checked after the closers the body registers with onTestFinished",
  async ({ redisUrl, keyPrefix }) => {
    await writeGhostRun(redisUrl, keyPrefix);

    onTestFinished(() =>
      withRedis(redisUrl, async (redis) => {
        await redis.zrem(engineKeysOf(keyPrefix).pending, GHOST_RUN);
      }),
    );
  },
);

testFailingWith(
  integrationTest,
  "a test leaving an index member without a record fails at its end with the checker's list",
  ({ keyPrefix }) => [`${UNDECLARED}\n  stored keys: ${ghostRunViolationOf(keyPrefix)}`],
  async ({ redisUrl, keyPrefix }) => {
    await writeGhostRun(redisUrl, keyPrefix);
  },
);

integrationTest(
  "a test leaving an index member without a record passes when it declares the checker's sentence",
  async ({ redisUrl, keyPrefix, expectInvariantViolation }) => {
    expectInvariantViolation("stored keys", ghostRunViolationOf(keyPrefix));

    await writeGhostRun(redisUrl, keyPrefix);
  },
);

testFailingWith(
  integrationTest,
  "a test declaring a checker sentence that never happens fails",
  ({ keyPrefix }) => [`${NEVER_HAPPENED}\n  stored keys: ${ghostRunViolationOf(keyPrefix)}`],
  async ({ keyPrefix, expectInvariantViolation }) => {
    expectInvariantViolation("stored keys", ghostRunViolationOf(keyPrefix));
  },
);

testFailingWith(
  integrationTest,
  "a test leaving a run executing under a lock that has not lapsed fails naming the run",
  () => [`${UNDECLARED}\n  run left executing: ${runLeftExecutingViolation()}`],
  async ({ redisUrl, keyPrefix }) => {
    await writeRunExecutingUnderALiveLock(redisUrl, keyPrefix);
  },
);

integrationTest(
  "a test leaving a run executing under a lock passes when it declares the run left executing",
  async ({ redisUrl, keyPrefix, expectInvariantViolation }) => {
    expectInvariantViolation("run left executing", runLeftExecutingViolation());

    await writeRunExecutingUnderALiveLock(redisUrl, keyPrefix);
  },
);

integrationTest(
  "a run executing under a lock the test made itself is not a run left executing",
  async ({ redisUrl, keyPrefix }) => {
    const [lockToken] = freshLockTokensOf(1);

    await writeRunExecutingUnderALiveLock(redisUrl, keyPrefix, lockToken);
  },
);

testFailingWith(
  integrationTest,
  "a test declaring a run left executing that never happens fails",
  () => [`${NEVER_HAPPENED}\n  run left executing: ${runLeftExecutingViolation()}`],
  async ({ expectInvariantViolation }) => {
    expectInvariantViolation("run left executing", runLeftExecutingViolation());
  },
);

function rejectWithNoHandler(): void {
  void Promise.reject(new Error("rejected on purpose"));
}

testFailingWith(
  integrationTest,
  "a test leaving a promise rejected with no handler fails naming the rejection",
  () => [`${UNDECLARED}\n  unhandled rejection: rejected on purpose`],
  async () => {
    rejectWithNoHandler();
  },
);

integrationTest(
  "a test leaving a promise rejected with no handler passes when it declares the rejection",
  async ({ expectInvariantViolation }) => {
    expectInvariantViolation("unhandled rejection", "rejected on purpose");

    rejectWithNoHandler();
  },
);

testFailingWith(
  integrationTest,
  "a test declaring an unhandled rejection that never happens fails",
  () => [`${NEVER_HAPPENED}\n  unhandled rejection: rejected on purpose`],
  async ({ expectInvariantViolation }) => {
    expectInvariantViolation("unhandled rejection", "rejected on purpose");
  },
);

testFailingWith(
  integrationTest,
  "a test emitting a process warning fails naming the warning",
  () => [`${UNDECLARED}\n  process warning: Warning: warned on purpose`],
  async () => {
    process.emitWarning("warned on purpose");
  },
);

integrationTest(
  "a test emitting a process warning passes when it declares the warning",
  async ({ expectInvariantViolation }) => {
    expectInvariantViolation("process warning", "Warning: warned on purpose");

    process.emitWarning("warned on purpose");
  },
);

testFailingWith(
  integrationTest,
  "a test declaring a process warning that never happens fails",
  () => [`${NEVER_HAPPENED}\n  process warning: Warning: warned on purpose`],
  async ({ expectInvariantViolation }) => {
    expectInvariantViolation("process warning", "Warning: warned on purpose");
  },
);

function logWorkerError(): void {
  const { logger } = createRecordingLogger();
  logger.error({ err: new Error("failed on purpose") }, "worker error");
}

testFailingWith(
  integrationTest,
  "a test whose recording logger receives a worker error fails naming the error",
  () => [`${UNDECLARED}\n  worker error: failed on purpose`],
  async () => {
    logWorkerError();
  },
);

integrationTest(
  "a test whose recording logger receives a worker error passes when it declares the error",
  async ({ expectInvariantViolation }) => {
    expectInvariantViolation("worker error", "failed on purpose");

    logWorkerError();
  },
);

testFailingWith(
  integrationTest,
  "a test declaring a worker error that never happens fails",
  () => [`${NEVER_HAPPENED}\n  worker error: failed on purpose`],
  async ({ expectInvariantViolation }) => {
    expectInvariantViolation("worker error", "failed on purpose");
  },
);
