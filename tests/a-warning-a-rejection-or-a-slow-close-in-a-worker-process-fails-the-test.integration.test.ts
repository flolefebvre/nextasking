import { expect, vi } from "vitest";

import { parseDuration } from "../src/durations.js";
import type { Duration } from "../src/durations.js";
import type { DispatchedRun } from "../src/index.js";
import { fixtureRecordsOf } from "./fixture-records.js";
import { integrationTest } from "./integration-test.js";
import { CLOSE_BOUND_PAST_THE_SHUTDOWN_TIMEOUT_IN_MILLISECONDS } from "./invariant-violations.js";
import type { FailingTestContext } from "./test-failing-with.js";
import { testFailingWith } from "./test-failing-with.js";
import { WORKER_PROCESS_READY_TIMEOUT_IN_MILLISECONDS, startWorkerProcess } from "./worker-process.js";
import { createWorkerProcessNextasking } from "./worker-process-nextasking.js";

const UNDECLARED = "The test ended with invariant violations it did not declare:";

const NEVER_HAPPENED = "The test declared invariant violations that never happened:";

const WORKER_PROCESS_WARNING = "Warning: warned in the worker process";

const WORKER_PROCESS_REJECTION = "rejected in the worker process";

const SLOW_CLOSE = "a worker process took longer than 3000 ms to exit after SIGTERM";

const TEST_TIMEOUT_PAST_ITS_WAITS_IN_MILLISECONDS = 5_000;

const DISPATCHED_RUN_OUTCOME_TIMEOUT_IN_MILLISECONDS = 10_000;

const BLOCK_EVENT_LOOP_RECORD_TIMEOUT_IN_MILLISECONDS = 10_000;

const EVENT_LOOP_BLOCK_IN_MILLISECONDS = 5_000;

const TERMINATED_SHUTDOWN_TIMEOUT: Duration = "1s";

const TERMINATED_CLOSE_BOUND_IN_MILLISECONDS =
  parseDuration(TERMINATED_SHUTDOWN_TIMEOUT) + CLOSE_BOUND_PAST_THE_SHUTDOWN_TIMEOUT_IN_MILLISECONDS;

const DISPATCH_SCENARIO_TIMEOUT_IN_MILLISECONDS =
  WORKER_PROCESS_READY_TIMEOUT_IN_MILLISECONDS +
  DISPATCHED_RUN_OUTCOME_TIMEOUT_IN_MILLISECONDS +
  TEST_TIMEOUT_PAST_ITS_WAITS_IN_MILLISECONDS;

const TERMINATE_SCENARIO_TIMEOUT_IN_MILLISECONDS =
  WORKER_PROCESS_READY_TIMEOUT_IN_MILLISECONDS +
  TERMINATED_CLOSE_BOUND_IN_MILLISECONDS +
  TEST_TIMEOUT_PAST_ITS_WAITS_IN_MILLISECONDS;

const BLOCKED_TERMINATE_SCENARIO_TIMEOUT_IN_MILLISECONDS =
  WORKER_PROCESS_READY_TIMEOUT_IN_MILLISECONDS +
  BLOCK_EVENT_LOOP_RECORD_TIMEOUT_IN_MILLISECONDS +
  EVENT_LOOP_BLOCK_IN_MILLISECONDS +
  TERMINATED_CLOSE_BOUND_IN_MILLISECONDS +
  TEST_TIMEOUT_PAST_ITS_WAITS_IN_MILLISECONDS;

type WorkerProcessNextasking = ReturnType<typeof createWorkerProcessNextasking>;

async function dispatchToAWorkerProcessUntilItsOutputShows(
  { redisUrl, keyPrefix }: FailingTestContext,
  dispatch: (nextasking: WorkerProcessNextasking) => Promise<DispatchedRun>,
  expectedOutput: string,
): Promise<void> {
  const workerProcess = await startWorkerProcess({ redisUrl, keyPrefix });
  const nextasking = createWorkerProcessNextasking(redisUrl, keyPrefix);
  try {
    const { runId } = await dispatch(nextasking);
    await vi.waitFor(
      () => {
        expect(workerProcess.output()).toContain(expectedOutput);
        expect(
          workerProcess.logLines.some((line) => line.message === "run completed" && line.fields["runId"] === runId),
        ).toBe(true);
      },
      { timeout: DISPATCHED_RUN_OUTCOME_TIMEOUT_IN_MILLISECONDS },
    );
  } finally {
    await workerProcess.stop();
    await nextasking.close();
  }
}

function emitAWarningInAWorkerProcess(context: FailingTestContext): Promise<void> {
  return dispatchToAWorkerProcessUntilItsOutputShows(
    context,
    (nextasking) => nextasking.dispatch("emit-process-warning", { warning: "warned in the worker process" }),
    WORKER_PROCESS_WARNING,
  );
}

function rejectWithNoHandlerInAWorkerProcess(context: FailingTestContext): Promise<void> {
  return dispatchToAWorkerProcessUntilItsOutputShows(
    context,
    (nextasking) => nextasking.dispatch("reject-with-no-handler", { reason: WORKER_PROCESS_REJECTION }),
    `unhandledRejection: Error: ${WORKER_PROCESS_REJECTION}`,
  );
}

async function terminateAWorkerProcess(
  { redisUrl, keyPrefix }: FailingTestContext,
  { isBlockedWhenTerminated }: { isBlockedWhenTerminated: boolean },
): Promise<void> {
  const workerProcess = await startWorkerProcess({
    redisUrl,
    keyPrefix,
    startWorkerOptions: { shutdownTimeout: TERMINATED_SHUTDOWN_TIMEOUT },
  });
  const nextasking = createWorkerProcessNextasking(redisUrl, keyPrefix);
  try {
    if (isBlockedWhenTerminated) {
      await nextasking.dispatch("block-event-loop", { blockInMilliseconds: EVENT_LOOP_BLOCK_IN_MILLISECONDS });
      await vi.waitFor(
        async () => expect(await fixtureRecordsOf(redisUrl, keyPrefix, "block-event-loop")).toHaveLength(1),
        { timeout: BLOCK_EVENT_LOOP_RECORD_TIMEOUT_IN_MILLISECONDS },
      );
    }
    workerProcess.kill("SIGTERM");
    await workerProcess.exited;
  } finally {
    await workerProcess.stop();
    await nextasking.close();
  }
}

testFailingWith(
  integrationTest,
  "a process warning in a worker process fails the test naming it",
  () => [`${UNDECLARED}\n  process warning: ${WORKER_PROCESS_WARNING}`],
  emitAWarningInAWorkerProcess,
  { timeout: DISPATCH_SCENARIO_TIMEOUT_IN_MILLISECONDS },
);

integrationTest(
  "a process warning in a worker process passes the test that declares it",
  { timeout: DISPATCH_SCENARIO_TIMEOUT_IN_MILLISECONDS },
  async ({ redisUrl, keyPrefix, expectInvariantViolation }) => {
    expectInvariantViolation("process warning", WORKER_PROCESS_WARNING);

    await emitAWarningInAWorkerProcess({ redisUrl, keyPrefix, expectInvariantViolation });
  },
);

testFailingWith(
  integrationTest,
  "a test declaring a process warning a worker process never emits fails",
  () => [`${NEVER_HAPPENED}\n  process warning: ${WORKER_PROCESS_WARNING}`],
  async (context) => {
    context.expectInvariantViolation("process warning", WORKER_PROCESS_WARNING);

    await terminateAWorkerProcess(context, { isBlockedWhenTerminated: false });
  },
  { timeout: TERMINATE_SCENARIO_TIMEOUT_IN_MILLISECONDS },
);

testFailingWith(
  integrationTest,
  "a promise rejected with no handler in a worker process fails the test naming the rejection",
  () => [`${UNDECLARED}\n  unhandled rejection: ${WORKER_PROCESS_REJECTION}`],
  rejectWithNoHandlerInAWorkerProcess,
  { timeout: DISPATCH_SCENARIO_TIMEOUT_IN_MILLISECONDS },
);

integrationTest(
  "a promise rejected with no handler in a worker process passes the test that declares it",
  { timeout: DISPATCH_SCENARIO_TIMEOUT_IN_MILLISECONDS },
  async ({ redisUrl, keyPrefix, expectInvariantViolation }) => {
    expectInvariantViolation("unhandled rejection", WORKER_PROCESS_REJECTION);

    await rejectWithNoHandlerInAWorkerProcess({ redisUrl, keyPrefix, expectInvariantViolation });
  },
);

testFailingWith(
  integrationTest,
  "a test declaring an unhandled rejection a worker process never has fails",
  () => [`${NEVER_HAPPENED}\n  unhandled rejection: ${WORKER_PROCESS_REJECTION}`],
  async (context) => {
    context.expectInvariantViolation("unhandled rejection", WORKER_PROCESS_REJECTION);

    await terminateAWorkerProcess(context, { isBlockedWhenTerminated: false });
  },
  { timeout: TERMINATE_SCENARIO_TIMEOUT_IN_MILLISECONDS },
);

testFailingWith(
  integrationTest,
  "a worker process exiting later than its shutdown timeout plus 2 s after SIGTERM fails the test",
  () => [`${UNDECLARED}\n  slow close: ${SLOW_CLOSE}`],
  (context) => terminateAWorkerProcess(context, { isBlockedWhenTerminated: true }),
  { timeout: BLOCKED_TERMINATE_SCENARIO_TIMEOUT_IN_MILLISECONDS },
);

integrationTest(
  "a worker process exiting later than its shutdown timeout plus 2 s after SIGTERM passes the test that declares it",
  { timeout: BLOCKED_TERMINATE_SCENARIO_TIMEOUT_IN_MILLISECONDS },
  async ({ redisUrl, keyPrefix, expectInvariantViolation }) => {
    expectInvariantViolation("slow close", SLOW_CLOSE);

    await terminateAWorkerProcess({ redisUrl, keyPrefix, expectInvariantViolation }, { isBlockedWhenTerminated: true });
  },
);

testFailingWith(
  integrationTest,
  "a test declaring a slow close of a worker process that exits in time fails",
  () => [`${NEVER_HAPPENED}\n  slow close: ${SLOW_CLOSE}`],
  async (context) => {
    context.expectInvariantViolation("slow close", SLOW_CLOSE);

    await terminateAWorkerProcess(context, { isBlockedWhenTerminated: false });
  },
  { timeout: TERMINATE_SCENARIO_TIMEOUT_IN_MILLISECONDS },
);
