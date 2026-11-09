import { expect, vi } from "vitest";

import { fixtureRecordsOf } from "./fixture-records.js";
import { integrationTest } from "./integration-test.js";
import type { FailingTestContext } from "./test-failing-with.js";
import { testFailingWith } from "./test-failing-with.js";
import { startWorkerProcess } from "./worker-process.js";
import { createWorkerProcessNextasking } from "./worker-process-nextasking.js";

const UNDECLARED = "The test ended with invariant violations it did not declare:";

const NEVER_HAPPENED = "The test declared invariant violations that never happened:";

const WORKER_PROCESS_WARNING = "Warning: warned in the worker process";

const WORKER_PROCESS_REJECTION = "rejected in the worker process";

const SLOW_CLOSE = "a worker process took longer than 3000 ms to exit after SIGTERM";

const SLOW_CLOSE_TIMEOUT_IN_MILLISECONDS = 30_000;

type WorkerProcessNextasking = ReturnType<typeof createWorkerProcessNextasking>;

async function dispatchToAWorkerProcessUntilItsOutputShows(
  { redisUrl, keyPrefix }: FailingTestContext,
  dispatch: (nextasking: WorkerProcessNextasking) => Promise<unknown>,
  expectedOutput: string,
): Promise<void> {
  const workerProcess = await startWorkerProcess({ redisUrl, keyPrefix });
  const nextasking = createWorkerProcessNextasking(redisUrl, keyPrefix);
  try {
    await dispatch(nextasking);
    await vi.waitFor(() => expect(workerProcess.output()).toContain(expectedOutput), { timeout: 10_000 });
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
    startWorkerOptions: { shutdownTimeout: "1s" },
  });
  const nextasking = createWorkerProcessNextasking(redisUrl, keyPrefix);
  try {
    if (isBlockedWhenTerminated) {
      await nextasking.dispatch("block-event-loop", { blockInMilliseconds: 5000 });
      await vi.waitFor(
        async () => expect(await fixtureRecordsOf(redisUrl, keyPrefix, "block-event-loop")).toHaveLength(1),
        { timeout: 10_000 },
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
);

integrationTest(
  "a process warning in a worker process passes the test that declares it",
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
);

testFailingWith(
  integrationTest,
  "a promise rejected with no handler in a worker process fails the test naming the rejection",
  () => [`${UNDECLARED}\n  unhandled rejection: ${WORKER_PROCESS_REJECTION}`],
  rejectWithNoHandlerInAWorkerProcess,
);

integrationTest(
  "a promise rejected with no handler in a worker process passes the test that declares it",
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
);

testFailingWith(
  integrationTest,
  "a worker process exiting later than its shutdown timeout plus 2 s after SIGTERM fails the test",
  () => [`${UNDECLARED}\n  slow close: ${SLOW_CLOSE}`],
  (context) => terminateAWorkerProcess(context, { isBlockedWhenTerminated: true }),
  { timeout: SLOW_CLOSE_TIMEOUT_IN_MILLISECONDS },
);

integrationTest(
  "a worker process exiting later than its shutdown timeout plus 2 s after SIGTERM passes the test that declares it",
  { timeout: SLOW_CLOSE_TIMEOUT_IN_MILLISECONDS },
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
);
