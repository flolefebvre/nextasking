import { expect } from "vitest";

import { engineTest, fetchedLockedRunsOf } from "../../tests/engine-test.js";
import type { RunToCreate } from "./engine.js";

const ONE_HOUR_IN_MILLISECONDS = 3_600_000;

function runOf(runId: string, task: string, extraFields: Partial<RunToCreate> = {}): RunToCreate {
  return { runId, task, payload: "{}", retries: 3, ...extraFields };
}

engineTest("busyRunCounts of an empty prefix counts nothing", async ({ engine }) => {
  const reply = await engine.busyRunCounts();

  expect(reply).toEqual({
    pendingRunCount: 0,
    executingRunCount: 0,
    waitingForConcurrencySlotRunCount: 0,
    dueRunCount: 0,
  });
});

engineTest(
  "busyRunCounts counts every task's pending, executing and waiting runs, and the runs due by the server's clock",
  async (context) => {
    const { engine } = context;
    await engine.enqueue({ runs: [runOf("retried-run", "send-email")] });
    const [retriedRun] = await fetchedLockedRunsOf(context, 1);
    await engine.settle({
      ...retriedRun!,
      outcome: {
        kind: "retry",
        error: { attempt: 1, name: "Error", message: "boom", stack: "", failedAt: "" },
        backoffInMilliseconds: 0,
      },
    });
    await engine.enqueue({ runs: [runOf("resize-1", "resize-image"), runOf("resize-2", "resize-image")] });
    await fetchedLockedRunsOf(context, 2, { "resize-image": 1 });
    await engine.enqueue({
      runs: [
        runOf("pending-email-1", "send-email"),
        runOf("pending-report", "build-report"),
        runOf("delayed-run", "send-email", { delayInMilliseconds: ONE_HOUR_IN_MILLISECONDS }),
      ],
    });

    const reply = await engine.busyRunCounts();

    expect(reply).toEqual({
      pendingRunCount: 2,
      executingRunCount: 1,
      waitingForConcurrencySlotRunCount: 1,
      dueRunCount: 1,
    });
  },
);
