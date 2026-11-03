import { describe, expect, test } from "vitest";

import { handBackStrandedJobs } from "./stranded-jobs.js";
import type { ActiveJob } from "./stranded-jobs.js";
import { createRecordingLogger } from "../tests/recording-logger.js";

const WORKER_ID = "worker-1";

const RESTORED_AT = 1_700_000_060_000;

const BEFORE_THE_RECONNECT = RESTORED_AT - 5_000;

const IN_HAND_JOB_ID = "job-in-hand";

async function handBackStrandedJobsAmong(
  activeJobs: ActiveJob[],
  moveActiveJobToWait: (jobId: string, lockToken: string) => Promise<void> = async () => undefined,
) {
  const { logger, lines } = createRecordingLogger();
  const movesToWait: { jobId: string; lockToken: string }[] = [];
  await handBackStrandedJobs({
    workerId: WORKER_ID,
    restoredAt: RESTORED_AT,
    isJobInHand: (jobId) => jobId === IN_HAND_JOB_ID,
    readActiveJobs: async () => activeJobs,
    moveActiveJobToWait: async (jobId, lockToken) => {
      movesToWait.push({ jobId, lockToken });
      await moveActiveJobToWait(jobId, lockToken);
    },
    logger,
  });
  return { movesToWait, lines };
}

describe("handBackStrandedJobs", () => {
  test("moves back to wait, under its own lock, an active job this worker's lock holds that no attempt of it runs, started before the reconnect", async () => {
    const { movesToWait, lines } = await handBackStrandedJobsAmong([
      { jobId: "job-7", lockToken: `${WORKER_ID}:3`, processedOn: BEFORE_THE_RECONNECT },
    ]);

    expect(movesToWait).toEqual([{ jobId: "job-7", lockToken: `${WORKER_ID}:3` }]);
    expect(lines).toEqual([]);
  });

  test.each([
    ["locked by another worker", { jobId: "job-7", lockToken: "worker-2:3", processedOn: BEFORE_THE_RECONNECT }],
    [
      "locked by a worker whose id this worker's id prefixes",
      { jobId: "job-7", lockToken: `${WORKER_ID}0:3`, processedOn: BEFORE_THE_RECONNECT },
    ],
    ["whose lock is gone", { jobId: "job-7", lockToken: undefined, processedOn: BEFORE_THE_RECONNECT }],
    ["in hand", { jobId: IN_HAND_JOB_ID, lockToken: `${WORKER_ID}:3`, processedOn: BEFORE_THE_RECONNECT }],
    ["started at the reconnect", { jobId: "job-7", lockToken: `${WORKER_ID}:3`, processedOn: RESTORED_AT }],
    ["started after the reconnect", { jobId: "job-7", lockToken: `${WORKER_ID}:3`, processedOn: RESTORED_AT + 1 }],
    ["with no start time", { jobId: "job-7", lockToken: `${WORKER_ID}:3`, processedOn: undefined }],
  ])("leaves alone an active job %s", async (_case, activeJob) => {
    const { movesToWait, lines } = await handBackStrandedJobsAmong([activeJob]);

    expect(movesToWait).toEqual([]);
    expect(lines).toEqual([]);
  });

  test("moves every stranded job among the active ones, and only those", async () => {
    const { movesToWait } = await handBackStrandedJobsAmong([
      { jobId: "job-7", lockToken: `${WORKER_ID}:0`, processedOn: BEFORE_THE_RECONNECT },
      { jobId: IN_HAND_JOB_ID, lockToken: `${WORKER_ID}:0`, processedOn: BEFORE_THE_RECONNECT },
      { jobId: "job-8", lockToken: "worker-2:0", processedOn: BEFORE_THE_RECONNECT },
      { jobId: "job-9", lockToken: `${WORKER_ID}:4`, processedOn: BEFORE_THE_RECONNECT },
    ]);

    expect(movesToWait).toEqual([
      { jobId: "job-7", lockToken: `${WORKER_ID}:0` },
      { jobId: "job-9", lockToken: `${WORKER_ID}:4` },
    ]);
  });

  test("writes one next job hand-back failed warning per move that fails, and still moves the others", async () => {
    const lockTakenError = new Error("Lock mismatch for job job-7. Cmd moveJobFromActiveToWait from active");

    const { movesToWait, lines } = await handBackStrandedJobsAmong(
      [
        { jobId: "job-7", lockToken: `${WORKER_ID}:0`, processedOn: BEFORE_THE_RECONNECT },
        { jobId: "job-9", lockToken: `${WORKER_ID}:4`, processedOn: BEFORE_THE_RECONNECT },
      ],
      async (jobId) => {
        if (jobId === "job-7") {
          throw lockTakenError;
        }
      },
    );

    expect(movesToWait.map((move) => move.jobId)).toEqual(["job-7", "job-9"]);
    expect(lines).toEqual([
      { level: "warn", fields: { jobId: "job-7", err: lockTakenError }, message: "next job hand-back failed" },
    ]);
  });

  test("writes one next job hand-back failed warning, and moves nothing, when the active jobs can't be read", async () => {
    const { logger, lines } = createRecordingLogger();
    const readError = new Error("ERR unknown command");
    const movedJobIds: string[] = [];

    await handBackStrandedJobs({
      workerId: WORKER_ID,
      restoredAt: RESTORED_AT,
      isJobInHand: () => false,
      readActiveJobs: async () => {
        throw readError;
      },
      moveActiveJobToWait: async (jobId) => {
        movedJobIds.push(jobId);
      },
      logger,
    });

    expect(movedJobIds).toEqual([]);
    expect(lines).toEqual([{ level: "warn", fields: { err: readError }, message: "next job hand-back failed" }]);
  });
});
