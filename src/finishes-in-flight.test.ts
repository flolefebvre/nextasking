import { ErrorCode } from "bullmq";
import { describe, expect, test } from "vitest";

import {
  createFinishesInFlight,
  fanOutCompletionInFlight,
  firingCompletionInFlight,
  runCompletionInFlight,
  runFailureInFlight,
} from "./finishes-in-flight.js";
import type { FinishInFlight, JobOnServer } from "./finishes-in-flight.js";
import { listenerLoggingAnsweredErrors } from "./connection.js";
import { loggerWithFields } from "./logger.js";
import type { Logger } from "./logger.js";
import { createRecordingLogger } from "../tests/recording-logger.js";

const JOB_ID = "job-7";

const TOKEN = "worker-1:3";

const NEXT_JOB_ID = "job-8";

const PROCESSED_ON = 1_700_000_000_000;

const RUN_FIELDS = { task: "send-welcome-email", runId: "run-7", attempt: 1 };

const DURATION_FIELDS = { runDurationInMilliseconds: 40, executionDurationInMilliseconds: 25 };

function missingLockError(jobId: string): Error {
  return Object.assign(new Error(`Missing lock for job ${jobId}. moveToFinished`), { code: ErrorCode.JobLockNotExist });
}

function lockMismatchError(jobId: string): Error {
  return Object.assign(new Error(`Lock mismatch for job ${jobId}. Cmd moveToFinished from active`), {
    code: ErrorCode.JobLockMismatch,
  });
}

async function settlePendingConfirmations(): Promise<void> {
  await new Promise((resolve) => setImmediate(resolve));
}

type NextJobHandBack = {
  findActiveJobIdLockedBy?: (token: string) => Promise<string | undefined>;
  moveActiveJobToWait?: (jobId: string, token: string) => Promise<void>;
};

function finishesInFlightReading(
  readJobOnServer: (jobId: string) => Promise<JobOnServer | undefined>,
  {
    findActiveJobIdLockedBy = async () => undefined,
    moveActiveJobToWait = async () => undefined,
  }: NextJobHandBack = {},
) {
  const { logger, lines } = createRecordingLogger();
  const readJobIds: string[] = [];
  const savedStepsRemovals: string[] = [];
  const failureNoticeSends: string[] = [];
  const scannedTokens: string[] = [];
  const movesToWait: { jobId: string; token: string }[] = [];
  const finishesInFlight = createFinishesInFlight({
    readJobOnServer: async (jobId) => {
      readJobIds.push(jobId);
      return readJobOnServer(jobId);
    },
    findActiveJobIdLockedBy: async (token) => {
      scannedTokens.push(token);
      return findActiveJobIdLockedBy(token);
    },
    moveActiveJobToWait: async (jobId, token) => {
      movesToWait.push({ jobId, token });
      await moveActiveJobToWait(jobId, token);
    },
    logger,
    logWorkerError: listenerLoggingAnsweredErrors(logger, "worker error"),
  });
  const trackCompletionOfJob = (jobId: string, processedOn: number | undefined = PROCESSED_ON) => {
    finishesInFlight.trackFinish(
      jobId,
      runCompletionInFlight({
        processedOn,
        token: TOKEN,
        runLogger: loggerWithFields(logger, RUN_FIELDS),
        durationFields: DURATION_FIELDS,
        removeSavedSteps: async () => {
          savedStepsRemovals.push(jobId);
        },
      }),
    );
  };
  const trackFailureOfJob = (jobId: string) => {
    finishesInFlight.trackFinish(
      jobId,
      runFailureInFlight({
        processedOn: PROCESSED_ON,
        attemptsMade: ATTEMPTS_MADE_AT_ACTIVATION,
        token: TOKEN,
        runLogger: loggerWithFields(logger, RUN_FIELDS),
        sendFailureNotice: async () => {
          failureNoticeSends.push(jobId);
        },
      }),
    );
  };
  const trackFinishOfJob = (jobId: string, finishLoggingTo: (logger: Logger) => FinishInFlight) => {
    finishesInFlight.trackFinish(jobId, finishLoggingTo(logger));
  };
  return {
    finishesInFlight,
    trackCompletionOfJob,
    trackFailureOfJob,
    trackFinishOfJob,
    lines,
    readJobIds,
    savedStepsRemovals,
    failureNoticeSends,
    scannedTokens,
    movesToWait,
  };
}

const ATTEMPTS_MADE_AT_ACTIVATION = 0;

function failureLockError(jobId: string): Error {
  return Object.assign(new Error(`Missing lock for job ${jobId}. moveToDelayed`), { code: ErrorCode.JobLockNotExist });
}

const runCompletedLine = {
  level: "info",
  fields: { ...DURATION_FIELDS, ...RUN_FIELDS },
  message: "run completed",
};

describe("the finishes in flight", () => {
  test("remove the saved steps of a run completion and drop the second lock error that arrives after it is confirmed", async () => {
    const { finishesInFlight, trackCompletionOfJob, lines, readJobIds, savedStepsRemovals } = finishesInFlightReading(
      async () => ({ state: "completed", processedOn: PROCESSED_ON, attemptsMade: 1 }),
    );
    trackCompletionOfJob(JOB_ID);

    finishesInFlight.routeWorkerError(missingLockError(JOB_ID));
    await settlePendingConfirmations();
    finishesInFlight.routeWorkerError(missingLockError(JOB_ID));
    await settlePendingConfirmations();

    expect(lines).toEqual([runCompletedLine]);
    expect(readJobIds).toEqual([JOB_ID]);
    expect(savedStepsRemovals).toEqual([JOB_ID]);
  });

  test.each(["completed", "failed"] as const)(
    "log nothing for a job the worker reports %s without a finish in flight",
    (workerEvent) => {
      const { finishesInFlight, lines, savedStepsRemovals } = finishesInFlightReading(async () => undefined);

      finishesInFlight.recordFinishReported(JOB_ID, workerEvent);

      expect(lines).toEqual([]);
      expect(savedStepsRemovals).toEqual([]);
    },
  );

  test("confirm a lock mismatch against the activation tracked last for the job", async () => {
    const { finishesInFlight, trackCompletionOfJob, lines } = finishesInFlightReading(async () => ({
      state: "completed",
      processedOn: PROCESSED_ON + 5_000,
      attemptsMade: 1,
    }));
    trackCompletionOfJob(JOB_ID);
    trackCompletionOfJob(JOB_ID, PROCESSED_ON + 5_000);

    finishesInFlight.routeWorkerError(lockMismatchError(JOB_ID));
    finishesInFlight.routeWorkerError(lockMismatchError(JOB_ID));
    await settlePendingConfirmations();

    expect(lines).toEqual([runCompletedLine]);
  });

  test("log as a worker error, without reading the server, a lock error naming a job with no completion in flight", async () => {
    const { finishesInFlight, trackCompletionOfJob, lines, readJobIds } = finishesInFlightReading(async () => ({
      state: "completed",
      processedOn: PROCESSED_ON,
      attemptsMade: 1,
    }));
    trackCompletionOfJob(JOB_ID);
    const otherJobLockError = missingLockError("job-8");

    finishesInFlight.routeWorkerError(otherJobLockError);
    await settlePendingConfirmations();

    expect(lines).toEqual([{ level: "error", fields: { err: otherJobLockError }, message: "worker error" }]);
    expect(readJobIds).toEqual([]);
  });

  test("log as a worker error a lock error whose message names no job, and any other answered error", async () => {
    const { finishesInFlight, trackCompletionOfJob, lines, readJobIds } = finishesInFlightReading(async () => ({
      state: "completed",
      processedOn: PROCESSED_ON,
      attemptsMade: 1,
    }));
    trackCompletionOfJob(JOB_ID);
    const unparsedLockError = Object.assign(new Error(`lock gone for ${JOB_ID}`), { code: ErrorCode.JobLockNotExist });
    const lookalikeError = new Error(`Missing lock for job ${JOB_ID}. moveToFinished`);
    const scriptError = new Error("ERR Error running script");

    finishesInFlight.routeWorkerError(unparsedLockError);
    finishesInFlight.routeWorkerError(lookalikeError);
    finishesInFlight.routeWorkerError(scriptError);
    await settlePendingConfirmations();

    expect(lines).toEqual([
      { level: "error", fields: { err: unparsedLockError }, message: "worker error" },
      { level: "error", fields: { err: lookalikeError }, message: "worker error" },
      { level: "error", fields: { err: scriptError }, message: "worker error" },
    ]);
    expect(readJobIds).toEqual([]);
  });

  test("log nothing for a lost connection", () => {
    const { finishesInFlight, lines } = finishesInFlightReading(async () => undefined);

    finishesInFlight.routeWorkerError(Object.assign(new Error("read ECONNRESET"), { code: "ECONNRESET" }));

    expect(lines).toEqual([]);
  });

  test.each([
    {
      trackedFinish: "completion",
      workerEvent: "completed",
      expectedLinesBeforeTheLaterLockError: [runCompletedLine],
      expectedSavedStepsRemovals: [JOB_ID],
    },
    {
      trackedFinish: "completion",
      workerEvent: "failed",
      expectedLinesBeforeTheLaterLockError: [],
      expectedSavedStepsRemovals: [],
    },
    {
      trackedFinish: "failure",
      workerEvent: "failed",
      expectedLinesBeforeTheLaterLockError: [],
      expectedSavedStepsRemovals: [],
    },
  ] as const)(
    "release a $trackedFinish without reading the server when the worker reports the job $workerEvent, so a later lock error is a worker error",
    async ({ trackedFinish, workerEvent, expectedLinesBeforeTheLaterLockError, expectedSavedStepsRemovals }) => {
      const {
        finishesInFlight,
        trackCompletionOfJob,
        trackFailureOfJob,
        lines,
        readJobIds,
        savedStepsRemovals,
        failureNoticeSends,
      } = finishesInFlightReading(async () => undefined);
      if (trackedFinish === "completion") {
        trackCompletionOfJob(JOB_ID);
      } else {
        trackFailureOfJob(JOB_ID);
      }
      const laterLockError = missingLockError(JOB_ID);

      finishesInFlight.recordFinishReported(JOB_ID, workerEvent);
      finishesInFlight.routeWorkerError(laterLockError);
      await settlePendingConfirmations();

      expect(lines).toEqual([
        ...expectedLinesBeforeTheLaterLockError,
        { level: "error", fields: { err: laterLockError }, message: "worker error" },
      ]);
      expect(readJobIds).toEqual([]);
      expect(savedStepsRemovals).toEqual(expectedSavedStepsRemovals);
      expect(failureNoticeSends).toEqual([]);
    },
  );
});

describe("a run failure in flight", () => {
  test.each([
    { state: "failed", expectedFailureNoticeSends: [JOB_ID] },
    { state: "delayed", expectedFailureNoticeSends: [] },
    { state: "waiting", expectedFailureNoticeSends: [] },
  ])(
    "drops its lock error silently when the server holds the job $state by this activation's stored failure, and sends the failure notice only for a final failure",
    async ({ state, expectedFailureNoticeSends }) => {
      const { finishesInFlight, trackFailureOfJob, lines, readJobIds, failureNoticeSends } = finishesInFlightReading(
        async () => ({ state, processedOn: PROCESSED_ON, attemptsMade: ATTEMPTS_MADE_AT_ACTIVATION + 1 }),
      );
      trackFailureOfJob(JOB_ID);

      finishesInFlight.routeWorkerError(failureLockError(JOB_ID));
      await settlePendingConfirmations();

      expect(lines).toEqual([]);
      expect(readJobIds).toEqual([JOB_ID]);
      expect(failureNoticeSends).toEqual(expectedFailureNoticeSends);
    },
  );

  test("is forgotten after its one lock error, so a later lock error is a worker error", async () => {
    const { finishesInFlight, trackFailureOfJob, lines } = finishesInFlightReading(async () => ({
      state: "delayed",
      processedOn: PROCESSED_ON,
      attemptsMade: ATTEMPTS_MADE_AT_ACTIVATION + 1,
    }));
    trackFailureOfJob(JOB_ID);
    const laterLockError = failureLockError(JOB_ID);

    finishesInFlight.routeWorkerError(failureLockError(JOB_ID));
    finishesInFlight.routeWorkerError(laterLockError);
    await settlePendingConfirmations();

    expect(lines).toEqual([{ level: "error", fields: { err: laterLockError }, message: "worker error" }]);
  });

  test.each([
    {
      heldJob: "waiting after a stall, its attempts unchanged",
      jobOnServer: { state: "waiting", processedOn: PROCESSED_ON, attemptsMade: ATTEMPTS_MADE_AT_ACTIVATION },
    },
    {
      heldJob: "still active",
      jobOnServer: { state: "active", processedOn: PROCESSED_ON, attemptsMade: ATTEMPTS_MADE_AT_ACTIVATION },
    },
    {
      heldJob: "failed by a later activation",
      jobOnServer: {
        state: "failed",
        processedOn: PROCESSED_ON + 5_000,
        attemptsMade: ATTEMPTS_MADE_AT_ACTIVATION + 1,
      },
    },
    {
      heldJob: "completed",
      jobOnServer: { state: "completed", processedOn: PROCESSED_ON, attemptsMade: ATTEMPTS_MADE_AT_ACTIVATION + 1 },
    },
  ])("logs its lock error as a worker error when the server holds the job $heldJob", async ({ jobOnServer }) => {
    const { finishesInFlight, trackFailureOfJob, lines } = finishesInFlightReading(async () => jobOnServer);
    trackFailureOfJob(JOB_ID);
    const lockError = failureLockError(JOB_ID);

    finishesInFlight.routeWorkerError(lockError);
    await settlePendingConfirmations();

    expect(lines).toEqual([{ level: "error", fields: { err: lockError }, message: "worker error" }]);
  });

  test("writes one run failure unconfirmed warning, with the lock error, when the job is gone from the server", async () => {
    const { finishesInFlight, trackFailureOfJob, lines } = finishesInFlightReading(async () => undefined);
    trackFailureOfJob(JOB_ID);
    const lockError = failureLockError(JOB_ID);

    finishesInFlight.routeWorkerError(lockError);
    await settlePendingConfirmations();

    expect(lines).toEqual([
      { level: "warn", fields: { err: lockError, ...RUN_FIELDS }, message: "run failure unconfirmed" },
    ]);
  });

  test("writes one run failure unconfirmed warning, with the read error, when the server can't be read", async () => {
    const readError = new Error("Connection is closed.");
    const { finishesInFlight, trackFailureOfJob, lines } = finishesInFlightReading(async () => {
      throw readError;
    });
    trackFailureOfJob(JOB_ID);

    finishesInFlight.routeWorkerError(failureLockError(JOB_ID));
    await settlePendingConfirmations();

    expect(lines).toEqual([
      { level: "warn", fields: { err: readError, ...RUN_FIELDS }, message: "run failure unconfirmed" },
    ]);
  });

  test("sends no failure notice when the server can't tell", async () => {
    const { finishesInFlight, trackFailureOfJob, failureNoticeSends } = finishesInFlightReading(async () => undefined);
    trackFailureOfJob(JOB_ID);

    finishesInFlight.routeWorkerError(failureLockError(JOB_ID));
    await settlePendingConfirmations();

    expect(failureNoticeSends).toEqual([]);
  });
});

const SCHEDULED_AT = new Date("2026-11-03T06:00:00.000Z");

describe.each([
  {
    finish: "a run completion in flight",
    finishLoggingTo: (logger: Logger) =>
      runCompletionInFlight({
        processedOn: PROCESSED_ON,
        token: TOKEN,
        runLogger: loggerWithFields(logger, RUN_FIELDS),
        durationFields: DURATION_FIELDS,
        removeSavedSteps: async () => undefined,
      }),
    confirmedLines: [runCompletedLine],
    unconfirmedMessage: "run completion unconfirmed",
    unconfirmedFields: RUN_FIELDS,
  },
  {
    finish: "a fan-out completion in flight",
    finishLoggingTo: (logger: Logger) =>
      fanOutCompletionInFlight({
        processedOn: PROCESSED_ON,
        token: TOKEN,
        logger,
        eventName: "invoice.paid",
        eventId: "event-7",
      }),
    confirmedLines: [],
    unconfirmedMessage: "fan-out completion unconfirmed",
    unconfirmedFields: { eventName: "invoice.paid", eventId: "event-7" },
  },
  {
    finish: "a firing completion in flight",
    finishLoggingTo: (logger: Logger) =>
      firingCompletionInFlight({
        processedOn: PROCESSED_ON,
        token: TOKEN,
        logger,
        taskName: "send-digest",
        scheduledAt: SCHEDULED_AT,
      }),
    confirmedLines: [],
    unconfirmedMessage: "firing completion unconfirmed",
    unconfirmedFields: { task: "send-digest", scheduledAt: SCHEDULED_AT.toISOString() },
  },
])("$finish", ({ finishLoggingTo, confirmedLines, unconfirmedMessage, unconfirmedFields }) => {
  test("drops both lock errors once the server holds the job completed by this activation, then forgets the job", async () => {
    const { finishesInFlight, trackFinishOfJob, lines, readJobIds } = finishesInFlightReading(async () => ({
      state: "completed",
      processedOn: PROCESSED_ON,
      attemptsMade: 1,
    }));
    trackFinishOfJob(JOB_ID, finishLoggingTo);
    const laterLockError = missingLockError(JOB_ID);

    finishesInFlight.routeWorkerError(missingLockError(JOB_ID));
    finishesInFlight.routeWorkerError(missingLockError(JOB_ID));
    await settlePendingConfirmations();
    finishesInFlight.routeWorkerError(laterLockError);
    await settlePendingConfirmations();

    expect(lines).toEqual([
      ...confirmedLines,
      { level: "error", fields: { err: laterLockError }, message: "worker error" },
    ]);
    expect(readJobIds).toEqual([JOB_ID]);
  });

  test.each([
    {
      heldJob: "in another state",
      jobOnServer: { state: "active", processedOn: PROCESSED_ON, attemptsMade: 0 },
      lockErrorOf: missingLockError,
      isConfirmationSettledBetweenErrors: false,
    },
    {
      heldJob: "completed by another activation",
      jobOnServer: { state: "completed", processedOn: PROCESSED_ON + 5_000, attemptsMade: 1 },
      lockErrorOf: lockMismatchError,
      isConfirmationSettledBetweenErrors: true,
    },
  ])(
    "logs each lock error as a worker error when the server holds the job $heldJob",
    async ({ jobOnServer, lockErrorOf, isConfirmationSettledBetweenErrors }) => {
      const { finishesInFlight, trackFinishOfJob, lines } = finishesInFlightReading(async () => jobOnServer);
      trackFinishOfJob(JOB_ID, finishLoggingTo);
      const completionLockError = lockErrorOf(JOB_ID);
      const failureLockError = lockErrorOf(JOB_ID);

      finishesInFlight.routeWorkerError(completionLockError);
      if (isConfirmationSettledBetweenErrors) {
        await settlePendingConfirmations();
      }
      finishesInFlight.routeWorkerError(failureLockError);
      await settlePendingConfirmations();

      expect(lines).toEqual([
        { level: "error", fields: { err: completionLockError }, message: "worker error" },
        { level: "error", fields: { err: failureLockError }, message: "worker error" },
      ]);
    },
  );

  test("writes one unconfirmed warning, with the first lock error, when the job is gone from the server", async () => {
    const { finishesInFlight, trackFinishOfJob, lines } = finishesInFlightReading(async () => undefined);
    trackFinishOfJob(JOB_ID, finishLoggingTo);
    const completionLockError = missingLockError(JOB_ID);

    finishesInFlight.routeWorkerError(completionLockError);
    finishesInFlight.routeWorkerError(missingLockError(JOB_ID));
    await settlePendingConfirmations();

    expect(lines).toEqual([
      { level: "warn", fields: { err: completionLockError, ...unconfirmedFields }, message: unconfirmedMessage },
    ]);
  });

  test("writes one unconfirmed warning, with the read error, when the server can't be read", async () => {
    const readError = new Error("Connection is closed.");
    const { finishesInFlight, trackFinishOfJob, lines } = finishesInFlightReading(async () => {
      throw readError;
    });
    trackFinishOfJob(JOB_ID, finishLoggingTo);

    finishesInFlight.routeWorkerError(missingLockError(JOB_ID));
    finishesInFlight.routeWorkerError(missingLockError(JOB_ID));
    await settlePendingConfirmations();

    expect(lines).toEqual([
      { level: "warn", fields: { err: readError, ...unconfirmedFields }, message: unconfirmedMessage },
    ]);
  });

  test("is confirmed without reading the server when the worker reports the job completed", async () => {
    const { finishesInFlight, trackFinishOfJob, lines, readJobIds } = finishesInFlightReading(async () => undefined);
    trackFinishOfJob(JOB_ID, finishLoggingTo);

    finishesInFlight.recordFinishReported(JOB_ID, "completed");

    expect(lines).toEqual(confirmedLines);
    expect(readJobIds).toEqual([]);
  });
});

const COMPLETED_BY_THIS_ACTIVATION: JobOnServer = { state: "completed", processedOn: PROCESSED_ON, attemptsMade: 1 };

describe("the next job a lost reply carried", () => {
  test.each([
    {
      outcome: "is moved back to wait through the lock of the finish's token, once,",
      activeJobIdLockedByToken: NEXT_JOB_ID,
      expectedMovesToWait: [{ jobId: NEXT_JOB_ID, token: TOKEN }],
    },
    {
      outcome: "moves nothing and logs nothing, when no active job holds the finish's token,",
      activeJobIdLockedByToken: undefined,
      expectedMovesToWait: [],
    },
  ])("$outcome after a run completion is confirmed", async ({ activeJobIdLockedByToken, expectedMovesToWait }) => {
    const { finishesInFlight, trackCompletionOfJob, lines, scannedTokens, movesToWait } = finishesInFlightReading(
      async () => COMPLETED_BY_THIS_ACTIVATION,
      { findActiveJobIdLockedBy: async (token) => (token === TOKEN ? activeJobIdLockedByToken : undefined) },
    );
    trackCompletionOfJob(JOB_ID);

    finishesInFlight.routeWorkerError(missingLockError(JOB_ID));
    finishesInFlight.routeWorkerError(missingLockError(JOB_ID));
    await settlePendingConfirmations();

    expect(lines).toEqual([runCompletedLine]);
    expect(scannedTokens).toEqual([TOKEN]);
    expect(movesToWait).toEqual(expectedMovesToWait);
  });

  test("is moved back to wait after a run failure is confirmed", async () => {
    const { finishesInFlight, trackFailureOfJob, lines, movesToWait } = finishesInFlightReading(
      async () => ({ state: "failed", processedOn: PROCESSED_ON, attemptsMade: ATTEMPTS_MADE_AT_ACTIVATION + 1 }),
      { findActiveJobIdLockedBy: async () => NEXT_JOB_ID },
    );
    trackFailureOfJob(JOB_ID);

    finishesInFlight.routeWorkerError(failureLockError(JOB_ID));
    await settlePendingConfirmations();

    expect(lines).toEqual([]);
    expect(movesToWait).toEqual([{ jobId: NEXT_JOB_ID, token: TOKEN }]);
  });

  test.each([
    { outcome: "a lost lock", jobOnServer: { state: "active", processedOn: PROCESSED_ON, attemptsMade: 0 } },
    { outcome: "a job gone from the server", jobOnServer: undefined },
  ])("is not looked for after $outcome", async ({ jobOnServer }) => {
    const { finishesInFlight, trackCompletionOfJob, scannedTokens, movesToWait } = finishesInFlightReading(
      async () => jobOnServer,
      { findActiveJobIdLockedBy: async () => NEXT_JOB_ID },
    );
    trackCompletionOfJob(JOB_ID);

    finishesInFlight.routeWorkerError(missingLockError(JOB_ID));
    finishesInFlight.routeWorkerError(missingLockError(JOB_ID));
    await settlePendingConfirmations();

    expect(scannedTokens).toEqual([]);
    expect(movesToWait).toEqual([]);
  });

  test("is not looked for when the worker reports the finish, or when the finish has no token", async () => {
    const { finishesInFlight, trackCompletionOfJob, trackFinishOfJob, scannedTokens } = finishesInFlightReading(
      async () => COMPLETED_BY_THIS_ACTIVATION,
      { findActiveJobIdLockedBy: async () => NEXT_JOB_ID },
    );
    trackCompletionOfJob(JOB_ID);
    trackFinishOfJob("job-9", (logger) =>
      fanOutCompletionInFlight({
        processedOn: PROCESSED_ON,
        token: undefined,
        logger,
        eventName: "invoice.paid",
        eventId: "event-9",
      }),
    );

    finishesInFlight.recordFinishReported(JOB_ID, "completed");
    finishesInFlight.routeWorkerError(missingLockError("job-9"));
    finishesInFlight.routeWorkerError(missingLockError("job-9"));
    await settlePendingConfirmations();

    expect(scannedTokens).toEqual([]);
  });

  test("writes one next job hand-back failed warning, moves nothing twice and still drops the lock errors, when the move finds the lock taken", async () => {
    const lockTakenError = lockMismatchError(NEXT_JOB_ID);
    const { finishesInFlight, trackCompletionOfJob, lines, movesToWait } = finishesInFlightReading(
      async () => COMPLETED_BY_THIS_ACTIVATION,
      {
        findActiveJobIdLockedBy: async () => NEXT_JOB_ID,
        moveActiveJobToWait: async () => {
          throw lockTakenError;
        },
      },
    );
    trackCompletionOfJob(JOB_ID);

    finishesInFlight.routeWorkerError(missingLockError(JOB_ID));
    await settlePendingConfirmations();
    finishesInFlight.routeWorkerError(missingLockError(JOB_ID));
    await settlePendingConfirmations();

    expect(lines).toEqual([
      runCompletedLine,
      { level: "warn", fields: { jobId: NEXT_JOB_ID, err: lockTakenError }, message: "next job hand-back failed" },
    ]);
    expect(movesToWait).toHaveLength(1);
  });

  test("writes one next job hand-back failed warning when the active jobs can't be read", async () => {
    const scanError = new Error("Connection is closed.");
    const { finishesInFlight, trackCompletionOfJob, lines, movesToWait } = finishesInFlightReading(
      async () => COMPLETED_BY_THIS_ACTIVATION,
      {
        findActiveJobIdLockedBy: async () => {
          throw scanError;
        },
      },
    );
    trackCompletionOfJob(JOB_ID);

    finishesInFlight.routeWorkerError(missingLockError(JOB_ID));
    finishesInFlight.routeWorkerError(missingLockError(JOB_ID));
    await settlePendingConfirmations();

    expect(lines).toEqual([
      runCompletedLine,
      { level: "warn", fields: { err: scanError }, message: "next job hand-back failed" },
    ]);
    expect(movesToWait).toEqual([]);
  });
});
