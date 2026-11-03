import { ErrorCode } from "bullmq";
import { describe, expect, test } from "vitest";

import { createCompletionsInFlight } from "./completions-in-flight.js";
import type { JobOnServer } from "./completions-in-flight.js";
import { listenerLoggingAnsweredErrors } from "./connection.js";
import { loggerWithFields } from "./logger.js";
import { createRecordingLogger } from "../tests/recording-logger.js";

const JOB_ID = "job-7";

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

function completionsInFlightReading(readJobOnServer: (jobId: string) => Promise<JobOnServer | undefined>) {
  const { logger, lines } = createRecordingLogger();
  const readJobIds: string[] = [];
  const savedStepsRemovals: string[] = [];
  const completionsInFlight = createCompletionsInFlight({
    readJobOnServer: async (jobId) => {
      readJobIds.push(jobId);
      return readJobOnServer(jobId);
    },
    logWorkerError: listenerLoggingAnsweredErrors(logger, "worker error"),
  });
  const trackCompletionOfJob = (jobId: string, processedOn: number | undefined = PROCESSED_ON) => {
    completionsInFlight.trackCompletion(jobId, {
      processedOn,
      runLogger: loggerWithFields(logger, RUN_FIELDS),
      durationFields: DURATION_FIELDS,
      removeSavedSteps: async () => {
        savedStepsRemovals.push(jobId);
      },
    });
  };
  return { completionsInFlight, trackCompletionOfJob, lines, readJobIds, savedStepsRemovals };
}

const runCompletedLine = {
  level: "info",
  fields: { ...DURATION_FIELDS, ...RUN_FIELDS },
  message: "run completed",
};

describe("the completions in flight", () => {
  test("log run completed once and drop both lock errors when the server holds the job completed by this activation", async () => {
    const { completionsInFlight, trackCompletionOfJob, lines, readJobIds, savedStepsRemovals } =
      completionsInFlightReading(async () => ({ state: "completed", processedOn: PROCESSED_ON }));
    trackCompletionOfJob(JOB_ID);

    completionsInFlight.routeWorkerError(missingLockError(JOB_ID));
    completionsInFlight.routeWorkerError(missingLockError(JOB_ID));
    await settlePendingConfirmations();

    expect(lines).toEqual([runCompletedLine]);
    expect(readJobIds).toEqual([JOB_ID]);
    expect(savedStepsRemovals).toEqual([JOB_ID]);
  });

  test("drop the second lock error that arrives after the completion is confirmed", async () => {
    const { completionsInFlight, trackCompletionOfJob, lines, readJobIds } = completionsInFlightReading(async () => ({
      state: "completed",
      processedOn: PROCESSED_ON,
    }));
    trackCompletionOfJob(JOB_ID);

    completionsInFlight.routeWorkerError(missingLockError(JOB_ID));
    await settlePendingConfirmations();
    completionsInFlight.routeWorkerError(missingLockError(JOB_ID));
    await settlePendingConfirmations();

    expect(lines).toEqual([runCompletedLine]);
    expect(readJobIds).toEqual([JOB_ID]);
  });

  test("forget the job once its second lock error arrives, so a later lock error is a worker error", async () => {
    const { completionsInFlight, trackCompletionOfJob, lines } = completionsInFlightReading(async () => ({
      state: "completed",
      processedOn: PROCESSED_ON,
    }));
    trackCompletionOfJob(JOB_ID);
    const laterLockError = missingLockError(JOB_ID);

    completionsInFlight.routeWorkerError(missingLockError(JOB_ID));
    completionsInFlight.routeWorkerError(missingLockError(JOB_ID));
    await settlePendingConfirmations();
    completionsInFlight.routeWorkerError(laterLockError);
    await settlePendingConfirmations();

    expect(lines).toEqual([
      runCompletedLine,
      { level: "error", fields: { err: laterLockError }, message: "worker error" },
    ]);
  });

  test("log run completed and forget the job when the worker reports it completed", async () => {
    const { completionsInFlight, trackCompletionOfJob, lines, readJobIds, savedStepsRemovals } =
      completionsInFlightReading(async () => ({ state: "completed", processedOn: PROCESSED_ON }));
    trackCompletionOfJob(JOB_ID);
    const laterLockError = missingLockError(JOB_ID);

    completionsInFlight.recordRunCompleted(JOB_ID);
    completionsInFlight.routeWorkerError(laterLockError);
    await settlePendingConfirmations();

    expect(lines).toEqual([
      runCompletedLine,
      { level: "error", fields: { err: laterLockError }, message: "worker error" },
    ]);
    expect(readJobIds).toEqual([]);
    expect(savedStepsRemovals).toEqual([JOB_ID]);
  });

  test("log nothing for a job the worker reports completed without a completion in flight", () => {
    const { completionsInFlight, lines, savedStepsRemovals } = completionsInFlightReading(async () => undefined);

    completionsInFlight.recordRunCompleted(JOB_ID);

    expect(lines).toEqual([]);
    expect(savedStepsRemovals).toEqual([]);
  });

  test.each([
    {
      heldJob: "in another state",
      jobOnServer: { state: "active", processedOn: PROCESSED_ON },
      lockErrorOf: missingLockError,
      isConfirmationSettledBetweenErrors: false,
    },
    {
      heldJob: "completed by another activation",
      jobOnServer: { state: "completed", processedOn: PROCESSED_ON + 5_000 },
      lockErrorOf: lockMismatchError,
      isConfirmationSettledBetweenErrors: true,
    },
  ])(
    "log each lock error as a worker error when the server holds the job $heldJob",
    async ({ jobOnServer, lockErrorOf, isConfirmationSettledBetweenErrors }) => {
      const { completionsInFlight, trackCompletionOfJob, lines } = completionsInFlightReading(async () => jobOnServer);
      trackCompletionOfJob(JOB_ID);
      const completionLockError = lockErrorOf(JOB_ID);
      const failureLockError = lockErrorOf(JOB_ID);

      completionsInFlight.routeWorkerError(completionLockError);
      if (isConfirmationSettledBetweenErrors) {
        await settlePendingConfirmations();
      }
      completionsInFlight.routeWorkerError(failureLockError);
      await settlePendingConfirmations();

      expect(lines).toEqual([
        { level: "error", fields: { err: completionLockError }, message: "worker error" },
        { level: "error", fields: { err: failureLockError }, message: "worker error" },
      ]);
    },
  );

  test("confirm a lock mismatch against the activation tracked last for the job", async () => {
    const { completionsInFlight, trackCompletionOfJob, lines } = completionsInFlightReading(async () => ({
      state: "completed",
      processedOn: PROCESSED_ON + 5_000,
    }));
    trackCompletionOfJob(JOB_ID);
    trackCompletionOfJob(JOB_ID, PROCESSED_ON + 5_000);

    completionsInFlight.routeWorkerError(lockMismatchError(JOB_ID));
    completionsInFlight.routeWorkerError(lockMismatchError(JOB_ID));
    await settlePendingConfirmations();

    expect(lines).toEqual([runCompletedLine]);
  });

  test("write one run completion unconfirmed warning, with the first lock error, when the job is gone from the server", async () => {
    const { completionsInFlight, trackCompletionOfJob, lines, savedStepsRemovals } = completionsInFlightReading(
      async () => undefined,
    );
    trackCompletionOfJob(JOB_ID);
    const completionLockError = missingLockError(JOB_ID);

    completionsInFlight.routeWorkerError(completionLockError);
    completionsInFlight.routeWorkerError(missingLockError(JOB_ID));
    await settlePendingConfirmations();

    expect(lines).toEqual([
      { level: "warn", fields: { err: completionLockError, ...RUN_FIELDS }, message: "run completion unconfirmed" },
    ]);
    expect(savedStepsRemovals).toEqual([]);
  });

  test("write one run completion unconfirmed warning, with the read error, when the server can't be read", async () => {
    const readError = new Error("Connection is closed.");
    const { completionsInFlight, trackCompletionOfJob, lines } = completionsInFlightReading(async () => {
      throw readError;
    });
    trackCompletionOfJob(JOB_ID);

    completionsInFlight.routeWorkerError(missingLockError(JOB_ID));
    completionsInFlight.routeWorkerError(missingLockError(JOB_ID));
    await settlePendingConfirmations();

    expect(lines).toEqual([
      { level: "warn", fields: { err: readError, ...RUN_FIELDS }, message: "run completion unconfirmed" },
    ]);
  });

  test("log as a worker error, without reading the server, a lock error naming a job with no completion in flight", async () => {
    const { completionsInFlight, trackCompletionOfJob, lines, readJobIds } = completionsInFlightReading(async () => ({
      state: "completed",
      processedOn: PROCESSED_ON,
    }));
    trackCompletionOfJob(JOB_ID);
    const otherJobLockError = missingLockError("job-8");

    completionsInFlight.routeWorkerError(otherJobLockError);
    await settlePendingConfirmations();

    expect(lines).toEqual([{ level: "error", fields: { err: otherJobLockError }, message: "worker error" }]);
    expect(readJobIds).toEqual([]);
  });

  test("log as a worker error a lock error whose message names no job, and any other answered error", async () => {
    const { completionsInFlight, trackCompletionOfJob, lines, readJobIds } = completionsInFlightReading(async () => ({
      state: "completed",
      processedOn: PROCESSED_ON,
    }));
    trackCompletionOfJob(JOB_ID);
    const unparsedLockError = Object.assign(new Error(`lock gone for ${JOB_ID}`), { code: ErrorCode.JobLockNotExist });
    const lookalikeError = new Error(`Missing lock for job ${JOB_ID}. moveToFinished`);
    const scriptError = new Error("ERR Error running script");

    completionsInFlight.routeWorkerError(unparsedLockError);
    completionsInFlight.routeWorkerError(lookalikeError);
    completionsInFlight.routeWorkerError(scriptError);
    await settlePendingConfirmations();

    expect(lines).toEqual([
      { level: "error", fields: { err: unparsedLockError }, message: "worker error" },
      { level: "error", fields: { err: lookalikeError }, message: "worker error" },
      { level: "error", fields: { err: scriptError }, message: "worker error" },
    ]);
    expect(readJobIds).toEqual([]);
  });

  test("log nothing for a lost connection", () => {
    const { completionsInFlight, lines } = completionsInFlightReading(async () => undefined);

    completionsInFlight.routeWorkerError(Object.assign(new Error("read ECONNRESET"), { code: "ECONNRESET" }));

    expect(lines).toEqual([]);
  });
});
