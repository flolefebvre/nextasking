import type { AttemptAbortCause } from "./attempt-abort.js";
import { errorFromThrownValue, errorRecordFor, failureReasonFor } from "./attempt-errors.js";
import { settleWithinTimeout } from "./attempt-timeout.js";
import { backoffInMillisecondsFor } from "./backoff.js";
import { backoffOf } from "./definitions.js";
import type { AnyTaskDefinition } from "./definitions.js";
import { parseDuration } from "./durations.js";
import type { DurationInMilliseconds, EpochMilliseconds } from "./durations.js";
import type { Engine, FailureNotice, LockedRun, SettleOutcome, SettleRequest } from "./engine/engine.js";
import { engineFailureNoticeOf } from "./engine-failure-notice.js";
import type { FailingRun, ListenerLeftOut, RunFailure, WorkerDefinitions } from "./engine-failure-notice.js";
import { createEngineStepStore } from "./engine-step-store.js";
import { runFieldsOf } from "./lifecycle-logs.js";
import { loggerWithFields } from "./logger.js";
import type { Logger } from "./logger.js";
import type { FailureReason } from "./run-queue.js";
import { executeTaskHandler, openStepEngine } from "./step-engine.js";
import type { HandlerOutcome, HandlerSender } from "./step-engine.js";

export type AttemptOutcome =
  | { kind: "complete" }
  | { kind: "fail"; reason: FailureReason; error: Error; failureNotice: FailureNotice | undefined }
  | { kind: "retry"; error: Error; retriesLeft: number; backoffInMilliseconds: DurationInMilliseconds }
  | { kind: "suspend"; resumesAt: EpochMilliseconds }
  | { kind: "yield" }
  | { kind: "handBack"; error: Error; failureNotice: FailureNotice | undefined };

export type AttemptResult = { attemptOutcome: AttemptOutcome; listenersLeftOut: ListenerLeftOut[] };

export type LockedRunAttempt = {
  engine: Pick<Engine, "saveStep" | "readStep">;
  task: AnyTaskDefinition;
  lockedRun: LockedRun;
  definitions: WorkerDefinitions;
  sender: HandlerSender;
  logger: Logger;
  abortSignal: AbortSignal;
  abortAttempt: (abortCause: AttemptAbortCause) => void;
  isWorkerDraining: () => boolean;
};

const ATTEMPT_OUTCOME_BY_HANDLER_OUTCOME_KIND: {
  [Kind in HandlerOutcome["kind"]]: (handlerOutcome: Extract<HandlerOutcome, { kind: Kind }>) => AttemptOutcome;
} = {
  completed: () => ({ kind: "complete" }),
  suspended: ({ resumesAt }) => ({ kind: "suspend", resumesAt }),
  yielded: () => ({ kind: "yield" }),
};

function payloadOf({ payload }: LockedRun): unknown {
  return payload === "" ? undefined : JSON.parse(payload);
}

export function failingAttemptOf(
  failingRun: FailingRun,
  failure: RunFailure,
  definitions: WorkerDefinitions,
): AttemptResult {
  const { failureNotice, listenersLeftOut } = engineFailureNoticeOf(failingRun, failure, definitions);
  return { attemptOutcome: { kind: "fail", ...failure, failureNotice }, listenersLeftOut };
}

function failedAttemptOf(
  { task, lockedRun, definitions }: LockedRunAttempt,
  thrownValue: unknown,
  randomBetweenZeroAndOne: number,
): AttemptResult {
  const error = errorFromThrownValue(thrownValue);
  const retriesLeft = lockedRun.retries + 1 - lockedRun.attempt;
  const reason = failureReasonFor(error, retriesLeft);
  if (reason !== undefined) {
    return failingAttemptOf(lockedRun, { error, reason }, definitions);
  }
  const backoffInMilliseconds = backoffInMillisecondsFor(
    parseDuration(backoffOf(task)),
    lockedRun.attempt,
    randomBetweenZeroAndOne,
  );
  return { attemptOutcome: { kind: "retry", error, retriesLeft, backoffInMilliseconds }, listenersLeftOut: [] };
}

export async function executeLockedRunAttempt(lockedRunAttempt: LockedRunAttempt): Promise<AttemptResult> {
  const { engine, task, lockedRun, sender, logger, abortSignal, abortAttempt, isWorkerDraining } = lockedRunAttempt;
  const { runId, lockToken, attempt, sleepsWokenAt, scheduledAt } = lockedRun;
  let handlerOutcome: HandlerOutcome;
  try {
    const stepEngine = openStepEngine({
      taskName: task.name,
      runId,
      lockToken,
      store: createEngineStepStore(engine, { runId, lockToken }),
      sender,
      abortSignal,
      isWorkerDraining,
      futureResumeTime: "suspend the run",
      ...(sleepsWokenAt === undefined ? {} : { sleepsWokenAt }),
      markLockLost: () => {
        abortAttempt("lock lost");
      },
    });
    try {
      handlerOutcome = await settleWithinTimeout(
        task.name,
        task.timeout,
        executeTaskHandler(stepEngine, task, {
          runId,
          attempt,
          logger: loggerWithFields(logger, runFieldsOf(lockedRun)),
          abortSignal,
          payload: payloadOf(lockedRun),
          scheduledAt: scheduledAt === undefined ? undefined : new Date(scheduledAt),
        }),
      );
    } finally {
      stepEngine.throwRecordedMisuse();
    }
  } catch (thrownValue) {
    return failedAttemptOf(lockedRunAttempt, thrownValue, Math.random());
  }
  const attemptOutcomeOf = ATTEMPT_OUTCOME_BY_HANDLER_OUTCOME_KIND[handlerOutcome.kind] as (
    handlerOutcome: HandlerOutcome,
  ) => AttemptOutcome;
  return { attemptOutcome: attemptOutcomeOf(handlerOutcome), listenersLeftOut: [] };
}

function settleOutcomeOf(attemptOutcome: AttemptOutcome, attempt: number): SettleOutcome {
  const errorRecordOf = (error: Error) => errorRecordFor(error, attempt, new Date());
  switch (attemptOutcome.kind) {
    case "complete":
    case "suspend":
    case "yield":
      return attemptOutcome;
    case "fail":
      return { ...attemptOutcome, error: errorRecordOf(attemptOutcome.error) };
    case "retry":
      return {
        kind: "retry",
        error: errorRecordOf(attemptOutcome.error),
        backoffInMilliseconds: attemptOutcome.backoffInMilliseconds,
      };
    case "handBack":
      return { ...attemptOutcome, error: errorRecordOf(attemptOutcome.error) };
  }
}

export function settleRequestOf(
  task: AnyTaskDefinition | undefined,
  { runId, lockToken, attempt }: LockedRun,
  attemptOutcome: AttemptOutcome,
): SettleRequest {
  const concurrencyLimit = task?.concurrency?.limit;
  return {
    runId,
    lockToken,
    outcome: settleOutcomeOf(attemptOutcome, attempt),
    ...(concurrencyLimit === undefined ? {} : { concurrencyLimit }),
  };
}
