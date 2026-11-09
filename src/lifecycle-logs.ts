import type { DurationInMilliseconds, EpochMilliseconds } from "./durations.js";
import { RECORD_VERSION } from "./engine/engine.js";
import type {
  DispatchOrEmitRequest,
  EnqueueReply,
  LockedRun,
  ParkedRun,
  SettleOutcome,
  SettleReply,
  StalledRun,
  SyncSchedulesReply,
  UnknownVersionReply,
} from "./engine/engine.js";
import type { ListenerLeftOut } from "./engine-failure-notice.js";
import type { RunId } from "./ids.js";
import type { AttemptOutcome } from "./locked-run-attempt.js";
import type { Logger } from "./logger.js";

export type LifecycleLine = {
  level: keyof Logger;
  message: string;
  fields: Record<string, unknown>;
};

export type RunLineSubject = Pick<LockedRun, "task" | "runId" | "attempt" | "sourceEvent" | "scheduledAt">;

export type SettledRun = RunLineSubject & Pick<LockedRun, "dispatchedAt" | "startedAt">;

export type SettledAttempt = {
  settledRun: SettledRun;
  attemptOutcome: AttemptOutcome;
  settleReply: SettleReply;
  elapsedSinceFetchInMilliseconds: DurationInMilliseconds;
  listenersLeftOut: readonly ListenerLeftOut[];
};

export function writeLifecycleLines(logger: Logger, lines: readonly LifecycleLine[]): void {
  for (const { level, message, fields } of lines) {
    logger[level](fields, message);
  }
}

export function runFieldsOf({ task, runId, attempt, sourceEvent, scheduledAt }: RunLineSubject) {
  return {
    task,
    runId,
    attempt,
    ...sourceEvent,
    ...(scheduledAt === undefined ? {} : { scheduledAt: new Date(scheduledAt).toISOString() }),
  };
}

export function enqueueLinesOf(request: DispatchOrEmitRequest, reply: EnqueueReply | undefined): LifecycleLine[] {
  const { runs, eventDedup } = request;
  if (eventDedup !== undefined) {
    if (runs.length === 0) {
      return [{ level: "warn", message: "no listeners for event", fields: { ...eventDedup } }];
    }
    if (reply?.kind === "duplicate") {
      return [{ level: "debug", message: "duplicate dispatch dropped", fields: { ...eventDedup } }];
    }
  }
  if (reply?.kind !== "enqueued") {
    return [];
  }
  const taskByRunId = new Map(runs.map(({ runId, task }) => [runId, task]));
  const runLinesOf = (runIds: readonly string[], message: string): LifecycleLine[] =>
    runIds.map((runId) => ({
      level: "debug",
      message,
      fields: { task: taskByRunId.get(runId), runId, ...eventDedup },
    }));
  return [
    ...(eventDedup === undefined ? runLinesOf(reply.createdRunIds, "run dispatched") : []),
    ...runLinesOf(reply.skippedRunIds, "duplicate dispatch dropped"),
  ];
}

export function workerErrorLineOf(error: Error): LifecycleLine {
  return { level: "error", message: "worker error", fields: { err: error } };
}

function shownRecordVersionOf(recordVersion: number | undefined): string {
  return recordVersion === undefined ? "a version" : `version ${String(recordVersion)}`;
}

export function settleRefusalLineOf(
  outcomeKind: SettleOutcome["kind"],
  runId: RunId,
  { recordVersion }: UnknownVersionReply,
): LifecycleLine {
  return workerErrorLineOf(
    new Error(
      `The ${outcomeKind} settle of run "${runId}" was refused: a newer release rewrote its record to ${shownRecordVersionOf(recordVersion)} this release cannot read.`,
    ),
  );
}

export function scheduleSyncLinesOf(reply: SyncSchedulesReply): LifecycleLine[] {
  if (reply.kind === "unknownVersion") {
    return [
      workerErrorLineOf(
        new Error(
          `The schedule sync was refused: a newer release wrote a schedule record of ${shownRecordVersionOf(reply.recordVersion)} this release cannot read, so the stored schedules were left as they are.`,
        ),
      ),
    ];
  }
  const changesByKind = { added: reply.added, changed: reply.changed, removed: reply.removed };
  return Object.entries(changesByKind).flatMap(([kind, scheduleSummaries]) =>
    scheduleSummaries.map(({ task, cron, timezone }): LifecycleLine => ({
      level: "info",
      message: `schedule ${kind}`,
      fields: { task, cron, timezone },
    })),
  );
}

export function pickupLineOf(lockedRun: LockedRun): LifecycleLine {
  return {
    level: "debug",
    message: lockedRun.isResume ? "run resumed" : "attempt started",
    fields: runFieldsOf(lockedRun),
  };
}

export function lockLostAbortLineOf(lockedRun: LockedRun): LifecycleLine {
  return { level: "debug", message: "attempt aborted: lock lost", fields: runFieldsOf(lockedRun) };
}

export function parkLineOf(parkedRun: ParkedRun): LifecycleLine {
  return { level: "debug", message: "run waiting for a concurrency slot", fields: runFieldsOf(parkedRun) };
}

export function passOverLineOf(recordVersion: number | undefined): LifecycleLine {
  return {
    level: "debug",
    message: "runs passed over: newer record version",
    fields: { recordVersion, supportedVersion: RECORD_VERSION },
  };
}

export function stallLineOf(stalledRun: StalledRun): LifecycleLine {
  return {
    level: "warn",
    message: "attempt stalled",
    fields: { ...runFieldsOf(stalledRun), stalls: stalledRun.stalls },
  };
}

type SettleLineContext = {
  runFields: ReturnType<typeof runFieldsOf>;
  durationFields: { runDurationInMilliseconds: number; executionDurationInMilliseconds: number };
  settleReply: SettleReply;
};

type SettleLineByOutcomeKind = {
  [Kind in SettleOutcome["kind"]]: (
    attemptOutcome: Extract<AttemptOutcome, { kind: Kind }>,
    context: SettleLineContext,
  ) => LifecycleLine;
};

const SETTLE_LINE_BY_OUTCOME_KIND: SettleLineByOutcomeKind = {
  complete: (_attemptOutcome, { runFields, durationFields }) => ({
    level: "info",
    message: "run completed",
    fields: { ...runFields, ...durationFields },
  }),
  fail: ({ error, reason }, { runFields, durationFields }) => ({
    level: "error",
    message: "run failed",
    fields: { ...runFields, err: error, reason, ...durationFields },
  }),
  retry: ({ error, retriesLeft, backoffInMilliseconds }, { runFields }) => ({
    level: "warn",
    message: "attempt failed",
    fields: { ...runFields, err: error, retriesLeft, backoffInMilliseconds },
  }),
  suspend: ({ resumesAt }, { runFields }) => ({
    level: "debug",
    message: "run suspended",
    fields: { ...runFields, resumesAt: new Date(resumesAt).toISOString() },
  }),
  yield: (_attemptOutcome, { runFields }) => ({
    level: "debug",
    message: "run yielded at shutdown",
    fields: runFields,
  }),
  handBack: ({ error }, { runFields, durationFields, settleReply }) =>
    settleReply.kind === "failed"
      ? {
          level: "error",
          message: "run failed",
          fields: { ...runFields, err: error, reason: settleReply.reason, ...durationFields },
        }
      : { level: "debug", message: "run handed back: unknown task", fields: runFields },
};

function finishedAtOf({ settledRun, settleReply, elapsedSinceFetchInMilliseconds }: SettledAttempt): EpochMilliseconds {
  return settleReply.kind === "completed" || settleReply.kind === "failed"
    ? settleReply.finishedAt
    : settledRun.startedAt + elapsedSinceFetchInMilliseconds;
}

export function settleLinesOf(settledAttempt: SettledAttempt): LifecycleLine[] {
  const { settledRun, attemptOutcome, settleReply, listenersLeftOut } = settledAttempt;
  const runFields = runFieldsOf(settledRun);
  if (settleReply.kind === "stale") {
    return [
      {
        level: "warn",
        message: "attempt outcome discarded: lock lost",
        fields: { ...runFields, outcome: attemptOutcome.kind },
      },
    ];
  }
  if (settleReply.kind === "unknownVersion") {
    return [settleRefusalLineOf(attemptOutcome.kind, settledRun.runId, settleReply)];
  }
  const finishedAt = finishedAtOf(settledAttempt);
  const durationFields = {
    runDurationInMilliseconds: finishedAt - settledRun.dispatchedAt,
    executionDurationInMilliseconds: finishedAt - settledRun.startedAt,
  };
  const settleLineOf = SETTLE_LINE_BY_OUTCOME_KIND[attemptOutcome.kind] as (
    attemptOutcome: AttemptOutcome,
    context: SettleLineContext,
  ) => LifecycleLine;
  const leftOutListenerLines =
    settleReply.kind === "failed" ? listenersLeftOut.map(({ error }) => workerErrorLineOf(error)) : [];
  return [...leftOutListenerLines, settleLineOf(attemptOutcome, { runFields, durationFields, settleReply })];
}
