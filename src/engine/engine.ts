import type { Redis } from "ioredis";

import type { ConcurrencyKey } from "../definitions.js";
import type { DurationInMilliseconds, EpochMilliseconds } from "../durations.js";
import type { RunId, StepId } from "../ids.js";
import type { ErrorRecord, FailureReason, SourceEvent } from "../run-queue.js";
import type { RunState } from "../run-states.js";
import { ENQUEUE_KEY_COUNT, ENQUEUE_SCRIPT } from "./enqueue-script.js";
import { engineKeysOf, hashTaggedPrefixOf } from "./engine-keys.js";
import type { LockToken } from "./engine-keys.js";
import { LUA_LIBRARY } from "./lua-library.js";
import { defineScriptCommand } from "./script-commands.js";
import type { ScriptArgument } from "./script-commands.js";

export const RECORD_VERSION = 1;

export const DEDUP_WINDOW_IN_MILLISECONDS: DurationInMilliseconds = 24 * 60 * 60 * 1000;

export const EXPIRED_DEDUP_ENTRIES_TRIMMED_PER_SET = 100;

export type SerializedPayload = string;

export type SerializedStepValue = string;

export type RunToCreate = {
  runId: RunId;
  task: string;
  payload: SerializedPayload;
  retries: number;
  concurrencyKey?: ConcurrencyKey;
  sourceEvent?: SourceEvent;
  delayInMilliseconds?: DurationInMilliseconds;
};

export type ParentFence = {
  parentRunId: RunId;
  lockToken: LockToken;
  stepId: StepId;
};

export type EnqueueRequest = {
  runs: readonly RunToCreate[];
  eventDedup?: SourceEvent;
  parentFence?: ParentFence;
};

export type DispatchOrEmitRequest = Omit<EnqueueRequest, "parentFence">;

export type StaleReply = { kind: "stale" };

export type AlreadyReply = { kind: "already" };

export type UnknownVersionReply = { kind: "unknownVersion"; recordVersion: number | undefined };

export type WrongStateReply = { kind: "wrongState"; state: RunState };

export type NotFoundReply = { kind: "notFound" };

export type DuplicateReply = { kind: "duplicate" };

export type NoScheduleReply = { kind: "noSchedule" };

export type NotDueReply = { kind: "notDue" };

export type EnqueuedReply = { kind: "enqueued"; createdRunIds: RunId[]; skippedRunIds: RunId[] };

export type EnqueueReply = EnqueuedReply | DuplicateReply | StaleReply | UnknownVersionReply;

export type FetchRequest = {
  lockTokens: readonly LockToken[];
  concurrencyLimitsByTask: Readonly<Record<string, number>>;
  lockDurationInMilliseconds: DurationInMilliseconds;
};

export type LockedRun = {
  lockToken: LockToken;
  runId: RunId;
  task: string;
  payload: SerializedPayload;
  attempt: number;
  retries: number;
  replays: number;
  isResume: boolean;
  dispatchedAt: EpochMilliseconds;
  startedAt: EpochMilliseconds;
  lockedUntil: EpochMilliseconds;
  scheduledAt?: EpochMilliseconds;
  sourceEvent?: SourceEvent;
  concurrencyKey?: ConcurrencyKey;
};

export type ParkedRun = { runId: RunId; task: string; attempt: number };

export type FetchedReply = {
  kind: "fetched";
  lockedRuns: LockedRun[];
  parkedRuns: ParkedRun[];
  passedOverRecordVersions: number[];
};

export type FetchReply = FetchedReply | UnknownVersionReply;

export type HeldLock = { runId: RunId; lockToken: LockToken };

export type RenewRequest = {
  heldLocks: readonly HeldLock[];
  lockDurationInMilliseconds: DurationInMilliseconds;
};

export type LockRenewal = { kind: "renewed"; lockedUntil: EpochMilliseconds } | StaleReply;

export type FailureNotice = {
  runs: readonly RunToCreate[];
  eventDedup: SourceEvent;
};

export type SettleOutcome =
  | { kind: "complete" }
  | { kind: "fail"; reason: FailureReason; error: ErrorRecord; failureNotice: FailureNotice | undefined }
  | { kind: "retry"; error: ErrorRecord; backoffInMilliseconds: DurationInMilliseconds }
  | { kind: "suspend"; resumesAt: EpochMilliseconds }
  | { kind: "yield" }
  | { kind: "handBack"; error: ErrorRecord; failureNotice: FailureNotice | undefined };

export type SettleRequest = {
  runId: RunId;
  lockToken: LockToken;
  outcome: SettleOutcome;
};

export type SettledReply =
  | { kind: "completed"; finishedAt: EpochMilliseconds }
  | { kind: "failed"; reason: FailureReason; finishedAt: EpochMilliseconds }
  | { kind: "backingOff"; nextAttemptAt: EpochMilliseconds }
  | { kind: "suspended"; resumesAt: EpochMilliseconds }
  | { kind: "yielded" }
  | { kind: "handedBack" };

export type SettleReply = SettledReply | StaleReply | AlreadyReply | UnknownVersionReply;

export type StalledRun = { runId: RunId; task: string; attempt: number; stalls: number };

export type ThirdStall = { runId: RunId; task: string; attempt: number; expiredLockToken: LockToken };

export type PromotedReply = {
  kind: "promoted";
  promotedRunCount: number;
  stalledRuns: StalledRun[];
  thirdStalls: ThirdStall[];
};

export type PromoteReply = PromotedReply | UnknownVersionReply;

export type StepToSave =
  | { kind: "value"; value: SerializedStepValue }
  | { kind: "sleep"; durationInMilliseconds: DurationInMilliseconds }
  | { kind: "sleepUntil"; resumesAt: EpochMilliseconds };

export type SaveStepRequest = {
  runId: RunId;
  lockToken: LockToken;
  stepId: StepId;
  step: StepToSave;
};

export type SaveStepReply =
  { kind: "saved"; resumesAt: EpochMilliseconds | undefined } | StaleReply | UnknownVersionReply;

export type ReadStepRequest = { runId: RunId; lockToken: LockToken; stepId: StepId };

export type ReadStepReply =
  { kind: "read"; savedValue: SerializedStepValue | undefined } | StaleReply | UnknownVersionReply;

export type FireRequest = {
  task: string;
  instant: EpochMilliseconds;
  instantAfter: EpochMilliseconds;
  runId: RunId;
  retries: number;
};

export type FireReply =
  | { kind: "fired"; runId: RunId; isRunCreated: boolean }
  | AlreadyReply
  | NotDueReply
  | NoScheduleReply
  | UnknownVersionReply;

export type ScheduleSummary = { task: string; cron: string; timezone: string };

export type ScheduleToSync = ScheduleSummary & { nextFiringAt: EpochMilliseconds };

export type SyncSchedulesRequest = { schedulesToSync: readonly ScheduleToSync[] };

export type SyncSchedulesReply =
  | { kind: "synced"; added: ScheduleSummary[]; changed: ScheduleSummary[]; removed: ScheduleSummary[] }
  | UnknownVersionReply;

export type RetentionLimit = { maxAgeInMilliseconds: DurationInMilliseconds; maxRunCount: number };

export type PruneRequest = { task: string; completedRetention: RetentionLimit; failedRetention: RetentionLimit };

export type PruneReply = { kind: "pruned"; prunedRunCount: number };

export type ReplayRequest = { runId: RunId };

export type ReplayReply = { kind: "replayed"; task: string } | NotFoundReply | WrongStateReply | UnknownVersionReply;

export type MakeDueReply = { kind: "madeDue"; madeDueRunCount: number };

export type Engine = {
  enqueue: (request: EnqueueRequest) => Promise<EnqueueReply>;
  fetch: (request: FetchRequest) => Promise<FetchReply>;
  renew: (request: RenewRequest) => Promise<LockRenewal[]>;
  settle: (request: SettleRequest) => Promise<SettleReply>;
  promote: () => Promise<PromoteReply>;
  saveStep: (request: SaveStepRequest) => Promise<SaveStepReply>;
  readStep: (request: ReadStepRequest) => Promise<ReadStepReply>;
  fire: (request: FireRequest) => Promise<FireReply>;
  syncSchedules: (request: SyncSchedulesRequest) => Promise<SyncSchedulesReply>;
  prune: (request: PruneRequest) => Promise<PruneReply>;
  replay: (request: ReplayRequest) => Promise<ReplayReply>;
  makeDue: () => Promise<MakeDueReply>;
};

export type ImplementedEngine = {
  enqueue: (request: DispatchOrEmitRequest) => Promise<EnqueueReply>;
};

function unreadableReplyError(scriptName: string, reply: unknown): Error {
  return new Error(`The ${scriptName} script sent a reply the engine cannot read: ${JSON.stringify(reply)}`);
}

function isStringList(value: unknown): value is string[] {
  return Array.isArray(value) && value.every((element) => typeof element === "string");
}

function enqueueReplyOf(reply: unknown): EnqueueReply {
  if (Array.isArray(reply) && reply.length === 1 && reply[0] === "duplicate") {
    return { kind: "duplicate" };
  }
  if (Array.isArray(reply) && reply.length === 3 && reply[0] === "enqueued") {
    const [, createdRunIds, skippedRunIds]: unknown[] = reply;
    if (isStringList(createdRunIds) && isStringList(skippedRunIds)) {
      return { kind: "enqueued", createdRunIds, skippedRunIds };
    }
  }
  throw unreadableReplyError("enqueue", reply);
}

function runArgumentsOf(run: RunToCreate): ScriptArgument[] {
  return [
    run.runId,
    run.task,
    run.payload,
    run.retries,
    run.concurrencyKey ?? "",
    run.sourceEvent?.eventName ?? "",
    run.sourceEvent?.eventId ?? "",
    run.delayInMilliseconds ?? 0,
  ];
}

export function createEngine(client: Redis, prefix: string): ImplementedEngine {
  const keys = engineKeysOf(prefix);
  const sharedArguments: ScriptArgument[] = [hashTaggedPrefixOf(prefix), RECORD_VERSION];
  const enqueueCommand = defineScriptCommand(
    client,
    "nextaskingEnqueue",
    `${LUA_LIBRARY}\n${ENQUEUE_SCRIPT}`,
    ENQUEUE_KEY_COUNT,
  );
  return {
    enqueue: async ({ runs, eventDedup }) =>
      enqueueReplyOf(
        await enqueueCommand.call(
          [keys.pending, keys.due, keys.tasks, keys.wakeUpMarker, keys.order],
          [
            ...sharedArguments,
            DEDUP_WINDOW_IN_MILLISECONDS,
            EXPIRED_DEDUP_ENTRIES_TRIMMED_PER_SET,
            eventDedup?.eventName ?? "",
            eventDedup?.eventId ?? "",
            runs.length,
            ...runs.flatMap(runArgumentsOf),
          ],
        ),
      ),
  };
}
