import { randomUUID } from "node:crypto";

import type { ChainableCommander, Redis } from "ioredis";

import type { RunFailedPayload } from "../built-in-events.js";
import type { ConcurrencyKey } from "../definitions.js";
import type { DurationInMilliseconds, EpochMilliseconds } from "../durations.js";
import type { RunId, StepId } from "../ids.js";
import type { ErrorRecord, FailureReason, SourceEvent } from "../run-queue.js";
import { isRunState } from "../run-states.js";
import type { RunState } from "../run-states.js";
import { ENQUEUE_SCRIPT, enqueueKeysOf } from "./enqueue-script.js";
import { engineKeysOf, hashTaggedPrefixOf } from "./engine-keys.js";
import type { LockToken } from "./engine-keys.js";
import {
  FETCH_SCRIPT,
  fetchKeysOf,
  MOST_LOCK_TOKENS_PER_FETCH,
  MOST_RUNS_PARKED_PER_FETCH,
  NEWER_VERSION_RECORDS_PASSED_OVER_PER_FETCH,
} from "./fetch-script.js";
import { FIRE_SCRIPT, fireKeysOf } from "./fire-script.js";
import { LUA_LIBRARY } from "./lua-library.js";
import { MAKE_DUE_SCRIPT, makeDueKeysOf } from "./make-due-script.js";
import {
  MOST_DUE_RUNS_PROMOTED_PER_PROMOTE,
  NEWER_VERSION_RECORDS_PASSED_OVER_PER_PROMOTE,
  PROMOTE_SCRIPT,
  promoteKeysOf,
} from "./promote-script.js";
import { NEWER_VERSION_RECORDS_PASSED_OVER_PER_PRUNE, PRUNE_SCRIPT, pruneKeysOf } from "./prune-script.js";
import { READ_STEP_SCRIPT, readStepKeysOf } from "./read-step-script.js";
import { RENEW_SCRIPT, renewKeysOf } from "./renew-script.js";
import { REPLAY_SCRIPT, replayKeysOf } from "./replay-script.js";
import { SAVE_STEP_SCRIPT, saveStepKeysOf } from "./save-step-script.js";
import { defineScriptCommand } from "./script-commands.js";
import type { ScriptArgument } from "./script-commands.js";
import { SETTLE_SCRIPT, settleKeysOf } from "./settle-script.js";
import { SYNC_SCHEDULES_SCRIPT, syncSchedulesKeysOf } from "./sync-schedules-script.js";

export const RECORD_VERSION = 1;

export const DEDUP_WINDOW_IN_MILLISECONDS: DurationInMilliseconds = 24 * 60 * 60 * 1000;

export const EXPIRED_DEDUP_ENTRIES_TRIMMED_PER_SET = 100;

export const MOST_RUNS_PRUNED_PER_PRUNE = 200;

export const EXPIRED_DEDUP_ENTRIES_TRIMMED_PER_PRUNE = 1000;

export const MOST_ERROR_TEXT_CHARACTERS = 4000;

export const CUT_ERROR_TEXT_MARKER = `… [cut at ${String(MOST_ERROR_TEXT_CHARACTERS)} characters]`;

export const UNKNOWN_TASK_FAILURE_DELAY_IN_MILLISECONDS: DurationInMilliseconds = 30 * 60 * 1000;

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
  stepValue: SerializedStepValue;
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

export type AlreadySentReply = { kind: "alreadySent"; savedValue: SerializedStepValue };

export type EnqueueReply = EnqueuedReply | AlreadySentReply | DuplicateReply | StaleReply | UnknownVersionReply;

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
  sleepsWokenAt?: EpochMilliseconds;
};

export type ParkedRun = { runId: RunId; task: string; attempt: number } & RunOrigin;

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
  payload: Omit<RunFailedPayload, "failedAt">;
  runs: readonly Omit<RunToCreate, "payload" | "delayInMilliseconds">[];
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
  concurrencyLimit?: number;
};

export type SettledReply =
  | { kind: "completed"; finishedAt: EpochMilliseconds }
  | { kind: "failed"; reason: FailureReason; finishedAt: EpochMilliseconds }
  | { kind: "backingOff"; nextAttemptAt: EpochMilliseconds }
  | { kind: "suspended"; resumesAt: EpochMilliseconds }
  | { kind: "yielded" }
  | { kind: "handedBack" };

export type SettleReply = SettledReply | StaleReply | AlreadyReply | UnknownVersionReply;

export type RunOrigin = { scheduledAt?: EpochMilliseconds; sourceEvent?: SourceEvent };

export type StalledRun = { runId: RunId; task: string; attempt: number; stalls: number } & RunOrigin;

export type ThirdStall = {
  runId: RunId;
  task: string;
  attempt: number;
  expiredLockToken: LockToken;
  replays: number;
  dispatchedAt: EpochMilliseconds;
  startedAt: EpochMilliseconds;
} & RunOrigin;

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

export type SavedSleepReply = { kind: "saved"; resumesAt: EpochMilliseconds; isDue: boolean };

export type SaveStepReply =
  { kind: "saved"; resumesAt: undefined } | SavedSleepReply | StaleReply | UnknownVersionReply;

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

export type DueSchedule = ScheduleSummary & { nextFiringAt: EpochMilliseconds };

export type DueSchedulesReply = { serverInstant: EpochMilliseconds; dueSchedules: DueSchedule[] };

export type TickRequest = {
  heldLocks: readonly HeldLock[];
  lockDurationInMilliseconds: DurationInMilliseconds;
  prune?: PruneRequest;
  isPromoteIncluded?: boolean;
  scheduledTaskNames?: readonly string[];
};

export type TickCommandReply<Reply> = { kind: "replied"; reply: Reply } | { kind: "failed"; error: Error };

export type TickReply = {
  promote: TickCommandReply<PromoteReply> | undefined;
  renewals: TickCommandReply<LockRenewal[]>;
  taskNames: TickCommandReply<string[]>;
  prune: TickCommandReply<PruneReply> | undefined;
  dueSchedules: TickCommandReply<DueSchedulesReply> | undefined;
};

export type Engine = {
  tick: (request: TickRequest) => Promise<TickReply>;
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
  serverInstant: () => Promise<EpochMilliseconds>;
};

function unreadableReplyError(scriptName: string, reply: unknown): Error {
  return new Error(`The ${scriptName} script sent a reply the engine cannot read: ${JSON.stringify(reply)}`);
}

function isStringList(value: unknown): value is string[] {
  return Array.isArray(value) && value.every((element) => typeof element === "string");
}

function isStoredValueList(value: unknown): value is (string | null)[] {
  return Array.isArray(value) && value.every((element) => typeof element === "string" || element === null);
}

function nullIfAbsent(storedValue: string | null | undefined): string | null {
  return storedValue ?? null;
}

function storedIntegerOf(storedValue: string | null): number | undefined {
  const integer = Number(storedValue);
  return storedValue !== null && storedValue !== "" && Number.isSafeInteger(integer) ? integer : undefined;
}

function recordVersionOf(storedVersion: unknown): number | undefined {
  return typeof storedVersion === "string" ? storedIntegerOf(storedVersion) : undefined;
}

function storedIntegersByFieldOf<Field extends string>(
  storedValuesByField: Record<Field, string | null | undefined>,
): Record<Field, number> | undefined {
  const integersByField: Partial<Record<Field, number>> = {};
  for (const field of Object.keys(storedValuesByField) as Field[]) {
    const integer = storedIntegerOf(nullIfAbsent(storedValuesByField[field]));
    if (integer === undefined) {
      return undefined;
    }
    integersByField[field] = integer;
  }
  return integersByField as Record<Field, number>;
}

function lockedRunOf(reply: unknown): LockedRun | undefined {
  if (!isStoredValueList(reply) || reply.length !== 16) {
    return undefined;
  }
  const [lockToken, runId, task, payload, ...storedFields] = reply.map(nullIfAbsent);
  const [attempt, retries, replays, isResume, dispatchedAt, startedAt, lockedUntil] = storedFields;
  const [scheduledAt, eventName, eventId, concurrencyKey, sleepsWokenAt] = storedFields.slice(7);
  const integersByField = storedIntegersByFieldOf({ attempt, retries, replays, dispatchedAt, startedAt, lockedUntil });
  if (lockToken == null || runId == null || task == null || payload == null || integersByField === undefined) {
    return undefined;
  }
  const storedScheduledAt = storedIntegerOf(nullIfAbsent(scheduledAt));
  const storedSleepsWokenAt = storedIntegerOf(nullIfAbsent(sleepsWokenAt));
  return {
    lockToken,
    runId,
    task,
    payload,
    ...integersByField,
    isResume: isResume === "1",
    ...(storedScheduledAt === undefined ? {} : { scheduledAt: storedScheduledAt }),
    ...(eventName == null || eventId == null ? {} : { sourceEvent: { eventName, eventId } }),
    ...(concurrencyKey == null ? {} : { concurrencyKey }),
    ...(storedSleepsWokenAt === undefined ? {} : { sleepsWokenAt: storedSleepsWokenAt }),
  };
}

function runOriginOf(storedOrigin: readonly (string | null | undefined)[]): RunOrigin {
  const [storedScheduledAt, eventName, eventId] = storedOrigin.map(nullIfAbsent);
  const scheduledAt = storedIntegerOf(nullIfAbsent(storedScheduledAt));
  return {
    ...(scheduledAt === undefined ? {} : { scheduledAt }),
    ...(eventName == null || eventId == null ? {} : { sourceEvent: { eventName, eventId } }),
  };
}

function parkedRunOf(reply: unknown): ParkedRun | undefined {
  if (!isStoredValueList(reply) || reply.length !== 6) {
    return undefined;
  }
  const [runId, task, storedAttempt, ...storedOrigin] = reply.map(nullIfAbsent);
  const attempt = storedIntegerOf(nullIfAbsent(storedAttempt));
  if (runId == null || task == null || attempt === undefined) {
    return undefined;
  }
  return { runId, task, attempt, ...runOriginOf(storedOrigin) };
}

function definedElementsOf<Element>(elements: readonly (Element | undefined)[]): Element[] | undefined {
  return elements.every((element) => element !== undefined) ? (elements as Element[]) : undefined;
}

function unknownVersionReplyOf(reply: unknown): UnknownVersionReply | undefined {
  if (Array.isArray(reply) && reply[0] === "unknownVersion" && reply.length <= 2) {
    return { kind: "unknownVersion", recordVersion: recordVersionOf(reply[1]) };
  }
  return undefined;
}

function fetchReplyOf(reply: unknown): FetchReply {
  const unknownVersionReply = unknownVersionReplyOf(reply);
  if (unknownVersionReply !== undefined) {
    return unknownVersionReply;
  }
  if (Array.isArray(reply) && reply.length === 4 && reply[0] === "fetched") {
    const [, lockedRunReplies, parkedRunReplies, storedVersions]: unknown[] = reply;
    if (Array.isArray(lockedRunReplies) && Array.isArray(parkedRunReplies) && isStringList(storedVersions)) {
      const lockedRuns = definedElementsOf(lockedRunReplies.map(lockedRunOf));
      const parkedRuns = definedElementsOf(parkedRunReplies.map(parkedRunOf));
      if (lockedRuns !== undefined && parkedRuns !== undefined) {
        const passedOverRecordVersions = storedVersions
          .map(recordVersionOf)
          .filter((recordVersion) => recordVersion !== undefined);
        return { kind: "fetched", lockedRuns, parkedRuns, passedOverRecordVersions };
      }
    }
  }
  throw unreadableReplyError("fetch", reply);
}

function lockRenewalOf(reply: unknown): LockRenewal | undefined {
  if (isStaleReply(reply)) {
    return { kind: "stale" };
  }
  if (Array.isArray(reply) && reply.length === 2 && reply[0] === "renewed" && Number.isSafeInteger(reply[1])) {
    return { kind: "renewed", lockedUntil: Number(reply[1]) };
  }
  return undefined;
}

function renewalsOf(reply: unknown): LockRenewal[] {
  const renewals = Array.isArray(reply) ? definedElementsOf(reply.map(lockRenewalOf)) : undefined;
  if (renewals === undefined) {
    throw unreadableReplyError("renew", reply);
  }
  return renewals;
}

const SETTLE_REPLIES_BY_KIND: Readonly<Record<string, SettleReply>> = {
  yielded: { kind: "yielded" },
  handedBack: { kind: "handedBack" },
  stale: { kind: "stale" },
  already: { kind: "already" },
};

const SETTLE_REPLIES_OF_INSTANT_BY_KIND: Readonly<Record<string, (instant: EpochMilliseconds) => SettleReply>> = {
  completed: (finishedAt) => ({ kind: "completed", finishedAt }),
  backingOff: (nextAttemptAt) => ({ kind: "backingOff", nextAttemptAt }),
  suspended: (resumesAt) => ({ kind: "suspended", resumesAt }),
};

const IS_FAILURE_REASON_BY_REASON: Readonly<Record<FailureReason, true>> = {
  "retries exhausted": true,
  "non-retriable": true,
  stalled: true,
  "unknown task": true,
};

function isFailureReason(value: unknown): value is FailureReason {
  return typeof value === "string" && Object.hasOwn(IS_FAILURE_REASON_BY_REASON, value);
}

function settleReplyOf(reply: unknown): SettleReply {
  const unknownVersionReply = unknownVersionReplyOf(reply);
  if (unknownVersionReply !== undefined) {
    return unknownVersionReply;
  }
  if (Array.isArray(reply) && reply.length === 1 && typeof reply[0] === "string") {
    const settleReply = SETTLE_REPLIES_BY_KIND[reply[0]];
    if (settleReply !== undefined) {
      return settleReply;
    }
  }
  if (Array.isArray(reply) && reply.length === 2 && typeof reply[0] === "string" && Number.isSafeInteger(reply[1])) {
    const settleReplyOfInstant = SETTLE_REPLIES_OF_INSTANT_BY_KIND[reply[0]];
    if (settleReplyOfInstant !== undefined) {
      return settleReplyOfInstant(Number(reply[1]));
    }
  }
  if (
    Array.isArray(reply) &&
    reply.length === 3 &&
    reply[0] === "failed" &&
    isFailureReason(reply[1]) &&
    Number.isSafeInteger(reply[2])
  ) {
    return { kind: "failed", reason: reply[1], finishedAt: Number(reply[2]) };
  }
  throw unreadableReplyError("settle", reply);
}

function stalledRunOf(reply: unknown): StalledRun | undefined {
  if (!isStoredValueList(reply) || reply.length !== 7) {
    return undefined;
  }
  const [runId, task, storedAttempt, storedStalls, ...storedOrigin] = reply.map(nullIfAbsent);
  const integersByField = storedIntegersByFieldOf({ attempt: storedAttempt, stalls: storedStalls });
  return runId == null || task == null || integersByField === undefined
    ? undefined
    : { runId, task, ...integersByField, ...runOriginOf(storedOrigin) };
}

function thirdStallOf(reply: unknown): ThirdStall | undefined {
  if (!isStoredValueList(reply) || reply.length !== 10) {
    return undefined;
  }
  const [runId, task, attempt, expiredLockToken, replays, dispatchedAt, startedAt, ...storedOrigin] =
    reply.map(nullIfAbsent);
  const integersByField = storedIntegersByFieldOf({ attempt, replays, dispatchedAt, startedAt });
  return runId == null || task == null || expiredLockToken == null || integersByField === undefined
    ? undefined
    : { runId, task, expiredLockToken, ...integersByField, ...runOriginOf(storedOrigin) };
}

function promoteReplyOf(reply: unknown): PromoteReply {
  const unknownVersionReply = unknownVersionReplyOf(reply);
  if (unknownVersionReply !== undefined) {
    return unknownVersionReply;
  }
  if (Array.isArray(reply) && reply.length === 4 && reply[0] === "promoted" && Number.isSafeInteger(reply[1])) {
    const [, promotedRunCount, stalledRunReplies, thirdStallReplies]: unknown[] = reply;
    if (Array.isArray(stalledRunReplies) && Array.isArray(thirdStallReplies)) {
      const stalledRuns = definedElementsOf(stalledRunReplies.map(stalledRunOf));
      const thirdStalls = definedElementsOf(thirdStallReplies.map(thirdStallOf));
      if (stalledRuns !== undefined && thirdStalls !== undefined) {
        return { kind: "promoted", promotedRunCount: Number(promotedRunCount), stalledRuns, thirdStalls };
      }
    }
  }
  throw unreadableReplyError("promote", reply);
}

function isStaleReply(reply: unknown): reply is ["stale"] {
  return Array.isArray(reply) && reply.length === 1 && reply[0] === "stale";
}

function fenceRefusalReplyOf(reply: unknown): StaleReply | UnknownVersionReply | undefined {
  return isStaleReply(reply) ? { kind: "stale" } : unknownVersionReplyOf(reply);
}

function saveStepReplyOf(reply: unknown): SaveStepReply {
  const fenceRefusalReply = fenceRefusalReplyOf(reply);
  if (fenceRefusalReply !== undefined) {
    return fenceRefusalReply;
  }
  if (Array.isArray(reply) && reply.length === 1 && reply[0] === "saved") {
    return { kind: "saved", resumesAt: undefined };
  }
  if (
    Array.isArray(reply) &&
    reply.length === 3 &&
    reply[0] === "saved" &&
    Number.isSafeInteger(reply[1]) &&
    (reply[2] === 0 || reply[2] === 1)
  ) {
    return { kind: "saved", resumesAt: Number(reply[1]), isDue: reply[2] === 1 };
  }
  throw unreadableReplyError("saveStep", reply);
}

function readStepReplyOf(reply: unknown): ReadStepReply {
  const fenceRefusalReply = fenceRefusalReplyOf(reply);
  if (fenceRefusalReply !== undefined) {
    return fenceRefusalReply;
  }
  if (isStringList(reply) && reply[0] === "read" && reply.length <= 2) {
    return { kind: "read", savedValue: reply[1] };
  }
  throw unreadableReplyError("readStep", reply);
}

function stepArgumentOf(step: StepToSave): ScriptArgument {
  if (step.kind === "value") {
    return step.value;
  }
  return step.kind === "sleep" ? step.durationInMilliseconds : step.resumesAt;
}

const FIRE_REFUSALS_BY_KIND: Readonly<Record<string, FireReply>> = {
  already: { kind: "already" },
  notDue: { kind: "notDue" },
  noSchedule: { kind: "noSchedule" },
};

function fireReplyOf(reply: unknown): FireReply {
  const unknownVersionReply = unknownVersionReplyOf(reply);
  if (unknownVersionReply !== undefined) {
    return unknownVersionReply;
  }
  if (Array.isArray(reply) && reply.length === 1 && typeof reply[0] === "string") {
    const fireRefusal = FIRE_REFUSALS_BY_KIND[reply[0]];
    if (fireRefusal !== undefined) {
      return fireRefusal;
    }
  }
  if (
    Array.isArray(reply) &&
    reply.length === 3 &&
    reply[0] === "fired" &&
    typeof reply[1] === "string" &&
    (reply[2] === 0 || reply[2] === 1)
  ) {
    return { kind: "fired", runId: reply[1], isRunCreated: reply[2] === 1 };
  }
  throw unreadableReplyError("fire", reply);
}

function scheduleSummaryOf(reply: unknown): ScheduleSummary | undefined {
  if (!isStringList(reply) || reply.length !== 3) {
    return undefined;
  }
  const [task, cron, timezone] = reply as [string, string, string];
  return { task, cron, timezone };
}

function scheduleSummariesOf(reply: unknown): ScheduleSummary[] | undefined {
  return Array.isArray(reply) ? definedElementsOf(reply.map(scheduleSummaryOf)) : undefined;
}

function syncSchedulesReplyOf(reply: unknown): SyncSchedulesReply {
  const unknownVersionReply = unknownVersionReplyOf(reply);
  if (unknownVersionReply !== undefined) {
    return unknownVersionReply;
  }
  if (Array.isArray(reply) && reply.length === 4 && reply[0] === "synced") {
    const [, addedReplies, changedReplies, removedReplies]: unknown[] = reply;
    const added = scheduleSummariesOf(addedReplies);
    const changed = scheduleSummariesOf(changedReplies);
    const removed = scheduleSummariesOf(removedReplies);
    if (added !== undefined && changed !== undefined && removed !== undefined) {
      return { kind: "synced", added, changed, removed };
    }
  }
  throw unreadableReplyError("syncSchedules", reply);
}

function replayReplyOf(reply: unknown): ReplayReply {
  const unknownVersionReply = unknownVersionReplyOf(reply);
  if (unknownVersionReply !== undefined) {
    return unknownVersionReply;
  }
  if (Array.isArray(reply) && reply.length === 1 && reply[0] === "notFound") {
    return { kind: "notFound" };
  }
  if (Array.isArray(reply) && reply.length === 2 && reply[0] === "wrongState" && isRunState(reply[1])) {
    return { kind: "wrongState", state: reply[1] };
  }
  if (Array.isArray(reply) && reply.length === 2 && reply[0] === "replayed" && typeof reply[1] === "string") {
    return { kind: "replayed", task: reply[1] };
  }
  throw unreadableReplyError("replay", reply);
}

function pruneReplyOf(reply: unknown): PruneReply {
  if (Array.isArray(reply) && reply.length === 2 && reply[0] === "pruned" && Number.isSafeInteger(reply[1])) {
    return { kind: "pruned", prunedRunCount: Number(reply[1]) };
  }
  throw unreadableReplyError("prune", reply);
}

function pruneArgumentsOf({ completedRetention, failedRetention }: PruneRequest): ScriptArgument[] {
  return [
    completedRetention.maxAgeInMilliseconds,
    completedRetention.maxRunCount,
    failedRetention.maxAgeInMilliseconds,
    failedRetention.maxRunCount,
    MOST_RUNS_PRUNED_PER_PRUNE,
    NEWER_VERSION_RECORDS_PASSED_OVER_PER_PRUNE,
    EXPIRED_DEDUP_ENTRIES_TRIMMED_PER_PRUNE,
  ];
}

function makeDueReplyOf(reply: unknown): MakeDueReply {
  if (Array.isArray(reply) && reply.length === 2 && reply[0] === "madeDue" && Number.isSafeInteger(reply[1])) {
    return { kind: "madeDue", madeDueRunCount: Number(reply[1]) };
  }
  throw unreadableReplyError("makeDue", reply);
}

function enqueueReplyOf(reply: unknown): EnqueueReply {
  const fenceRefusalReply = fenceRefusalReplyOf(reply);
  if (fenceRefusalReply !== undefined) {
    return fenceRefusalReply;
  }
  if (Array.isArray(reply) && reply.length === 1 && reply[0] === "duplicate") {
    return { kind: "duplicate" };
  }
  if (isStringList(reply) && reply.length === 2 && reply[0] === "alreadySent") {
    return { kind: "alreadySent", savedValue: reply[1]! };
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

export function errorTextCutOf(text: string): string {
  const characters = Array.from(text);
  if (characters.length <= MOST_ERROR_TEXT_CHARACTERS) {
    return text;
  }
  return `${characters.slice(0, MOST_ERROR_TEXT_CHARACTERS).join("")}${CUT_ERROR_TEXT_MARKER}`;
}

type TextAroundFailedAt = [textBefore: string, textAfter: string];

const NO_TEXT_AROUND_FAILED_AT: TextAroundFailedAt = ["", ""];

function textAroundFailedAtOf(record: object): TextAroundFailedAt {
  const sentinel = randomUUID();
  const [textBefore, textAfter, ...textsAfterAnotherSentinel] = JSON.stringify({
    ...record,
    failedAt: sentinel,
  }).split(sentinel);
  if (textBefore === undefined || textAfter === undefined || textsAfterAnotherSentinel.length > 0) {
    throw new Error(`The serialized record does not hold its failedAt sentinel exactly once`);
  }
  return [textBefore, textAfter];
}

function errorWithTextsCutOf<CutError extends { message: string; stack: string }>(error: CutError): CutError {
  return { ...error, message: errorTextCutOf(error.message), stack: errorTextCutOf(error.stack) };
}

function failureNoticeArgumentsOf(failureNotice: FailureNotice | undefined): ScriptArgument[] {
  if (failureNotice === undefined || failureNotice.runs.length === 0) {
    return [...NO_TEXT_AROUND_FAILED_AT, "", "", 0];
  }
  const { payload, runs, eventDedup } = failureNotice;
  return [
    ...textAroundFailedAtOf({ ...payload, error: errorWithTextsCutOf(payload.error) }),
    eventDedup.eventName,
    eventDedup.eventId,
    runs.length,
    ...runs.flatMap((run) => runArgumentsOf({ ...run, payload: "" })),
  ];
}

function settleArgumentsOf(outcome: SettleOutcome): ScriptArgument[] {
  return [
    ...("error" in outcome ? textAroundFailedAtOf(errorWithTextsCutOf(outcome.error)) : NO_TEXT_AROUND_FAILED_AT),
    outcome.kind === "retry" ? outcome.backoffInMilliseconds : "",
    outcome.kind === "suspend" ? outcome.resumesAt : "",
    outcome.kind === "fail" ? outcome.reason : "",
    outcome.kind === "handBack" ? UNKNOWN_TASK_FAILURE_DELAY_IN_MILLISECONDS : "",
    ...failureNoticeArgumentsOf("failureNotice" in outcome ? outcome.failureNotice : undefined),
  ];
}

type ScriptRunner = (scriptArguments: readonly ScriptArgument[]) => Promise<unknown>;

type PipelinedScriptRunnerWithKeys = ((
  scriptKeys: readonly string[],
  scriptArguments: readonly ScriptArgument[],
) => Promise<unknown>) & {
  appendTo: (
    pipeline: ChainableCommander,
    scriptKeys: readonly string[],
    scriptArguments: readonly ScriptArgument[],
  ) => void;
};

type PipelinedScriptRunner = ScriptRunner & {
  appendTo: (pipeline: ChainableCommander, scriptArguments: readonly ScriptArgument[]) => void;
};

const PROMOTE_ARGUMENTS: readonly ScriptArgument[] = [
  MOST_DUE_RUNS_PROMOTED_PER_PROMOTE,
  NEWER_VERSION_RECORDS_PASSED_OVER_PER_PROMOTE,
];

function renewArgumentsOf({ heldLocks, lockDurationInMilliseconds }: RenewRequest): ScriptArgument[] {
  return [
    lockDurationInMilliseconds,
    heldLocks.length,
    ...heldLocks.flatMap(({ runId, lockToken }) => [runId, lockToken]),
  ];
}

function taskNamesOf(reply: unknown): string[] {
  if (!isStringList(reply)) {
    throw new Error(`The SMEMBERS of the task names sent a reply the engine cannot read: ${JSON.stringify(reply)}`);
  }
  return reply;
}

function serverInstantOf(reply: unknown): EpochMilliseconds {
  if (Array.isArray(reply) && reply.length === 2) {
    const [seconds, microseconds] = reply.map(Number);
    if (Number.isSafeInteger(seconds) && Number.isSafeInteger(microseconds)) {
      return seconds! * 1000 + Math.floor(microseconds! / 1000);
    }
  }
  throw new Error(`The TIME of the server sent a reply the engine cannot read: ${JSON.stringify(reply)}`);
}

type PipelineResult = [error: Error | null, reply: unknown];

function tickCommandReplyOf<Reply>(
  [error, reply]: PipelineResult,
  readReply: (reply: unknown) => Reply,
): TickCommandReply<Reply> {
  if (error !== null) {
    return { kind: "failed", error };
  }
  try {
    return { kind: "replied", reply: readReply(reply) };
  } catch (readError) {
    return { kind: "failed", error: readError as Error };
  }
}

const SCHEDULE_FIELDS_READ_BY_THE_TICK = ["version", "cron", "timezone", "nextFiringAt"] as const;

function dueScheduleOf(task: string, storedFields: unknown, serverInstant: EpochMilliseconds): DueSchedule | undefined {
  if (!isStoredValueList(storedFields) || storedFields.length !== SCHEDULE_FIELDS_READ_BY_THE_TICK.length) {
    throw unreadableScheduleError(task, storedFields);
  }
  const [storedVersion, cron, timezone, storedNextFiringAt] = storedFields;
  if (recordVersionOf(storedVersion) !== RECORD_VERSION) {
    return undefined;
  }
  const nextFiringAt = storedIntegerOf(nullIfAbsent(storedNextFiringAt));
  if (cron == null || timezone == null || nextFiringAt === undefined) {
    throw unreadableScheduleError(task, storedFields);
  }
  return nextFiringAt <= serverInstant ? { task, cron, timezone, nextFiringAt } : undefined;
}

function unreadableScheduleError(task: string, storedFields: unknown): Error {
  return new Error(`The schedule record of task "${task}" cannot be read: ${JSON.stringify(storedFields)}`);
}

function dueSchedulesReplyOf(
  [timeResult, ...scheduleResults]: readonly PipelineResult[],
  scheduledTaskNames: readonly string[],
): TickCommandReply<DueSchedulesReply> {
  const error = [timeResult, ...scheduleResults].find((result) => result?.[0] != null)?.[0];
  if (error != null) {
    return { kind: "failed", error };
  }
  return tickCommandReplyOf([null, timeResult?.[1]], (timeReply) => {
    const serverInstant = serverInstantOf(timeReply);
    const dueSchedules = scheduledTaskNames.flatMap((task, position) => {
      const dueSchedule = dueScheduleOf(task, scheduleResults[position]?.[1], serverInstant);
      return dueSchedule === undefined ? [] : [dueSchedule];
    });
    return { serverInstant, dueSchedules };
  });
}

function scheduledTaskNamesOf(request: TickRequest): readonly string[] {
  return request.scheduledTaskNames ?? [];
}

function tickReplyOf(results: PipelineResult[] | null, request: TickRequest): TickReply {
  if (results === null) {
    throw new Error("The tick pipeline was discarded before it ran");
  }
  const remainingResults = [...results];
  const nextResultsOf = (count: number): PipelineResult[] => {
    if (remainingResults.length < count) {
      throw new Error(`The tick pipeline answered ${String(results.length)} commands, fewer than it sent`);
    }
    return remainingResults.splice(0, count);
  };
  const nextCommandReplyOf = <Reply>(readReply: (reply: unknown) => Reply): TickCommandReply<Reply> =>
    tickCommandReplyOf(nextResultsOf(1)[0]!, readReply);
  const scheduledTaskNames = scheduledTaskNamesOf(request);
  return {
    promote: request.isPromoteIncluded === false ? undefined : nextCommandReplyOf(promoteReplyOf),
    renewals: nextCommandReplyOf(renewalsOf),
    taskNames: nextCommandReplyOf(taskNamesOf),
    prune: request.prune === undefined ? undefined : nextCommandReplyOf(pruneReplyOf),
    dueSchedules:
      scheduledTaskNames.length === 0
        ? undefined
        : dueSchedulesReplyOf(nextResultsOf(1 + scheduledTaskNames.length), scheduledTaskNames),
  };
}

export function createEngine(client: Redis, prefix: string): Engine {
  const keys = engineKeysOf(prefix);
  const sharedArguments: ScriptArgument[] = [hashTaggedPrefixOf(prefix), RECORD_VERSION];
  const scriptRunnerWithKeysOf = (
    commandName: string,
    script: string,
    keyCount: number,
  ): PipelinedScriptRunnerWithKeys => {
    const command = defineScriptCommand(client, commandName, `${LUA_LIBRARY}\n${script}`, keyCount);
    const runScript = (scriptKeys: readonly string[], scriptArguments: readonly ScriptArgument[]) =>
      command.call(scriptKeys, [...sharedArguments, ...scriptArguments]);
    return Object.assign(runScript, {
      appendTo: (
        pipeline: ChainableCommander,
        scriptKeys: readonly string[],
        scriptArguments: readonly ScriptArgument[],
      ) => {
        command.appendTo(pipeline, scriptKeys, [...sharedArguments, ...scriptArguments]);
      },
    });
  };
  const scriptRunnerOf = (
    commandName: string,
    script: string,
    scriptKeys: readonly string[],
  ): PipelinedScriptRunner => {
    const runScriptWithKeys = scriptRunnerWithKeysOf(commandName, script, scriptKeys.length);
    const runScript = (scriptArguments: readonly ScriptArgument[]) => runScriptWithKeys(scriptKeys, scriptArguments);
    return Object.assign(runScript, {
      appendTo: (pipeline: ChainableCommander, scriptArguments: readonly ScriptArgument[]) => {
        runScriptWithKeys.appendTo(pipeline, scriptKeys, scriptArguments);
      },
    });
  };
  const runEnqueueScript = scriptRunnerOf("nextaskingEnqueue", ENQUEUE_SCRIPT, enqueueKeysOf(keys));
  const runFetchScript = scriptRunnerOf("nextaskingFetch", FETCH_SCRIPT, fetchKeysOf(keys));
  const runRenewScript = scriptRunnerOf("nextaskingRenew", RENEW_SCRIPT, renewKeysOf(keys));
  const runSettleScript = scriptRunnerOf("nextaskingSettle", SETTLE_SCRIPT, settleKeysOf(keys));
  const runPromoteScript = scriptRunnerOf("nextaskingPromote", PROMOTE_SCRIPT, promoteKeysOf(keys));
  const runSaveStepScript = scriptRunnerWithKeysOf("nextaskingSaveStep", SAVE_STEP_SCRIPT, 1);
  const runReadStepScript = scriptRunnerWithKeysOf("nextaskingReadStep", READ_STEP_SCRIPT, 1);
  const runMakeDueScript = scriptRunnerOf("nextaskingMakeDue", MAKE_DUE_SCRIPT, makeDueKeysOf(keys));
  const runSyncSchedulesScript = scriptRunnerOf(
    "nextaskingSyncSchedules",
    SYNC_SCHEDULES_SCRIPT,
    syncSchedulesKeysOf(keys),
  );
  const runFireScript = scriptRunnerOf("nextaskingFire", FIRE_SCRIPT, fireKeysOf(keys));
  const runReplayScript = scriptRunnerOf("nextaskingReplay", REPLAY_SCRIPT, replayKeysOf(keys));
  const runPruneScript = scriptRunnerWithKeysOf("nextaskingPrune", PRUNE_SCRIPT, 3);
  return {
    serverInstant: async () => serverInstantOf(await client.time()),
    replay: async ({ runId }) => replayReplyOf(await runReplayScript([runId])),
    prune: async (request) =>
      pruneReplyOf(await runPruneScript(pruneKeysOf(keys, request.task), pruneArgumentsOf(request))),
    fire: async ({ task, instant, instantAfter, runId, retries }) =>
      fireReplyOf(await runFireScript([task, instant, instantAfter, runId, retries])),
    syncSchedules: async ({ schedulesToSync }) =>
      syncSchedulesReplyOf(
        await runSyncSchedulesScript([
          schedulesToSync.length,
          ...schedulesToSync.flatMap(({ task, cron, timezone, nextFiringAt }) => [task, cron, timezone, nextFiringAt]),
        ]),
      ),
    makeDue: async () =>
      makeDueReplyOf(
        await runMakeDueScript([MOST_DUE_RUNS_PROMOTED_PER_PROMOTE, NEWER_VERSION_RECORDS_PASSED_OVER_PER_PROMOTE]),
      ),
    saveStep: async ({ runId, lockToken, stepId, step }) =>
      saveStepReplyOf(
        await runSaveStepScript(saveStepKeysOf(keys, runId), [
          runId,
          lockToken,
          stepId,
          step.kind,
          stepArgumentOf(step),
        ]),
      ),
    readStep: async ({ runId, lockToken, stepId }) =>
      readStepReplyOf(await runReadStepScript(readStepKeysOf(keys, runId), [runId, lockToken, stepId])),
    promote: async () => promoteReplyOf(await runPromoteScript(PROMOTE_ARGUMENTS)),
    tick: async (request) => {
      const pipeline = client.pipeline();
      if (request.isPromoteIncluded !== false) {
        runPromoteScript.appendTo(pipeline, PROMOTE_ARGUMENTS);
      }
      runRenewScript.appendTo(pipeline, renewArgumentsOf(request));
      pipeline.smembers(keys.tasks);
      if (request.prune !== undefined) {
        runPruneScript.appendTo(pipeline, pruneKeysOf(keys, request.prune.task), pruneArgumentsOf(request.prune));
      }
      const scheduledTaskNames = scheduledTaskNamesOf(request);
      if (scheduledTaskNames.length > 0) {
        pipeline.time();
        for (const task of scheduledTaskNames) {
          pipeline.hmget(keys.schedule(task), ...SCHEDULE_FIELDS_READ_BY_THE_TICK);
        }
      }
      return tickReplyOf(await pipeline.exec(), request);
    },
    settle: async ({ runId, lockToken, outcome, concurrencyLimit }) =>
      settleReplyOf(
        await runSettleScript([
          runId,
          lockToken,
          outcome.kind,
          DEDUP_WINDOW_IN_MILLISECONDS,
          concurrencyLimit ?? "",
          ...settleArgumentsOf(outcome),
        ]),
      ),
    renew: async (request) => renewalsOf(await runRenewScript(renewArgumentsOf(request))),
    fetch: async ({ lockTokens, concurrencyLimitsByTask, lockDurationInMilliseconds }) => {
      const concurrencyLimits = Object.entries(concurrencyLimitsByTask);
      return fetchReplyOf(
        await runFetchScript([
          lockDurationInMilliseconds,
          MOST_LOCK_TOKENS_PER_FETCH,
          MOST_RUNS_PARKED_PER_FETCH,
          NEWER_VERSION_RECORDS_PASSED_OVER_PER_FETCH,
          lockTokens.length,
          ...lockTokens,
          concurrencyLimits.length,
          ...concurrencyLimits.flat(),
        ]),
      );
    },
    enqueue: async ({ runs, eventDedup, parentFence }) =>
      enqueueReplyOf(
        await runEnqueueScript([
          DEDUP_WINDOW_IN_MILLISECONDS,
          EXPIRED_DEDUP_ENTRIES_TRIMMED_PER_SET,
          eventDedup?.eventName ?? "",
          eventDedup?.eventId ?? "",
          parentFence?.parentRunId ?? "",
          parentFence?.lockToken ?? "",
          parentFence?.stepId ?? "",
          parentFence?.stepValue ?? "",
          runs.length,
          ...runs.flatMap(runArgumentsOf),
        ]),
      ),
  };
}
