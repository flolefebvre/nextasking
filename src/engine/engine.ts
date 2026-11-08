import { randomUUID } from "node:crypto";

import type { Redis } from "ioredis";

import type { RunFailedPayload } from "../built-in-events.js";
import type { ConcurrencyKey } from "../definitions.js";
import type { DurationInMilliseconds, EpochMilliseconds } from "../durations.js";
import type { RunId, StepId } from "../ids.js";
import type { ErrorRecord, FailureReason, SourceEvent } from "../run-queue.js";
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
import { LUA_LIBRARY } from "./lua-library.js";
import { MAKE_DUE_SCRIPT, makeDueKeysOf } from "./make-due-script.js";
import {
  MOST_DUE_RUNS_PROMOTED_PER_PROMOTE,
  NEWER_VERSION_RECORDS_PASSED_OVER_PER_PROMOTE,
  PROMOTE_SCRIPT,
  promoteKeysOf,
} from "./promote-script.js";
import { READ_STEP_SCRIPT, readStepKeysOf } from "./read-step-script.js";
import { RENEW_SCRIPT, renewKeysOf } from "./renew-script.js";
import { SAVE_STEP_SCRIPT, saveStepKeysOf } from "./save-step-script.js";
import { SYNC_SCHEDULES_SCRIPT, syncSchedulesKeysOf } from "./sync-schedules-script.js";
import { SETTLE_SCRIPT, settleKeysOf } from "./settle-script.js";
import { defineScriptCommand } from "./script-commands.js";
import type { ScriptArgument } from "./script-commands.js";

export const RECORD_VERSION = 1;

export const DEDUP_WINDOW_IN_MILLISECONDS: DurationInMilliseconds = 24 * 60 * 60 * 1000;

export const EXPIRED_DEDUP_ENTRIES_TRIMMED_PER_SET = 100;

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

export type ImplementedEngine = Pick<
  Engine,
  "enqueue" | "fetch" | "renew" | "settle" | "promote" | "saveStep" | "readStep" | "syncSchedules" | "makeDue"
>;

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
  if (!isStoredValueList(reply) || reply.length !== 15) {
    return undefined;
  }
  const [lockToken, runId, task, payload, ...storedFields] = reply.map(nullIfAbsent);
  const [attempt, retries, replays, isResume, dispatchedAt, startedAt, lockedUntil] = storedFields;
  const [scheduledAt, eventName, eventId, concurrencyKey] = storedFields.slice(7);
  const integersByField = storedIntegersByFieldOf({ attempt, retries, replays, dispatchedAt, startedAt, lockedUntil });
  if (lockToken == null || runId == null || task == null || payload == null || integersByField === undefined) {
    return undefined;
  }
  const storedScheduledAt = storedIntegerOf(nullIfAbsent(scheduledAt));
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
  };
}

function parkedRunOf(reply: unknown): ParkedRun | undefined {
  if (!isStoredValueList(reply) || reply.length !== 3) {
    return undefined;
  }
  const [runId, task, storedAttempt] = reply;
  const attempt = storedIntegerOf(nullIfAbsent(storedAttempt));
  return runId == null || task == null || attempt === undefined ? undefined : { runId, task, attempt };
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
  if (!isStringList(reply) || reply.length !== 4) {
    return undefined;
  }
  const [runId, task, storedAttempt, storedStalls] = reply;
  const integersByField = storedIntegersByFieldOf({ attempt: storedAttempt, stalls: storedStalls });
  return runId === undefined || task === undefined || integersByField === undefined
    ? undefined
    : { runId, task, ...integersByField };
}

function thirdStallOf(reply: unknown): ThirdStall | undefined {
  if (!isStringList(reply) || reply.length !== 4) {
    return undefined;
  }
  const [runId, task, storedAttempt, expiredLockToken] = reply;
  const attempt = storedIntegerOf(nullIfAbsent(storedAttempt));
  return runId === undefined || task === undefined || attempt === undefined || expiredLockToken === undefined
    ? undefined
    : { runId, task, attempt, expiredLockToken };
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
  if (Array.isArray(reply) && reply.length === 2 && reply[0] === "saved" && Number.isSafeInteger(reply[1])) {
    return { kind: "saved", resumesAt: Number(reply[1]) };
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

type ScriptRunnerWithKeys = (
  scriptKeys: readonly string[],
  scriptArguments: readonly ScriptArgument[],
) => Promise<unknown>;

export function createEngine(client: Redis, prefix: string): ImplementedEngine {
  const keys = engineKeysOf(prefix);
  const sharedArguments: ScriptArgument[] = [hashTaggedPrefixOf(prefix), RECORD_VERSION];
  const scriptRunnerWithKeysOf = (commandName: string, script: string, keyCount: number): ScriptRunnerWithKeys => {
    const command = defineScriptCommand(client, commandName, `${LUA_LIBRARY}\n${script}`, keyCount);
    return (scriptKeys, scriptArguments) => command.call(scriptKeys, [...sharedArguments, ...scriptArguments]);
  };
  const scriptRunnerOf = (commandName: string, script: string, scriptKeys: readonly string[]): ScriptRunner => {
    const runScript = scriptRunnerWithKeysOf(commandName, script, scriptKeys.length);
    return (scriptArguments) => runScript(scriptKeys, scriptArguments);
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
  return {
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
    promote: async () =>
      promoteReplyOf(
        await runPromoteScript([MOST_DUE_RUNS_PROMOTED_PER_PROMOTE, NEWER_VERSION_RECORDS_PASSED_OVER_PER_PROMOTE]),
      ),
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
    renew: async ({ heldLocks, lockDurationInMilliseconds }) =>
      renewalsOf(
        await runRenewScript([
          lockDurationInMilliseconds,
          heldLocks.length,
          ...heldLocks.flatMap(({ runId, lockToken }) => [runId, lockToken]),
        ]),
      ),
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
