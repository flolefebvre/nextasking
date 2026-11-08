import type { Redis } from "ioredis";

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
import { RENEW_SCRIPT, renewKeysOf } from "./renew-script.js";
import { SETTLE_SCRIPT, settleKeysOf } from "./settle-script.js";
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
  concurrencyLimit?: number;
};

export type ImplementedSettleOutcome = Extract<SettleOutcome, { kind: "complete" | "yield" | "handBack" }>;

export type ImplementedSettleRequest = Omit<SettleRequest, "outcome"> & { outcome: ImplementedSettleOutcome };

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
  fetch: (request: FetchRequest) => Promise<FetchReply>;
  renew: (request: RenewRequest) => Promise<LockRenewal[]>;
  settle: (request: ImplementedSettleRequest) => Promise<SettleReply>;
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
  if (Array.isArray(reply) && reply.length === 1 && reply[0] === "stale") {
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
  if (Array.isArray(reply) && reply.length === 2 && reply[0] === "completed" && Number.isSafeInteger(reply[1])) {
    return { kind: "completed", finishedAt: Number(reply[1]) };
  }
  throw unreadableReplyError("settle", reply);
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

type ScriptRunner = (scriptArguments: readonly ScriptArgument[]) => Promise<unknown>;

export function createEngine(client: Redis, prefix: string): ImplementedEngine {
  const keys = engineKeysOf(prefix);
  const sharedArguments: ScriptArgument[] = [hashTaggedPrefixOf(prefix), RECORD_VERSION];
  const scriptRunnerOf = (commandName: string, script: string, scriptKeys: readonly string[]): ScriptRunner => {
    const command = defineScriptCommand(client, commandName, `${LUA_LIBRARY}\n${script}`, scriptKeys.length);
    return (scriptArguments) => command.call(scriptKeys, [...sharedArguments, ...scriptArguments]);
  };
  const runEnqueueScript = scriptRunnerOf("nextaskingEnqueue", ENQUEUE_SCRIPT, enqueueKeysOf(keys));
  const runFetchScript = scriptRunnerOf("nextaskingFetch", FETCH_SCRIPT, fetchKeysOf(keys));
  const runRenewScript = scriptRunnerOf("nextaskingRenew", RENEW_SCRIPT, renewKeysOf(keys));
  const runSettleScript = scriptRunnerOf("nextaskingSettle", SETTLE_SCRIPT, settleKeysOf(keys));
  return {
    settle: async ({ runId, lockToken, outcome, concurrencyLimit }) =>
      settleReplyOf(
        await runSettleScript([runId, lockToken, outcome.kind, DEDUP_WINDOW_IN_MILLISECONDS, concurrencyLimit ?? ""]),
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
    enqueue: async ({ runs, eventDedup }) =>
      enqueueReplyOf(
        await runEnqueueScript([
          DEDUP_WINDOW_IN_MILLISECONDS,
          EXPIRED_DEDUP_ENTRIES_TRIMMED_PER_SET,
          eventDedup?.eventName ?? "",
          eventDedup?.eventId ?? "",
          runs.length,
          ...runs.flatMap(runArgumentsOf),
        ]),
      ),
  };
}
