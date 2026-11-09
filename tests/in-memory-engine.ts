import type { ConcurrencyKey } from "../src/definitions.js";
import type { DurationInMilliseconds, EpochMilliseconds } from "../src/durations.js";
import { compareInSortedSetMemberOrder, storedRunIdOf } from "../src/engine/engine-keys.js";
import type { LockToken, RunIdentity, StoredRunId } from "../src/engine/engine-keys.js";
import {
  DEDUP_WINDOW_IN_MILLISECONDS,
  errorTextCutOf,
  EXPIRED_DEDUP_ENTRIES_TRIMMED_PER_PRUNE,
  EXPIRED_DEDUP_ENTRIES_TRIMMED_PER_SET,
  MOST_RUNS_PRUNED_PER_PRUNE,
  UNKNOWN_TASK_FAILURE_DELAY_IN_MILLISECONDS,
} from "../src/engine/engine.js";
import type {
  EnqueueReply,
  EnqueueRequest,
  Engine,
  FailureNotice,
  DueSchedulesReply,
  FetchReply,
  FetchRequest,
  FireReply,
  FireRequest,
  LockedRun,
  LockRenewal,
  MadeDueRun,
  MakeDueReply,
  MakeDueRequest,
  ParkedRun,
  PromoteReply,
  PruneReply,
  PruneRequest,
  ReadStepReply,
  ReadStepRequest,
  RenewRequest,
  RetentionLimit,
  RunOrigin,
  RunToCreate,
  SaveStepReply,
  SaveStepRequest,
  ScheduleSummary,
  SerializedPayload,
  SerializedStepValue,
  SettleReply,
  SettleRequest,
  StalledRun,
  SyncSchedulesReply,
  SyncSchedulesRequest,
  ThirdStall,
  TickCommandReply,
  TickReply,
  TickRequest,
} from "../src/engine/engine.js";
import { MOST_LOCK_TOKENS_PER_FETCH, MOST_RUNS_PARKED_PER_FETCH } from "../src/engine/fetch-script.js";
import { MOST_DUE_RUNS_PROMOTED_PER_PROMOTE } from "../src/engine/promote-script.js";
import type { RunId, StepId } from "../src/ids.js";
import type { ErrorRecord, FailureReason, SourceEvent } from "../src/run-queue.js";
import type { RunState } from "../src/run-states.js";

export type InMemoryEngineMethod =
  | "enqueue"
  | "fetch"
  | "renew"
  | "settle"
  | "promote"
  | "saveStep"
  | "readStep"
  | "prune"
  | "tick"
  | "fire"
  | "syncSchedules"
  | "serverInstant"
  | "makeDue";

export type InMemoryEngineCall = { method: InMemoryEngineMethod; request: unknown };

export type InMemoryStoredRun = {
  task: string;
  runId: RunId;
  state: RunState;
  attempt: number;
  retries: number;
  replays: number;
  stalls: number;
  order: number | undefined;
  payload: SerializedPayload;
  dispatchedAt: EpochMilliseconds;
  startedAt: EpochMilliseconds | undefined;
  finishedAt: EpochMilliseconds | undefined;
  lockToken: LockToken | undefined;
  lockedUntil: EpochMilliseconds | undefined;
  endedLockToken: LockToken | undefined;
  endedAs: string | undefined;
  startsAt: EpochMilliseconds | undefined;
  resumesAt: EpochMilliseconds | undefined;
  nextAttemptAt: EpochMilliseconds | undefined;
  reason: FailureReason | undefined;
  concurrencyKey: ConcurrencyKey | undefined;
  sourceEvent: SourceEvent | undefined;
  scheduledAt: EpochMilliseconds | undefined;
  firstFoundUnknownAt: EpochMilliseconds | undefined;
  sleepsWokenAt: EpochMilliseconds | undefined;
  isLockAResume: boolean | undefined;
  lockSleepsWokenAt: EpochMilliseconds | undefined;
  errors: string[];
  steps: Map<StepId, SerializedStepValue>;
};

export type HeldCalls = { release: () => void };

export type InMemoryStoredSchedule = ScheduleSummary & {
  nextFiringAt: EpochMilliseconds;
  retries: number;
  previousFiringAt: EpochMilliseconds | undefined;
};

export type InMemoryEngine = Pick<Engine, InMemoryEngineMethod> & {
  calls: readonly InMemoryEngineCall[];
  holdCalls: (method: InMemoryEngineMethod) => HeldCalls;
  waitForWakeUp: (timeoutInMilliseconds: DurationInMilliseconds, stopSignal?: AbortSignal) => Promise<void>;
  isWakeUpMarkerArmed: () => boolean;
  storedRunOf: (task: string, runId: RunId) => Readonly<InMemoryStoredRun> | undefined;
  storedScheduleOf: (task: string) => Readonly<InMemoryStoredSchedule> | undefined;
  runIdsInState: (state: RunState) => RunId[];
  expireLock: (task: string, runId: RunId) => void;
  backdateEndedRun: (task: string, runId: RunId, durationInMilliseconds: DurationInMilliseconds) => void;
  backdateFirstFoundUnknownAt: (task: string, runId: RunId, durationInMilliseconds: DurationInMilliseconds) => void;
};

export type InMemoryEngineOptions = { now?: () => EpochMilliseconds };

type DueState = "delayed" | "suspended" | "backingOff";

const DUE_INSTANT_FIELD_BY_STATE: Readonly<Record<DueState, "startsAt" | "resumesAt" | "nextAttemptAt">> = {
  delayed: "startsAt",
  suspended: "resumesAt",
  backingOff: "nextAttemptAt",
};

const SAVED_SLEEP_PATTERN = /^\d+$/;

function isDueState(state: RunState): state is DueState {
  return Object.hasOwn(DUE_INSTANT_FIELD_BY_STATE, state);
}

function dueInstantOf(storedRun: InMemoryStoredRun): EpochMilliseconds | undefined {
  return isDueState(storedRun.state) ? storedRun[DUE_INSTANT_FIELD_BY_STATE[storedRun.state]] : undefined;
}

function concurrencyScopeOf(task: string, concurrencyKey: ConcurrencyKey | undefined): string {
  return JSON.stringify([task, concurrencyKey ?? ""]);
}

function byScoreThenMember<Entry extends { score: number; member: string }>(left: Entry, right: Entry): number {
  return left.score - right.score || compareInSortedSetMemberOrder(left.member, right.member);
}

function errorWithTextsCutOf(error: ErrorRecord): ErrorRecord {
  return { ...error, message: errorTextCutOf(error.message), stack: errorTextCutOf(error.stack) };
}

function invalidArgumentError(message: string): Error {
  return new Error(`invalid argument: ${message}`);
}

export function createInMemoryEngine({ now = () => Date.now() }: InMemoryEngineOptions = {}): InMemoryEngine {
  const runsByStoredRunId = new Map<StoredRunId, InMemoryStoredRun>();
  const lockedRunByToken = new Map<LockToken, { storedRunId: StoredRunId; expiresAt: EpochMilliseconds }>();
  const slotSetsByScope = new Map<string, Set<StoredRunId>>();
  const dedupExpiryByStoredRunIdByTask = new Map<string, Map<StoredRunId, EpochMilliseconds>>();
  const dedupExpiryByEventIdByEventName = new Map<string, Map<string, EpochMilliseconds>>();
  const taskNames = new Set<string>();
  const schedulesByTask = new Map<string, InMemoryStoredSchedule>();
  const calls: InMemoryEngineCall[] = [];
  const heldCallReleasesByMethod = new Map<InMemoryEngineMethod, Promise<void>>();
  const wakeUpWaiters: (() => void)[] = [];
  let lastOrder = 0;
  let isWakeUpMarkerArmed = false;

  const slotSetOf = (task: string, concurrencyKey: ConcurrencyKey | undefined): Set<StoredRunId> => {
    const scope = concurrencyScopeOf(task, concurrencyKey);
    const slotSet = slotSetsByScope.get(scope) ?? new Set<StoredRunId>();
    slotSetsByScope.set(scope, slotSet);
    return slotSet;
  };

  const storedRunOf = (task: string, runId: RunId) => runsByStoredRunId.get(storedRunIdOf(task, runId));

  const runEntriesInState = (state: RunState, scoreOf: (storedRun: InMemoryStoredRun) => number | undefined) =>
    [...runsByStoredRunId.entries()]
      .filter(([, storedRun]) => storedRun.state === state)
      .map(([storedRunId, storedRun]) => ({ member: storedRunId, storedRun, score: scoreOf(storedRun) ?? 0 }))
      .sort(byScoreThenMember);

  const pendingRunEntries = () => runEntriesInState("pending", ({ order }) => order);

  const armWakeUpMarker = () => {
    isWakeUpMarkerArmed = true;
    const wakeUpWaiter = wakeUpWaiters.shift();
    if (wakeUpWaiter !== undefined) {
      isWakeUpMarkerArmed = false;
      wakeUpWaiter();
    }
  };

  const isLiveDedupEntry = (
    expiryByMember: Map<string, EpochMilliseconds> | undefined,
    member: string,
    instant: EpochMilliseconds,
  ) => {
    const expiresAt = expiryByMember?.get(member);
    return expiresAt !== undefined && expiresAt > instant;
  };

  const trimExpiredDedupEntries = (
    expiryByMember: Map<string, EpochMilliseconds> | undefined,
    instant: EpochMilliseconds,
    mostTrimmed: number,
  ) => {
    const expiredMembers = [...(expiryByMember?.entries() ?? [])]
      .filter(([, expiresAt]) => expiresAt <= instant)
      .map(([member, expiresAt]) => ({ member, score: expiresAt }))
      .sort(byScoreThenMember)
      .slice(0, mostTrimmed);
    for (const { member } of expiredMembers) {
      expiryByMember?.delete(member);
    }
  };

  const enterPending = (storedRun: InMemoryStoredRun, keptOrder: number | undefined) => {
    if (keptOrder === undefined) {
      lastOrder += 1;
    }
    storedRun.state = "pending";
    storedRun.order = keptOrder ?? lastOrder;
    armWakeUpMarker();
  };

  const enterDueState = (storedRun: InMemoryStoredRun, state: DueState, dueInstant: EpochMilliseconds) => {
    storedRun.state = state;
    storedRun[DUE_INSTANT_FIELD_BY_STATE[state]] = dueInstant;
  };

  const releaseSlotAndWakeParkedRuns = (
    { task, runId, concurrencyKey }: InMemoryStoredRun,
    concurrencyLimit: number | undefined,
  ) => {
    const slotSet = slotSetOf(task, concurrencyKey);
    slotSet.delete(storedRunIdOf(task, runId));
    let wokenRunCount = Number.POSITIVE_INFINITY;
    if (concurrencyLimit !== undefined) {
      wokenRunCount = concurrencyLimit - slotSet.size;
      if (wokenRunCount <= 0) {
        return;
      }
    }
    const waitingRunEntries = runEntriesInState("waitingForConcurrencySlot", ({ order }) => order).filter(
      ({ storedRun }) => storedRun.task === task && storedRun.concurrencyKey === concurrencyKey,
    );
    for (const { storedRun } of waitingRunEntries.slice(0, wokenRunCount)) {
      enterPending(storedRun, storedRun.order);
    }
  };

  const endLock = (storedRun: InMemoryStoredRun, endedAs: string, concurrencyLimit: number | undefined) => {
    if (storedRun.lockToken !== undefined) {
      lockedRunByToken.delete(storedRun.lockToken);
    }
    storedRun.endedLockToken = storedRun.lockToken;
    storedRun.endedAs = endedAs;
    storedRun.lockToken = undefined;
    storedRun.lockedUntil = undefined;
    releaseSlotAndWakeParkedRuns(storedRun, concurrencyLimit);
  };

  const createRun = (run: RunToCreate, instant: EpochMilliseconds) => {
    const storedRun: InMemoryStoredRun = {
      task: run.task,
      runId: run.runId,
      state: "pending",
      attempt: 1,
      retries: run.retries,
      replays: 0,
      stalls: 0,
      order: undefined,
      payload: run.payload,
      dispatchedAt: instant,
      startedAt: undefined,
      finishedAt: undefined,
      lockToken: undefined,
      lockedUntil: undefined,
      endedLockToken: undefined,
      endedAs: undefined,
      startsAt: undefined,
      resumesAt: undefined,
      nextAttemptAt: undefined,
      reason: undefined,
      concurrencyKey: run.concurrencyKey,
      sourceEvent: run.sourceEvent,
      scheduledAt: undefined,
      firstFoundUnknownAt: undefined,
      sleepsWokenAt: undefined,
      isLockAResume: undefined,
      lockSleepsWokenAt: undefined,
      errors: [],
      steps: new Map(),
    };
    runsByStoredRunId.set(storedRunIdOf(run.task, run.runId), storedRun);
    taskNames.add(run.task);
    const delayInMilliseconds = run.delayInMilliseconds ?? 0;
    if (delayInMilliseconds > 0) {
      enterDueState(storedRun, "delayed", instant + delayInMilliseconds);
    } else {
      enterPending(storedRun, undefined);
    }
  };

  const dedupExpiryByMemberOf = <Name>(expiryByMemberByName: Map<Name, Map<string, EpochMilliseconds>>, name: Name) => {
    const expiryByMember = expiryByMemberByName.get(name) ?? new Map<string, EpochMilliseconds>();
    expiryByMemberByName.set(name, expiryByMember);
    return expiryByMember;
  };

  const enqueueRuns = (
    runs: readonly RunToCreate[],
    eventDedup: SourceEvent | undefined,
    instant: EpochMilliseconds,
  ) => {
    const createdRuns: RunIdentity[] = [];
    const skippedRuns: RunIdentity[] = [];
    for (const run of runs) {
      const storedRunId = storedRunIdOf(run.task, run.runId);
      const runIdentity = { task: run.task, runId: run.runId };
      if (
        runsByStoredRunId.has(storedRunId) ||
        isLiveDedupEntry(dedupExpiryByStoredRunIdByTask.get(run.task), storedRunId, instant)
      ) {
        skippedRuns.push(runIdentity);
      } else {
        createRun(run, instant);
        createdRuns.push(runIdentity);
      }
    }
    if (eventDedup !== undefined) {
      dedupExpiryByMemberOf(dedupExpiryByEventIdByEventName, eventDedup.eventName).set(
        eventDedup.eventId,
        instant + DEDUP_WINDOW_IN_MILLISECONDS,
      );
    }
    return { createdRuns, skippedRuns };
  };

  const currentlyLockedRunOf = (
    storedRun: InMemoryStoredRun | undefined,
    lockToken: LockToken,
  ): InMemoryStoredRun | undefined =>
    storedRun?.state === "executing" && storedRun.lockToken === lockToken ? storedRun : undefined;

  const lockedRunReplyOf = (
    storedRun: InMemoryStoredRun,
    isResume: boolean,
    sleepsWokenAt: EpochMilliseconds | undefined,
  ): LockedRun => ({
    lockToken: storedRun.lockToken ?? "",
    runId: storedRun.runId,
    task: storedRun.task,
    payload: storedRun.payload,
    attempt: storedRun.attempt,
    retries: storedRun.retries,
    replays: storedRun.replays,
    isResume,
    dispatchedAt: storedRun.dispatchedAt,
    startedAt: storedRun.startedAt ?? 0,
    lockedUntil: storedRun.lockedUntil ?? 0,
    ...runOriginOf(storedRun),
    ...(storedRun.concurrencyKey === undefined ? {} : { concurrencyKey: storedRun.concurrencyKey }),
    ...(sleepsWokenAt === undefined ? {} : { sleepsWokenAt }),
  });

  const runOriginOf = (storedRun: InMemoryStoredRun): RunOrigin => ({
    ...(storedRun.scheduledAt === undefined ? {} : { scheduledAt: storedRun.scheduledAt }),
    ...(storedRun.sourceEvent === undefined ? {} : { sourceEvent: storedRun.sourceEvent }),
  });

  const parkedRunReplyOf = (storedRun: InMemoryStoredRun): ParkedRun => ({
    runId: storedRun.runId,
    task: storedRun.task,
    attempt: storedRun.attempt,
    ...runOriginOf(storedRun),
  });

  const enqueue = async ({ runs, eventDedup, parentFence }: EnqueueRequest): Promise<EnqueueReply> => {
    if (runs.length === 0) {
      throw invalidArgumentError("an enqueue creates at least one run");
    }
    const parentRun =
      parentFence === undefined
        ? undefined
        : currentlyLockedRunOf(storedRunOf(parentFence.parentTask, parentFence.parentRunId), parentFence.lockToken);
    if (parentFence !== undefined) {
      if (parentRun === undefined) {
        return { kind: "stale" };
      }
      const savedValue = parentRun.steps.get(parentFence.stepId);
      if (savedValue !== undefined) {
        return { kind: "alreadySent", savedValue };
      }
    }
    const instant = now();
    if (
      eventDedup !== undefined &&
      isLiveDedupEntry(dedupExpiryByEventIdByEventName.get(eventDedup.eventName), eventDedup.eventId, instant)
    ) {
      return { kind: "duplicate" };
    }
    if (eventDedup !== undefined) {
      trimExpiredDedupEntries(
        dedupExpiryByEventIdByEventName.get(eventDedup.eventName),
        instant,
        EXPIRED_DEDUP_ENTRIES_TRIMMED_PER_SET,
      );
    }
    for (const task of new Set(runs.map((run) => run.task))) {
      trimExpiredDedupEntries(dedupExpiryByStoredRunIdByTask.get(task), instant, EXPIRED_DEDUP_ENTRIES_TRIMMED_PER_SET);
    }
    const { createdRuns, skippedRuns } = enqueueRuns(runs, eventDedup, instant);
    if (parentFence !== undefined && parentRun !== undefined) {
      parentRun.steps.set(parentFence.stepId, parentFence.stepValue);
      parentRun.stalls = 0;
    }
    return { kind: "enqueued", createdRuns, skippedRuns };
  };

  const fetch = async ({
    lockTokens,
    concurrencyLimitsByTask,
    lockDurationInMilliseconds,
  }: FetchRequest): Promise<FetchReply> => {
    if (lockTokens.length === 0) {
      throw invalidArgumentError("a fetch carries at least one lock token");
    }
    if (lockTokens.length > MOST_LOCK_TOKENS_PER_FETCH) {
      throw invalidArgumentError(`a fetch carries at most ${String(MOST_LOCK_TOKENS_PER_FETCH)} lock tokens`);
    }
    if (lockTokens.some((lockToken) => lockToken === "") || new Set(lockTokens).size !== lockTokens.length) {
      throw invalidArgumentError("lock tokens must be distinct and not empty");
    }
    const instant = now();
    const lockedRuns: LockedRun[] = [];
    const parkedRuns: ParkedRun[] = [];
    const freshLockTokens: LockToken[] = [];
    for (const lockToken of lockTokens) {
      const lockedRun = lockedRunByToken.get(lockToken);
      const storedRun =
        lockedRun !== undefined && lockedRun.expiresAt > instant
          ? currentlyLockedRunOf(runsByStoredRunId.get(lockedRun.storedRunId), lockToken)
          : undefined;
      if (storedRun !== undefined) {
        lockedRuns.push(lockedRunReplyOf(storedRun, storedRun.isLockAResume === true, storedRun.lockSleepsWokenAt));
      } else {
        freshLockTokens.push(lockToken);
      }
    }
    let usedLockTokenCount = 0;
    while (usedLockTokenCount < freshLockTokens.length && parkedRuns.length < MOST_RUNS_PARKED_PER_FETCH) {
      const [headEntry] = pendingRunEntries();
      if (headEntry === undefined) {
        break;
      }
      const { member: storedRunId, storedRun } = headEntry;
      const concurrencyLimit = concurrencyLimitsByTask[storedRun.task];
      const slotSet = slotSetOf(storedRun.task, storedRun.concurrencyKey);
      if (concurrencyLimit !== undefined && slotSet.size >= concurrencyLimit) {
        storedRun.state = "waitingForConcurrencySlot";
        parkedRuns.push(parkedRunReplyOf(storedRun));
      } else {
        const lockToken = freshLockTokens[usedLockTokenCount]!;
        usedLockTokenCount += 1;
        const isResume = storedRun.resumesAt !== undefined;
        const { sleepsWokenAt } = storedRun;
        storedRun.resumesAt = undefined;
        storedRun.sleepsWokenAt = undefined;
        storedRun.isLockAResume = isResume;
        storedRun.lockSleepsWokenAt = sleepsWokenAt;
        storedRun.state = "executing";
        storedRun.lockToken = lockToken;
        storedRun.lockedUntil = instant + lockDurationInMilliseconds;
        storedRun.startedAt = instant;
        slotSet.add(storedRunId);
        lockedRunByToken.set(lockToken, { storedRunId, expiresAt: instant + lockDurationInMilliseconds });
        lockedRuns.push(lockedRunReplyOf(storedRun, isResume, sleepsWokenAt));
      }
    }
    if (pendingRunEntries().length > 0) {
      armWakeUpMarker();
    }
    return { kind: "fetched", lockedRuns, parkedRuns, passedOverRecordVersions: [] };
  };

  const renew = async ({ heldLocks, lockDurationInMilliseconds }: RenewRequest): Promise<LockRenewal[]> => {
    const instant = now();
    const lockedUntil = instant + lockDurationInMilliseconds;
    return heldLocks.map(({ task, runId, lockToken }) => {
      const storedRun = currentlyLockedRunOf(storedRunOf(task, runId), lockToken);
      if (storedRun === undefined) {
        return { kind: "stale" };
      }
      storedRun.lockedUntil = lockedUntil;
      lockedRunByToken.set(lockToken, { storedRunId: storedRunIdOf(task, runId), expiresAt: lockedUntil });
      return { kind: "renewed", lockedUntil };
    });
  };

  const failRun = (
    storedRun: InMemoryStoredRun,
    reason: FailureReason,
    error: ErrorRecord,
    failureNotice: FailureNotice | undefined,
    instant: EpochMilliseconds,
  ): SettleReply => {
    const failedAt = new Date(instant).toISOString();
    endRun(storedRun, "failed", instant);
    storedRun.reason = reason;
    storedRun.errors.push(JSON.stringify({ ...errorWithTextsCutOf(error), failedAt }));
    if (failureNotice !== undefined && failureNotice.runs.length > 0) {
      const noticeError = failureNotice.payload.error;
      const noticePayload = JSON.stringify({
        ...failureNotice.payload,
        error: {
          ...noticeError,
          message: errorTextCutOf(noticeError.message),
          stack: errorTextCutOf(noticeError.stack),
        },
        failedAt,
      });
      enqueueRuns(
        failureNotice.runs.map((noticeRun) => ({ ...noticeRun, payload: noticePayload })),
        failureNotice.eventDedup,
        instant,
      );
    }
    return { kind: "failed", reason, finishedAt: instant };
  };

  const endRun = (storedRun: InMemoryStoredRun, finalState: "completed" | "failed", instant: number) => {
    dedupExpiryByMemberOf(dedupExpiryByStoredRunIdByTask, storedRun.task).set(
      storedRunIdOf(storedRun.task, storedRun.runId),
      instant + DEDUP_WINDOW_IN_MILLISECONDS,
    );
    storedRun.state = finalState;
    storedRun.finishedAt = instant;
  };

  const settle = async ({ task, runId, lockToken, outcome, concurrencyLimit }: SettleRequest): Promise<SettleReply> => {
    const storedRun = storedRunOf(task, runId);
    if (storedRun === undefined) {
      return { kind: "stale" };
    }
    if (storedRun.state !== "executing" || storedRun.lockToken !== lockToken) {
      if (storedRun.endedLockToken !== lockToken || storedRun.endedAs !== outcome.kind) {
        return { kind: "stale" };
      }
      return outcome.kind === "handBack" &&
        storedRun.state === "failed" &&
        storedRun.reason !== undefined &&
        storedRun.finishedAt !== undefined
        ? { kind: "failed", reason: storedRun.reason, finishedAt: storedRun.finishedAt }
        : { kind: "already" };
    }
    const instant = now();
    endLock(storedRun, outcome.kind, concurrencyLimit);
    if (outcome.kind !== "handBack") {
      storedRun.firstFoundUnknownAt = undefined;
    }
    const appendError = (error: ErrorRecord) => {
      storedRun.errors.push(
        JSON.stringify({ ...errorWithTextsCutOf(error), failedAt: new Date(instant).toISOString() }),
      );
    };
    switch (outcome.kind) {
      case "complete":
        storedRun.steps.clear();
        endRun(storedRun, "completed", instant);
        return { kind: "completed", finishedAt: instant };
      case "fail":
        return failRun(storedRun, outcome.reason, outcome.error, outcome.failureNotice, instant);
      case "retry": {
        appendError(outcome.error);
        const nextAttemptAt = instant + outcome.backoffInMilliseconds;
        storedRun.attempt += 1;
        storedRun.stalls = 0;
        enterDueState(storedRun, "backingOff", nextAttemptAt);
        return { kind: "backingOff", nextAttemptAt };
      }
      case "suspend":
        enterDueState(storedRun, "suspended", outcome.resumesAt);
        return { kind: "suspended", resumesAt: outcome.resumesAt };
      case "handBack":
        if (
          storedRun.firstFoundUnknownAt !== undefined &&
          instant - storedRun.firstFoundUnknownAt >= UNKNOWN_TASK_FAILURE_DELAY_IN_MILLISECONDS
        ) {
          return failRun(storedRun, "unknown task", outcome.error, outcome.failureNotice, instant);
        }
        storedRun.firstFoundUnknownAt ??= instant;
        enterPending(storedRun, storedRun.order);
        return { kind: "handedBack" };
      case "yield":
        enterPending(storedRun, storedRun.order);
        return { kind: "yielded" };
    }
  };

  const promote = async (): Promise<PromoteReply> => {
    const instant = now();
    const stalledRuns: StalledRun[] = [];
    const thirdStalls: ThirdStall[] = [];
    const expiredLockEntries = runEntriesInState("executing", ({ lockedUntil }) => lockedUntil).filter(
      ({ score }) => score <= instant,
    );
    for (const { storedRun } of expiredLockEntries) {
      if (storedRun.stalls >= 2) {
        thirdStalls.push({
          runId: storedRun.runId,
          task: storedRun.task,
          attempt: storedRun.attempt,
          expiredLockToken: storedRun.lockToken ?? "",
          replays: storedRun.replays,
          dispatchedAt: storedRun.dispatchedAt,
          startedAt: storedRun.startedAt ?? 0,
          ...runOriginOf(storedRun),
        });
      } else {
        endLock(storedRun, "stalled", undefined);
        storedRun.stalls += 1;
        enterPending(storedRun, storedRun.order);
        stalledRuns.push({
          runId: storedRun.runId,
          task: storedRun.task,
          attempt: storedRun.attempt,
          stalls: storedRun.stalls,
          ...runOriginOf(storedRun),
        });
      }
    }
    const promotedRunCount = promoteDueRuns(instant, undefined, () => undefined).length;
    return { kind: "promoted", promotedRunCount, stalledRuns, thirdStalls };
  };

  const promoteDueRuns = (
    dueBefore: EpochMilliseconds,
    task: string | undefined,
    beforePromotingSuspendedRun: (storedRun: InMemoryStoredRun) => void,
  ): MadeDueRun[] => {
    const dueRunEntries = [...runsByStoredRunId.entries()]
      .filter(([, storedRun]) => task === undefined || storedRun.task === task)
      .map(([storedRunId, storedRun]) => ({ member: storedRunId, storedRun, score: dueInstantOf(storedRun) }))
      .filter(
        (entry): entry is typeof entry & { score: number } => entry.score !== undefined && entry.score <= dueBefore,
      )
      .sort(byScoreThenMember)
      .slice(0, MOST_DUE_RUNS_PROMOTED_PER_PROMOTE);
    for (const { storedRun } of dueRunEntries) {
      if (storedRun.state === "suspended") {
        beforePromotingSuspendedRun(storedRun);
      }
      storedRun.startsAt = undefined;
      storedRun.nextAttemptAt = undefined;
      enterPending(storedRun, undefined);
    }
    return dueRunEntries.map(({ storedRun }) => ({ task: storedRun.task, runId: storedRun.runId }));
  };

  const makeDue = async ({ task }: MakeDueRequest = {}): Promise<MakeDueReply> => {
    const instant = now();
    const madeDueRuns = promoteDueRuns(Number.POSITIVE_INFINITY, task, (storedRun) => {
      for (const [stepId, savedValue] of storedRun.steps) {
        if (savedValue === String(storedRun.resumesAt)) {
          storedRun.steps.set(stepId, String(instant));
        }
      }
      storedRun.resumesAt = instant;
      storedRun.sleepsWokenAt = instant;
    });
    return { kind: "madeDue", madeDueRuns };
  };

  const saveStep = async ({ task, runId, lockToken, stepId, step }: SaveStepRequest): Promise<SaveStepReply> => {
    if (stepId === "") {
      throw invalidArgumentError("stepId must not be empty");
    }
    const storedRun = currentlyLockedRunOf(storedRunOf(task, runId), lockToken);
    if (storedRun === undefined) {
      return { kind: "stale" };
    }
    const instant = now();
    const savedValue = storedRun.steps.get(stepId);
    if (savedValue !== undefined) {
      if (step.kind === "value") {
        return { kind: "saved", resumesAt: undefined };
      }
      if (!SAVED_SLEEP_PATTERN.test(savedValue)) {
        throw invalidArgumentError(`step ${stepId} is saved but is not a sleep`);
      }
      const savedResumesAt = Number(savedValue);
      return { kind: "saved", resumesAt: savedResumesAt, isDue: savedResumesAt <= instant };
    }
    storedRun.stalls = 0;
    if (step.kind === "value") {
      storedRun.steps.set(stepId, step.value);
      return { kind: "saved", resumesAt: undefined };
    }
    const resumesAt = step.kind === "sleep" ? instant + step.durationInMilliseconds : step.resumesAt;
    storedRun.steps.set(stepId, String(resumesAt));
    return { kind: "saved", resumesAt, isDue: resumesAt <= instant };
  };

  const readStep = async ({ task, runId, lockToken, stepId }: ReadStepRequest): Promise<ReadStepReply> => {
    const storedRun = currentlyLockedRunOf(storedRunOf(task, runId), lockToken);
    if (storedRun === undefined) {
      return { kind: "stale" };
    }
    return { kind: "read", savedValue: storedRun.steps.get(stepId) };
  };

  const prune = async ({ task, completedRetention, failedRetention }: PruneRequest): Promise<PruneReply> => {
    const instant = now();
    const prunedStoredRunIds = new Set<StoredRunId>();
    const endedRunEntriesOf = (state: "completed" | "failed") =>
      runEntriesInState(state, ({ finishedAt }) => finishedAt).filter(
        ({ member, storedRun }) => storedRun.task === task && !prunedStoredRunIds.has(member),
      );
    const pruneUpToTheBound = (storedRunIds: readonly StoredRunId[]) => {
      for (const storedRunId of storedRunIds.slice(0, MOST_RUNS_PRUNED_PER_PRUNE - prunedStoredRunIds.size)) {
        prunedStoredRunIds.add(storedRunId);
      }
    };
    const retentionByState: [state: "completed" | "failed", retention: RetentionLimit][] = [
      ["completed", completedRetention],
      ["failed", failedRetention],
    ];
    for (const [state, { maxAgeInMilliseconds }] of retentionByState) {
      pruneUpToTheBound(
        endedRunEntriesOf(state)
          .filter(({ score }) => score + maxAgeInMilliseconds <= instant)
          .map(({ member }) => member),
      );
    }
    for (const [state, { maxRunCount }] of retentionByState) {
      const keptRunEntries = endedRunEntriesOf(state);
      pruneUpToTheBound(
        keptRunEntries.slice(0, Math.max(0, keptRunEntries.length - maxRunCount)).map(({ member }) => member),
      );
    }
    for (const storedRunId of prunedStoredRunIds) {
      runsByStoredRunId.delete(storedRunId);
    }
    trimExpiredDedupEntries(dedupExpiryByStoredRunIdByTask.get(task), instant, EXPIRED_DEDUP_ENTRIES_TRIMMED_PER_PRUNE);
    return { kind: "pruned", prunedRunCount: prunedStoredRunIds.size };
  };

  const fire = async ({ task, instant, instantAfter, runId, retries }: FireRequest): Promise<FireReply> => {
    if (task === "" || runId === "") {
      throw invalidArgumentError("a fire carries its task and run id");
    }
    if (instantAfter <= instant) {
      throw invalidArgumentError("instantAfter must come after instant");
    }
    const storedSchedule = schedulesByTask.get(task);
    if (storedSchedule === undefined) {
      return { kind: "noSchedule" };
    }
    if (storedSchedule.nextFiringAt !== instant) {
      return { kind: "already" };
    }
    const firedAt = now();
    if (instant > firedAt) {
      return { kind: "notDue" };
    }
    const { createdRuns } = enqueueRuns([{ runId, task, payload: "", retries }], undefined, firedAt);
    const isRunCreated = createdRuns.length === 1;
    if (isRunCreated) {
      storedRunOf(task, runId)!.scheduledAt = instant;
    }
    storedSchedule.nextFiringAt = instantAfter;
    storedSchedule.previousFiringAt = instant;
    return { kind: "fired", runId, isRunCreated };
  };

  const syncSchedules = async ({ schedulesToSync }: SyncSchedulesRequest): Promise<SyncSchedulesReply> => {
    const listedTasks = new Set<string>();
    for (const { task, cron, timezone } of schedulesToSync) {
      if (task === "" || cron === "" || timezone === "") {
        throw invalidArgumentError("a schedule carries its task, cron and timezone");
      }
      if (listedTasks.has(task)) {
        throw invalidArgumentError(`task ${task} is listed twice`);
      }
      listedTasks.add(task);
    }
    const storedSchedules = [...schedulesByTask.values()]
      .map((storedSchedule) => ({ member: storedSchedule.task, score: storedSchedule.nextFiringAt, storedSchedule }))
      .sort(byScoreThenMember)
      .map(({ storedSchedule }) => storedSchedule);
    const added: ScheduleSummary[] = [];
    const changed: ScheduleSummary[] = [];
    for (const { task, cron, timezone, nextFiringAt, retries } of schedulesToSync) {
      const storedSchedule = schedulesByTask.get(task);
      if (storedSchedule !== undefined && storedSchedule.cron === cron && storedSchedule.timezone === timezone) {
        storedSchedule.retries = retries;
        continue;
      }
      (storedSchedule === undefined ? added : changed).push({ task, cron, timezone });
      schedulesByTask.set(task, {
        task,
        cron,
        timezone,
        nextFiringAt,
        retries,
        previousFiringAt: storedSchedule?.previousFiringAt,
      });
    }
    const removed = storedSchedules
      .filter(({ task }) => !listedTasks.has(task))
      .map(({ task, cron, timezone }) => ({ task, cron, timezone }));
    for (const { task } of removed) {
      schedulesByTask.delete(task);
    }
    return { kind: "synced", added, changed, removed };
  };

  const replied = <Reply>(reply: Reply): TickCommandReply<Reply> => ({ kind: "replied", reply });

  const dueSchedulesOf = (scheduledTaskNames: readonly string[]): DueSchedulesReply => {
    const serverInstant = now();
    const dueSchedules = scheduledTaskNames.flatMap((task) => {
      const storedSchedule = schedulesByTask.get(task);
      if (storedSchedule === undefined || storedSchedule.nextFiringAt > serverInstant) {
        return [];
      }
      const { cron, timezone, nextFiringAt } = storedSchedule;
      return [{ task, cron, timezone, nextFiringAt }];
    });
    return { serverInstant, dueSchedules };
  };

  const tick = async ({
    heldLocks,
    lockDurationInMilliseconds,
    prune: pruneRequest,
    isPromoteIncluded = true,
    scheduledTaskNames = [],
  }: TickRequest): Promise<TickReply> => ({
    promote: isPromoteIncluded ? replied(await promote()) : undefined,
    renewals: replied(await renew({ heldLocks, lockDurationInMilliseconds })),
    taskNames: replied([...taskNames]),
    prune: pruneRequest === undefined ? undefined : replied(await prune(pruneRequest)),
    dueSchedules: scheduledTaskNames.length === 0 ? undefined : replied(dueSchedulesOf(scheduledTaskNames)),
  });

  const recorded =
    <Request, Reply>(method: InMemoryEngineMethod, call: (request: Request) => Promise<Reply>) =>
    async (request: Request): Promise<Reply> => {
      calls.push({ method, request });
      await heldCallReleasesByMethod.get(method);
      return call(request);
    };

  return {
    enqueue: recorded("enqueue", enqueue),
    fetch: recorded("fetch", fetch),
    renew: recorded("renew", renew),
    settle: recorded("settle", settle),
    promote: () => recorded("promote", promote)(undefined),
    saveStep: recorded("saveStep", saveStep),
    readStep: recorded("readStep", readStep),
    prune: recorded("prune", prune),
    tick: recorded("tick", tick),
    fire: recorded("fire", fire),
    syncSchedules: recorded("syncSchedules", syncSchedules),
    serverInstant: () => recorded("serverInstant", async () => now())(undefined),
    makeDue: (request) => recorded("makeDue", makeDue)(request),
    calls,
    holdCalls: (method) => {
      let release = () => undefined as void;
      heldCallReleasesByMethod.set(
        method,
        new Promise<void>((resolve) => {
          release = resolve;
        }),
      );
      return {
        release: () => {
          heldCallReleasesByMethod.delete(method);
          release();
        },
      };
    },
    waitForWakeUp: (timeoutInMilliseconds, stopSignal) =>
      new Promise<void>((resolve) => {
        if (isWakeUpMarkerArmed) {
          isWakeUpMarkerArmed = false;
          resolve();
          return;
        }
        const endWait = () => {
          clearTimeout(timeoutTimer);
          stopSignal?.removeEventListener("abort", stopWait);
          resolve();
        };
        const stopWait = () => {
          wakeUpWaiters.splice(wakeUpWaiters.indexOf(endWait), 1);
          endWait();
        };
        const timeoutTimer = setTimeout(stopWait, timeoutInMilliseconds);
        wakeUpWaiters.push(endWait);
        stopSignal?.addEventListener("abort", stopWait, { once: true });
      }),
    isWakeUpMarkerArmed: () => isWakeUpMarkerArmed,
    storedRunOf,
    storedScheduleOf: (task) => schedulesByTask.get(task),
    runIdsInState: (state) => runEntriesInState(state, ({ order }) => order).map(({ storedRun }) => storedRun.runId),
    expireLock: (task, runId) => {
      const storedRun = storedRunOf(task, runId);
      if (storedRun?.state !== "executing") {
        throw new Error(`run "${runId}" holds no lock to expire`);
      }
      storedRun.lockedUntil = now() - 1;
    },
    backdateFirstFoundUnknownAt: (task, runId, durationInMilliseconds) => {
      const storedRun = storedRunOf(task, runId);
      if (storedRun?.firstFoundUnknownAt === undefined) {
        throw new Error(`run "${runId}" was never found unknown`);
      }
      storedRun.firstFoundUnknownAt -= durationInMilliseconds;
    },
    backdateEndedRun: (task, runId, durationInMilliseconds) => {
      const storedRun = storedRunOf(task, runId);
      if (storedRun?.finishedAt === undefined || (storedRun.state !== "completed" && storedRun.state !== "failed")) {
        throw new Error(`run "${runId}" has not ended`);
      }
      storedRun.finishedAt -= durationInMilliseconds;
      const dedupExpiryByStoredRunId = dedupExpiryByStoredRunIdByTask.get(task);
      const dedupExpiresAt = dedupExpiryByStoredRunId?.get(storedRunIdOf(task, runId));
      if (dedupExpiresAt !== undefined) {
        dedupExpiryByStoredRunId?.set(storedRunIdOf(task, runId), dedupExpiresAt - durationInMilliseconds);
      }
    },
  };
}
