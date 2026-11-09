import type { ConcurrencyKey } from "../src/definitions.js";
import type { DurationInMilliseconds, EpochMilliseconds } from "../src/durations.js";
import type { LockToken } from "../src/engine/engine-keys.js";
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
  FetchReply,
  FetchRequest,
  FireReply,
  FireRequest,
  LockedRun,
  LockRenewal,
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
  | "serverInstant";

export type InMemoryEngineCall = { method: InMemoryEngineMethod; request: unknown };

export type InMemoryStoredRun = {
  task: string;
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
  errors: string[];
  steps: Map<StepId, SerializedStepValue>;
};

export type HeldCalls = { release: () => void };

export type InMemoryStoredSchedule = ScheduleSummary & {
  nextFiringAt: EpochMilliseconds;
  previousFiringAt: EpochMilliseconds | undefined;
};

export type InMemoryEngine = Pick<Engine, InMemoryEngineMethod> & {
  calls: readonly InMemoryEngineCall[];
  holdCalls: (method: InMemoryEngineMethod) => HeldCalls;
  waitForWakeUp: (timeoutInMilliseconds: DurationInMilliseconds) => Promise<void>;
  isWakeUpMarkerArmed: () => boolean;
  storedRunOf: (runId: RunId) => Readonly<InMemoryStoredRun> | undefined;
  storedScheduleOf: (task: string) => Readonly<InMemoryStoredSchedule> | undefined;
  runIdsInState: (state: RunState) => RunId[];
  expireLock: (runId: RunId) => void;
  backdateEndedRun: (runId: RunId, durationInMilliseconds: DurationInMilliseconds) => void;
  backdateFirstFoundUnknownAt: (runId: RunId, durationInMilliseconds: DurationInMilliseconds) => void;
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

function byScoreThenRunId<Entry extends { score: number; runId: RunId }>(left: Entry, right: Entry): number {
  if (left.score !== right.score) {
    return left.score - right.score;
  }
  if (left.runId === right.runId) {
    return 0;
  }
  return left.runId < right.runId ? -1 : 1;
}

function errorWithTextsCutOf(error: ErrorRecord): ErrorRecord {
  return { ...error, message: errorTextCutOf(error.message), stack: errorTextCutOf(error.stack) };
}

function invalidArgumentError(message: string): Error {
  return new Error(`invalid argument: ${message}`);
}

export function createInMemoryEngine({ now = () => Date.now() }: InMemoryEngineOptions = {}): InMemoryEngine {
  const runsById = new Map<RunId, InMemoryStoredRun>();
  const lockedRunByToken = new Map<LockToken, { runId: RunId; expiresAt: EpochMilliseconds }>();
  const slotSetsByScope = new Map<string, Set<RunId>>();
  const dedupExpiryByRunIdByTask = new Map<string, Map<RunId, EpochMilliseconds>>();
  const dedupExpiryByEventIdByEventName = new Map<string, Map<string, EpochMilliseconds>>();
  const taskNames = new Set<string>();
  const schedulesByTask = new Map<string, InMemoryStoredSchedule>();
  const calls: InMemoryEngineCall[] = [];
  const heldCallReleasesByMethod = new Map<InMemoryEngineMethod, Promise<void>>();
  const wakeUpWaiters: (() => void)[] = [];
  let lastOrder = 0;
  let isWakeUpMarkerArmed = false;

  const slotSetOf = (task: string, concurrencyKey: ConcurrencyKey | undefined): Set<RunId> => {
    const scope = concurrencyScopeOf(task, concurrencyKey);
    const slotSet = slotSetsByScope.get(scope) ?? new Set<RunId>();
    slotSetsByScope.set(scope, slotSet);
    return slotSet;
  };

  const runEntriesInState = (state: RunState, scoreOf: (storedRun: InMemoryStoredRun) => number | undefined) =>
    [...runsById.entries()]
      .filter(([, storedRun]) => storedRun.state === state)
      .map(([runId, storedRun]) => ({ runId, storedRun, score: scoreOf(storedRun) ?? 0 }))
      .sort(byScoreThenRunId);

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
      .map(([member, expiresAt]) => ({ runId: member, score: expiresAt }))
      .sort(byScoreThenRunId)
      .slice(0, mostTrimmed);
    for (const { runId: member } of expiredMembers) {
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
    runId: RunId,
    { task, concurrencyKey }: InMemoryStoredRun,
    concurrencyLimit: number | undefined,
  ) => {
    const slotSet = slotSetOf(task, concurrencyKey);
    slotSet.delete(runId);
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

  const endLock = (
    runId: RunId,
    storedRun: InMemoryStoredRun,
    endedAs: string,
    concurrencyLimit: number | undefined,
  ) => {
    if (storedRun.lockToken !== undefined) {
      lockedRunByToken.delete(storedRun.lockToken);
    }
    storedRun.endedLockToken = storedRun.lockToken;
    storedRun.endedAs = endedAs;
    storedRun.lockToken = undefined;
    storedRun.lockedUntil = undefined;
    releaseSlotAndWakeParkedRuns(runId, storedRun, concurrencyLimit);
  };

  const createRun = (run: RunToCreate, instant: EpochMilliseconds) => {
    const storedRun: InMemoryStoredRun = {
      task: run.task,
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
      errors: [],
      steps: new Map(),
    };
    runsById.set(run.runId, storedRun);
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
    const createdRunIds: RunId[] = [];
    const skippedRunIds: RunId[] = [];
    for (const run of runs) {
      if (runsById.has(run.runId) || isLiveDedupEntry(dedupExpiryByRunIdByTask.get(run.task), run.runId, instant)) {
        skippedRunIds.push(run.runId);
      } else {
        createRun(run, instant);
        createdRunIds.push(run.runId);
      }
    }
    if (eventDedup !== undefined) {
      dedupExpiryByMemberOf(dedupExpiryByEventIdByEventName, eventDedup.eventName).set(
        eventDedup.eventId,
        instant + DEDUP_WINDOW_IN_MILLISECONDS,
      );
    }
    return { createdRunIds, skippedRunIds };
  };

  const currentlyLockedRunOf = (runId: RunId, lockToken: LockToken): InMemoryStoredRun | undefined => {
    const storedRun = runsById.get(runId);
    return storedRun?.state === "executing" && storedRun.lockToken === lockToken ? storedRun : undefined;
  };

  const lockedRunReplyOf = (
    runId: RunId,
    storedRun: InMemoryStoredRun,
    isResume: boolean,
    sleepsWokenAt: EpochMilliseconds | undefined,
  ): LockedRun => ({
    lockToken: storedRun.lockToken ?? "",
    runId,
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

  const parkedRunReplyOf = (runId: RunId, storedRun: InMemoryStoredRun): ParkedRun => ({
    runId,
    task: storedRun.task,
    attempt: storedRun.attempt,
    ...runOriginOf(storedRun),
  });

  const enqueue = async ({ runs, eventDedup, parentFence }: EnqueueRequest): Promise<EnqueueReply> => {
    if (runs.length === 0) {
      throw invalidArgumentError("an enqueue creates at least one run");
    }
    if (parentFence !== undefined) {
      const parentRun = currentlyLockedRunOf(parentFence.parentRunId, parentFence.lockToken);
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
      trimExpiredDedupEntries(dedupExpiryByRunIdByTask.get(task), instant, EXPIRED_DEDUP_ENTRIES_TRIMMED_PER_SET);
    }
    const { createdRunIds, skippedRunIds } = enqueueRuns(runs, eventDedup, instant);
    if (parentFence !== undefined) {
      const parentRun = runsById.get(parentFence.parentRunId)!;
      parentRun.steps.set(parentFence.stepId, parentFence.stepValue);
      parentRun.stalls = 0;
    }
    return { kind: "enqueued", createdRunIds, skippedRunIds };
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
          ? currentlyLockedRunOf(lockedRun.runId, lockToken)
          : undefined;
      if (lockedRun !== undefined && storedRun !== undefined) {
        lockedRuns.push(lockedRunReplyOf(lockedRun.runId, storedRun, false, undefined));
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
      const { runId, storedRun } = headEntry;
      const concurrencyLimit = concurrencyLimitsByTask[storedRun.task];
      const slotSet = slotSetOf(storedRun.task, storedRun.concurrencyKey);
      if (concurrencyLimit !== undefined && slotSet.size >= concurrencyLimit) {
        storedRun.state = "waitingForConcurrencySlot";
        parkedRuns.push(parkedRunReplyOf(runId, storedRun));
      } else {
        const lockToken = freshLockTokens[usedLockTokenCount]!;
        usedLockTokenCount += 1;
        const isResume = storedRun.resumesAt !== undefined;
        const { sleepsWokenAt } = storedRun;
        storedRun.resumesAt = undefined;
        storedRun.sleepsWokenAt = undefined;
        storedRun.state = "executing";
        storedRun.lockToken = lockToken;
        storedRun.lockedUntil = instant + lockDurationInMilliseconds;
        storedRun.startedAt = instant;
        slotSet.add(runId);
        lockedRunByToken.set(lockToken, { runId, expiresAt: instant + lockDurationInMilliseconds });
        lockedRuns.push(lockedRunReplyOf(runId, storedRun, isResume, sleepsWokenAt));
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
    return heldLocks.map(({ runId, lockToken }) => {
      const storedRun = currentlyLockedRunOf(runId, lockToken);
      if (storedRun === undefined) {
        return { kind: "stale" };
      }
      storedRun.lockedUntil = lockedUntil;
      lockedRunByToken.set(lockToken, { runId, expiresAt: lockedUntil });
      return { kind: "renewed", lockedUntil };
    });
  };

  const failRun = (
    runId: RunId,
    storedRun: InMemoryStoredRun,
    reason: FailureReason,
    error: ErrorRecord,
    failureNotice: FailureNotice | undefined,
    instant: EpochMilliseconds,
  ): SettleReply => {
    const failedAt = new Date(instant).toISOString();
    endRun(runId, storedRun, "failed", instant);
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

  const endRun = (runId: RunId, storedRun: InMemoryStoredRun, finalState: "completed" | "failed", instant: number) => {
    dedupExpiryByMemberOf(dedupExpiryByRunIdByTask, storedRun.task).set(runId, instant + DEDUP_WINDOW_IN_MILLISECONDS);
    storedRun.state = finalState;
    storedRun.finishedAt = instant;
  };

  const settle = async ({ runId, lockToken, outcome, concurrencyLimit }: SettleRequest): Promise<SettleReply> => {
    const storedRun = runsById.get(runId);
    if (storedRun === undefined) {
      return { kind: "stale" };
    }
    if (storedRun.state !== "executing" || storedRun.lockToken !== lockToken) {
      return storedRun.endedLockToken === lockToken && storedRun.endedAs === outcome.kind
        ? { kind: "already" }
        : { kind: "stale" };
    }
    const instant = now();
    endLock(runId, storedRun, outcome.kind, concurrencyLimit);
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
        endRun(runId, storedRun, "completed", instant);
        return { kind: "completed", finishedAt: instant };
      case "fail":
        return failRun(runId, storedRun, outcome.reason, outcome.error, outcome.failureNotice, instant);
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
          return failRun(runId, storedRun, "unknown task", outcome.error, outcome.failureNotice, instant);
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
    for (const { runId, storedRun } of expiredLockEntries) {
      if (storedRun.stalls >= 2) {
        thirdStalls.push({
          runId,
          task: storedRun.task,
          attempt: storedRun.attempt,
          expiredLockToken: storedRun.lockToken ?? "",
          replays: storedRun.replays,
          dispatchedAt: storedRun.dispatchedAt,
          startedAt: storedRun.startedAt ?? 0,
          ...runOriginOf(storedRun),
        });
      } else {
        endLock(runId, storedRun, "stalled", undefined);
        storedRun.stalls += 1;
        enterPending(storedRun, storedRun.order);
        stalledRuns.push({
          runId,
          task: storedRun.task,
          attempt: storedRun.attempt,
          stalls: storedRun.stalls,
          ...runOriginOf(storedRun),
        });
      }
    }
    const dueRunEntries = [...runsById.entries()]
      .map(([runId, storedRun]) => ({ runId, storedRun, score: dueInstantOf(storedRun) }))
      .filter((entry): entry is typeof entry & { score: number } => entry.score !== undefined && entry.score <= instant)
      .sort(byScoreThenRunId)
      .slice(0, MOST_DUE_RUNS_PROMOTED_PER_PROMOTE);
    for (const { storedRun } of dueRunEntries) {
      storedRun.startsAt = undefined;
      storedRun.nextAttemptAt = undefined;
      enterPending(storedRun, undefined);
    }
    return { kind: "promoted", promotedRunCount: dueRunEntries.length, stalledRuns, thirdStalls };
  };

  const saveStep = async ({ runId, lockToken, stepId, step }: SaveStepRequest): Promise<SaveStepReply> => {
    if (stepId === "") {
      throw invalidArgumentError("stepId must not be empty");
    }
    const storedRun = currentlyLockedRunOf(runId, lockToken);
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

  const readStep = async ({ runId, lockToken, stepId }: ReadStepRequest): Promise<ReadStepReply> => {
    const storedRun = currentlyLockedRunOf(runId, lockToken);
    if (storedRun === undefined) {
      return { kind: "stale" };
    }
    return { kind: "read", savedValue: storedRun.steps.get(stepId) };
  };

  const prune = async ({ task, completedRetention, failedRetention }: PruneRequest): Promise<PruneReply> => {
    const instant = now();
    const prunedRunIds = new Set<RunId>();
    const endedRunEntriesOf = (state: "completed" | "failed") =>
      runEntriesInState(state, ({ finishedAt }) => finishedAt).filter(
        ({ runId, storedRun }) => storedRun.task === task && !prunedRunIds.has(runId),
      );
    const pruneUpToTheBound = (runIds: readonly RunId[]) => {
      for (const runId of runIds.slice(0, MOST_RUNS_PRUNED_PER_PRUNE - prunedRunIds.size)) {
        prunedRunIds.add(runId);
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
          .map(({ runId }) => runId),
      );
    }
    for (const [state, { maxRunCount }] of retentionByState) {
      const keptRunEntries = endedRunEntriesOf(state);
      pruneUpToTheBound(
        keptRunEntries.slice(0, Math.max(0, keptRunEntries.length - maxRunCount)).map(({ runId }) => runId),
      );
    }
    for (const runId of prunedRunIds) {
      runsById.delete(runId);
    }
    trimExpiredDedupEntries(dedupExpiryByRunIdByTask.get(task), instant, EXPIRED_DEDUP_ENTRIES_TRIMMED_PER_PRUNE);
    return { kind: "pruned", prunedRunCount: prunedRunIds.size };
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
    const { createdRunIds } = enqueueRuns([{ runId, task, payload: "", retries }], undefined, firedAt);
    const isRunCreated = createdRunIds.length === 1;
    if (isRunCreated) {
      runsById.get(runId)!.scheduledAt = instant;
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
      .map((storedSchedule) => ({ runId: storedSchedule.task, score: storedSchedule.nextFiringAt, storedSchedule }))
      .sort(byScoreThenRunId)
      .map(({ storedSchedule }) => storedSchedule);
    const added: ScheduleSummary[] = [];
    const changed: ScheduleSummary[] = [];
    for (const { task, cron, timezone, nextFiringAt } of schedulesToSync) {
      const storedSchedule = schedulesByTask.get(task);
      if (storedSchedule !== undefined && storedSchedule.cron === cron && storedSchedule.timezone === timezone) {
        continue;
      }
      (storedSchedule === undefined ? added : changed).push({ task, cron, timezone });
      schedulesByTask.set(task, {
        task,
        cron,
        timezone,
        nextFiringAt,
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

  const tick = async ({
    heldLocks,
    lockDurationInMilliseconds,
    prune: pruneRequest,
    isPromoteIncluded = true,
  }: TickRequest): Promise<TickReply> => ({
    promote: isPromoteIncluded ? replied(await promote()) : undefined,
    renewals: replied(await renew({ heldLocks, lockDurationInMilliseconds })),
    taskNames: replied([...taskNames]),
    prune: pruneRequest === undefined ? undefined : replied(await prune(pruneRequest)),
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
    waitForWakeUp: (timeoutInMilliseconds) =>
      new Promise<void>((resolve) => {
        if (isWakeUpMarkerArmed) {
          isWakeUpMarkerArmed = false;
          resolve();
          return;
        }
        const wakeUp = () => {
          clearTimeout(timeoutTimer);
          resolve();
        };
        const timeoutTimer = setTimeout(() => {
          wakeUpWaiters.splice(wakeUpWaiters.indexOf(wakeUp), 1);
          resolve();
        }, timeoutInMilliseconds);
        wakeUpWaiters.push(wakeUp);
      }),
    isWakeUpMarkerArmed: () => isWakeUpMarkerArmed,
    storedRunOf: (runId) => runsById.get(runId),
    storedScheduleOf: (task) => schedulesByTask.get(task),
    runIdsInState: (state) => runEntriesInState(state, ({ order }) => order).map(({ runId }) => runId),
    expireLock: (runId) => {
      const storedRun = runsById.get(runId);
      if (storedRun?.state !== "executing") {
        throw new Error(`run "${runId}" holds no lock to expire`);
      }
      storedRun.lockedUntil = now() - 1;
    },
    backdateFirstFoundUnknownAt: (runId, durationInMilliseconds) => {
      const storedRun = runsById.get(runId);
      if (storedRun?.firstFoundUnknownAt === undefined) {
        throw new Error(`run "${runId}" was never found unknown`);
      }
      storedRun.firstFoundUnknownAt -= durationInMilliseconds;
    },
    backdateEndedRun: (runId, durationInMilliseconds) => {
      const storedRun = runsById.get(runId);
      if (storedRun?.finishedAt === undefined || (storedRun.state !== "completed" && storedRun.state !== "failed")) {
        throw new Error(`run "${runId}" has not ended`);
      }
      storedRun.finishedAt -= durationInMilliseconds;
      const dedupExpiryByRunId = dedupExpiryByRunIdByTask.get(storedRun.task);
      const dedupExpiresAt = dedupExpiryByRunId?.get(runId);
      if (dedupExpiresAt !== undefined) {
        dedupExpiryByRunId?.set(runId, dedupExpiresAt - durationInMilliseconds);
      }
    },
  };
}
