import { randomBytes } from "node:crypto";

import { abortCauseOf, AttemptAbortedError } from "./attempt-abort.js";
import type { AttemptAbortCause } from "./attempt-abort.js";
import { errorFromThrownValue, StalledRunError } from "./attempt-errors.js";
import type { AnyTaskDefinition } from "./definitions.js";
import type { DurationInMilliseconds, EpochMilliseconds } from "./durations.js";
import type { LockToken } from "./engine/engine-keys.js";
import type { Engine, LockedRun, PruneRequest, ThirdStall, TickReply, TickRequest } from "./engine/engine.js";
import type { WorkerDefinitions } from "./definitions.js";
import { declaredSchedulesOf, fireDueSchedules } from "./worker-schedules.js";
import {
  lockLostAbortLineOf,
  parkLineOf,
  passOverLineOf,
  pickupLineOf,
  settleLinesOf,
  settleRefusalLineOf,
  stallLineOf,
  workerErrorLineOf,
  writeLifecycleLines,
} from "./lifecycle-logs.js";
import { executeLockedRunAttempt, failingAttemptOf, handBackAttemptOf, settleRequestOf } from "./locked-run-attempt.js";
import type { AttemptResult } from "./locked-run-attempt.js";
import type { Logger } from "./logger.js";
import { answeredDespiteOutage, delayUnlessAborted, untilAborted } from "./redis-outage.js";
import type { RedisOutage } from "./redis-outage.js";
import type { HandlerSender } from "./step-engine.js";
import type { WorkerTiming } from "./worker-timing.js";

export type WorkerLoopEngine = Pick<Engine, "fetch" | "settle" | "tick" | "saveStep" | "readStep" | "fire">;

export type WaitForWakeUp = (timeoutInMilliseconds: DurationInMilliseconds) => Promise<void>;

export function wakeUpBlockInMillisecondsOf(timing: WorkerTiming): DurationInMilliseconds {
  return timing.tickIntervalInMilliseconds;
}

export type WorkerLoopConfig = {
  engine: WorkerLoopEngine;
  definitions: WorkerDefinitions;
  timing: WorkerTiming;
  logger: Logger;
  concurrency: number;
  sender: HandlerSender;
  waitForWakeUp: WaitForWakeUp;
  redisOutage: RedisOutage;
};

export type WorkerLoop = {
  stopFetching: () => Promise<void>;
  stopTicking: () => Promise<void>;
  startDraining: () => void;
  abortAttemptsAtShutdownDeadline: () => number;
  stopSettling: () => void;
  waitForAttemptsInFlight: () => Promise<void>;
};

type AttemptInFlight = {
  lockedRun: LockedRun;
  lockExpiresAtByWorkerClock: EpochMilliseconds;
  abortController: AbortController;
  isExecuting: boolean;
  settlement: Promise<void>;
};

const OUTCOME_KINDS_KEPT_AT_SHUTDOWN = new Set<AttemptResult["attemptOutcome"]["kind"]>(["complete", "suspend"]);

export function failedCommandErrorsOf({ promote, renewals, taskNames, prune, dueSchedules }: TickReply): Error[] {
  return [promote, renewals, taskNames, prune, dueSchedules].flatMap((commandReply) =>
    commandReply?.kind === "failed" ? [commandReply.error] : [],
  );
}

function freshLockTokenOf(): LockToken {
  return randomBytes(16).toString("hex");
}

function concurrencyLimitsByTaskOf(tasksByName: ReadonlyMap<string, AnyTaskDefinition>): Record<string, number> {
  return Object.fromEntries(
    [...tasksByName.values()].flatMap((task) =>
      task.concurrency === undefined ? [] : [[task.name, task.concurrency.limit]],
    ),
  );
}

function nextTaskNameInTurnOf(
  taskNames: readonly string[],
  lastPrunedTaskName: string | undefined,
): string | undefined {
  const sortedTaskNames = [...taskNames].sort();
  return (
    sortedTaskNames.find((taskName) => lastPrunedTaskName === undefined || taskName > lastPrunedTaskName) ??
    sortedTaskNames[0]
  );
}

export function startWorkerLoop(config: WorkerLoopConfig): WorkerLoop {
  const { engine, definitions, timing, logger, concurrency, redisOutage } = config;
  const { lockDurationInMilliseconds } = timing;
  const concurrencyLimitsByTask = concurrencyLimitsByTaskOf(definitions.tasksByName);
  const scheduledTaskNames = declaredSchedulesOf(definitions).map(({ task }) => task);
  const attemptsInFlightByLockToken = new Map<LockToken, AttemptInFlight>();
  let isFetching = true;
  let isFetchCued = false;
  let fetchCueWaiter: (() => void) | undefined;
  let pendingWakeUp: Promise<void> | undefined;
  let knownTaskNames: readonly string[] = [];
  let lastPrunedTaskName: string | undefined;
  let tickInProgress: Promise<void> | undefined;
  let fetchPassOverEndsAt: EpochMilliseconds = 0;
  let promotePassOverEndsAt: EpochMilliseconds = 0;
  let isDraining = false;
  let isShutdownDeadlinePassed = false;
  const fetchStopController = new AbortController();
  const tickStopController = new AbortController();
  const settleStopController = new AbortController();
  const loggedPassedOverRecordVersions = new Set<number | undefined>();
  const loggedUnreadableSchedules = new Set<string>();

  const logPassOver = (recordVersions: readonly (number | undefined)[]) => {
    const recordVersionsNotLoggedYet = [...new Set(recordVersions)].filter(
      (recordVersion) => !loggedPassedOverRecordVersions.has(recordVersion),
    );
    for (const recordVersion of recordVersionsNotLoggedYet) {
      loggedPassedOverRecordVersions.add(recordVersion);
    }
    writeLifecycleLines(logger, recordVersionsNotLoggedYet.map(passOverLineOf));
  };

  const writeWorkerError = (error: unknown) => {
    writeLifecycleLines(logger, [workerErrorLineOf(errorFromThrownValue(error))]);
  };

  const writeWorkerErrorUnlessStopped = (stopSignal: AbortSignal) => (error: unknown) => {
    if (!stopSignal.aborted) {
      writeWorkerError(error);
    }
  };

  const cueFetch = () => {
    isFetchCued = true;
    fetchCueWaiter?.();
  };

  const fetchCued = () =>
    isFetchCued
      ? Promise.resolve()
      : new Promise<void>((resolve) => {
          fetchCueWaiter = resolve;
        });

  const keepWaitingForWakeUp = () => {
    if (pendingWakeUp !== undefined || !isFetching || concurrency - attemptsInFlightByLockToken.size <= 0) {
      return;
    }
    pendingWakeUp = (async () => {
      await config.waitForWakeUp(wakeUpBlockInMillisecondsOf(timing));
    })().then(
      () => {
        pendingWakeUp = undefined;
        cueFetch();
      },
      (error: unknown) => {
        pendingWakeUp = undefined;
        if (!redisOutage.isRedisAway(error)) {
          writeWorkerError(error);
        }
      },
    );
  };

  const abortAttempt = (attemptInFlight: AttemptInFlight, abortCause: AttemptAbortCause): boolean => {
    const { abortController, lockedRun } = attemptInFlight;
    if (abortController.signal.aborted || !attemptInFlight.isExecuting) {
      return false;
    }
    abortController.abort(new AttemptAbortedError(abortCause, lockedRun.runId));
    if (abortCause === "lock lost") {
      writeLifecycleLines(logger, [lockLostAbortLineOf(lockedRun)]);
    }
    return true;
  };

  const stopAttemptsPastTheirLock = () => {
    const now = Date.now();
    for (const attemptInFlight of attemptsInFlightByLockToken.values()) {
      if (now >= attemptInFlight.lockExpiresAtByWorkerClock) {
        abortAttempt(attemptInFlight, "lock lost");
      }
    }
  };

  const attemptResultOf = async (
    task: AnyTaskDefinition | undefined,
    attemptInFlight: AttemptInFlight,
  ): Promise<AttemptResult> => {
    const { lockedRun, abortController } = attemptInFlight;
    if (task === undefined) {
      await delayUnlessAborted(timing.unknownNameHandBackDelayInMilliseconds, abortController.signal);
      return handBackAttemptOf(lockedRun, definitions);
    }
    return executeLockedRunAttempt({
      engine,
      task,
      lockedRun,
      definitions,
      sender: config.sender,
      logger,
      abortSignal: abortController.signal,
      abortAttempt: (abortCause) => {
        abortAttempt(attemptInFlight, abortCause);
      },
      redisOutage,
      isWorkerDraining: () => isDraining,
    });
  };

  const settledAttemptResultOf = async (
    task: AnyTaskDefinition | undefined,
    attemptInFlight: AttemptInFlight,
  ): Promise<AttemptResult | undefined> => {
    const attemptResult = await attemptResultOf(task, attemptInFlight);
    attemptInFlight.isExecuting = false;
    const abortCause = abortCauseOf(attemptInFlight.abortController.signal);
    if (abortCause === "lock lost") {
      return undefined;
    }
    return abortCause === "shutdown deadline" && !OUTCOME_KINDS_KEPT_AT_SHUTDOWN.has(attemptResult.attemptOutcome.kind)
      ? { attemptOutcome: { kind: "yield" }, listenersLeftOut: [] }
      : attemptResult;
  };

  const executeAttempt = async (
    task: AnyTaskDefinition | undefined,
    attemptInFlight: AttemptInFlight,
    fetchRepliedAt: EpochMilliseconds,
  ) => {
    const { lockedRun } = attemptInFlight;
    const attemptResult = await settledAttemptResultOf(task, attemptInFlight);
    if (attemptResult === undefined || settleStopController.signal.aborted) {
      return;
    }
    const { attemptOutcome, listenersLeftOut } = attemptResult;
    const settleRequest = settleRequestOf(task, lockedRun, attemptOutcome);
    const settleReply = await answeredDespiteOutage(redisOutage, () => engine.settle(settleRequest), {
      resendStopSignal: settleStopController.signal,
    });
    writeLifecycleLines(
      logger,
      settleLinesOf({
        settledRun: lockedRun,
        attemptOutcome,
        settleReply,
        elapsedSinceFetchInMilliseconds: Date.now() - fetchRepliedAt,
        listenersLeftOut,
      }),
    );
  };

  const startAttempt = (lockedRun: LockedRun, fetchSentAt: EpochMilliseconds, fetchRepliedAt: EpochMilliseconds) => {
    const task = definitions.tasksByName.get(lockedRun.task);
    if (task !== undefined) {
      writeLifecycleLines(logger, [pickupLineOf(lockedRun)]);
    }
    const attemptInFlight: AttemptInFlight = {
      lockedRun,
      lockExpiresAtByWorkerClock: fetchSentAt + lockDurationInMilliseconds,
      abortController: new AbortController(),
      isExecuting: true,
      settlement: Promise.resolve(),
    };
    attemptsInFlightByLockToken.set(lockedRun.lockToken, attemptInFlight);
    if (isShutdownDeadlinePassed) {
      abortAttempt(attemptInFlight, "shutdown deadline");
    }
    attemptInFlight.settlement = executeAttempt(task, attemptInFlight, fetchRepliedAt)
      .catch(writeWorkerErrorUnlessStopped(settleStopController.signal))
      .finally(() => {
        attemptsInFlightByLockToken.delete(lockedRun.lockToken);
        cueFetch();
      });
  };

  const giveBackLateLock = async (lockedRun: LockedRun) => {
    const settleRequest = settleRequestOf(definitions.tasksByName.get(lockedRun.task), lockedRun, { kind: "yield" });
    try {
      const settleReply = await answeredDespiteOutage(redisOutage, () => engine.settle(settleRequest), {
        resendStopSignal: fetchStopController.signal,
      });
      if (settleReply.kind === "unknownVersion") {
        writeLifecycleLines(logger, [settleRefusalLineOf("yield", lockedRun.runId, settleReply)]);
      }
    } catch (error) {
      if (!redisOutage.isRedisAway(error)) {
        writeWorkerErrorUnlessStopped(fetchStopController.signal)(error);
      }
    }
  };

  const fetchOnce = async () => {
    const freeCapacity = concurrency - attemptsInFlightByLockToken.size;
    if (freeCapacity <= 0) {
      return;
    }
    const lockTokens = Array.from({ length: Math.min(freeCapacity, timing.mostLockTokensPerFetch) }, freshLockTokenOf);
    const fetchSentAt = Date.now();
    const reply = await answeredDespiteOutage(
      redisOutage,
      () => engine.fetch({ lockTokens, concurrencyLimitsByTask, lockDurationInMilliseconds }),
      { resendStopSignal: fetchStopController.signal },
    );
    if (reply.kind === "unknownVersion") {
      fetchPassOverEndsAt = Date.now() + timing.passOverWaitInMilliseconds;
      logPassOver([reply.recordVersion]);
      return;
    }
    const fetchRepliedAt = Date.now();
    logPassOver(reply.passedOverRecordVersions);
    writeLifecycleLines(logger, reply.parkedRuns.map(parkLineOf));
    if (fetchRepliedAt > fetchSentAt + lockDurationInMilliseconds) {
      await Promise.all(reply.lockedRuns.map(giveBackLateLock));
      return;
    }
    for (const lockedRun of reply.lockedRuns) {
      startAttempt(lockedRun, fetchSentAt, fetchRepliedAt);
    }
  };

  const fetchLoopEnded = (async () => {
    while (isFetching) {
      isFetchCued = false;
      await fetchOnce().catch(writeWorkerErrorUnlessStopped(fetchStopController.signal));
      if (!isFetching) {
        break;
      }
      const passOverWaitLeftInMilliseconds = fetchPassOverEndsAt - Date.now();
      if (passOverWaitLeftInMilliseconds > 0) {
        await delayUnlessAborted(passOverWaitLeftInMilliseconds, fetchStopController.signal);
        continue;
      }
      keepWaitingForWakeUp();
      await fetchCued();
    }
  })();

  const pruneRequestOfThisTick = (): PruneRequest | undefined => {
    const taskName = nextTaskNameInTurnOf(knownTaskNames, lastPrunedTaskName);
    return taskName === undefined
      ? undefined
      : {
          task: taskName,
          completedRetention: timing.completedRunRetention,
          failedRetention: timing.failedRunRetention,
        };
  };

  const failThirdStall = async (thirdStall: ThirdStall) => {
    const { task, runId, expiredLockToken, attempt } = thirdStall;
    const { attemptOutcome, listenersLeftOut } = failingAttemptOf(
      thirdStall,
      { error: new StalledRunError(runId, timing.stallCountThatFailsARun), reason: "stalled" },
      definitions,
    );
    const settleRequest = settleRequestOf(
      definitions.tasksByName.get(task),
      { task, runId, lockToken: expiredLockToken, attempt },
      attemptOutcome,
    );
    try {
      const settleReply = await answeredDespiteOutage(redisOutage, () => engine.settle(settleRequest), {
        resendStopSignal: tickStopController.signal,
      });
      if (settleReply.kind === "failed" || settleReply.kind === "unknownVersion") {
        writeLifecycleLines(
          logger,
          settleLinesOf({
            settledRun: thirdStall,
            attemptOutcome,
            settleReply,
            elapsedSinceFetchInMilliseconds: 0,
            listenersLeftOut,
          }),
        );
      }
    } catch (error) {
      writeWorkerErrorUnlessStopped(tickStopController.signal)(error);
    }
  };

  const tickOnce = async () => {
    const tickSentAt = Date.now();
    const lockRenewalDeadline = tickSentAt + timing.lockTimeLeftThatTriggersRenewalInMilliseconds;
    const renewedAttempts = [...attemptsInFlightByLockToken.values()].filter(
      ({ lockExpiresAtByWorkerClock, abortController }) =>
        lockExpiresAtByWorkerClock < lockRenewalDeadline && !abortController.signal.aborted,
    );
    const prune = pruneRequestOfThisTick();
    const tickRequest: TickRequest = {
      heldLocks: renewedAttempts.map(({ lockedRun: { task, runId, lockToken } }) => ({ task, runId, lockToken })),
      lockDurationInMilliseconds,
      ...(prune === undefined ? {} : { prune }),
      isPromoteIncluded: tickSentAt >= promotePassOverEndsAt,
      ...(scheduledTaskNames.length === 0 ? {} : { scheduledTaskNames }),
    };
    const awayErrorOf = (reply: TickReply): Error | undefined =>
      failedCommandErrorsOf(reply).find((error) => redisOutage.isRedisAway(error));
    const reply = await answeredDespiteOutage(redisOutage, () => engine.tick(tickRequest), {
      resendStopSignal: tickStopController.signal,
    });
    lastPrunedTaskName = prune?.task;
    if (reply.renewals.kind === "replied") {
      for (const [position, renewal] of reply.renewals.reply.entries()) {
        const renewedAttempt = renewedAttempts[position];
        if (renewedAttempt === undefined) {
          continue;
        }
        if (renewal.kind === "renewed") {
          renewedAttempt.lockExpiresAtByWorkerClock = tickSentAt + lockDurationInMilliseconds;
        } else {
          abortAttempt(renewedAttempt, "lock lost");
        }
      }
    }
    if (reply.taskNames.kind === "replied") {
      knownTaskNames = reply.taskNames.reply;
    }
    writeLifecycleLines(
      logger,
      failedCommandErrorsOf(reply)
        .filter((error) => !redisOutage.isRedisAway(error))
        .map(workerErrorLineOf),
    );
    if (reply.promote?.kind === "replied" && reply.promote.reply.kind === "unknownVersion") {
      promotePassOverEndsAt = Date.now() + timing.passOverWaitInMilliseconds;
      logPassOver([reply.promote.reply.recordVersion]);
    }
    const secondRoundTrips: Promise<void>[] = [];
    if (reply.promote?.kind === "replied" && reply.promote.reply.kind === "promoted") {
      const { stalledRuns, thirdStalls } = reply.promote.reply;
      writeLifecycleLines(logger, stalledRuns.map(stallLineOf));
      secondRoundTrips.push(...thirdStalls.map(failThirdStall));
    }
    if (reply.dueSchedules?.kind === "replied" && reply.dueSchedules.reply.dueSchedules.length > 0) {
      secondRoundTrips.push(
        fireDueSchedules(
          {
            engine,
            definitions,
            logger,
            redisOutage,
            stopSignal: tickStopController.signal,
            loggedUnreadableSchedules,
          },
          reply.dueSchedules.reply,
        ),
      );
    }
    await Promise.all(secondRoundTrips);
    const awayError = awayErrorOf(reply);
    if (awayError !== undefined) {
      await untilAborted(redisOutage.waitForRedis(awayError, tickStopController.signal), tickStopController.signal);
    }
  };

  const tickTimer = setInterval(() => {
    stopAttemptsPastTheirLock();
    if (tickInProgress !== undefined) {
      return;
    }
    tickInProgress = tickOnce()
      .catch(writeWorkerErrorUnlessStopped(tickStopController.signal))
      .finally(() => {
        tickInProgress = undefined;
        cueFetch();
      });
  }, timing.tickIntervalInMilliseconds);

  return {
    stopFetching: async () => {
      isFetching = false;
      fetchStopController.abort();
      cueFetch();
      await fetchLoopEnded;
    },
    stopTicking: async () => {
      clearInterval(tickTimer);
      tickStopController.abort();
      await tickInProgress;
    },
    startDraining: () => {
      isDraining = true;
    },
    abortAttemptsAtShutdownDeadline: () => {
      isShutdownDeadlinePassed = true;
      return [...attemptsInFlightByLockToken.values()].filter((attemptInFlight) =>
        abortAttempt(attemptInFlight, "shutdown deadline"),
      ).length;
    },
    stopSettling: () => {
      settleStopController.abort();
    },
    waitForAttemptsInFlight: async () => {
      while (attemptsInFlightByLockToken.size > 0) {
        await Promise.all([...attemptsInFlightByLockToken.values()].map(({ settlement }) => settlement));
      }
    },
  };
}
