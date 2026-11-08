import { randomBytes } from "node:crypto";

import { AttemptAbortedError } from "./attempt-abort.js";
import type { AnyTaskDefinition } from "./definitions.js";
import type { DurationInMilliseconds, EpochMilliseconds } from "./durations.js";
import type { LockToken } from "./engine/engine-keys.js";
import type { Engine, LockedRun, PruneRequest } from "./engine/engine.js";
import type { WorkerDefinitions } from "./engine-failure-notice.js";
import { parkLineOf, pickupLineOf, settleLinesOf, workerErrorLineOf, writeLifecycleLines } from "./lifecycle-logs.js";
import { executeLockedRunAttempt, settleRequestOf } from "./locked-run-attempt.js";
import type { Logger } from "./logger.js";
import type { HandlerSender } from "./step-engine.js";
import type { WorkerTiming } from "./worker-timing.js";

export type WorkerLoopEngine = Pick<Engine, "fetch" | "settle" | "tick" | "saveStep" | "readStep">;

export type WaitForWakeUp = (timeoutInMilliseconds: DurationInMilliseconds) => Promise<void>;

export type WorkerLoopConfig = {
  engine: WorkerLoopEngine;
  definitions: WorkerDefinitions;
  timing: WorkerTiming;
  logger: Logger;
  concurrency: number;
  sender: HandlerSender;
  waitForWakeUp: WaitForWakeUp;
};

export type WorkerLoop = {
  stopFetching: () => Promise<void>;
  stopTicking: () => Promise<void>;
  waitForAttemptsInFlight: () => Promise<void>;
};

type AttemptInFlight = {
  lockedRun: LockedRun;
  lockExpiresAtByWorkerClock: EpochMilliseconds;
  settlement: Promise<void>;
};

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
  const { engine, definitions, timing, logger, concurrency } = config;
  const { lockDurationInMilliseconds } = timing;
  const concurrencyLimitsByTask = concurrencyLimitsByTaskOf(definitions.tasksByName);
  const attemptsInFlightByLockToken = new Map<LockToken, AttemptInFlight>();
  let isFetching = true;
  let isFetchCued = false;
  let fetchCueWaiter: (() => void) | undefined;
  let pendingWakeUp: Promise<void> | undefined;
  let knownTaskNames: readonly string[] = [];
  let lastPrunedTaskName: string | undefined;
  let tickInProgress: Promise<void> | undefined;

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

  const wakeUpOrCue = () => {
    pendingWakeUp ??= config.waitForWakeUp(timing.tickIntervalInMilliseconds).finally(() => {
      pendingWakeUp = undefined;
    });
    return Promise.race([pendingWakeUp, fetchCued()]);
  };

  const executeAttempt = async (task: AnyTaskDefinition, lockedRun: LockedRun, fetchRepliedAt: EpochMilliseconds) => {
    const abortController = new AbortController();
    const { attemptOutcome, listenersLeftOut } = await executeLockedRunAttempt({
      engine,
      task,
      lockedRun,
      definitions,
      sender: config.sender,
      logger,
      abortSignal: abortController.signal,
      abortAttempt: (abortCause) => {
        abortController.abort(new AttemptAbortedError(abortCause, lockedRun.runId));
      },
      isWorkerDraining: () => false,
    });
    writeLifecycleLines(
      logger,
      listenersLeftOut.map(({ error }) => workerErrorLineOf(error)),
    );
    const settleReply = await engine.settle(settleRequestOf(task, lockedRun, attemptOutcome));
    writeLifecycleLines(
      logger,
      settleLinesOf({
        lockedRun,
        attemptOutcome,
        settleReply,
        elapsedSinceFetchInMilliseconds: Date.now() - fetchRepliedAt,
      }),
    );
  };

  const startAttempt = (lockedRun: LockedRun, fetchSentAt: EpochMilliseconds, fetchRepliedAt: EpochMilliseconds) => {
    const task = definitions.tasksByName.get(lockedRun.task);
    if (task === undefined) {
      return;
    }
    writeLifecycleLines(logger, [pickupLineOf(lockedRun)]);
    const settlement = executeAttempt(task, lockedRun, fetchRepliedAt)
      .catch(() => undefined)
      .finally(() => {
        attemptsInFlightByLockToken.delete(lockedRun.lockToken);
        cueFetch();
      });
    attemptsInFlightByLockToken.set(lockedRun.lockToken, {
      lockedRun,
      lockExpiresAtByWorkerClock: fetchSentAt + lockDurationInMilliseconds,
      settlement,
    });
  };

  const fetchOnce = async () => {
    const freeCapacity = concurrency - attemptsInFlightByLockToken.size;
    if (freeCapacity <= 0) {
      return;
    }
    const lockTokens = Array.from({ length: Math.min(freeCapacity, timing.mostLockTokensPerFetch) }, freshLockTokenOf);
    const fetchSentAt = Date.now();
    const reply = await engine.fetch({ lockTokens, concurrencyLimitsByTask, lockDurationInMilliseconds });
    if (reply.kind !== "fetched") {
      return;
    }
    const fetchRepliedAt = Date.now();
    writeLifecycleLines(logger, reply.parkedRuns.map(parkLineOf));
    for (const lockedRun of reply.lockedRuns) {
      startAttempt(lockedRun, fetchSentAt, fetchRepliedAt);
    }
  };

  const fetchLoopEnded = (async () => {
    while (isFetching) {
      isFetchCued = false;
      await fetchOnce().catch(() => undefined);
      if (!isFetching) {
        break;
      }
      await (concurrency - attemptsInFlightByLockToken.size > 0 ? wakeUpOrCue() : fetchCued());
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

  const tickOnce = async () => {
    const tickSentAt = Date.now();
    const lockRenewalDeadline = tickSentAt + timing.lockTimeLeftThatTriggersRenewalInMilliseconds;
    const renewedAttempts = [...attemptsInFlightByLockToken.values()].filter(
      ({ lockExpiresAtByWorkerClock }) => lockExpiresAtByWorkerClock < lockRenewalDeadline,
    );
    const prune = pruneRequestOfThisTick();
    const reply = await engine.tick({
      heldLocks: renewedAttempts.map(({ lockedRun: { runId, lockToken } }) => ({ runId, lockToken })),
      lockDurationInMilliseconds,
      ...(prune === undefined ? {} : { prune }),
    });
    for (const [position, renewal] of reply.renewals.entries()) {
      const renewedAttempt = renewedAttempts[position];
      if (renewal.kind === "renewed" && renewedAttempt !== undefined) {
        renewedAttempt.lockExpiresAtByWorkerClock = tickSentAt + lockDurationInMilliseconds;
      }
    }
    lastPrunedTaskName = prune?.task;
    knownTaskNames = reply.taskNames;
  };

  const tickTimer = setInterval(() => {
    if (tickInProgress !== undefined) {
      return;
    }
    tickInProgress = tickOnce()
      .catch(() => undefined)
      .finally(() => {
        tickInProgress = undefined;
        cueFetch();
      });
  }, timing.tickIntervalInMilliseconds);

  return {
    stopFetching: async () => {
      isFetching = false;
      cueFetch();
      await fetchLoopEnded;
    },
    stopTicking: async () => {
      clearInterval(tickTimer);
      await tickInProgress;
    },
    waitForAttemptsInFlight: async () => {
      while (attemptsInFlightByLockToken.size > 0) {
        await Promise.all([...attemptsInFlightByLockToken.values()].map(({ settlement }) => settlement));
      }
    },
  };
}
