import { randomBytes } from "node:crypto";

import { AttemptAbortedError } from "./attempt-abort.js";
import type { AttemptAbortCause } from "./attempt-abort.js";
import { errorFromThrownValue, StalledRunError } from "./attempt-errors.js";
import type { AnyTaskDefinition } from "./definitions.js";
import type { DurationInMilliseconds, EpochMilliseconds } from "./durations.js";
import type { LockToken } from "./engine/engine-keys.js";
import type { Engine, LockedRun, PruneRequest, ThirdStall } from "./engine/engine.js";
import type { WorkerDefinitions } from "./engine-failure-notice.js";
import {
  lockLostAbortLineOf,
  parkLineOf,
  pickupLineOf,
  settleLinesOf,
  stallLineOf,
  workerErrorLineOf,
  writeLifecycleLines,
} from "./lifecycle-logs.js";
import { executeLockedRunAttempt, failingAttemptOf, settleRequestOf } from "./locked-run-attempt.js";
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
  abortController: AbortController;
  isExecuting: boolean;
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

  const keepWaitingForWakeUp = () => {
    if (pendingWakeUp !== undefined || !isFetching || concurrency - attemptsInFlightByLockToken.size <= 0) {
      return;
    }
    pendingWakeUp = (async () => {
      await config.waitForWakeUp(timing.tickIntervalInMilliseconds);
    })().then(
      () => {
        pendingWakeUp = undefined;
        cueFetch();
      },
      () => {
        pendingWakeUp = undefined;
      },
    );
  };

  const abortAttempt = (attemptInFlight: AttemptInFlight, abortCause: AttemptAbortCause) => {
    const { abortController, lockedRun } = attemptInFlight;
    if (abortController.signal.aborted || !attemptInFlight.isExecuting) {
      return;
    }
    abortController.abort(new AttemptAbortedError(abortCause, lockedRun.runId));
    if (abortCause === "lock lost") {
      writeLifecycleLines(logger, [lockLostAbortLineOf(lockedRun)]);
    }
  };

  const stopAttemptsPastTheirLock = () => {
    const now = Date.now();
    for (const attemptInFlight of attemptsInFlightByLockToken.values()) {
      if (now >= attemptInFlight.lockExpiresAtByWorkerClock) {
        abortAttempt(attemptInFlight, "lock lost");
      }
    }
  };

  const executeAttempt = async (
    task: AnyTaskDefinition,
    attemptInFlight: AttemptInFlight,
    fetchRepliedAt: EpochMilliseconds,
  ) => {
    const { lockedRun, abortController } = attemptInFlight;
    const { attemptOutcome, listenersLeftOut } = await executeLockedRunAttempt({
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
      isWorkerDraining: () => false,
    });
    attemptInFlight.isExecuting = false;
    if (abortController.signal.aborted) {
      return;
    }
    const settleReply = await engine.settle(settleRequestOf(task, lockedRun, attemptOutcome));
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
    if (task === undefined) {
      return;
    }
    writeLifecycleLines(logger, [pickupLineOf(lockedRun)]);
    const attemptInFlight: AttemptInFlight = {
      lockedRun,
      lockExpiresAtByWorkerClock: fetchSentAt + lockDurationInMilliseconds,
      abortController: new AbortController(),
      isExecuting: true,
      settlement: Promise.resolve(),
    };
    attemptsInFlightByLockToken.set(lockedRun.lockToken, attemptInFlight);
    attemptInFlight.settlement = executeAttempt(task, attemptInFlight, fetchRepliedAt)
      .catch(() => undefined)
      .finally(() => {
        attemptsInFlightByLockToken.delete(lockedRun.lockToken);
        cueFetch();
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
    const { runId, expiredLockToken, attempt } = thirdStall;
    const { attemptOutcome, listenersLeftOut } = failingAttemptOf(
      thirdStall,
      { error: new StalledRunError(runId, timing.stallCountThatFailsARun), reason: "stalled" },
      definitions,
    );
    try {
      const settleReply = await engine.settle(
        settleRequestOf(
          definitions.tasksByName.get(thirdStall.task),
          { runId, lockToken: expiredLockToken, attempt },
          attemptOutcome,
        ),
      );
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
      writeLifecycleLines(logger, [workerErrorLineOf(errorFromThrownValue(error))]);
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
    const reply = await engine.tick({
      heldLocks: renewedAttempts.map(({ lockedRun: { runId, lockToken } }) => ({ runId, lockToken })),
      lockDurationInMilliseconds,
      ...(prune === undefined ? {} : { prune }),
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
      [reply.promote, reply.renewals, reply.taskNames, reply.prune].flatMap((commandReply) =>
        commandReply?.kind === "failed" ? [workerErrorLineOf(commandReply.error)] : [],
      ),
    );
    if (reply.promote?.kind === "replied" && reply.promote.reply.kind === "promoted") {
      const { stalledRuns, thirdStalls } = reply.promote.reply;
      writeLifecycleLines(logger, stalledRuns.map(stallLineOf));
      await Promise.all(thirdStalls.map(failThirdStall));
    }
  };

  const tickTimer = setInterval(() => {
    stopAttemptsPastTheirLock();
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
