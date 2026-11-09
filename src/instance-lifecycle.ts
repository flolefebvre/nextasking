import { isUnansweredError } from "./connection.js";
import type { RedisConnection, RedisConnections } from "./connection.js";
import type { DurationInMilliseconds } from "./durations.js";
import type { Engine } from "./engine/engine.js";
import type { WorkerDefinitions } from "./definitions.js";
import { syncDeclaredSchedules } from "./worker-schedules.js";
import { InstanceClosedError } from "./errors.js";
import type { Logger } from "./logger.js";
import type { RedisClient } from "./redis-client.js";
import { answeredDespiteOutage, untilAborted } from "./redis-outage.js";
import type { RedisOutage } from "./redis-outage.js";
import { checkServerInfoReading, readServerInfo } from "./server-info.js";
import type { ServerInfoReading } from "./server-info.js";
import { isSettledWithin } from "./settled-within.js";
import {
  MISSING_MANUAL_SIGNAL_HANDLING_WARNING,
  exitOnShutdownSignals,
  isDevelopment,
  isSignalHandlingLeftToTheApp,
} from "./shutdown-signals.js";
import type { HandlerSender } from "./step-engine.js";
import { startWorkerLoop, wakeUpBlockInMillisecondsOf } from "./worker-loop.js";
import type { WaitForWakeUp, WorkerLoop } from "./worker-loop.js";
import { callOnWorkerNotStarted, workerOptionsFrom } from "./worker-options.js";
import type { StartWorkerOptions, WorkerOptions } from "./worker-options.js";
import { workerTiming } from "./worker-timing.js";
import type { WorkerTiming } from "./worker-timing.js";

export type InstanceLifecycleConfig = {
  connections: RedisConnections;
  engineOn: (client: RedisClient) => Engine;
  wakeUpWaitOn: (blockingClient: RedisClient) => WaitForWakeUp;
  senderOn: (engine: Engine) => HandlerSender;
  definitions: WorkerDefinitions;
  logger: Logger;
  timing?: WorkerTiming;
  env?: NodeJS.ProcessEnv;
};

export type InstanceCalls = { client: RedisClient; engine: Engine };

type OpenedCommandConnection = {
  commandConnection: RedisConnection;
  instanceCalls: InstanceCalls;
  workerEngine: Engine;
};

export type InstanceLifecycle = {
  callWhileOpen: <Result>(call: (instanceCalls: InstanceCalls) => Promise<Result>) => Promise<Result>;
  startWorker: (options?: StartWorkerOptions) => Promise<void>;
  close: () => Promise<void>;
};

type RunningWorker = {
  loop: WorkerLoop;
  blockingConnection: RedisConnection;
  timing: WorkerTiming;
  shutdownTimeoutInMilliseconds: DurationInMilliseconds;
};

type WorkerBoot = {
  opened: OpenedCommandConnection;
  timing: WorkerTiming;
  workerOptions: WorkerOptions;
  redisOutage: RedisOutage;
};

let isWorkerStartedInThisProcess = false;

function ignoreRejection(promise: Promise<unknown>): void {
  promise.catch(() => undefined);
}

export function createInstanceLifecycle(config: InstanceLifecycleConfig): InstanceLifecycle {
  const { connections, definitions, logger, env = process.env } = config;
  const closeController = new AbortController();
  const commandConnectionClosedController = new AbortController();
  let openedCommandConnection: OpenedCommandConnection | undefined;
  let runningWorker: RunningWorker | undefined;
  let isThisInstanceTheProcessWorker = false;
  let stopExitingOnShutdownSignals: (() => void) | undefined;
  let closing: Promise<void> | undefined;

  const rejectIfClosed = () => {
    if (closeController.signal.aborted) {
      throw new InstanceClosedError();
    }
  };

  const openCommandConnection = (): OpenedCommandConnection => {
    if (openedCommandConnection === undefined) {
      const commandConnection = connections.createCommandConnection();
      const { instanceCalls, workerCalls } = commandConnection;
      openedCommandConnection = {
        commandConnection,
        instanceCalls: { client: instanceCalls, engine: config.engineOn(instanceCalls) },
        workerEngine: config.engineOn(workerCalls),
      };
    }
    return openedCommandConnection;
  };

  const releaseTheProcessWorker = () => {
    if (isThisInstanceTheProcessWorker) {
      isThisInstanceTheProcessWorker = false;
      isWorkerStartedInThisProcess = false;
    }
  };

  const callWhileOpen = async <Result>(call: (instanceCalls: InstanceCalls) => Promise<Result>): Promise<Result> => {
    rejectIfClosed();
    const { instanceCalls } = openCommandConnection();
    try {
      return await untilAborted(call(instanceCalls), commandConnectionClosedController.signal);
    } catch (error) {
      if (closeController.signal.aborted && isUnansweredError(error)) {
        throw new InstanceClosedError();
      }
      throw error;
    }
  };

  const startTheWorker = ({ opened: { workerEngine: engine }, timing, workerOptions, redisOutage }: WorkerBoot) => {
    const blockingConnection = connections.createBlockingConnection(wakeUpBlockInMillisecondsOf(timing));
    const loop = startWorkerLoop({
      engine,
      definitions,
      timing,
      logger,
      concurrency: workerOptions.concurrency,
      sender: config.senderOn(engine),
      waitForWakeUp: config.wakeUpWaitOn(blockingConnection.workerCalls),
      redisOutage,
    });
    runningWorker = {
      loop,
      blockingConnection,
      timing,
      shutdownTimeoutInMilliseconds: workerOptions.shutdownTimeoutInMilliseconds,
    };
    logger.info({}, "worker started");
  };

  const answeredUnlessClosed = <Answer>(redisOutage: RedisOutage, call: () => Promise<Answer>): Promise<Answer> =>
    untilAborted(
      answeredDespiteOutage(redisOutage, call, { resendStopSignal: closeController.signal }),
      closeController.signal,
    );

  const bootTheWorker = async (workerBoot: WorkerBoot, serverInfoReading: Promise<ServerInfoReading>) => {
    const { opened, redisOutage } = workerBoot;
    checkServerInfoReading(await serverInfoReading, logger);
    await syncDeclaredSchedules({
      engine: {
        serverInstant: () => answeredUnlessClosed(redisOutage, () => opened.workerEngine.serverInstant()),
        syncSchedules: (request) => answeredUnlessClosed(redisOutage, () => opened.workerEngine.syncSchedules(request)),
      },
      definitions,
      logger,
    });
    rejectIfClosed();
    startTheWorker(workerBoot);
  };

  const finishBootInBackground = async (
    boot: Promise<void>,
    onWorkerNotStarted: StartWorkerOptions["onWorkerNotStarted"],
  ) => {
    try {
      await boot;
    } catch (error) {
      if (closeController.signal.aborted) {
        return;
      }
      releaseTheProcessWorker();
      logger.error({ err: error }, "worker not started");
      await callOnWorkerNotStarted(onWorkerNotStarted, error, logger);
    }
  };

  const startWorker = async (options: StartWorkerOptions = {}) => {
    rejectIfClosed();
    const workerOptions = workerOptionsFrom(options);
    if (isWorkerStartedInThisProcess) {
      throw new Error("The nextasking worker is already started in this process");
    }
    isWorkerStartedInThisProcess = true;
    isThisInstanceTheProcessWorker = true;
    const timing = config.timing ?? workerTiming();
    const opened = openCommandConnection();
    const redisOutage = connections.workerRedisOutageOf(opened.commandConnection, timing.tickIntervalInMilliseconds);
    const serverInfoReading = answeredUnlessClosed(redisOutage, () =>
      readServerInfo(opened.commandConnection.workerCalls),
    );
    ignoreRejection(serverInfoReading);
    const boot = bootTheWorker({ opened, timing, workerOptions, redisOutage }, serverInfoReading);
    ignoreRejection(boot);
    const isFirstRoundTripAnswered = await isSettledWithin(
      serverInfoReading.catch(() => undefined),
      connections.giveUpAfterInMilliseconds,
    );
    if (isFirstRoundTripAnswered) {
      try {
        await boot;
      } catch (error) {
        releaseTheProcessWorker();
        throw error;
      }
    } else {
      void finishBootInBackground(boot, options.onWorkerNotStarted);
    }
    if (isSignalHandlingLeftToTheApp(env)) {
      stopExitingOnShutdownSignals = exitOnShutdownSignals(close, logger);
    } else if (!isDevelopment(env)) {
      logger.warn({}, MISSING_MANUAL_SIGNAL_HANDLING_WARNING);
    }
  };

  const shutDownWorker = async ({ loop, blockingConnection, timing, shutdownTimeoutInMilliseconds }: RunningWorker) => {
    loop.startDraining();
    const fetchingStopped = loop.stopFetching();
    await connections.closeConnection(blockingConnection);
    const drained = fetchingStopped.then(() => loop.waitForAttemptsInFlight());
    if (!(await isSettledWithin(drained, shutdownTimeoutInMilliseconds))) {
      if (loop.abortAttemptsAtShutdownDeadline() > 0) {
        logger.warn({}, "attempts aborted at shutdown deadline");
      }
      if (!(await isSettledWithin(drained, timing.abortedAttemptsGraceInMilliseconds))) {
        loop.stopSettling();
      }
    }
    return { loopStopped: Promise.all([drained, loop.stopTicking()]) };
  };

  const closeInstance = async () => {
    closeController.abort(new InstanceClosedError());
    const worker = runningWorker;
    const timing = worker?.timing ?? config.timing ?? workerTiming();
    const closeStartedAt = Date.now();
    const { loopStopped } = worker === undefined ? { loopStopped: Promise.resolve() } : await shutDownWorker(worker);
    if (openedCommandConnection !== undefined) {
      await connections.closeConnection(
        openedCommandConnection.commandConnection,
        timing.commandConnectionCloseBoundInMilliseconds,
      );
    }
    commandConnectionClosedController.abort(new InstanceClosedError());
    const closeBoundInMilliseconds =
      (worker === undefined ? 0 : worker.shutdownTimeoutInMilliseconds + timing.abortedAttemptsGraceInMilliseconds) +
      timing.commandConnectionCloseBoundInMilliseconds;
    const closeTimeLeftInMilliseconds = closeStartedAt + closeBoundInMilliseconds - Date.now();
    if (closeTimeLeftInMilliseconds > 0) {
      await isSettledWithin(loopStopped, closeTimeLeftInMilliseconds);
    }
    if (worker !== undefined) {
      logger.info({}, "worker stopped");
    }
    releaseTheProcessWorker();
    stopExitingOnShutdownSignals?.();
    stopExitingOnShutdownSignals = undefined;
  };

  const close = (): Promise<void> => {
    closing ??= closeInstance();
    return closing;
  };

  return { callWhileOpen, startWorker, close };
}
