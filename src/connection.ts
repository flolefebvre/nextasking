import { parseDuration } from "./durations.js";
import type { Duration, DurationInMilliseconds } from "./durations.js";
import { RedisUnavailableError } from "./errors.js";
import type { RedisCallIds } from "./errors.js";
import type { Logger } from "./logger.js";
import {
  createNodeRedisDriver,
  isOutOfMemoryReply,
  isUnansweredCommandError,
  unansweredWithinDeadlineError,
} from "./node-redis-driver.js";
import type { CreateRedisDriver, RedisDriver } from "./node-redis-driver.js";
import type { CommandResult, RedisClient, RedisCommand } from "./redis-client.js";
import { delayUnlessAborted, untilAborted } from "./redis-outage.js";
import type { RedisOutage } from "./redis-outage.js";
import { isSettledWithin } from "./settled-within.js";

export type ConnectionConfig = {
  url: string;
  giveUpAfter?: Duration;
};

const DEFAULT_GIVE_UP_AFTER: Duration = "5s";

export function giveUpAfterInMillisecondsOf(connection: ConnectionConfig): DurationInMilliseconds {
  try {
    return parseDuration(connection.giveUpAfter ?? DEFAULT_GIVE_UP_AFTER);
  } catch (error) {
    throw new Error(`Invalid connection.giveUpAfter: ${(error as Error).message}`, { cause: error });
  }
}

class ConnectionClosedError extends Error {
  override readonly name = "ConnectionClosedError";

  constructor() {
    super("The Redis connection was closed by its nextasking instance");
  }
}

export function isUnansweredError(error: unknown): boolean {
  return isUnansweredCommandError(error) || error instanceof ConnectionClosedError;
}

export function isRedisAwayError(error: unknown): boolean {
  return isUnansweredError(error) || isOutOfMemoryReply(error);
}

export type RedisOutageSources = {
  outageTracker: ConnectionOutageTracker;
  waitUntilReady: (stopSignal?: AbortSignal) => Promise<void>;
  outOfMemoryRetryDelayInMilliseconds: DurationInMilliseconds;
};

export function createRedisOutage({
  outageTracker,
  waitUntilReady,
  outOfMemoryRetryDelayInMilliseconds,
}: RedisOutageSources): RedisOutage {
  const outOfMemorySubject = {};
  return {
    isRedisAway: isRedisAwayError,
    waitForRedis: async (awayError, stopSignal) => {
      if (!isOutOfMemoryReply(awayError)) {
        await waitUntilReady(stopSignal);
        return;
      }
      outageTracker.reportClientError(outOfMemorySubject, awayError);
      await delayUnlessAborted(outOfMemoryRetryDelayInMilliseconds, stopSignal);
    },
    noteRedisAnswered: () => {
      outageTracker.reportClientReady(outOfMemorySubject);
    },
  };
}

export type ConnectionOutageTracker = {
  reportClientError: (client: object, error: unknown) => void;
  reportClientReady: (client: object) => void;
  forgetClient: (client: object) => void;
  onConnectionRestored: (listener: () => void) => () => void;
};

export function createConnectionOutageTracker(logger: Logger): ConnectionOutageTracker {
  const clientsDown = new Set<object>();
  const connectionRestoredListeners = new Set<() => void>();
  return {
    reportClientError: (client, error) => {
      if (clientsDown.size === 0) {
        logger.error({ err: error }, "connection error");
      }
      clientsDown.add(client);
    },
    reportClientReady: (client) => {
      if (clientsDown.delete(client) && clientsDown.size === 0) {
        logger.info({}, "connection restored");
        for (const listener of connectionRestoredListeners) {
          listener();
        }
      }
    },
    forgetClient: (client) => {
      clientsDown.delete(client);
    },
    onConnectionRestored: (listener) => {
      const subscribedListener = () => {
        listener();
      };
      connectionRestoredListeners.add(subscribedListener);
      return () => {
        connectionRestoredListeners.delete(subscribedListener);
      };
    },
  };
}

export type RedisConnection = {
  readonly instanceCalls: RedisClient;
  readonly workerCalls: RedisClient;
};

export type RedisConnections = {
  readonly giveUpAfterInMilliseconds: DurationInMilliseconds;
  createCommandConnection: () => RedisConnection;
  createBlockingConnection: () => RedisConnection;
  waitUntilReady: (connection: RedisConnection, stopSignal?: AbortSignal) => Promise<void>;
  closeConnection: (connection: RedisConnection, closeBoundInMilliseconds?: DurationInMilliseconds) => Promise<void>;
  workerRedisOutageOf: (
    connection: RedisConnection,
    outOfMemoryRetryDelayInMilliseconds: DurationInMilliseconds,
  ) => RedisOutage;
  callOnRedis: <Result>(callIds: RedisCallIds, call: () => Promise<Result>) => Promise<Result>;
  onConnectionRestored: (listener: () => void) => () => void;
};

type ReadyWait = { promise: Promise<void>; markReady: () => void; markClosed: () => void };

type ConnectionState = {
  readonly driver: RedisDriver;
  readonly closedController: AbortController;
  errorWhileDown: unknown;
  readyWait: ReadyWait | undefined;
};

type Send<Reply> = (abortSignal?: AbortSignal) => Promise<Reply>;

function unrefTimer(timer: NodeJS.Timeout): NodeJS.Timeout {
  timer.unref();
  return timer;
}

function batchRetryErrorOf(results: readonly CommandResult[]): Error | undefined {
  return results.find(
    (result): result is { kind: "failed"; error: Error } => result.kind === "failed" && isUnansweredError(result.error),
  )?.error;
}

function noRetryError(): undefined {
  return undefined;
}

export function createRedisConnections(
  connection: ConnectionConfig,
  logger: Logger,
  createDriver: CreateRedisDriver = createNodeRedisDriver,
): RedisConnections {
  const giveUpAfterInMilliseconds = giveUpAfterInMillisecondsOf(connection);
  const outageTracker = createConnectionOutageTracker(logger);
  const stateByConnection = new WeakMap<RedisConnection, ConnectionState>();

  const stateOf = (redisConnection: RedisConnection): ConnectionState => {
    const state = stateByConnection.get(redisConnection);
    if (state === undefined) {
      throw new Error("This Redis connection was not opened by these connections");
    }
    return state;
  };

  const readyWaitOf = (state: ConnectionState): Promise<void> => {
    if (state.closedController.signal.aborted) {
      return Promise.reject(new ConnectionClosedError());
    }
    if (state.driver.isReady) {
      return Promise.resolve();
    }
    if (state.readyWait === undefined) {
      let markReady = () => undefined as void;
      let markClosed = () => undefined as void;
      const promise = new Promise<void>((resolve, reject) => {
        markReady = resolve;
        markClosed = () => {
          reject(new ConnectionClosedError());
        };
      });
      promise.catch(() => undefined);
      state.readyWait = { promise, markReady, markClosed };
    }
    return state.readyWait.promise;
  };

  const throwIfClosed = (state: ConnectionState) => {
    if (state.closedController.signal.aborted) {
      throw new ConnectionClosedError();
    }
  };

  const unansweredErrorAtTheDeadline = (state: ConnectionState): Error =>
    !state.driver.isReady && isUnansweredCommandError(state.errorWhileDown)
      ? (state.errorWhileDown as Error)
      : unansweredWithinDeadlineError();

  const sentWithinTheDeadline = async <Reply>(
    state: ConnectionState,
    send: Send<Reply>,
    retryErrorOf: (reply: Reply) => Error | undefined,
  ): Promise<Reply> => {
    const deadlineController = new AbortController();
    const deadlineTimer = unrefTimer(
      setTimeout(() => {
        deadlineController.abort(unansweredErrorAtTheDeadline(state));
      }, giveUpAfterInMilliseconds),
    );
    const deadlineSignal = deadlineController.signal;
    try {
      for (;;) {
        throwIfClosed(state);
        let retryError: Error | undefined;
        try {
          const reply = await untilAborted(send(deadlineSignal), deadlineSignal);
          retryError = retryErrorOf(reply);
          if (retryError === undefined) {
            return reply;
          }
        } catch (error) {
          if (deadlineSignal.aborted || !isUnansweredError(error)) {
            throw deadlineSignal.aborted ? (deadlineSignal.reason as Error) : error;
          }
          retryError = error as Error;
        }
        if (state.closedController.signal.aborted) {
          throw retryError;
        }
        await untilAborted(readyWaitOf(state), deadlineSignal);
      }
    } finally {
      clearTimeout(deadlineTimer);
    }
  };

  const sentWithoutDeadline = async <Reply>(state: ConnectionState, send: Send<Reply>): Promise<Reply> => {
    throwIfClosed(state);
    return send();
  };

  const instanceCallsOf = (state: ConnectionState): RedisClient => ({
    send: (command: RedisCommand) =>
      sentWithinTheDeadline(state, (abortSignal) => state.driver.send(command, abortSignal), noRetryError),
    sendBatch: (commands: readonly RedisCommand[]) =>
      sentWithinTheDeadline(state, (abortSignal) => state.driver.sendBatch(commands, abortSignal), batchRetryErrorOf),
  });

  const workerCallsOf = (state: ConnectionState): RedisClient => ({
    send: (command: RedisCommand) => sentWithoutDeadline(state, () => state.driver.send(command)),
    sendBatch: (commands: readonly RedisCommand[]) =>
      sentWithoutDeadline(state, () => state.driver.sendBatch(commands)),
  });

  const openConnection = (): RedisConnection => {
    const opened: { connection?: RedisConnection; state?: ConnectionState } = {};
    const openState = () =>
      opened.state === undefined || opened.state.closedController.signal.aborted ? undefined : opened.state;
    const driver = createDriver(connection.url, {
      onConnect: () => undefined,
      onReady: () => {
        const state = openState();
        if (state === undefined || opened.connection === undefined) {
          return;
        }
        state.errorWhileDown = undefined;
        state.readyWait?.markReady();
        state.readyWait = undefined;
        outageTracker.reportClientReady(opened.connection);
      },
      onError: (error) => {
        const state = openState();
        if (state === undefined || opened.connection === undefined) {
          return;
        }
        state.errorWhileDown = error;
        outageTracker.reportClientError(opened.connection, error);
      },
    });
    const state: ConnectionState = {
      driver,
      closedController: new AbortController(),
      errorWhileDown: undefined,
      readyWait: undefined,
    };
    const redisConnection: RedisConnection = {
      instanceCalls: instanceCallsOf(state),
      workerCalls: workerCallsOf(state),
    };
    opened.connection = redisConnection;
    opened.state = state;
    stateByConnection.set(redisConnection, state);
    driver.connect();
    return redisConnection;
  };

  return {
    giveUpAfterInMilliseconds,
    createCommandConnection: openConnection,
    createBlockingConnection: openConnection,
    waitUntilReady: (redisConnection, stopSignal) => untilAborted(readyWaitOf(stateOf(redisConnection)), stopSignal),
    workerRedisOutageOf: (redisConnection, outOfMemoryRetryDelayInMilliseconds) =>
      createRedisOutage({
        outageTracker,
        waitUntilReady: (stopSignal) => untilAborted(readyWaitOf(stateOf(redisConnection)), stopSignal),
        outOfMemoryRetryDelayInMilliseconds,
      }),
    closeConnection: async (redisConnection, closeBoundInMilliseconds) => {
      const state = stateOf(redisConnection);
      if (state.closedController.signal.aborted) {
        return;
      }
      state.closedController.abort(new ConnectionClosedError());
      state.readyWait?.markClosed();
      state.readyWait = undefined;
      outageTracker.forgetClient(redisConnection);
      if (closeBoundInMilliseconds !== undefined && state.driver.isReady) {
        await isSettledWithin(
          state.driver.close().catch(() => undefined),
          closeBoundInMilliseconds,
        );
      }
      state.driver.destroy();
    },
    onConnectionRestored: outageTracker.onConnectionRestored,
    callOnRedis: async (callIds, call) => {
      try {
        return await call();
      } catch (error) {
        if (isUnansweredError(error)) {
          throw new RedisUnavailableError({ kind: "unanswered", giveUpAfterInMilliseconds }, callIds, error);
        }
        if (isOutOfMemoryReply(error)) {
          throw new RedisUnavailableError({ kind: "outOfMemory" }, callIds, error);
        }
        throw error;
      }
    },
  };
}
