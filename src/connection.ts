import { parseDuration } from "./durations.js";
import type { Duration, DurationInMilliseconds, EpochMilliseconds } from "./durations.js";
import { RedisUnavailableError } from "./errors.js";
import type { RedisCallIds } from "./errors.js";
import type { Logger } from "./logger.js";
import {
  createNodeRedisDriver,
  isOutOfMemoryReply,
  isReadOnlyReply,
  isUnansweredCommandError,
  isUnblockedReply,
  unansweredWithinDeadlineError,
} from "./node-redis-driver.js";
import type { CreateRedisDriver, RedisDriver } from "./node-redis-driver.js";
import { createReadOnlyStretch } from "./read-only-stretch.js";
import type { CommandResult, RedisClient, RedisCommand } from "./redis-client.js";
import { delayUnlessAborted, untilAborted } from "./redis-outage.js";
import type { RedisOutage } from "./redis-outage.js";
import { isSettledWithin } from "./settled-within.js";

export type ConnectionConfig = {
  url: string;
  giveUpAfter?: Duration;
};

const DEFAULT_GIVE_UP_AFTER: Duration = "5s";

export const READ_ONLY_RECONNECT_PAUSE_IN_MILLISECONDS: DurationInMilliseconds = 250;

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
  createBlockingConnection: (longestBlockInMilliseconds: DurationInMilliseconds) => RedisConnection;
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
  readonly subject: object;
  readonly answerBoundInMilliseconds: DurationInMilliseconds;
  readonly closedController: AbortController;
  errorWhileDown: unknown;
  readyWait: ReadyWait | undefined;
  lastReadyAt: EpochMilliseconds;
  isReconnectingByForce: boolean;
  readOnlyReconnect: Promise<void> | undefined;
  handshakeTimer: NodeJS.Timeout | undefined;
  readonly callsInFlight: { sentAt: EpochMilliseconds }[];
  watchdogTimer: NodeJS.Timeout | undefined;
};

type Send<Reply> = (abortSignal?: AbortSignal) => Promise<Reply>;

function unrefTimer(timer: NodeJS.Timeout): NodeJS.Timeout {
  timer.unref();
  return timer;
}

function isRetriedError(error: unknown): boolean {
  return isUnansweredError(error) || isReadOnlyReply(error);
}

function failedResultErrorOf(
  isErrorToRetry: (error: unknown) => boolean,
): (results: readonly CommandResult[]) => Error | undefined {
  return (results) =>
    results.find(
      (result): result is { kind: "failed"; error: Error } => result.kind === "failed" && isErrorToRetry(result.error),
    )?.error;
}

function unrefDelayUnlessAborted(delayInMilliseconds: DurationInMilliseconds, abortSignal: AbortSignal): Promise<void> {
  return new Promise<void>((resolve) => {
    const endDelay = () => {
      clearTimeout(delayTimer);
      abortSignal.removeEventListener("abort", endDelay);
      resolve();
    };
    const delayTimer = unrefTimer(setTimeout(endDelay, delayInMilliseconds));
    abortSignal.addEventListener("abort", endDelay, { once: true });
    if (abortSignal.aborted) {
      endDelay();
    }
  });
}

function noRetryError(): undefined {
  return undefined;
}

function noErrors(): readonly Error[] {
  return [];
}

function commandErrorsOf(results: readonly CommandResult[]): readonly (Error | undefined)[] {
  return results.map((result) => (result.kind === "failed" ? result.error : undefined));
}

function commandNameOf(command: RedisCommand): string {
  return command.kind === "script" ? command.script.name : command.name;
}

type ReadOnlyAnswer = { commandName: string; readOnlyReply: Error };

function readOnlyAnswersOf(
  commands: readonly RedisCommand[],
  commandErrors: readonly (Error | undefined)[],
): ReadOnlyAnswer[] {
  return commands.flatMap((command, position) => {
    const commandError = commandErrors[position];
    return commandError !== undefined && isReadOnlyReply(commandError)
      ? [{ commandName: commandNameOf(command), readOnlyReply: commandError }]
      : [];
  });
}

export function createRedisConnections(
  connection: ConnectionConfig,
  logger: Logger,
  createDriver: CreateRedisDriver = createNodeRedisDriver,
): RedisConnections {
  const giveUpAfterInMilliseconds = giveUpAfterInMillisecondsOf(connection);
  const outageTracker = createConnectionOutageTracker(logger);
  const readOnlyStretch = createReadOnlyStretch({ outageTracker, giveUpAfterInMilliseconds });
  const stateByConnection = new WeakMap<RedisConnection, ConnectionState>();
  let openConnectionCount = 0;

  const stateOf = (redisConnection: RedisConnection): ConnectionState => {
    const state = stateByConnection.get(redisConnection);
    if (state === undefined) {
      throw new Error("This Redis connection was not opened by these connections");
    }
    return state;
  };

  const isClosed = (state: ConnectionState) => state.closedController.signal.aborted;

  const readyWaitOf = (state: ConnectionState): Promise<void> => {
    if (isClosed(state)) {
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
    if (isClosed(state)) {
      throw new ConnectionClosedError();
    }
  };

  const reconnectByForce = (state: ConnectionState, cause: Error) => {
    if (isClosed(state) || state.isReconnectingByForce) {
      return;
    }
    state.isReconnectingByForce = true;
    clearTimeout(state.handshakeTimer);
    state.errorWhileDown = cause;
    outageTracker.reportClientError(state.subject, cause);
    state.driver.forceReconnect();
  };

  const overdueAtOf = (state: ConnectionState): EpochMilliseconds =>
    Math.max(Math.min(...state.callsInFlight.map(({ sentAt }) => sentAt)), state.lastReadyAt) +
    state.answerBoundInMilliseconds;

  const watchTheCallsInFlight = (state: ConnectionState) => {
    clearTimeout(state.watchdogTimer);
    state.watchdogTimer = undefined;
    if (isClosed(state) || !state.driver.isReady || state.callsInFlight.length === 0) {
      return;
    }
    const overdueAt = overdueAtOf(state);
    state.watchdogTimer = unrefTimer(
      setTimeout(
        () => {
          state.watchdogTimer = undefined;
          if (state.driver.isReady && state.callsInFlight.length > 0 && Date.now() >= overdueAtOf(state)) {
            reconnectByForce(state, unansweredWithinDeadlineError());
          }
          watchTheCallsInFlight(state);
        },
        Math.max(overdueAt - Date.now(), 0),
      ),
    );
  };

  const answeredWithinTheAnswerBound = <Reply>(state: ConnectionState, send: () => Promise<Reply>): Promise<Reply> => {
    const callInFlight = { sentAt: Date.now() };
    state.callsInFlight.push(callInFlight);
    if (state.watchdogTimer === undefined) {
      watchTheCallsInFlight(state);
    }
    return send().finally(() => {
      state.callsInFlight.splice(state.callsInFlight.indexOf(callInFlight), 1);
      if (state.callsInFlight.length === 0) {
        clearTimeout(state.watchdogTimer);
        state.watchdogTimer = undefined;
      }
    });
  };

  const unansweredErrorAtTheDeadline = (state: ConnectionState, lastReadOnlyReply: Error | undefined): Error =>
    !state.driver.isReady && isUnansweredCommandError(state.errorWhileDown)
      ? (state.errorWhileDown as Error)
      : (lastReadOnlyReply ?? unansweredWithinDeadlineError());

  const reconnectByForceUnlessUnderWay = (state: ConnectionState) => {
    if (state.driver.isReady && !state.isReconnectingByForce) {
      state.isReconnectingByForce = true;
      state.driver.forceReconnect();
    }
  };

  const reconnectedAfterReadOnly = (state: ConnectionState): Promise<void> => {
    if (state.readOnlyReconnect === undefined) {
      const readOnlyReconnect = (async () => {
        await unrefDelayUnlessAborted(READ_ONLY_RECONNECT_PAUSE_IN_MILLISECONDS, state.closedController.signal);
        throwIfClosed(state);
        reconnectByForceUnlessUnderWay(state);
        await readyWaitOf(state);
      })().finally(() => {
        state.readOnlyReconnect = undefined;
      });
      readOnlyReconnect.catch(() => undefined);
      state.readOnlyReconnect = readOnlyReconnect;
    }
    return state.readOnlyReconnect;
  };

  const sentWithinTheDeadline = async <Reply>(
    state: ConnectionState,
    send: Send<Reply>,
    retryErrorOf: (reply: Reply) => Error | undefined,
  ): Promise<Reply> => {
    const deadlineController = new AbortController();
    let lastReadOnlyReply: Error | undefined;
    const deadlineTimer = unrefTimer(
      setTimeout(() => {
        deadlineController.abort(unansweredErrorAtTheDeadline(state, lastReadOnlyReply));
      }, giveUpAfterInMilliseconds),
    );
    const deadlineSignal = deadlineController.signal;
    try {
      for (;;) {
        throwIfClosed(state);
        let retryError: Error | undefined;
        try {
          const reply = await untilAborted(
            answeredWithinTheAnswerBound(state, () => send(deadlineSignal)),
            deadlineSignal,
          );
          retryError = retryErrorOf(reply);
          if (retryError === undefined) {
            return reply;
          }
        } catch (error) {
          if (deadlineSignal.aborted || !isRetriedError(error)) {
            throw deadlineSignal.aborted ? (deadlineSignal.reason as Error) : error;
          }
          retryError = error as Error;
        }
        if (isClosed(state)) {
          throw retryError;
        }
        if (isReadOnlyReply(retryError)) {
          lastReadOnlyReply = retryError;
        }
        await untilAborted(
          isReadOnlyReply(retryError) ? reconnectedAfterReadOnly(state) : readyWaitOf(state),
          deadlineSignal,
        );
      }
    } finally {
      clearTimeout(deadlineTimer);
    }
  };

  const sentWithoutDeadline = async <Reply>(
    state: ConnectionState,
    commands: readonly RedisCommand[],
    send: Send<Reply>,
    commandErrorsOf: (reply: Reply) => readonly (Error | undefined)[],
  ): Promise<Reply> => {
    for (;;) {
      throwIfClosed(state);
      let readOnlyAnswers: ReadOnlyAnswer[];
      try {
        const reply = await answeredWithinTheAnswerBound(state, () => send());
        readOnlyAnswers = readOnlyAnswersOf(commands, commandErrorsOf(reply));
        if (readOnlyAnswers.length === 0) {
          readOnlyStretch.noteAccepted(commands.map(commandNameOf));
          return reply;
        }
      } catch (error) {
        if (isUnblockedReply(error) && !isClosed(state)) {
          reconnectByForceUnlessUnderWay(state);
          await untilAborted(readyWaitOf(state), state.closedController.signal);
          continue;
        }
        if (!isReadOnlyReply(error) || isClosed(state)) {
          throw error;
        }
        readOnlyAnswers = readOnlyAnswersOf(
          commands,
          commands.map(() => error as Error),
        );
      }
      const [firstReadOnlyAnswer] = readOnlyAnswers;
      if (firstReadOnlyAnswer !== undefined) {
        readOnlyStretch.noteAnsweredReadOnly(
          readOnlyAnswers.map(({ commandName }) => commandName),
          firstReadOnlyAnswer.readOnlyReply,
        );
      }
      await untilAborted(reconnectedAfterReadOnly(state), state.closedController.signal);
    }
  };

  const instanceCallsOf = (state: ConnectionState): RedisClient => ({
    send: (command: RedisCommand) =>
      sentWithinTheDeadline(state, (abortSignal) => state.driver.send(command, abortSignal), noRetryError),
    sendBatch: (commands: readonly RedisCommand[]) =>
      sentWithinTheDeadline(
        state,
        (abortSignal) => state.driver.sendBatch(commands, abortSignal),
        failedResultErrorOf(isRetriedError),
      ),
  });

  const workerCallsOf = (state: ConnectionState): RedisClient => ({
    send: (command: RedisCommand) => sentWithoutDeadline(state, [command], () => state.driver.send(command), noErrors),
    sendBatch: (commands: readonly RedisCommand[]) =>
      sentWithoutDeadline(state, commands, () => state.driver.sendBatch(commands), commandErrorsOf),
  });

  const openConnection = (longestBlockInMilliseconds: DurationInMilliseconds): RedisConnection => {
    const opened: { state?: ConnectionState } = {};
    const openState = () => (opened.state === undefined || isClosed(opened.state) ? undefined : opened.state);
    const driver = createDriver(connection.url, {
      onConnect: () => {
        const state = openState();
        if (state === undefined) {
          return;
        }
        state.isReconnectingByForce = false;
        clearTimeout(state.handshakeTimer);
        state.handshakeTimer = unrefTimer(
          setTimeout(() => {
            if (!state.driver.isReady) {
              reconnectByForce(state, unansweredWithinDeadlineError());
            }
          }, giveUpAfterInMilliseconds),
        );
      },
      onReady: () => {
        const state = openState();
        if (state === undefined) {
          return;
        }
        clearTimeout(state.handshakeTimer);
        state.isReconnectingByForce = false;
        state.lastReadyAt = Date.now();
        state.errorWhileDown = undefined;
        state.readyWait?.markReady();
        state.readyWait = undefined;
        watchTheCallsInFlight(state);
        outageTracker.reportClientReady(state.subject);
      },
      onError: (error) => {
        const state = openState();
        if (state === undefined) {
          return;
        }
        clearTimeout(state.handshakeTimer);
        state.errorWhileDown = error;
        outageTracker.reportClientError(state.subject, error);
      },
    });
    const subject = {};
    const state: ConnectionState = {
      driver,
      subject,
      answerBoundInMilliseconds: longestBlockInMilliseconds + giveUpAfterInMilliseconds,
      closedController: new AbortController(),
      errorWhileDown: undefined,
      readyWait: undefined,
      lastReadyAt: Date.now(),
      isReconnectingByForce: false,
      readOnlyReconnect: undefined,
      handshakeTimer: undefined,
      callsInFlight: [],
      watchdogTimer: undefined,
    };
    const redisConnection: RedisConnection = {
      instanceCalls: instanceCallsOf(state),
      workerCalls: workerCallsOf(state),
    };
    opened.state = state;
    stateByConnection.set(redisConnection, state);
    openConnectionCount += 1;
    driver.connect();
    return redisConnection;
  };

  return {
    giveUpAfterInMilliseconds,
    createCommandConnection: () => openConnection(0),
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
      if (isClosed(state)) {
        return;
      }
      state.closedController.abort(new ConnectionClosedError());
      clearTimeout(state.handshakeTimer);
      clearTimeout(state.watchdogTimer);
      state.readyWait?.markClosed();
      state.readyWait = undefined;
      outageTracker.forgetClient(state.subject);
      openConnectionCount -= 1;
      if (openConnectionCount === 0) {
        readOnlyStretch.forget();
      }
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
        if (isUnansweredError(error) || isReadOnlyReply(error)) {
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
