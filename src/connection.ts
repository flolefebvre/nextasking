import { parseDuration } from "./durations.js";
import type { Duration, DurationInMilliseconds, EpochMilliseconds } from "./durations.js";
import { RedisUnavailableError } from "./errors.js";
import type { RedisCallIds } from "./errors.js";
import type { Logger } from "./logger.js";
import {
  createNodeRedisDriver,
  isErrorReply,
  isOutOfMemoryReply,
  isReadOnlyReply,
  isUnansweredCommandError,
  isUnblockedReply,
  isUrlTheDriverReads,
  unansweredWithinDeadlineError,
} from "./node-redis-driver.js";
import type { CreateRedisDriver, RedisDriver } from "./node-redis-driver.js";
import { createRefusedWriteStretch } from "./refused-write-stretch.js";
import { builtInCommandOf } from "./redis-client.js";
import type { CommandResult, RedisClient, RedisCommand } from "./redis-client.js";
import { delayUnlessAborted, untilAborted } from "./redis-outage.js";
import type { RedisOutage } from "./redis-outage.js";
import { isSettledWithin } from "./settled-within.js";

export type ConnectionConfig = {
  url: string;
  giveUpAfter?: Duration;
};

const DEFAULT_GIVE_UP_AFTER: Duration = "5s";

const ANSWER_CHECK_COMMAND = builtInCommandOf("PING");

export const READ_ONLY_RECONNECT_PAUSE_IN_MILLISECONDS: DurationInMilliseconds = 250;

export function giveUpAfterInMillisecondsOf(connection: ConnectionConfig): DurationInMilliseconds {
  try {
    return parseDuration(connection.giveUpAfter ?? DEFAULT_GIVE_UP_AFTER);
  } catch (error) {
    throw new Error(`Invalid connection.giveUpAfter: ${(error as Error).message}`, { cause: error });
  }
}

const URL_SCHEME_PATTERN = /^[a-z][a-z0-9+.-]*:\/\//i;

function isUrlMissing(url: string | undefined): boolean {
  return url === undefined || url === "";
}

function shownUrlOf(url: string): string {
  const scheme = URL_SCHEME_PATTERN.exec(url)?.[0] ?? "";
  const afterScheme = url.slice(scheme.length);
  const userinfoEnd = afterScheme.lastIndexOf("@");
  if (userinfoEnd === -1) {
    return url;
  }
  const userinfo = afterScheme.slice(0, userinfoEnd);
  const usernameEnd = userinfo.indexOf(":");
  const shownUserinfo = usernameEnd === -1 ? "***" : `${userinfo.slice(0, usernameEnd)}:***`;
  return `${scheme}${shownUserinfo}${afterScheme.slice(userinfoEnd)}`;
}

function rejectUrlTheDriverCannotRead(connection: ConnectionConfig): void {
  if (!isUrlMissing(connection.url) && !isUrlTheDriverReads(connection.url)) {
    throw new Error(
      `Invalid connection.url: ${JSON.stringify(shownUrlOf(connection.url))} is not a valid redis:// or rediss:// URL, such as "redis://localhost:6379"`,
    );
  }
}

function rejectMissingUrl(connection: ConnectionConfig): void {
  if (isUrlMissing(connection.url)) {
    throw new Error(
      "Missing connection.url: it is undefined or empty, set the variable your app reads it from, such as REDIS_URL",
    );
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
  waitUntilReady: (stopSignal?: AbortSignal) => Promise<void>;
  outOfMemoryRetryDelayInMilliseconds: DurationInMilliseconds;
};

export function createRedisOutage({
  waitUntilReady,
  outOfMemoryRetryDelayInMilliseconds,
}: RedisOutageSources): RedisOutage {
  return {
    isRedisAway: isRedisAwayError,
    waitForRedis: async (awayError, stopSignal) => {
      if (isOutOfMemoryReply(awayError)) {
        await delayUnlessAborted(outOfMemoryRetryDelayInMilliseconds, stopSignal);
        return;
      }
      await waitUntilReady(stopSignal);
    },
  };
}

export type ConnectionOutageTracker = {
  trackClient: (client: object, checkAnswering: () => Promise<void>) => void;
  reportClientError: (client: object, error: unknown) => void;
  reportClientGaveUp: (client: object, error: unknown) => void;
  reportClientReady: (client: object) => void;
  forgetClient: (client: object) => void;
};

type OutageHold = { kind: "down" } | { kind: "suspect" };

export function createConnectionOutageTracker(logger: Logger): ConnectionOutageTracker {
  const holdBySubject = new Map<object, OutageHold>();
  const checkAnsweringByClient = new Map<object, () => Promise<void>>();

  const holdDown = (subject: object, error: unknown) => {
    if (holdBySubject.size === 0) {
      logger.error({ err: error }, "connection error");
    }
    holdBySubject.set(subject, { kind: "down" });
  };

  const release = (subject: object) => {
    if (holdBySubject.delete(subject) && holdBySubject.size === 0) {
      logger.info({}, "connection restored");
    }
  };

  const suspect = (client: object, checkAnswering: () => Promise<void>) => {
    const suspectHold: OutageHold = { kind: "suspect" };
    holdBySubject.set(client, suspectHold);
    checkAnswering().then(
      () => {
        if (holdBySubject.get(client) === suspectHold) {
          release(client);
        }
      },
      () => undefined,
    );
  };

  return {
    trackClient: (client, checkAnswering) => {
      checkAnsweringByClient.set(client, checkAnswering);
    },
    reportClientError: holdDown,
    reportClientGaveUp: (client, error) => {
      holdDown(client, error);
      for (const [otherClient, checkAnswering] of checkAnsweringByClient) {
        if (!holdBySubject.has(otherClient)) {
          suspect(otherClient, checkAnswering);
        }
      }
    },
    reportClientReady: release,
    forgetClient: (client) => {
      checkAnsweringByClient.delete(client);
      if (holdBySubject.get(client)?.kind === "suspect") {
        release(client);
        return;
      }
      holdBySubject.delete(client);
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

type RefusedAnswer = { commandName: string; refusalReply: Error };

function answersRefusedAs(
  isRefusalReply: (error: unknown) => boolean,
  commands: readonly RedisCommand[],
  commandErrors: readonly (Error | undefined)[],
): RefusedAnswer[] {
  return commands.flatMap((command, position) => {
    const commandError = commandErrors[position];
    return commandError !== undefined && isRefusalReply(commandError)
      ? [{ commandName: commandNameOf(command), refusalReply: commandError }]
      : [];
  });
}

function isRefusedWriteReply(error: unknown): boolean {
  return isReadOnlyReply(error) || isOutOfMemoryReply(error);
}

function noteRefusedAnswers(
  refusedAnswers: readonly RefusedAnswer[],
  noteRefusal: (writeCommandNames: readonly string[], firstRefusalReply: Error) => void,
): void {
  const [firstRefusedAnswer] = refusedAnswers;
  if (firstRefusedAnswer !== undefined) {
    noteRefusal(
      refusedAnswers.map(({ commandName }) => commandName),
      firstRefusedAnswer.refusalReply,
    );
  }
}

export function createRedisConnections(
  connection: ConnectionConfig,
  logger: Logger,
  createDriver: CreateRedisDriver = createNodeRedisDriver,
): RedisConnections {
  const giveUpAfterInMilliseconds = giveUpAfterInMillisecondsOf(connection);
  rejectUrlTheDriverCannotRead(connection);
  const outageTracker = createConnectionOutageTracker(logger);
  const refusedWriteStretch = createRefusedWriteStretch({ outageTracker, giveUpAfterInMilliseconds });
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
    outageTracker.reportClientGaveUp(state.subject, cause);
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

  const answeredCheckOf = (state: ConnectionState): Promise<void> => {
    if (isClosed(state) || !state.driver.isReady) {
      return Promise.reject(new Error("A Redis connection that is not ready is not checked"));
    }
    return answeredWithinTheAnswerBound(state, () => state.driver.send(ANSWER_CHECK_COMMAND)).then(
      () => undefined,
      (error: unknown) => {
        if (!isErrorReply(error)) {
          throw error;
        }
      },
    );
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
    commandErrorsOfReply: (reply: Reply) => readonly (Error | undefined)[],
  ): Promise<Reply> => {
    const noteAnswered = (commandErrors: readonly (Error | undefined)[]): { isAnsweredReadOnly: boolean } => {
      const readOnlyAnswers = answersRefusedAs(isReadOnlyReply, commands, commandErrors);
      const outOfMemoryAnswers = answersRefusedAs(isOutOfMemoryReply, commands, commandErrors);
      if (readOnlyAnswers.length === 0 && outOfMemoryAnswers.length === 0) {
        refusedWriteStretch.noteAccepted(commands.map(commandNameOf));
      }
      noteRefusedAnswers(readOnlyAnswers, refusedWriteStretch.noteAnsweredReadOnly);
      noteRefusedAnswers(outOfMemoryAnswers, refusedWriteStretch.noteAnsweredOutOfMemory);
      return { isAnsweredReadOnly: readOnlyAnswers.length > 0 };
    };
    for (;;) {
      throwIfClosed(state);
      let reply: Reply;
      try {
        reply = await answeredWithinTheAnswerBound(state, () => send());
      } catch (error) {
        if (isUnblockedReply(error) && !isClosed(state)) {
          reconnectByForceUnlessUnderWay(state);
          await untilAborted(readyWaitOf(state), state.closedController.signal);
          continue;
        }
        if (!isRefusedWriteReply(error) || isClosed(state)) {
          throw error;
        }
        if (!noteAnswered(commands.map(() => error as Error)).isAnsweredReadOnly) {
          throw error;
        }
        await untilAborted(reconnectedAfterReadOnly(state), state.closedController.signal);
        continue;
      }
      if (isClosed(state)) {
        if (answersRefusedAs(isReadOnlyReply, commands, commandErrorsOfReply(reply)).length > 0) {
          throw new ConnectionClosedError();
        }
        return reply;
      }
      if (!noteAnswered(commandErrorsOfReply(reply)).isAnsweredReadOnly) {
        return reply;
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
    rejectMissingUrl(connection);
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
    outageTracker.trackClient(subject, () => answeredCheckOf(state));
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
        refusedWriteStretch.forget();
      }
      if (closeBoundInMilliseconds !== undefined && state.driver.isReady) {
        await isSettledWithin(
          state.driver.close().catch(() => undefined),
          closeBoundInMilliseconds,
        );
      }
      state.driver.destroy();
    },
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
