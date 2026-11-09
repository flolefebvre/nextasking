import { Redis } from "ioredis";
import type { RedisOptions } from "ioredis";

import { parseDuration } from "./durations.js";
import type { Duration, DurationInMilliseconds } from "./durations.js";
import { RedisUnavailableError } from "./errors.js";
import type { RedisCallIds } from "./errors.js";
import type { Logger } from "./logger.js";
import { delayUnlessAborted, untilAborted } from "./redis-outage.js";
import type { RedisOutage } from "./redis-outage.js";
import { isSettledWithin } from "./settled-within.js";

export type ConnectionConfig = {
  url: string;
  giveUpAfter?: Duration;
};

export type RedisClientRole = "instance" | "worker";

const DEFAULT_GIVE_UP_AFTER: Duration = "5s";

const KEEP_ALIVE_INITIAL_DELAY_IN_MILLISECONDS = 30_000;

const RECONNECT_AND_RESEND = 2;

const UNANSWERED_ERROR_MESSAGES = new Set([
  "Command timed out",
  "Connection is closed.",
  "Command aborted due to connection close",
]);

const UNANSWERED_ERROR_MESSAGE_PREFIXES = ["Socket timeout"];

const UNANSWERED_ERROR_NAMES = new Set(["MaxRetriesPerRequestError", "ConnectionNotReadyError", "ClientClosedError"]);

const NETWORK_ERROR_CODES = new Set([
  "ECONNREFUSED",
  "ECONNRESET",
  "ECONNABORTED",
  "ETIMEDOUT",
  "EPIPE",
  "ENOTFOUND",
  "EAI_AGAIN",
  "EHOSTUNREACH",
  "ENETUNREACH",
]);

export function giveUpAfterInMillisecondsOf(connection: ConnectionConfig): DurationInMilliseconds {
  try {
    return parseDuration(connection.giveUpAfter ?? DEFAULT_GIVE_UP_AFTER);
  } catch (error) {
    throw new Error(`Invalid connection.giveUpAfter: ${(error as Error).message}`, { cause: error });
  }
}

export function reconnectDecisionOn(error: Error): false | typeof RECONNECT_AND_RESEND {
  return error.message.includes("READONLY") ? RECONNECT_AND_RESEND : false;
}

export function isUnansweredError(error: unknown): boolean {
  if (!(error instanceof Error)) {
    return false;
  }
  const { code } = error as { code?: unknown };
  return (
    UNANSWERED_ERROR_MESSAGES.has(error.message) ||
    UNANSWERED_ERROR_MESSAGE_PREFIXES.some((prefix) => error.message.startsWith(prefix)) ||
    UNANSWERED_ERROR_NAMES.has(error.name) ||
    (typeof code === "string" && NETWORK_ERROR_CODES.has(code))
  );
}

const OUT_OF_MEMORY_REPLY_PATTERN = /(?:^|[\s:-])OOM command not allowed\b/;

export function isOutOfMemoryReply(error: unknown): boolean {
  return error instanceof Error && error.name === "ReplyError" && OUT_OF_MEMORY_REPLY_PATTERN.test(error.message);
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

export function listenerLoggingAnsweredErrors(logger: Logger, message: string): (error: unknown) => void {
  return (error) => {
    if (!isUnansweredError(error)) {
      logger.error({ err: error }, message);
    }
  };
}

class ConnectionNotReadyError extends Error {
  override readonly name = "ConnectionNotReadyError";
}

class ClientClosedError extends Error {
  override readonly name = "ClientClosedError";

  constructor() {
    super("The Redis client was closed by its nextasking instance");
  }
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

class OwnedRedisClient extends Redis {
  private readonly url: string;
  private readonly clientOptions: RedisOptions;
  private readonly adopt: (client: OwnedRedisClient) => void;

  constructor(url: string, clientOptions: RedisOptions, adopt: (client: OwnedRedisClient) => void) {
    super(url, clientOptions);
    this.url = url;
    this.clientOptions = clientOptions;
    this.adopt = adopt;
    adopt(this);
  }

  override duplicate(override?: Partial<RedisOptions>): Redis {
    return new OwnedRedisClient(this.url, { ...this.clientOptions, ...override }, this.adopt);
  }
}

export type RedisConnections = {
  readonly giveUpAfterInMilliseconds: DurationInMilliseconds;
  createClient: (role: RedisClientRole) => Redis;
  waitUntilReadyWithinGiveUpAfter: (client: Redis) => Promise<void>;
  waitUntilReadyWithoutDeadline: (client: Redis, stopSignal?: AbortSignal) => Promise<void>;
  closeClient: (client: Redis, quitBoundInMilliseconds?: DurationInMilliseconds) => Promise<void>;
  workerRedisOutageOf: (client: Redis, outOfMemoryRetryDelayInMilliseconds: DurationInMilliseconds) => RedisOutage;
  callOnRedis: <Result>(callIds: RedisCallIds, call: () => Promise<Result>) => Promise<Result>;
  onConnectionRestored: (listener: () => void) => () => void;
};

export function createRedisConnections(connection: ConnectionConfig, logger: Logger): RedisConnections {
  const giveUpAfterInMilliseconds = giveUpAfterInMillisecondsOf(connection);
  const outageTracker = createConnectionOutageTracker(logger);
  const lastErrorByClient = new WeakMap<Redis, unknown>();

  const adopt = (client: OwnedRedisClient) => {
    client.on("error", (error: unknown) => {
      lastErrorByClient.set(client, error);
      outageTracker.reportClientError(client, error);
    });
    client.on("ready", () => {
      lastErrorByClient.delete(client);
      outageTracker.reportClientReady(client);
    });
    client.on("end", () => {
      outageTracker.forgetClient(client);
    });
  };

  const optionsByRole: Record<RedisClientRole, RedisOptions> = {
    worker: {},
    instance: { commandTimeout: giveUpAfterInMilliseconds, socketTimeout: giveUpAfterInMilliseconds },
  };

  const pendingReadyWaitByClient = new WeakMap<Redis, Promise<void>>();
  const endPendingReadyWaitByClient = new WeakMap<Redis, () => void>();
  const closedClients = new WeakSet<Redis>();

  const startReadyWait = (client: Redis): Promise<void> => {
    const readyWait = new Promise<void>((resolve, reject) => {
      const endWait = () => {
        clearTimeout(giveUpTimer);
        client.removeListener("ready", markReady);
        pendingReadyWaitByClient.delete(client);
        endPendingReadyWaitByClient.delete(client);
      };
      const markReady = () => {
        endWait();
        resolve();
      };
      const giveUpTimer = setTimeout(() => {
        endWait();
        reject(
          new ConnectionNotReadyError(`Redis isn't ready, its status is "${client.status}"`, {
            cause: lastErrorByClient.get(client),
          }),
        );
      }, giveUpAfterInMilliseconds);
      client.once("ready", markReady);
      endPendingReadyWaitByClient.set(client, () => {
        endWait();
        reject(new ClientClosedError());
      });
    });
    readyWait.catch(() => undefined);
    pendingReadyWaitByClient.set(client, readyWait);
    return readyWait;
  };

  const waitUntilReadyWithinGiveUpAfter = (client: Redis): Promise<void> => {
    if (closedClients.has(client)) {
      return Promise.reject(new ClientClosedError());
    }
    if (client.status === "ready") {
      return Promise.resolve();
    }
    return pendingReadyWaitByClient.get(client) ?? startReadyWait(client);
  };

  const waitUntilReadyWithoutDeadline = async (client: Redis, stopSignal?: AbortSignal): Promise<void> => {
    for (;;) {
      try {
        await untilAborted(waitUntilReadyWithinGiveUpAfter(client), stopSignal);
        return;
      } catch (error) {
        if (!(error instanceof ConnectionNotReadyError)) {
          throw error;
        }
      }
    }
  };

  return {
    giveUpAfterInMilliseconds,
    waitUntilReadyWithoutDeadline,
    workerRedisOutageOf: (client, outOfMemoryRetryDelayInMilliseconds) =>
      createRedisOutage({
        outageTracker,
        waitUntilReady: (stopSignal) => waitUntilReadyWithoutDeadline(client, stopSignal),
        outOfMemoryRetryDelayInMilliseconds,
      }),
    createClient: (role) =>
      new OwnedRedisClient(
        connection.url,
        {
          keepAlive: KEEP_ALIVE_INITIAL_DELAY_IN_MILLISECONDS,
          maxRetriesPerRequest: null,
          reconnectOnError: reconnectDecisionOn,
          ...optionsByRole[role],
        },
        adopt,
      ),
    waitUntilReadyWithinGiveUpAfter,
    closeClient: async (client, quitBoundInMilliseconds) => {
      closedClients.add(client);
      endPendingReadyWaitByClient.get(client)?.();
      if (quitBoundInMilliseconds !== undefined && client.status === "ready") {
        const quitAnswered = client.quit().then(
          () => undefined,
          () => undefined,
        );
        await isSettledWithin(quitAnswered, quitBoundInMilliseconds);
      }
      client.disconnect();
    },
    onConnectionRestored: outageTracker.onConnectionRestored,
    callOnRedis: async (callIds, call) => {
      try {
        return await call();
      } catch (error) {
        if (isUnansweredError(error)) {
          throw new RedisUnavailableError({ kind: "unanswered", giveUpAfterInMilliseconds }, callIds, causeOf(error));
        }
        if (isOutOfMemoryReply(error)) {
          throw new RedisUnavailableError({ kind: "outOfMemory" }, callIds, error);
        }
        throw error;
      }
    },
  };
}

function causeOf(unansweredError: unknown): unknown {
  return unansweredError instanceof ConnectionNotReadyError && unansweredError.cause !== undefined
    ? unansweredError.cause
    : unansweredError;
}

export async function closeRedisClient(client: Redis): Promise<void> {
  if (client.status === "ready") {
    await client.quit();
    return;
  }
  client.disconnect();
}
