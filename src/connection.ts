import { Redis } from "ioredis";
import type { RedisOptions } from "ioredis";

import { parseDuration } from "./durations.js";
import type { Duration, DurationInMilliseconds } from "./durations.js";
import { RedisUnavailableError } from "./errors.js";
import type { RedisCallIds } from "./errors.js";
import type { Logger } from "./logger.js";

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

const UNANSWERED_ERROR_NAMES = new Set(["MaxRetriesPerRequestError", "ConnectionNotReadyError"]);

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

class ConnectionNotReadyError extends Error {
  override readonly name = "ConnectionNotReadyError";
}

export type ConnectionOutageTracker = {
  reportClientError: (client: object, error: unknown) => void;
  reportClientReady: (client: object) => void;
  forgetClient: (client: object) => void;
};

export function createConnectionOutageTracker(logger: Logger): ConnectionOutageTracker {
  const clientsDown = new Set<object>();
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
      }
    },
    forgetClient: (client) => {
      clientsDown.delete(client);
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
  callOnRedis: <Result>(callIds: RedisCallIds, call: () => Promise<Result>) => Promise<Result>;
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

  const waitUntilReadyWithinGiveUpAfter = async (client: Redis) => {
    if (client.status === "ready") {
      return;
    }
    let timeoutTimer: NodeJS.Timeout | undefined;
    let markReady = () => undefined as void;
    try {
      await new Promise<void>((resolve, reject) => {
        markReady = resolve;
        client.once("ready", markReady);
        timeoutTimer = setTimeout(() => {
          reject(
            new ConnectionNotReadyError(`Redis isn't ready, its status is "${client.status}"`, {
              cause: lastErrorByClient.get(client),
            }),
          );
        }, giveUpAfterInMilliseconds);
      });
    } finally {
      clearTimeout(timeoutTimer);
      client.removeListener("ready", markReady);
    }
  };

  return {
    giveUpAfterInMilliseconds,
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
    callOnRedis: async (callIds, call) => {
      try {
        return await call();
      } catch (error) {
        if (isUnansweredError(error)) {
          throw new RedisUnavailableError(giveUpAfterInMilliseconds, callIds, causeOf(error));
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
