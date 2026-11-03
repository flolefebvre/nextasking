import { Redis } from "ioredis";

import { parseDuration } from "./durations.js";
import type { Duration, DurationInMilliseconds } from "./durations.js";

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

export type RedisClientRole = "instance" | "worker";

export function createRedisClient(connection: ConnectionConfig, role: RedisClientRole): Redis {
  const client = new Redis(connection.url, {
    lazyConnect: true,
    ...(role === "worker" ? { maxRetriesPerRequest: null } : {}),
  });
  client.on("error", () => undefined);
  return client;
}

export async function closeRedisClient(client: Redis): Promise<void> {
  if (client.status === "ready") {
    await client.quit();
    return;
  }
  client.disconnect();
}
