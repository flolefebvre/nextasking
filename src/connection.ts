import { Redis } from "ioredis";

export type ConnectionConfig = {
  url: string;
};

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
