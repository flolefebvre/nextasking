import { createClient } from "@redis/client";

import { createNodeRedisDriver } from "../src/node-redis-driver.js";
import type { RedisClient } from "../src/redis-client.js";

function testClientOf(redisUrl: string) {
  return createClient({ url: redisUrl, RESP: 2 }).on("error", () => undefined);
}

export type TestClient = ReturnType<typeof testClientOf>;

export function testClientNeverReconnectingOf(redisUrl: string): TestClient {
  return createClient({ url: redisUrl, RESP: 2, socket: { reconnectStrategy: false } }).on("error", () => undefined);
}

export function connectedTestClientOf(redisUrl: string): Promise<TestClient> {
  return testClientOf(redisUrl).connect();
}

export async function withRedisClient<Result>(
  redisUrl: string,
  use: (client: RedisClient) => Promise<Result>,
): Promise<Result> {
  let markReady = () => undefined as void;
  const ready = new Promise<void>((resolve) => {
    markReady = resolve;
  });
  const driver = createNodeRedisDriver(redisUrl, {
    onConnect: () => undefined,
    onReady: markReady,
    onError: () => undefined,
  });
  driver.connect();
  try {
    await ready;
    return await use(driver);
  } finally {
    await driver.close().catch(() => undefined);
    driver.destroy();
  }
}

export async function withRedis<Result>(
  redisUrl: string,
  use: (redis: TestClient, client: RedisClient) => Promise<Result>,
): Promise<Result> {
  const redis = testClientOf(redisUrl);
  try {
    await redis.connect();
    return await withRedisClient(redisUrl, (client) => use(redis, client));
  } finally {
    await redis.close().catch(() => undefined);
  }
}
