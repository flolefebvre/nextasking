import { setTimeout as sleep } from "node:timers/promises";

import { expect, vi } from "vitest";

import { integrationTest } from "../tests/integration-test.js";
import { createRecordingLogger } from "../tests/recording-logger.js";
import { startTcpProxy } from "../tests/tcp-proxy.js";
import type { TcpProxy } from "../tests/tcp-proxy.js";
import { createRedisConnections, isRedisAwayError } from "./connection.js";
import type { RedisConnection, RedisConnections } from "./connection.js";
import { builtInCommandOf } from "./redis-client.js";

const GIVE_UP_AFTER_IN_MILLISECONDS = 1000;

const TIMER_SLACK_IN_MILLISECONDS = 50;

type ProxiedConnections = { proxy: TcpProxy; connections: RedisConnections };

async function withProxiedConnections(
  redisUrl: string,
  use: (proxied: ProxiedConnections) => Promise<void>,
): Promise<void> {
  const proxy = await startTcpProxy(redisUrl);
  const connections = createRedisConnections({ url: proxy.url, giveUpAfter: "1s" }, createRecordingLogger().logger);
  try {
    await use({ proxy, connections });
  } finally {
    await proxy.stop();
  }
}

async function elapsedAndOutcomeOf(call: Promise<unknown>): Promise<{ elapsed: number; outcome: unknown }> {
  const startedAt = performance.now();
  const outcome = await call.catch((error: unknown) => error);
  return { elapsed: performance.now() - startedAt, outcome };
}

async function readyAgain(connections: RedisConnections, connection: RedisConnection): Promise<void> {
  await vi.waitFor(() => connections.waitUntilReady(connection), { timeout: 10_000 });
}

integrationTest(
  "a worker call in flight on a connection that stops answering rejects as Redis away at giveUpAfter, and is answered once it answers again",
  { timeout: 20_000 },
  async ({ redisUrl, keyPrefix }) => {
    await withProxiedConnections(redisUrl, async ({ proxy, connections }) => {
      const connection = connections.createCommandConnection();
      try {
        await connections.waitUntilReady(connection);
        proxy.stopAnswering();

        const { elapsed, outcome } = await elapsedAndOutcomeOf(
          connection.workerCalls.send(builtInCommandOf("GET", `${keyPrefix}in-flight`)),
        );
        proxy.answerAgain();
        await readyAgain(connections, connection);
        const reply = await connection.workerCalls.send(builtInCommandOf("ECHO", "answered again"));

        expect(isRedisAwayError(outcome)).toBe(true);
        expect(elapsed).toBeGreaterThanOrEqual(GIVE_UP_AFTER_IN_MILLISECONDS - TIMER_SLACK_IN_MILLISECONDS);
        expect(elapsed).toBeLessThan(2 * GIVE_UP_AFTER_IN_MILLISECONDS);
        expect(reply).toBe("answered again");
      } finally {
        await connections.closeConnection(connection);
      }
    });
  },
);

integrationTest(
  "a blocking pop in flight on a connection that stops answering rejects as Redis away at its block plus giveUpAfter, and the next pop answers once it answers again",
  { timeout: 20_000 },
  async ({ redisUrl, keyPrefix }) => {
    await withProxiedConnections(redisUrl, async ({ proxy, connections }) => {
      const blockInMilliseconds = 1000;
      const connection = connections.createBlockingConnection(blockInMilliseconds);
      const markerKey = `${keyPrefix}marker`;
      try {
        await connections.waitUntilReady(connection);
        proxy.stopAnswering();

        const { elapsed, outcome } = await elapsedAndOutcomeOf(
          connection.workerCalls.send(builtInCommandOf("BZPOPMIN", markerKey, blockInMilliseconds / 1000)),
        );
        proxy.answerAgain();
        await readyAgain(connections, connection);
        const nextPop = await connection.workerCalls.send(builtInCommandOf("BZPOPMIN", markerKey, 0.1));

        expect(isRedisAwayError(outcome)).toBe(true);
        expect(elapsed).toBeGreaterThanOrEqual(
          blockInMilliseconds + GIVE_UP_AFTER_IN_MILLISECONDS - TIMER_SLACK_IN_MILLISECONDS,
        );
        expect(elapsed).toBeLessThan(blockInMilliseconds + 2 * GIVE_UP_AFTER_IN_MILLISECONDS);
        expect(nextPop).toBeNull();
      } finally {
        await connections.closeConnection(connection);
      }
    });
  },
);

integrationTest(
  "a connection whose handshakes go unanswered opens one new connection per giveUpAfter, and is ready once the server answers again",
  { timeout: 20_000 },
  async ({ redisUrl }) => {
    await withProxiedConnections(redisUrl, async ({ proxy, connections }) => {
      proxy.stopAnswering();
      const connection = connections.createCommandConnection();
      try {
        await sleep(3.5 * GIVE_UP_AFTER_IN_MILLISECONDS);
        const acceptedWhileUnanswered = proxy.acceptedConnectionCount();
        proxy.answerAgain();
        await readyAgain(connections, connection);
        const reply = await connection.instanceCalls.send(builtInCommandOf("ECHO", "ready"));

        expect(acceptedWhileUnanswered).toBeGreaterThanOrEqual(3);
        expect(acceptedWhileUnanswered).toBeLessThanOrEqual(5);
        expect(reply).toBe("ready");
      } finally {
        await connections.closeConnection(connection);
      }
    });
  },
);
