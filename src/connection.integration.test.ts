import { setTimeout as sleep } from "node:timers/promises";

import { expect, vi } from "vitest";

import { integrationTest } from "../tests/integration-test.js";
import { measuredRejectionOf } from "../tests/measured-duration.js";
import { createRecordingLogger } from "../tests/recording-logger.js";
import { startTcpProxy } from "../tests/tcp-proxy.js";
import type { TcpProxy } from "../tests/tcp-proxy.js";
import { createRedisConnections, isRedisAwayError } from "./connection.js";
import type { RedisConnection, RedisConnections } from "./connection.js";
import { createNodeRedisDriver } from "./node-redis-driver.js";
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

        const {
          result: rejection,
          elapsedInMilliseconds,
          elapsedNetOfStallInMilliseconds,
        } = await measuredRejectionOf(() =>
          connection.workerCalls.send(builtInCommandOf("GET", `${keyPrefix}in-flight`)),
        );
        proxy.answerAgain();
        await readyAgain(connections, connection);
        const reply = await connection.workerCalls.send(builtInCommandOf("ECHO", "answered again"));

        expect(isRedisAwayError(rejection)).toBe(true);
        expect(elapsedInMilliseconds).toBeGreaterThanOrEqual(
          GIVE_UP_AFTER_IN_MILLISECONDS - TIMER_SLACK_IN_MILLISECONDS,
        );
        expect(elapsedNetOfStallInMilliseconds).toBeLessThan(2 * GIVE_UP_AFTER_IN_MILLISECONDS);
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

        const {
          result: rejection,
          elapsedInMilliseconds,
          elapsedNetOfStallInMilliseconds,
        } = await measuredRejectionOf(() =>
          connection.workerCalls.send(builtInCommandOf("BZPOPMIN", markerKey, blockInMilliseconds / 1000)),
        );
        proxy.answerAgain();
        await readyAgain(connections, connection);
        const nextPop = await connection.workerCalls.send(builtInCommandOf("BZPOPMIN", markerKey, 0.1));

        expect(isRedisAwayError(rejection)).toBe(true);
        expect(elapsedInMilliseconds).toBeGreaterThanOrEqual(
          blockInMilliseconds + GIVE_UP_AFTER_IN_MILLISECONDS - TIMER_SLACK_IN_MILLISECONDS,
        );
        expect(elapsedNetOfStallInMilliseconds).toBeLessThan(blockInMilliseconds + 2 * GIVE_UP_AFTER_IN_MILLISECONDS);
        expect(nextPop).toBeNull();
      } finally {
        await connections.closeConnection(connection);
      }
    });
  },
);

integrationTest(
  "a connection reset before its first ready, then refused past giveUpAfter, opens exactly one connection once the server is back",
  { timeout: 20_000 },
  async ({ redisUrl }) => {
    await withProxiedConnections(redisUrl, async ({ proxy, connections }) => {
      proxy.stopAnswering();
      const connection = connections.createCommandConnection();
      try {
        await vi.waitFor(() => expect(proxy.acceptedConnectionCount()).toBe(1), { timeout: 5_000 });
        await proxy.cut();
        proxy.answerAgain();
        await sleep(GIVE_UP_AFTER_IN_MILLISECONDS + 2_500);
        const acceptedBeforeTheRestore = proxy.acceptedConnectionCount();
        await proxy.restore();
        await readyAgain(connections, connection);
        await sleep(3_000);
        const acceptedAfterTheRestore = proxy.acceptedConnectionCount() - acceptedBeforeTheRestore;
        const reply = await connection.instanceCalls.send(builtInCommandOf("ECHO", "back"));

        expect(acceptedAfterTheRestore).toBe(1);
        expect(reply).toBe("back");
      } finally {
        await connections.closeConnection(connection);
      }
    });
  },
);

integrationTest(
  "a driver reconnected by force while it waits out its reconnect backoff opens exactly one connection once the server is back",
  { timeout: 20_000 },
  async ({ redisUrl }) => {
    const proxy = await startTcpProxy(redisUrl);
    let errorCountWhileRefused = 0;
    let markReady = () => undefined as void;
    const ready = new Promise<void>((resolve) => {
      markReady = resolve;
    });
    const driver = createNodeRedisDriver(proxy.url, {
      onConnect: () => undefined,
      onReady: markReady,
      onError: () => {
        errorCountWhileRefused += 1;
      },
    });
    try {
      await proxy.cut();
      driver.connect();
      await vi.waitFor(() => expect(errorCountWhileRefused).toBeGreaterThanOrEqual(3), { timeout: 5_000 });
      for (let forcedReconnectCount = 0; forcedReconnectCount < 5; forcedReconnectCount += 1) {
        driver.forceReconnect();
        await sleep(150);
      }
      await sleep(2_500);
      const errorCountBefore = errorCountWhileRefused;
      await sleep(2_000);
      const errorsInTwoSeconds = errorCountWhileRefused - errorCountBefore;
      await proxy.restore();
      await ready;
      await sleep(3_000);
      const reply = await driver.send(builtInCommandOf("ECHO", "back"));

      expect(errorsInTwoSeconds).toBeLessThanOrEqual(2);
      expect(proxy.acceptedConnectionCount()).toBe(1);
      expect(reply).toBe("back");
    } finally {
      driver.destroy();
      await proxy.stop();
    }
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
