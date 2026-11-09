import { once } from "node:events";

import { ReplyError } from "ioredis";
import type { Redis } from "ioredis";
import { afterEach, describe, expect, test, vi } from "vitest";

import {
  createConnectionOutageTracker,
  createRedisConnections,
  createRedisOutage,
  isRedisAwayError,
  isUnansweredError,
  reconnectDecisionOn,
} from "./connection.js";
import type { RedisConnections } from "./connection.js";
import { createRecordingLogger } from "../tests/recording-logger.js";

const CONCURRENT_CALLER_COUNT = 20;

function networkError(code: string): Error {
  return Object.assign(new Error(`connect ${code} 127.0.0.1:6379`), { code });
}

async function withNeverConnectingClient(
  body: (connections: RedisConnections, client: Redis) => Promise<void>,
): Promise<void> {
  const connections = createRedisConnections(
    { url: "redis://127.0.0.1:1", giveUpAfter: "1s" },
    createRecordingLogger().logger,
  );
  const client = connections.createClient("instance");
  const ended = once(client, "end");
  client.disconnect();
  await ended;
  try {
    await body(connections, client);
  } finally {
    client.disconnect();
  }
}

function waitsOfConcurrentCallers(connections: RedisConnections, client: Redis): Promise<PromiseSettledResult<void>[]> {
  return Promise.allSettled(
    Array.from({ length: CONCURRENT_CALLER_COUNT }, () => connections.waitUntilReadyWithinGiveUpAfter(client)),
  );
}

function fulfilledResultsOfAllCallers(): PromiseSettledResult<void>[] {
  return Array.from({ length: CONCURRENT_CALLER_COUNT }, () => ({ status: "fulfilled", value: undefined }));
}

function rejectedResultsOfAllCallersWithCause(cause: unknown): PromiseSettledResult<void>[] {
  return Array.from({ length: CONCURRENT_CALLER_COUNT }, () => ({
    status: "rejected",
    reason: expect.objectContaining({ name: "ConnectionNotReadyError", cause }),
  }));
}

describe("waitUntilReadyWithinGiveUpAfter", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  test("shares one ready listener between concurrent callers, resolves them all on one ready event, and removes it", async () => {
    await withNeverConnectingClient(async (connections, client) => {
      const readyListenerCountBefore = client.listenerCount("ready");

      const waits = waitsOfConcurrentCallers(connections, client);
      const readyListenerCountWhileWaiting = client.listenerCount("ready");
      client.emit("ready");

      expect(readyListenerCountWhileWaiting).toBe(readyListenerCountBefore + 1);
      expect(await waits).toEqual(fulfilledResultsOfAllCallers());
      expect(client.listenerCount("ready")).toBe(readyListenerCountBefore);
    });
  });

  test("rejects every concurrent caller with the client's last error as cause after giveUpAfter, and removes the listener", async () => {
    await withNeverConnectingClient(async (connections, client) => {
      vi.useFakeTimers();
      const connectionRefused = networkError("ECONNREFUSED");
      client.emit("error", connectionRefused);
      const readyListenerCountBefore = client.listenerCount("ready");

      const waits = waitsOfConcurrentCallers(connections, client);
      const readyListenerCountWhileWaiting = client.listenerCount("ready");
      await vi.advanceTimersByTimeAsync(connections.giveUpAfterInMilliseconds);

      expect(readyListenerCountWhileWaiting).toBe(readyListenerCountBefore + 1);
      expect(await waits).toEqual(rejectedResultsOfAllCallersWithCause(connectionRefused));
      expect(client.listenerCount("ready")).toBe(readyListenerCountBefore);
      expect(vi.getTimerCount()).toBe(0);
    });
  });

  test("starts a fresh wait with a full giveUpAfter after a wait that gave up", async () => {
    await withNeverConnectingClient(async (connections, client) => {
      vi.useFakeTimers();
      const firstWaits = waitsOfConcurrentCallers(connections, client);
      await vi.advanceTimersByTimeAsync(connections.giveUpAfterInMilliseconds);
      await firstWaits;

      const secondWaits = waitsOfConcurrentCallers(connections, client);
      await vi.advanceTimersByTimeAsync(connections.giveUpAfterInMilliseconds - 1);
      client.emit("ready");

      expect(await secondWaits).toEqual(fulfilledResultsOfAllCallers());
    });
  });

  test("starts a fresh wait after a wait that resolved", async () => {
    await withNeverConnectingClient(async (connections, client) => {
      vi.useFakeTimers();
      const firstWaits = waitsOfConcurrentCallers(connections, client);
      client.emit("ready");
      await firstWaits;

      const secondWaits = waitsOfConcurrentCallers(connections, client);
      await vi.advanceTimersByTimeAsync(connections.giveUpAfterInMilliseconds);

      expect((await secondWaits).map((result) => result.status)).toEqual(
        Array.from({ length: CONCURRENT_CALLER_COUNT }, () => "rejected"),
      );
    });
  });

  test("returns at once without a listener or a timer when the client is ready, with no wait pending or with one", async () => {
    await withNeverConnectingClient(async (connections, client) => {
      vi.useFakeTimers();
      const readyListenerCountBefore = client.listenerCount("ready");
      const pendingWait = connections.waitUntilReadyWithinGiveUpAfter(client);
      client.status = "ready";

      const resultsWithOnePending = await waitsOfConcurrentCallers(connections, client);
      const readyListenerCountWithOnePending = client.listenerCount("ready");
      const timerCountWithOnePending = vi.getTimerCount();
      client.emit("ready");
      await pendingWait;
      const waitsWithNonePending = waitsOfConcurrentCallers(connections, client);

      expect(readyListenerCountWithOnePending).toBe(readyListenerCountBefore + 1);
      expect(timerCountWithOnePending).toBe(1);
      expect(client.listenerCount("ready")).toBe(readyListenerCountBefore);
      expect(vi.getTimerCount()).toBe(0);
      expect(resultsWithOnePending).toEqual(fulfilledResultsOfAllCallers());
      expect(await waitsWithNonePending).toEqual(fulfilledResultsOfAllCallers());
    });
  });
});

describe("the connection outage tracker", () => {
  test("logs one error and one restored line for an outage that every client goes through, flapping included", () => {
    const { logger, lines } = createRecordingLogger();
    const outageTracker = createConnectionOutageTracker(logger);
    const instanceClient = {};
    const workerClient = {};
    const connectionReset = networkError("ECONNRESET");

    outageTracker.reportClientError(instanceClient, connectionReset);
    outageTracker.reportClientError(workerClient, networkError("ECONNREFUSED"));
    outageTracker.reportClientReady(instanceClient);
    outageTracker.reportClientError(instanceClient, networkError("ECONNREFUSED"));
    outageTracker.reportClientReady(workerClient);
    outageTracker.reportClientReady(instanceClient);

    expect(lines).toEqual([
      { level: "error", fields: { err: connectionReset }, message: "connection error" },
      { level: "info", fields: {}, message: "connection restored" },
    ]);
  });

  test("logs a second pair for a second outage", () => {
    const { logger, lines } = createRecordingLogger();
    const outageTracker = createConnectionOutageTracker(logger);
    const client = {};

    outageTracker.reportClientError(client, networkError("ECONNREFUSED"));
    outageTracker.reportClientReady(client);
    outageTracker.reportClientError(client, networkError("ECONNREFUSED"));
    outageTracker.reportClientReady(client);

    expect(lines.map((line) => line.message)).toEqual([
      "connection error",
      "connection restored",
      "connection error",
      "connection restored",
    ]);
  });

  test("logs nothing for a client that becomes ready without an error", () => {
    const { logger, lines } = createRecordingLogger();
    const outageTracker = createConnectionOutageTracker(logger);

    outageTracker.reportClientReady({});

    expect(lines).toEqual([]);
  });

  test("calls each connection-restored listener once per outage, when it logs connection restored, until it unsubscribes", () => {
    const { logger, lines } = createRecordingLogger();
    const outageTracker = createConnectionOutageTracker(logger);
    const instanceClient = {};
    const workerClient = {};
    const messagesLoggedWhenRestored: string[][] = [];
    const stopListening = outageTracker.onConnectionRestored(() => {
      messagesLoggedWhenRestored.push(lines.map((line) => line.message));
    });

    outageTracker.reportClientReady(instanceClient);
    outageTracker.reportClientError(instanceClient, networkError("ECONNRESET"));
    outageTracker.reportClientError(workerClient, networkError("ECONNRESET"));
    outageTracker.reportClientReady(instanceClient);
    outageTracker.reportClientReady(workerClient);
    stopListening();
    outageTracker.reportClientError(workerClient, networkError("ECONNRESET"));
    outageTracker.reportClientReady(workerClient);

    expect(messagesLoggedWhenRestored).toEqual([["connection error", "connection restored"]]);
  });

  test("ends an outage silently when the last client still down is closed, and logs the next one", () => {
    const { logger, lines } = createRecordingLogger();
    const outageTracker = createConnectionOutageTracker(logger);
    const closedClient = {};
    const otherClient = {};

    outageTracker.reportClientError(closedClient, networkError("ECONNREFUSED"));
    outageTracker.forgetClient(closedClient);
    outageTracker.reportClientError(otherClient, networkError("ECONNREFUSED"));

    expect(lines.map((line) => line.message)).toEqual(["connection error", "connection error"]);
  });
});

describe("reconnectDecisionOn", () => {
  test("reconnects and resends the command on a READONLY reply, as during a failover", () => {
    expect(reconnectDecisionOn(new ReplyError("READONLY You can't write against a read only replica."))).toBe(2);
  });

  test("leaves the connection alone on any other error", () => {
    expect(reconnectDecisionOn(new ReplyError("ERR unknown command 'CONFIG'"))).toBe(false);
  });
});

describe("isUnansweredError", () => {
  test.each([
    ["a command timeout", new Error("Command timed out")],
    ["a closed connection", new Error("Connection is closed.")],
    ["a pipeline command aborted by a closed connection", new Error("Command aborted due to connection close")],
    ["a socket timeout", new Error("Socket timeout. Expecting data, but didn't receive any in 5000ms.")],
    ["a refused connection", networkError("ECONNREFUSED")],
    ["a reset connection", networkError("ECONNRESET")],
    ["an unknown host", networkError("ENOTFOUND")],
    [
      "exhausted retries",
      Object.assign(new Error("Reached the max retries per request limit"), {
        name: "MaxRetriesPerRequestError",
      }),
    ],
  ])("is true for %s", (_case, error) => {
    expect(isUnansweredError(error)).toBe(true);
  });

  test.each([
    ["a reply error", new ReplyError("ERR wrong number of arguments")],
    ["an error of the call itself", new Error("Job is not in failed state")],
    ["a value that isn't an error", "Command timed out"],
  ])("is false for %s", (_case, error) => {
    expect(isUnansweredError(error)).toBe(false);
  });
});

const OUT_OF_MEMORY_MESSAGE = "OOM command not allowed when used memory > 'maxmemory'.";

describe("isRedisAwayError", () => {
  test.each([
    ["an unanswered command", new Error("Connection is closed.")],
    ["the server's out-of-memory reply to a command", new ReplyError(OUT_OF_MEMORY_MESSAGE)],
    [
      "the out-of-memory error a script's redis.call raises, as Redis 7.2 and later and Valkey wrap it",
      new ReplyError(
        `ERR Error running script (call to 71e6319f97b0fe8bdfa1c5df3ce4489946dda479): @user_script:1: ${OUT_OF_MEMORY_MESSAGE}`,
      ),
    ],
    [
      "the out-of-memory error a script's redis.call raises, as Redis 7.0 wraps it",
      new ReplyError(
        `ERR Error running script (call to 71e6319f97b0fe8bdfa1c5df3ce4489946dda479): @user_script:1: @user_script: 1: -${OUT_OF_MEMORY_MESSAGE}`,
      ),
    ],
  ])("is true for %s", (_case, error) => {
    expect(isRedisAwayError(error)).toBe(true);
  });

  test.each([
    ["a server still loading its dataset", new ReplyError("LOADING Redis is loading the dataset in memory")],
    ["a busy script", new ReplyError("BUSY Redis is busy running a script.")],
    ["a key of the wrong type", new ReplyError("WRONGTYPE Operation against a key holding the wrong kind of value")],
    ["an error that is not a reply and only quotes the message", new Error(`handler saw: ${OUT_OF_MEMORY_MESSAGE}`)],
  ])("is false for %s", (_case, error) => {
    expect(isRedisAwayError(error)).toBe(false);
  });
});

describe("the worker's Redis outage", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  function outageOfRecordedLines(waitUntilReady = () => Promise.resolve()) {
    const { logger, lines } = createRecordingLogger();
    const redisOutage = createRedisOutage({
      outageTracker: createConnectionOutageTracker(logger),
      waitUntilReady,
      outOfMemoryRetryDelayInMilliseconds: 1000,
    });
    return { redisOutage, lines };
  }

  test("an out-of-memory reply lasting several waits opens one connection error, closed by the first answer", async () => {
    vi.useFakeTimers();
    const { redisOutage, lines } = outageOfRecordedLines();
    const outOfMemoryReply = new ReplyError(OUT_OF_MEMORY_MESSAGE);

    for (let waitCount = 0; waitCount < 3; waitCount += 1) {
      const wait = redisOutage.waitForRedis(outOfMemoryReply);
      await vi.advanceTimersByTimeAsync(1000);
      await wait;
    }
    redisOutage.noteRedisAnswered();
    redisOutage.noteRedisAnswered();

    expect(lines.map(({ message }) => message)).toEqual(["connection error", "connection restored"]);
    expect(lines[0]?.fields).toEqual({ err: outOfMemoryReply });
  });

  test("an out-of-memory wait lasts the retry delay, or ends on its stop signal", async () => {
    vi.useFakeTimers();
    const { redisOutage } = outageOfRecordedLines();
    const stopController = new AbortController();
    let isWaitOver = false;
    void redisOutage.waitForRedis(new ReplyError(OUT_OF_MEMORY_MESSAGE)).then(() => {
      isWaitOver = true;
    });
    const stoppedWait = redisOutage.waitForRedis(new ReplyError(OUT_OF_MEMORY_MESSAGE), stopController.signal);

    stopController.abort();
    await stoppedWait;
    await vi.advanceTimersByTimeAsync(999);
    const isWaitOverBefore = isWaitOver;
    await vi.advanceTimersByTimeAsync(1);

    expect(isWaitOverBefore).toBe(false);
    expect(isWaitOver).toBe(true);
  });

  test("an unanswered command waits for the client to be ready and writes no line itself", async () => {
    const waitUntilReady = vi.fn(() => Promise.resolve());
    const { redisOutage, lines } = outageOfRecordedLines(waitUntilReady);

    await redisOutage.waitForRedis(new Error("Connection is closed."));
    redisOutage.noteRedisAnswered();

    expect(waitUntilReady).toHaveBeenCalledTimes(1);
    expect(lines).toEqual([]);
  });
});

describe("waitUntilReadyWithoutDeadline", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  test("keeps waiting past giveUpAfter with one shared ready listener, and resolves on the ready event", async () => {
    await withNeverConnectingClient(async (connections, client) => {
      vi.useFakeTimers();
      const readyListenerCountBefore = client.listenerCount("ready");
      let isReady = false;
      const waits = Array.from({ length: CONCURRENT_CALLER_COUNT }, () =>
        connections.waitUntilReadyWithoutDeadline(client).then(() => {
          isReady = true;
        }),
      );

      await vi.advanceTimersByTimeAsync(3 * connections.giveUpAfterInMilliseconds);
      const isReadyAfterThreeGiveUps = isReady;
      const readyListenerCountWhileWaiting = client.listenerCount("ready");
      client.emit("ready");
      await Promise.all(waits);

      expect(isReadyAfterThreeGiveUps).toBe(false);
      expect(readyListenerCountWhileWaiting).toBe(readyListenerCountBefore + 1);
      expect(client.listenerCount("ready")).toBe(readyListenerCountBefore);
    });
  });

  test("ends on its stop signal, and its listener goes at the next giveUpAfter", async () => {
    await withNeverConnectingClient(async (connections, client) => {
      vi.useFakeTimers();
      const readyListenerCountBefore = client.listenerCount("ready");
      const stopController = new AbortController();
      const stopReason = new Error("stopped");

      const wait = connections.waitUntilReadyWithoutDeadline(client, stopController.signal);
      stopController.abort(stopReason);
      const outcome = await wait.catch((error: unknown) => error);
      await vi.advanceTimersByTimeAsync(connections.giveUpAfterInMilliseconds);

      expect(outcome).toBe(stopReason);
      expect(client.listenerCount("ready")).toBe(readyListenerCountBefore);
      expect(vi.getTimerCount()).toBe(0);
    });
  });
});
