import {
  ClientClosedError,
  ConnectionTimeoutError,
  DisconnectsClientError,
  ErrorReply,
  SocketClosedUnexpectedlyError,
  TimeoutError,
} from "@redis/client";

import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";

import {
  createConnectionOutageTracker,
  createRedisConnections,
  createRedisOutage,
  isRedisAwayError,
  isUnansweredError,
  READ_ONLY_RECONNECT_PAUSE_IN_MILLISECONDS,
} from "./connection.js";
import type { RedisConnection, RedisConnections } from "./connection.js";
import { createFakeRedisDrivers, fakeReplyCommandOf } from "../tests/fake-redis-drivers.js";
import type { FakeRedisDriver } from "../tests/fake-redis-drivers.js";
import { builtInCommandOf } from "./redis-client.js";
import { createRecordingLogger } from "../tests/recording-logger.js";
import type { RecordedLogLine } from "../tests/recording-logger.js";

const GIVE_UP_AFTER_IN_MILLISECONDS = 1000;

const BURST_CALLER_COUNT = 20;

const UNBLOCKED_MESSAGE =
  "UNBLOCKED force unblock from blocking operation, instance state changed (master -> replica?)";

function onePairNaming(err: Error | undefined): RecordedLogLine[] {
  return [
    { level: "error", fields: { err }, message: "connection error" },
    { level: "info", fields: {}, message: "connection restored" },
  ];
}

function networkError(code: string): Error {
  return Object.assign(new Error(`connect ${code} 127.0.0.1:6379`), { code });
}

type ConnectionScenario = {
  connections: RedisConnections;
  connection: RedisConnection;
  driver: FakeRedisDriver;
  lines: RecordedLogLine[];
};

function connectionScenarioOf(prepareDriver: (driver: FakeRedisDriver) => void = () => undefined): ConnectionScenario {
  const { logger, lines } = createRecordingLogger();
  const fakeRedisDrivers = createFakeRedisDrivers();
  fakeRedisDrivers.onCreate(prepareDriver);
  const connections = createRedisConnections(
    { url: "redis://127.0.0.1:1", giveUpAfter: "1s" },
    logger,
    fakeRedisDrivers.createDriver,
  );
  const connection = connections.createCommandConnection();
  const [driver] = fakeRedisDrivers.createdDrivers();
  if (driver === undefined) {
    throw new Error("no driver was created");
  }
  return { connections, connection, driver, lines };
}

function answered(reply: unknown) {
  return fakeReplyCommandOf("GET", () => Promise.resolve(reply));
}

function settledAtOf(
  promise: Promise<unknown>,
  startedAt: number,
): Promise<{ outcome: unknown; afterMilliseconds: number }> {
  return promise.then(
    (outcome: unknown) => ({ outcome, afterMilliseconds: Date.now() - startedAt }),
    (outcome: unknown) => ({ outcome, afterMilliseconds: Date.now() - startedAt }),
  );
}

async function forceReconnectCountsAround(
  driver: FakeRedisDriver | undefined,
  instantInMilliseconds: number,
): Promise<[before: number | undefined, at: number | undefined]> {
  await vi.advanceTimersByTimeAsync(instantInMilliseconds - 1);
  const forceReconnectCountBefore = driver?.forceReconnectCount;
  await vi.advanceTimersByTimeAsync(1);
  return [forceReconnectCountBefore, driver?.forceReconnectCount];
}

beforeEach(() => {
  vi.useFakeTimers({ now: Date.UTC(2030, 0, 1) });
});

afterEach(() => {
  vi.useRealTimers();
});

describe("the instance calls", () => {
  test("a burst of calls before the first ready each reject at giveUpAfter counted from its own start, leaving no timer", async () => {
    const { connection, driver } = connectionScenarioOf();
    const startedAt = Date.now();
    const firstBurst = Array.from({ length: BURST_CALLER_COUNT / 2 }, () =>
      settledAtOf(connection.instanceCalls.send(answered("OK")), startedAt),
    );
    await vi.advanceTimersByTimeAsync(GIVE_UP_AFTER_IN_MILLISECONDS / 2);
    const secondBurst = Array.from({ length: BURST_CALLER_COUNT / 2 }, () =>
      settledAtOf(connection.instanceCalls.send(answered("OK")), startedAt),
    );
    const timerCountWhileHeld = vi.getTimerCount();

    await vi.advanceTimersByTimeAsync(GIVE_UP_AFTER_IN_MILLISECONDS);
    const outcomes = await Promise.all([...firstBurst, ...secondBurst]);

    expect(timerCountWhileHeld).toBe(BURST_CALLER_COUNT);
    expect(outcomes.map(({ afterMilliseconds }) => afterMilliseconds)).toEqual([
      ...Array.from({ length: BURST_CALLER_COUNT / 2 }, () => GIVE_UP_AFTER_IN_MILLISECONDS),
      ...Array.from({ length: BURST_CALLER_COUNT / 2 }, () => 1.5 * GIVE_UP_AFTER_IN_MILLISECONDS),
    ]);
    expect(outcomes.every(({ outcome }) => outcome instanceof TimeoutError)).toBe(true);
    expect(vi.getTimerCount()).toBe(0);
    expect(driver.sentCommands).toHaveLength(BURST_CALLER_COUNT);
  });

  test("a call rejected at the deadline carries the connection's last error while it is down", async () => {
    const { connection, driver } = connectionScenarioOf();
    const connectionRefused = networkError("ECONNREFUSED");
    driver.loseConnection(connectionRefused);

    const outcome = connection.instanceCalls.send(answered("OK")).catch((error: unknown) => error);
    await vi.advanceTimersByTimeAsync(GIVE_UP_AFTER_IN_MILLISECONDS);

    expect(await outcome).toBe(connectionRefused);
  });

  test("a burst of calls held before the first ready are each written once at the ready, and answered", async () => {
    const { connection, driver } = connectionScenarioOf();
    const replies = Array.from({ length: BURST_CALLER_COUNT }, (_, position) =>
      connection.instanceCalls.send(answered(position)),
    );

    await vi.advanceTimersByTimeAsync(GIVE_UP_AFTER_IN_MILLISECONDS - 1);
    driver.becomeReady();

    expect(await Promise.all(replies)).toEqual(Array.from({ length: BURST_CALLER_COUNT }, (_, position) => position));
    expect(driver.sentCommands).toHaveLength(BURST_CALLER_COUNT);
    expect(vi.getTimerCount()).toBe(0);
  });

  test("a call rejected by a reset is resent once the connection is ready again, within its deadline", async () => {
    const { connection, driver } = connectionScenarioOf((fakeRedisDriver) => {
      fakeRedisDriver.becomeReady();
    });
    driver.isAnswering = false;
    const reply = connection.instanceCalls.send(answered("resent"));
    await vi.advanceTimersByTimeAsync(0);

    driver.loseConnection(networkError("ECONNRESET"));
    driver.isAnswering = true;
    await vi.advanceTimersByTimeAsync(GIVE_UP_AFTER_IN_MILLISECONDS / 2);
    driver.becomeReady();

    expect(await reply).toBe("resent");
    expect(driver.sentCommands).toEqual(["GET", "GET"]);
  });

  test("a batch with a command rejected by a reset is resent whole once the connection is ready again", async () => {
    const { connection, driver } = connectionScenarioOf((fakeRedisDriver) => {
      fakeRedisDriver.becomeReady();
    });
    driver.isAnswering = false;
    const results = connection.instanceCalls.sendBatch([answered("first"), answered("second")]);
    await vi.advanceTimersByTimeAsync(0);

    driver.loseConnection(networkError("ECONNRESET"));
    driver.isAnswering = true;
    driver.becomeReady();

    expect(await results).toEqual([
      { kind: "replied", reply: "first" },
      { kind: "replied", reply: "second" },
    ]);
    expect(driver.sentCommands).toEqual(["GET", "GET", "GET", "GET"]);
  });

  test("an error reply goes to the caller at once, without a resend", async () => {
    const { connection, driver } = connectionScenarioOf((fakeRedisDriver) => {
      fakeRedisDriver.becomeReady();
    });
    const wrongType = new ErrorReply("WRONGTYPE Operation against a key holding the wrong kind of value");

    const outcome = connection.instanceCalls
      .send(fakeReplyCommandOf("LRANGE", () => Promise.reject(wrongType)))
      .catch((error: unknown) => error);

    expect(await outcome).toBe(wrongType);
    expect(driver.sentCommands).toEqual(["LRANGE"]);
  });
});

describe("the worker calls", () => {
  test("a call held while the connection is down waits past giveUpAfter and is answered at the ready", async () => {
    const { connection, driver } = connectionScenarioOf();

    const reply = connection.workerCalls.send(answered("late"));
    await vi.advanceTimersByTimeAsync(10 * GIVE_UP_AFTER_IN_MILLISECONDS);
    const timerCountWhileHeld = vi.getTimerCount();
    driver.becomeReady();

    expect(await reply).toBe("late");
    expect(timerCountWhileHeld).toBe(0);
  });

  test("a call rejected by a reset goes to the caller, for the outage seam to resend", async () => {
    const { connection, driver } = connectionScenarioOf((fakeRedisDriver) => {
      fakeRedisDriver.becomeReady();
    });
    driver.isAnswering = false;
    const connectionReset = networkError("ECONNRESET");
    const outcome = connection.workerCalls.send(answered("lost")).catch((error: unknown) => error);
    await vi.advanceTimersByTimeAsync(0);

    driver.loseConnection(connectionReset);

    expect(await outcome).toBe(connectionReset);
  });
});

describe("a connection that stops answering without closing", () => {
  test("an instance call in flight rejects at giveUpAfter, and the connection is reconnected by force at that instant", async () => {
    const { connection, driver, lines } = connectionScenarioOf((fakeRedisDriver) => {
      fakeRedisDriver.becomeReady();
    });
    driver.isAnswering = false;
    const startedAt = Date.now();

    const outcome = settledAtOf(connection.instanceCalls.send(answered("lost")), startedAt);
    const forceReconnectCounts = await forceReconnectCountsAround(driver, GIVE_UP_AFTER_IN_MILLISECONDS);

    expect(forceReconnectCounts).toEqual([0, 1]);
    expect(await outcome).toEqual({
      outcome: expect.any(TimeoutError),
      afterMilliseconds: GIVE_UP_AFTER_IN_MILLISECONDS,
    });
    expect(lines.map(({ message }) => message)).toEqual(["connection error"]);
  });

  test("a worker call in flight is rejected by the forced reconnect at giveUpAfter, as an away error", async () => {
    const { connection, driver } = connectionScenarioOf((fakeRedisDriver) => {
      fakeRedisDriver.becomeReady();
    });
    driver.isAnswering = false;

    const outcome = connection.workerCalls.send(answered("lost")).catch((error: unknown) => error);
    await vi.advanceTimersByTimeAsync(GIVE_UP_AFTER_IN_MILLISECONDS);

    expect(driver.forceReconnectCount).toBe(1);
    expect(isRedisAwayError(await outcome)).toBe(true);
  });

  test("a call written at the ready after a wait gets its full giveUpAfter from that ready", async () => {
    const { connection, driver } = connectionScenarioOf();
    driver.isAnswering = false;
    const outcome = connection.workerCalls.send(answered("late")).catch((error: unknown) => error);
    await vi.advanceTimersByTimeAsync(5 * GIVE_UP_AFTER_IN_MILLISECONDS);

    driver.becomeReady();
    const forceReconnectCounts = await forceReconnectCountsAround(driver, GIVE_UP_AFTER_IN_MILLISECONDS);

    expect(forceReconnectCounts).toEqual([0, 1]);
    expect(isRedisAwayError(await outcome)).toBe(true);
  });

  test("a blocking connection waits for its longest block plus giveUpAfter before it reconnects by force", async () => {
    const { logger } = createRecordingLogger();
    const fakeRedisDrivers = createFakeRedisDrivers();
    fakeRedisDrivers.onCreate((fakeRedisDriver) => {
      fakeRedisDriver.becomeReady();
      fakeRedisDriver.isAnswering = false;
    });
    const connections = createRedisConnections(
      { url: "redis://127.0.0.1:1", giveUpAfter: "1s" },
      logger,
      fakeRedisDrivers.createDriver,
    );
    const connection = connections.createBlockingConnection(3000);
    const [driver] = fakeRedisDrivers.createdDrivers();

    const outcome = connection.workerCalls.send(answered(null)).catch((error: unknown) => error);
    const forceReconnectCounts = await forceReconnectCountsAround(driver, 3000 + GIVE_UP_AFTER_IN_MILLISECONDS);

    expect(forceReconnectCounts).toEqual([0, 1]);
    expect(isRedisAwayError(await outcome)).toBe(true);
  });

  test("a handshake left unanswered for giveUpAfter is reconnected by force, again for each new one", async () => {
    const { driver } = connectionScenarioOf();

    driver.startHandshake();
    await vi.advanceTimersByTimeAsync(GIVE_UP_AFTER_IN_MILLISECONDS);
    driver.startHandshake();
    await vi.advanceTimersByTimeAsync(GIVE_UP_AFTER_IN_MILLISECONDS - 1);
    const forceReconnectCountBeforeTheSecond = driver.forceReconnectCount;
    driver.becomeReady();
    await vi.advanceTimersByTimeAsync(10 * GIVE_UP_AFTER_IN_MILLISECONDS);

    expect(forceReconnectCountBeforeTheSecond).toBe(1);
    expect(driver.forceReconnectCount).toBe(1);
  });

  test("a handshake that fails with an error is left to the driver's own retries, never reconnected by force", async () => {
    const { driver } = connectionScenarioOf();

    driver.startHandshake();
    driver.loseConnection(networkError("ECONNRESET"));
    await vi.advanceTimersByTimeAsync(5 * GIVE_UP_AFTER_IN_MILLISECONDS);

    expect(driver.forceReconnectCount).toBe(0);
    expect(vi.getTimerCount()).toBe(0);
  });

  test("an answered connection idle for longer than giveUpAfter is never reconnected and logs nothing", async () => {
    const { connection, driver, lines } = connectionScenarioOf((fakeRedisDriver) => {
      fakeRedisDriver.becomeReady();
    });

    await connection.instanceCalls.send(answered("first"));
    await vi.advanceTimersByTimeAsync(10 * GIVE_UP_AFTER_IN_MILLISECONDS);
    const reply = await connection.instanceCalls.send(answered("next"));

    expect(reply).toBe("next");
    expect(driver.forceReconnectCount).toBe(0);
    expect(lines).toEqual([]);
    expect(vi.getTimerCount()).toBe(0);
  });

  test("none of its timers keeps the process alive, and none is left once the connection is closed", async () => {
    const setTimeoutSpy = vi.spyOn(globalThis, "setTimeout");
    const { connections, connection, driver } = connectionScenarioOf((fakeRedisDriver) => {
      fakeRedisDriver.becomeReady();
    });
    driver.isAnswering = false;
    const outcomes = Promise.all([
      connection.instanceCalls.send(answered("lost")).catch((error: unknown) => error),
      connection.workerCalls.send(answered("lost")).catch((error: unknown) => error),
    ]);
    driver.startHandshake();
    await vi.advanceTimersByTimeAsync(GIVE_UP_AFTER_IN_MILLISECONDS / 2);
    const timers = setTimeoutSpy.mock.results.map(({ value }) => value as NodeJS.Timeout);

    await connections.closeConnection(connection);
    await outcomes;

    expect(timers.length).toBeGreaterThanOrEqual(3);
    expect(timers.map((timer) => timer.hasRef())).toEqual(timers.map(() => false));
    expect(vi.getTimerCount()).toBe(0);
  });
});

describe("a write answered READONLY, as during a failover", () => {
  const READ_ONLY_STRETCH_IN_MILLISECONDS = 2000;

  function demotedPrimaryScenarioOf(readOnlyStretchInMilliseconds: number) {
    const scenario = connectionScenarioOf((fakeRedisDriver) => {
      fakeRedisDriver.becomeReady();
      fakeRedisDriver.onForceReconnect = () => {
        setTimeout(() => {
          fakeRedisDriver.becomeReady();
        }, 5);
      };
    });
    let isReadOnly = true;
    setTimeout(() => {
      isReadOnly = false;
    }, readOnlyStretchInMilliseconds);
    const readOnlyReplies: ErrorReply[] = [];
    const writeReply = async () => {
      if (isReadOnly) {
        const readOnlyReply = new ErrorReply("READONLY You can't write against a read only replica.");
        readOnlyReplies.push(readOnlyReply);
        throw readOnlyReply;
      }
      return "OK";
    };
    const write = () => fakeReplyCommandOf("SET", writeReply);
    const blockedWaitUnblockedOnce = () => {
      let isUnblocked = false;
      return fakeReplyCommandOf("BZPOPMIN", async () => {
        if (!isUnblocked) {
          isUnblocked = true;
          throw new ErrorReply(UNBLOCKED_MESSAGE);
        }
        return writeReply();
      });
    };
    return { ...scenario, write, blockedWaitUnblockedOnce, readOnlyReplies };
  }

  test("a worker's blocked wait answered UNBLOCKED reconnects by force at once, logging nothing, and the READONLY stretch after it past giveUpAfter logs one pair naming the READONLY reply", async () => {
    const { connection, driver, lines, blockedWaitUnblockedOnce, readOnlyReplies } = demotedPrimaryScenarioOf(
      READ_ONLY_STRETCH_IN_MILLISECONDS,
    );

    const reply = connection.workerCalls.send(blockedWaitUnblockedOnce());
    await vi.advanceTimersByTimeAsync(0);
    const forceReconnectCountAtTheUnblocked = driver.forceReconnectCount;
    const linesAtTheUnblocked = [...lines];
    await vi.advanceTimersByTimeAsync(READ_ONLY_STRETCH_IN_MILLISECONDS + READ_ONLY_RECONNECT_PAUSE_IN_MILLISECONDS);

    expect(await reply).toBe("OK");
    expect(forceReconnectCountAtTheUnblocked).toBe(1);
    expect(linesAtTheUnblocked).toEqual([]);
    expect(lines).toEqual(onePairNaming(readOnlyReplies[0]));
  });

  test("two blocked waits answered UNBLOCKED together start one forced reconnect, and an UNBLOCKED reply alone opens no stretch", async () => {
    const { connection, driver, lines, blockedWaitUnblockedOnce } = demotedPrimaryScenarioOf(0);

    const replies = Promise.all([
      connection.workerCalls.send(blockedWaitUnblockedOnce()),
      connection.workerCalls.send(blockedWaitUnblockedOnce()),
    ]);
    await vi.advanceTimersByTimeAsync(0);
    const forceReconnectCountAtTheUnblocked = driver.forceReconnectCount;
    await vi.advanceTimersByTimeAsync(10 * GIVE_UP_AFTER_IN_MILLISECONDS);

    expect(await replies).toEqual(["OK", "OK"]);
    expect(forceReconnectCountAtTheUnblocked).toBe(1);
    expect(driver.forceReconnectCount).toBe(1);
    expect(lines).toEqual([]);
  });

  test("worker writes on both connections answered READONLY past giveUpAfter, a drop overlapping, write one connection error with the first reply and one connection restored once a write is accepted", async () => {
    const { connections, connection, driver, lines, write, readOnlyReplies } = demotedPrimaryScenarioOf(
      READ_ONLY_STRETCH_IN_MILLISECONDS,
    );
    const blockingConnection = connections.createBlockingConnection(0);
    const commandOutcome = connection.workerCalls.send(write()).catch((error: unknown) => error);
    const blockingReply = blockingConnection.workerCalls.send(write());
    await vi.advanceTimersByTimeAsync(GIVE_UP_AFTER_IN_MILLISECONDS);
    const messagesAtGiveUpAfter = lines.map(({ message }) => message);

    driver.loseConnection(networkError("ECONNRESET"));
    await vi.advanceTimersByTimeAsync(GIVE_UP_AFTER_IN_MILLISECONDS / 4);
    driver.becomeReady();
    const messagesOnceTheDropIsOver = lines.map(({ message }) => message);
    await vi.advanceTimersByTimeAsync(READ_ONLY_STRETCH_IN_MILLISECONDS);
    await commandOutcome;

    expect(await blockingReply).toBe("OK");
    expect(messagesAtGiveUpAfter).toEqual(["connection error"]);
    expect(messagesOnceTheDropIsOver).toEqual(["connection error"]);
    expect(lines).toEqual(onePairNaming(readOnlyReplies[0]));
  });

  test("a read accepted during a worker's stretch past giveUpAfter does not end it", async () => {
    const { connection, driver, lines, write } = demotedPrimaryScenarioOf(10 * GIVE_UP_AFTER_IN_MILLISECONDS);
    driver.infoReply = () => Promise.resolve("# Replication\r\nrole:slave\r\n");
    void connection.workerCalls.send(write()).catch(() => undefined);
    await vi.advanceTimersByTimeAsync(GIVE_UP_AFTER_IN_MILLISECONDS);

    await connection.workerCalls.send(builtInCommandOf("INFO"));

    expect(lines.map(({ message }) => message)).toEqual(["connection error"]);
  });

  test("instance writes alone, as on an instance with no worker, never open the pair", async () => {
    const { connection, lines, write } = demotedPrimaryScenarioOf(10 * GIVE_UP_AFTER_IN_MILLISECONDS);

    const outcome = connection.instanceCalls.send(write()).catch((error: unknown) => error);
    await vi.advanceTimersByTimeAsync(5 * GIVE_UP_AFTER_IN_MILLISECONDS);
    await outcome;

    expect(lines).toEqual([]);
  });

  test.each([
    { closedAfterInMilliseconds: GIVE_UP_AFTER_IN_MILLISECONDS / 2, expectedMessages: [] },
    { closedAfterInMilliseconds: GIVE_UP_AFTER_IN_MILLISECONDS, expectedMessages: ["connection error"] },
  ])(
    "closing the instance's connections $closedAfterInMilliseconds ms into a worker's stretch writes $expectedMessages and leaves no timer",
    async ({ closedAfterInMilliseconds, expectedMessages }) => {
      const { connections, connection, lines, write } = demotedPrimaryScenarioOf(10 * GIVE_UP_AFTER_IN_MILLISECONDS);
      const blockingConnection = connections.createBlockingConnection(0);
      const outcomes = Promise.all([
        connection.workerCalls.send(write()).catch((error: unknown) => error),
        blockingConnection.workerCalls.send(write()).catch((error: unknown) => error),
      ]);
      await vi.advanceTimersByTimeAsync(closedAfterInMilliseconds);

      await connections.closeConnection(blockingConnection);
      await connections.closeConnection(connection);
      await outcomes;
      await vi.advanceTimersByTimeAsync(20 * GIVE_UP_AFTER_IN_MILLISECONDS);

      expect(lines.map(({ message }) => message)).toEqual(expectedMessages);
      expect(vi.getTimerCount()).toBe(0);
    },
  );

  test("a worker write is answered once the server accepts writes again within giveUpAfter, the caller seeing no error and nothing logged, through a bounded number of reconnects", async () => {
    const shortStretchInMilliseconds = GIVE_UP_AFTER_IN_MILLISECONDS / 2;
    const { connection, driver, lines, write } = demotedPrimaryScenarioOf(shortStretchInMilliseconds);

    const reply = connection.workerCalls.send(write());
    await vi.advanceTimersByTimeAsync(shortStretchInMilliseconds + READ_ONLY_RECONNECT_PAUSE_IN_MILLISECONDS);
    await vi.advanceTimersByTimeAsync(10 * GIVE_UP_AFTER_IN_MILLISECONDS);

    expect(await reply).toBe("OK");
    expect(driver.forceReconnectCount).toBeGreaterThan(0);
    expect(driver.forceReconnectCount).toBeLessThanOrEqual(
      shortStretchInMilliseconds / READ_ONLY_RECONNECT_PAUSE_IN_MILLISECONDS + 1,
    );
    expect(lines).toEqual([]);
  });

  test("a worker batch with a write answered READONLY is resent whole once the server accepts writes again", async () => {
    const { connection, write } = demotedPrimaryScenarioOf(READ_ONLY_STRETCH_IN_MILLISECONDS / 4);

    const results = connection.workerCalls.sendBatch([answered("read"), write()]);
    await vi.advanceTimersByTimeAsync(READ_ONLY_STRETCH_IN_MILLISECONDS);

    expect(await results).toEqual([
      { kind: "replied", reply: "read" },
      { kind: "replied", reply: "OK" },
    ]);
  });

  test("an instance write is answered when the stretch is shorter than giveUpAfter, and rejects with the last READONLY reply at giveUpAfter otherwise", async () => {
    const shortStretch = demotedPrimaryScenarioOf(GIVE_UP_AFTER_IN_MILLISECONDS / 2);
    const longStretch = demotedPrimaryScenarioOf(10 * GIVE_UP_AFTER_IN_MILLISECONDS);

    const shortReply = shortStretch.connection.instanceCalls.send(shortStretch.write());
    const longOutcome = longStretch.connection.instanceCalls.send(longStretch.write()).catch((error: unknown) => error);
    await vi.advanceTimersByTimeAsync(GIVE_UP_AFTER_IN_MILLISECONDS);

    expect(await shortReply).toBe("OK");
    expect(longStretch.readOnlyReplies.length).toBeGreaterThan(1);
    expect(await longOutcome).toBe(longStretch.readOnlyReplies.at(-1));
  });

  test("an instance write whose resend after a READONLY reply is rejected unanswered rejects with that READONLY reply at giveUpAfter", async () => {
    const { connection, driver, write, readOnlyReplies } = demotedPrimaryScenarioOf(10 * GIVE_UP_AFTER_IN_MILLISECONDS);
    const outcome = connection.instanceCalls.send(write()).catch((error: unknown) => error);
    await vi.advanceTimersByTimeAsync(0);
    driver.isAnswering = false;
    await vi.advanceTimersByTimeAsync(GIVE_UP_AFTER_IN_MILLISECONDS / 2);

    driver.onForceReconnect = () => undefined;
    driver.forceReconnect();
    await vi.advanceTimersByTimeAsync(GIVE_UP_AFTER_IN_MILLISECONDS / 2);

    expect(readOnlyReplies).toHaveLength(1);
    expect(await outcome).toBe(readOnlyReplies[0]);
  });

  test("an instance write answered READONLY on a connection then dropped rejects with the drop's error at giveUpAfter", async () => {
    const { connection, driver, write } = demotedPrimaryScenarioOf(10 * GIVE_UP_AFTER_IN_MILLISECONDS);
    const connectionRefused = networkError("ECONNREFUSED");
    const outcome = connection.instanceCalls.send(write()).catch((error: unknown) => error);
    await vi.advanceTimersByTimeAsync(GIVE_UP_AFTER_IN_MILLISECONDS / 2);

    driver.onForceReconnect = () => undefined;
    driver.loseConnection(connectionRefused);
    await vi.advanceTimersByTimeAsync(GIVE_UP_AFTER_IN_MILLISECONDS / 2);

    expect(await outcome).toBe(connectionRefused);
  });
});

describe("closeConnection", () => {
  test("closes a ready driver within the bound, then destroys it", async () => {
    const { connections, connection, driver } = connectionScenarioOf((fakeRedisDriver) => {
      fakeRedisDriver.becomeReady();
    });

    await connections.closeConnection(connection, 1000);

    expect([driver.closeCount, driver.destroyCount]).toEqual([1, 1]);
  });

  test("gives up on a close never answered at the bound, and destroys", async () => {
    const { connections, connection, driver } = connectionScenarioOf((fakeRedisDriver) => {
      fakeRedisDriver.becomeReady();
      fakeRedisDriver.closeReply = "never answered";
    });
    let isClosed = false;

    const closing = connections.closeConnection(connection, 1000).then(() => {
      isClosed = true;
    });
    await vi.advanceTimersByTimeAsync(999);
    const isClosedBeforeTheBound = isClosed;
    await vi.advanceTimersByTimeAsync(1);
    await closing;

    expect(isClosedBeforeTheBound).toBe(false);
    expect(driver.destroyCount).toBe(1);
    expect(vi.getTimerCount()).toBe(0);
  });

  test("destroys a driver that is not ready, or that has no close bound, without closing it", async () => {
    const reconnecting = connectionScenarioOf();
    const ready = connectionScenarioOf((fakeRedisDriver) => {
      fakeRedisDriver.becomeReady();
    });

    await reconnecting.connections.closeConnection(reconnecting.connection, 1000);
    await ready.connections.closeConnection(ready.connection);

    expect([reconnecting.driver.closeCount, ready.driver.closeCount]).toEqual([0, 0]);
    expect([reconnecting.driver.destroyCount, ready.driver.destroyCount]).toEqual([1, 1]);
  });

  test("rejects every call held or in flight and every wait for ready, leaving no timer", async () => {
    const { connections, connection } = connectionScenarioOf();
    const outcomes = Promise.all([
      connection.instanceCalls.send(answered("held")).catch((error: unknown) => error),
      connection.workerCalls.send(answered("held")).catch((error: unknown) => error),
      connections.waitUntilReady(connection).catch((error: unknown) => error),
    ]);

    await connections.closeConnection(connection);

    expect((await outcomes).map((outcome) => isUnansweredError(outcome))).toEqual([true, true, true]);
    expect(vi.getTimerCount()).toBe(0);
  });

  test("makes a later call and a later wait for ready reject at once as unanswered", async () => {
    const { connections, connection } = connectionScenarioOf();
    await connections.closeConnection(connection);

    const outcomes = await Promise.all([
      connection.instanceCalls.send(answered("late")).catch((error: unknown) => error),
      connections.waitUntilReady(connection).catch((error: unknown) => error),
    ]);

    expect(outcomes.map((outcome) => isUnansweredError(outcome))).toEqual([true, true]);
    expect(vi.getTimerCount()).toBe(0);
  });

  test("ends the outage of a connection closed while down silently", async () => {
    const { connections, connection, driver, lines } = connectionScenarioOf();
    driver.loseConnection(networkError("ECONNREFUSED"));

    await connections.closeConnection(connection);
    driver.becomeReady();

    expect(lines.map(({ message }) => message)).toEqual(["connection error"]);
  });
});

describe("waitUntilReady", () => {
  test("keeps waiting past giveUpAfter with no timer, and resolves every waiter on the ready", async () => {
    const { connections, connection, driver } = connectionScenarioOf();
    let readyWaiterCount = 0;
    const waits = Array.from({ length: BURST_CALLER_COUNT }, () =>
      connections.waitUntilReady(connection).then(() => {
        readyWaiterCount += 1;
      }),
    );

    await vi.advanceTimersByTimeAsync(3 * GIVE_UP_AFTER_IN_MILLISECONDS);
    const readyWaiterCountBeforeTheReady = readyWaiterCount;
    const timerCountWhileWaiting = vi.getTimerCount();
    driver.becomeReady();
    await Promise.all(waits);

    expect(readyWaiterCountBeforeTheReady).toBe(0);
    expect(timerCountWhileWaiting).toBe(0);
    expect(readyWaiterCount).toBe(BURST_CALLER_COUNT);
  });

  test("ends on its stop signal", async () => {
    const { connections, connection } = connectionScenarioOf();
    const stopController = new AbortController();
    const stopReason = new Error("stopped");

    const wait = connections.waitUntilReady(connection, stopController.signal);
    stopController.abort(stopReason);

    expect(await wait.catch((error: unknown) => error)).toBe(stopReason);
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

describe("isUnansweredError", () => {
  test.each([
    ["a command timeout", new TimeoutError()],
    ["a connection closed by the server", new SocketClosedUnexpectedlyError()],
    ["a command rejected by a destroy", new DisconnectsClientError()],
    ["a connect timeout", new ConnectionTimeoutError()],
    ["a command sent on a closed client", new ClientClosedError()],
    ["a refused connection", networkError("ECONNREFUSED")],
    ["a reset connection", networkError("ECONNRESET")],
    ["an unknown host", networkError("ENOTFOUND")],
  ])("is true for %s", (_case, error) => {
    expect(isUnansweredError(error)).toBe(true);
  });

  test.each([
    ["a reply error", new ErrorReply("ERR wrong number of arguments")],
    ["an error of the call itself", new Error("Job is not in failed state")],
    ["an error that only words itself as a timeout", new Error("Command timed out")],
    ["a value that isn't an error", "Socket closed unexpectedly"],
  ])("is false for %s", (_case, error) => {
    expect(isUnansweredError(error)).toBe(false);
  });
});

const OUT_OF_MEMORY_MESSAGE = "OOM command not allowed when used memory > 'maxmemory'.";

describe("isRedisAwayError", () => {
  test.each([
    ["an unanswered command", new SocketClosedUnexpectedlyError()],
    ["the server's out-of-memory reply to a command", new ErrorReply(OUT_OF_MEMORY_MESSAGE)],
    [
      "the out-of-memory error a script's redis.call raises, as Redis 7.2 and later and Valkey wrap it",
      new ErrorReply(
        `ERR Error running script (call to 71e6319f97b0fe8bdfa1c5df3ce4489946dda479): @user_script:1: ${OUT_OF_MEMORY_MESSAGE}`,
      ),
    ],
    [
      "the out-of-memory error a script's redis.call raises, as Redis 7.0 wraps it",
      new ErrorReply(
        `ERR Error running script (call to 71e6319f97b0fe8bdfa1c5df3ce4489946dda479): @user_script:1: @user_script: 1: -${OUT_OF_MEMORY_MESSAGE}`,
      ),
    ],
  ])("is true for %s", (_case, error) => {
    expect(isRedisAwayError(error)).toBe(true);
  });

  test.each([
    ["a server still loading its dataset", new ErrorReply("LOADING Redis is loading the dataset in memory")],
    ["a busy script", new ErrorReply("BUSY Redis is busy running a script.")],
    ["a key of the wrong type", new ErrorReply("WRONGTYPE Operation against a key holding the wrong kind of value")],
    ["an error that is not a reply and only quotes the message", new Error(`handler saw: ${OUT_OF_MEMORY_MESSAGE}`)],
  ])("is false for %s", (_case, error) => {
    expect(isRedisAwayError(error)).toBe(false);
  });
});

describe("the worker's Redis outage", () => {
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
    const { redisOutage, lines } = outageOfRecordedLines();
    const outOfMemoryReply = new ErrorReply(OUT_OF_MEMORY_MESSAGE);

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
    const { redisOutage } = outageOfRecordedLines();
    const stopController = new AbortController();
    let isWaitOver = false;
    void redisOutage.waitForRedis(new ErrorReply(OUT_OF_MEMORY_MESSAGE)).then(() => {
      isWaitOver = true;
    });
    const stoppedWait = redisOutage.waitForRedis(new ErrorReply(OUT_OF_MEMORY_MESSAGE), stopController.signal);

    stopController.abort();
    await stoppedWait;
    await vi.advanceTimersByTimeAsync(999);
    const isWaitOverBefore = isWaitOver;
    await vi.advanceTimersByTimeAsync(1);

    expect(isWaitOverBefore).toBe(false);
    expect(isWaitOver).toBe(true);
  });

  test("an unanswered command waits for the connection to be ready and writes no line itself", async () => {
    const waitUntilReady = vi.fn(() => Promise.resolve());
    const { redisOutage, lines } = outageOfRecordedLines(waitUntilReady);

    await redisOutage.waitForRedis(new SocketClosedUnexpectedlyError());
    redisOutage.noteRedisAnswered();

    expect(waitUntilReady).toHaveBeenCalledTimes(1);
    expect(lines).toEqual([]);
  });
});

describe("callOnRedis", () => {
  const { connections } = connectionScenarioOf();

  test("turns an unanswered command into RedisUnavailableError naming giveUpAfter, with the call's ids", async () => {
    const unanswered = new TimeoutError();

    const call = connections.callOnRedis({ runId: "welcome-ada" }, () => Promise.reject(unanswered));

    await expect(call).rejects.toThrow(
      expect.objectContaining({
        name: "RedisUnavailableError",
        message: "Redis didn't answer within 1s",
        cause: unanswered,
        runId: "welcome-ada",
      }),
    );
  });

  test("turns an out-of-memory reply into RedisUnavailableError saying so, with the reply as its cause", async () => {
    const outOfMemoryReply = new ErrorReply(OUT_OF_MEMORY_MESSAGE);

    const call = connections.callOnRedis({ eventId: "invoice-42-paid" }, () => Promise.reject(outOfMemoryReply));

    await expect(call).rejects.toThrow(
      expect.objectContaining({
        name: "RedisUnavailableError",
        message: "Redis refused the command: it is out of memory",
        cause: outOfMemoryReply,
        eventId: "invoice-42-paid",
      }),
    );
  });

  test("turns a READONLY reply into RedisUnavailableError naming giveUpAfter, with the reply as its cause", async () => {
    const readOnlyReply = new ErrorReply("READONLY You can't write against a read only replica.");

    const call = connections.callOnRedis({ runId: "welcome-ada" }, () => Promise.reject(readOnlyReply));

    await expect(call).rejects.toThrow(
      expect.objectContaining({
        name: "RedisUnavailableError",
        message: "Redis didn't answer within 1s",
        cause: readOnlyReply,
        runId: "welcome-ada",
      }),
    );
  });

  test("lets any other error through untouched", async () => {
    const scriptError = new ErrorReply("ERR Error running script");

    await expect(connections.callOnRedis({}, () => Promise.reject(scriptError))).rejects.toBe(scriptError);
  });
});
