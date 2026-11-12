import type { RedisClient } from "./redis-client.js";
import * as v from "valibot";
import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";

import { SocketClosedUnexpectedlyError } from "@redis/client";

import { createRedisConnections } from "./connection.js";
import { defineScheduledTask, defineTask } from "./definitions.js";
import type { AnyTaskDefinition, TaskHandler } from "./definitions.js";
import type { DispatchedRun } from "./enqueue-requests.js";
import type { Engine, RunToCreate, SettleRequest } from "./engine/engine.js";
import { InstanceClosedError } from "./errors.js";
import { createInstanceLifecycle } from "./instance-lifecycle.js";
import type { InstanceLifecycle } from "./instance-lifecycle.js";
import { MISSING_MANUAL_SIGNAL_HANDLING_WARNING } from "./shutdown-signals.js";
import type { HandlerSender } from "./step-engine.js";
import { DEFAULT_WORKER_TIMING } from "./worker-timing.js";
import { createFakeRedisDrivers, engineOnFakeDriver, wakeUpWaitOnFakeDriver } from "../tests/fake-redis-drivers.js";
import type { FakeRedisDriver, FakeRedisDrivers } from "../tests/fake-redis-drivers.js";
import { createHandlerGate, gatedBetweenTwoSteps, gatedWithNoStep } from "../tests/handler-gate.js";
import type { HandlerGate } from "../tests/handler-gate.js";
import { createInMemoryEngine } from "../tests/in-memory-engine.js";
import type { InMemoryEngine } from "../tests/in-memory-engine.js";
import { CONFIG_DISABLED, infoReplyWith } from "../tests/info-replies.js";
import type { InfoReplyFields } from "../tests/info-replies.js";
import { spyOnProcessSignals } from "../tests/process-signal-spies.js";
import type { ProcessSignalSpies } from "../tests/process-signal-spies.js";
import { createRecordingLogger } from "../tests/recording-logger.js";
import type { RecordedLogLine } from "../tests/recording-logger.js";
import { unhandledRejectionsDuring } from "../tests/unhandled-rejections.js";
import { workerDefinitionsOf } from "../tests/worker-definitions.js";

const VALKEY_AT_THE_FLOOR: InfoReplyFields = {
  valkeyVersion: "8.0.1",
  redisVersion: "7.2.4",
  maxmemoryPolicy: "noeviction",
};

const GIVE_UP_AFTER_IN_MILLISECONDS = 5000;

const SHUTDOWN_TIMEOUT_IN_MILLISECONDS = 5000;

const CLOSE_BOUND_IN_MILLISECONDS =
  SHUTDOWN_TIMEOUT_IN_MILLISECONDS +
  DEFAULT_WORKER_TIMING.abortedAttemptsGraceInMilliseconds +
  DEFAULT_WORKER_TIMING.commandConnectionCloseBoundInMilliseconds;

type LifecycleScenario = {
  lifecycle: InstanceLifecycle;
  engine: InMemoryEngine;
  lines: RecordedLogLine[];
  fakeRedisDrivers: FakeRedisDrivers;
  commandDriver: () => FakeRedisDriver;
  blockingDrivers: () => FakeRedisDriver[];
};

type LifecycleScenarioOptions = {
  tasks?: readonly AnyTaskDefinition[];
  engine?: InMemoryEngine;
  prepareDriver?: (fakeRedisDriver: FakeRedisDriver) => void;
  engineOn?: (engine: InMemoryEngine, client: RedisClient, commandDriver: () => FakeRedisDriver) => Engine;
  senderOn?: (engine: Engine) => HandlerSender;
  env?: NodeJS.ProcessEnv;
};

function connectionResetError(): Error {
  return Object.assign(new Error("read ECONNRESET"), { code: "ECONNRESET" });
}

const NO_SENDER: HandlerSender = {
  dispatch: () => Promise.reject(new Error("this scenario sends nothing")),
  emit: () => Promise.reject(new Error("this scenario sends nothing")),
};

const startedLifecycles: InstanceLifecycle[] = [];

function readyAtTheFloor(fakeRedisDriver: FakeRedisDriver): void {
  fakeRedisDriver.infoReply = async () => infoReplyWith(VALKEY_AT_THE_FLOOR);
  fakeRedisDriver.becomeReady();
}

function lifecycleScenarioOf({
  tasks = [],
  engine = createInMemoryEngine(),
  prepareDriver = readyAtTheFloor,
  engineOn = (inMemoryEngine, client) => engineOnFakeDriver(inMemoryEngine, client),
  senderOn = () => NO_SENDER,
  env = { NODE_ENV: "development" },
}: LifecycleScenarioOptions = {}): LifecycleScenario {
  const { logger, lines } = createRecordingLogger();
  const fakeRedisDrivers = createFakeRedisDrivers();
  fakeRedisDrivers.onCreate(prepareDriver);
  const connections = createRedisConnections(
    { url: "redis://127.0.0.1:1", giveUpAfter: "5s" },
    logger,
    fakeRedisDrivers.createDriver,
  );
  const commandDriver = () => {
    const [firstDriver] = fakeRedisDrivers.createdDrivers();
    if (firstDriver === undefined) {
      throw new Error("no command connection was opened");
    }
    return firstDriver;
  };
  const lifecycle = createInstanceLifecycle({
    connections,
    engineOn: (client) => engineOn(engine, client, commandDriver),
    wakeUpWaitOn: (blockingClient) => wakeUpWaitOnFakeDriver(engine, blockingClient),
    senderOn,
    definitions: workerDefinitionsOf(tasks),
    logger,
    timing: DEFAULT_WORKER_TIMING,
    env,
  });
  startedLifecycles.push(lifecycle);
  return {
    lifecycle,
    engine,
    lines,
    fakeRedisDrivers,
    commandDriver,
    blockingDrivers: () => fakeRedisDrivers.createdDrivers().slice(1),
  };
}

function runsOf(task: string, count: number): RunToCreate[] {
  return Array.from({ length: count }, (_, position) => ({
    runId: `${task}-${String(position + 1)}`,
    task,
    payload: "{}",
    retries: 3,
  }));
}

function messagesOf(lines: readonly RecordedLogLine[]): string[] {
  return lines.map(({ message }) => message);
}

function settleOutcomeKindsOf(engine: InMemoryEngine): string[] {
  return engine.calls
    .filter(({ method }) => method === "settle")
    .map(({ request }) => (request as SettleRequest).outcome.kind);
}

type SettledPromise = { isSettled: () => boolean; settledAt: () => number | undefined };

function settlementOf(promise: Promise<unknown>): SettledPromise {
  let settledAt: number | undefined;
  promise.then(
    () => {
      settledAt = Date.now();
    },
    () => {
      settledAt = Date.now();
    },
  );
  return { isSettled: () => settledAt !== undefined, settledAt: () => settledAt };
}

function gatedTaskOf(handlerOf: (handlerGate: HandlerGate) => TaskHandler<object>): {
  task: AnyTaskDefinition;
  handlerGate: HandlerGate;
} {
  const handlerGate = createHandlerGate();
  return {
    task: defineTask("send-email", { schema: v.object({}), handler: handlerOf(handlerGate) }) as AnyTaskDefinition,
    handlerGate,
  };
}

function scenarioOfABackgroundBootBelowTheFloor(): LifecycleScenario {
  return lifecycleScenarioOf({
    prepareDriver: (fakeRedisDriver) => {
      fakeRedisDriver.infoReply = async () => infoReplyWith({ redisVersion: "7.2.4", maxmemoryPolicy: "noeviction" });
    },
  });
}

async function scenarioOfARunningWorker(
  handlerOf: (handlerGate: HandlerGate) => TaskHandler<object>,
  options: LifecycleScenarioOptions = {},
): Promise<LifecycleScenario & { handlerGate: HandlerGate }> {
  const { task, handlerGate } = gatedTaskOf(handlerOf);
  const engine = options.engine ?? createInMemoryEngine();
  await engine.enqueue({ runs: runsOf("send-email", 1) });
  const scenario = lifecycleScenarioOf({ ...options, tasks: [task], engine });
  await scenario.lifecycle.startWorker({ shutdownTimeout: "5s" });
  await vi.advanceTimersByTimeAsync(0);
  return { ...scenario, handlerGate };
}

beforeEach(() => {
  vi.useFakeTimers({ now: Date.UTC(2030, 0, 1) });
});

afterEach(async () => {
  const closings = startedLifecycles.splice(0).map((lifecycle) => lifecycle.close());
  await vi.advanceTimersByTimeAsync(CLOSE_BOUND_IN_MILLISECONDS);
  await Promise.all(closings);
  vi.useRealTimers();
});

describe("the boot", () => {
  test("reads one INFO, syncs the schedules, opens the blocking client and starts the loop, then writes worker started", async () => {
    const dailyReportTask = defineScheduledTask("daily-report", {
      schedule: { cron: "0 6 * * *", timezone: "UTC" },
      handler: async () => undefined,
    }) as AnyTaskDefinition;
    const { lifecycle, engine, lines, commandDriver, blockingDrivers } = lifecycleScenarioOf({
      tasks: [dailyReportTask],
    });

    await lifecycle.startWorker();
    await vi.advanceTimersByTimeAsync(DEFAULT_WORKER_TIMING.tickIntervalInMilliseconds);

    expect(commandDriver().sentCommands.slice(0, 3)).toEqual(["INFO", "serverInstant", "syncSchedules"]);
    expect(commandDriver().sentCommands).toEqual(expect.arrayContaining(["fetch", "tick"]));
    expect(engine.storedScheduleOf("daily-report")).toBeDefined();
    expect(blockingDrivers()).toHaveLength(1);
    expect(blockingDrivers()[0]?.sentCommands).toContain("BZPOPMIN");
    expect(messagesOf(lines)).toEqual(["schedule added", "worker started"]);
  });

  test("refuses a server below the floor: startWorker rejects with the floor and no worker starts", async () => {
    const { lifecycle, lines, engine, blockingDrivers } = lifecycleScenarioOf({
      prepareDriver: (fakeRedisDriver) => {
        readyAtTheFloor(fakeRedisDriver);
        fakeRedisDriver.infoReply = async () => infoReplyWith({ redisVersion: "7.2.4", maxmemoryPolicy: "noeviction" });
      },
    });

    await expect(lifecycle.startWorker()).rejects.toThrow(
      new Error("Redis 7.2.4 is below the floor: Valkey 8 or Redis 7.4"),
    );
    expect(blockingDrivers()).toEqual([]);
    expect(engine.calls).toEqual([]);
    expect(messagesOf(lines)).toEqual([]);
    await expect(lifecycleScenarioOf().lifecycle.startWorker()).resolves.toBeUndefined();
  });

  test("warns server version unreadable and starts when INFO has no version line", async () => {
    const { lifecycle, lines } = lifecycleScenarioOf({
      prepareDriver: (fakeRedisDriver) => {
        readyAtTheFloor(fakeRedisDriver);
        fakeRedisDriver.infoReply = async () => infoReplyWith({ maxmemoryPolicy: "noeviction" });
      },
    });

    await lifecycle.startWorker();

    expect(lines).toEqual([
      { level: "warn", message: "server version unreadable", fields: { err: expect.any(Error) } },
      { level: "info", message: "worker started", fields: {} },
    ]);
  });

  test("warns maxmemory-policy unreadable with both failures and starts when neither INFO nor CONFIG GET yields the policy", async () => {
    const configFailure = CONFIG_DISABLED();
    const { lifecycle, lines } = lifecycleScenarioOf({
      prepareDriver: (fakeRedisDriver) => {
        readyAtTheFloor(fakeRedisDriver);
        fakeRedisDriver.infoReply = async () => infoReplyWith({ valkeyVersion: "8.0.1" });
        fakeRedisDriver.configReply = () => Promise.reject(configFailure);
      },
    });

    await lifecycle.startWorker();

    expect(lines).toEqual([
      {
        level: "warn",
        message: "maxmemory-policy unreadable",
        fields: {
          err: new AggregateError(
            [new Error("INFO has no maxmemory_policy line"), configFailure],
            "maxmemory-policy is readable from neither INFO nor CONFIG GET",
          ),
        },
      },
      { level: "info", message: "worker started", fields: {} },
    ]);
  });

  test("starts without a warning on valkey_version 8", async () => {
    const { lifecycle, lines } = lifecycleScenarioOf();

    await lifecycle.startWorker();

    expect(lines).toEqual([{ level: "info", message: "worker started", fields: {} }]);
  });

  test("warns that NEXT_MANUAL_SIG_HANDLE is not set outside development, once started", async () => {
    const { lifecycle, lines } = lifecycleScenarioOf({ env: {} });

    await lifecycle.startWorker();

    expect(messagesOf(lines)).toEqual(["worker started", MISSING_MANUAL_SIGNAL_HANDLING_WARNING]);
  });

  test("refuses a second worker in the process", async () => {
    await lifecycleScenarioOf().lifecycle.startWorker();

    await expect(lifecycleScenarioOf().lifecycle.startWorker()).rejects.toThrow(
      "The nextasking worker is already started in this process",
    );
  });

  test("while Redis is down, resolves once giveUpAfter has passed and starts the worker once Redis answers", async () => {
    const { lifecycle, lines, commandDriver } = lifecycleScenarioOf({
      prepareDriver: (fakeRedisDriver) => {
        fakeRedisDriver.infoReply = async () => infoReplyWith(VALKEY_AT_THE_FLOOR);
      },
    });

    const started = settlementOf(lifecycle.startWorker());
    await vi.advanceTimersByTimeAsync(GIVE_UP_AFTER_IN_MILLISECONDS - 1);
    const isStartedBeforeGiveUpAfter = started.isSettled();
    await vi.advanceTimersByTimeAsync(1);
    const isStartedAtGiveUpAfter = started.isSettled();
    await vi.advanceTimersByTimeAsync(3 * GIVE_UP_AFTER_IN_MILLISECONDS);
    const linesWhileDown = messagesOf(lines);
    commandDriver().becomeReady();
    await vi.advanceTimersByTimeAsync(0);

    expect([isStartedBeforeGiveUpAfter, isStartedAtGiveUpAfter]).toEqual([false, true]);
    expect(linesWhileDown).toEqual([]);
    expect(commandDriver().sentCommands.filter((commandName) => commandName === "INFO")).toEqual(["INFO"]);
    expect(messagesOf(lines)).toEqual(["worker started"]);
  });

  test("a connection lost between the INFO and the schedule sync waits for Redis, resends, and starts the worker", async () => {
    const { lifecycle, lines, commandDriver } = lifecycleScenarioOf({
      prepareDriver: (fakeRedisDriver) => {
        fakeRedisDriver.infoReply = async () => {
          fakeRedisDriver.isAnswering = false;
          setTimeout(() => {
            fakeRedisDriver.loseConnection(connectionResetError());
          }, 0);
          return infoReplyWith(VALKEY_AT_THE_FLOOR);
        };
        fakeRedisDriver.becomeReady();
      },
    });

    const starting = lifecycle.startWorker();
    await vi.advanceTimersByTimeAsync(3 * GIVE_UP_AFTER_IN_MILLISECONDS);
    const linesWhileDown = messagesOf(lines);
    commandDriver().isAnswering = true;
    commandDriver().becomeReady();
    await vi.advanceTimersByTimeAsync(0);
    await starting;

    expect(linesWhileDown).toEqual(["connection error"]);
    expect(commandDriver().sentCommands.filter((commandName) => commandName === "serverInstant")).toEqual([
      "serverInstant",
      "serverInstant",
    ]);
    expect(messagesOf(lines)).toEqual(["connection error", "connection restored", "worker started"]);
  });

  test("a background boot that fails writes worker not started and calls onWorkerNotStarted with the error", async () => {
    const { lifecycle, lines, commandDriver, blockingDrivers } = scenarioOfABackgroundBootBelowTheFloor();
    const onWorkerNotStarted = vi.fn();

    await Promise.all([
      lifecycle.startWorker({ onWorkerNotStarted }),
      vi.advanceTimersByTimeAsync(GIVE_UP_AFTER_IN_MILLISECONDS),
    ]);
    commandDriver().becomeReady();
    await vi.advanceTimersByTimeAsync(0);

    const floorError = new Error("Redis 7.2.4 is below the floor: Valkey 8 or Redis 7.4");
    expect(lines).toEqual([{ level: "error", message: "worker not started", fields: { err: floorError } }]);
    expect(onWorkerNotStarted).toHaveBeenCalledWith(floorError);
    expect(blockingDrivers()).toEqual([]);
  });

  test.each([
    {
      failure: "throws",
      onWorkerNotStartedFailingWith: (callbackError: Error) => () => {
        throw callbackError;
      },
    },
    {
      failure: "rejects",
      onWorkerNotStartedFailingWith: (callbackError: Error) => () => Promise.reject(callbackError),
    },
  ])(
    "a background boot that fails, with an onWorkerNotStarted that $failure, writes worker not started then onWorkerNotStarted failed with what it threw, and rethrows nothing",
    async ({ onWorkerNotStartedFailingWith }) => {
      const callbackError = new Error("the app's logger could not flush");
      const { lifecycle, lines, commandDriver } = scenarioOfABackgroundBootBelowTheFloor();
      let startOutcome: unknown;

      const unhandledRejections = await unhandledRejectionsDuring(async () => {
        startOutcome = await Promise.all([
          lifecycle.startWorker({ onWorkerNotStarted: onWorkerNotStartedFailingWith(callbackError) }),
          vi.advanceTimersByTimeAsync(GIVE_UP_AFTER_IN_MILLISECONDS),
        ]).then(([started]) => started);
        commandDriver().becomeReady();
        await vi.advanceTimersByTimeAsync(0);
      });

      expect(lines).toEqual([
        {
          level: "error",
          message: "worker not started",
          fields: { err: new Error("Redis 7.2.4 is below the floor: Valkey 8 or Redis 7.4") },
        },
        { level: "error", message: "onWorkerNotStarted failed", fields: { err: callbackError } },
      ]);
      expect(lines[1]?.fields["err"]).toBe(callbackError);
      expect(startOutcome).toBeUndefined();
      expect(unhandledRejections).toEqual([]);
    },
  );

  test("a close during a boot waiting for Redis resolves, and no worker starts", async () => {
    const { lifecycle, lines, engine, commandDriver, blockingDrivers } = lifecycleScenarioOf({
      prepareDriver: (fakeRedisDriver) => {
        fakeRedisDriver.infoReply = async () => infoReplyWith(VALKEY_AT_THE_FLOOR);
      },
    });
    await Promise.all([lifecycle.startWorker(), vi.advanceTimersByTimeAsync(GIVE_UP_AFTER_IN_MILLISECONDS)]);

    await lifecycle.close();
    commandDriver().becomeReady();
    await vi.advanceTimersByTimeAsync(CLOSE_BOUND_IN_MILLISECONDS);

    expect(commandDriver().destroyCount).toBe(1);
    expect(blockingDrivers()).toEqual([]);
    expect(engine.calls).toEqual([]);
    expect(messagesOf(lines)).toEqual([]);
    expect(vi.getTimerCount()).toBe(0);
    await expect(lifecycleScenarioOf().lifecycle.startWorker()).resolves.toBeUndefined();
  });

  test("a close while startWorker still waits for Redis rejects it at once with InstanceClosedError", async () => {
    const { lifecycle, lines } = lifecycleScenarioOf({ prepareDriver: () => undefined });
    const starting = lifecycle.startWorker();
    const startOutcome = starting.catch((error: unknown) => error);
    await vi.advanceTimersByTimeAsync(100);

    await lifecycle.close();

    expect(await startOutcome).toBeInstanceOf(InstanceClosedError);
    expect(messagesOf(lines)).toEqual([]);
    expect(vi.getTimerCount()).toBe(0);
  });
});

describe("the exit on a shutdown signal", () => {
  const MANUAL_SIGNAL_HANDLING: NodeJS.ProcessEnv = { NODE_ENV: "production", NEXT_MANUAL_SIG_HANDLE: "1" };

  let processSignals: ProcessSignalSpies;

  beforeEach(() => {
    processSignals = spyOnProcessSignals();
  });

  afterEach(() => {
    processSignals.restore();
  });

  function onWorkerStoppedRecordingLinesOf(lines: readonly RecordedLogLine[]) {
    const messagesWhenCalled: string[][] = [];
    const onWorkerStopped = () => {
      messagesWhenCalled.push(messagesOf(lines));
    };
    return { onWorkerStopped, messagesWhenCalled };
  }

  test("calls onWorkerStopped once, after worker stopped, then exits with code 0", async () => {
    const { lifecycle, lines } = lifecycleScenarioOf({ env: MANUAL_SIGNAL_HANDLING });
    const { onWorkerStopped, messagesWhenCalled } = onWorkerStoppedRecordingLinesOf(lines);
    await lifecycle.startWorker({ onWorkerStopped });

    processSignals.sendSignal("SIGTERM");
    await vi.advanceTimersByTimeAsync(CLOSE_BOUND_IN_MILLISECONDS);

    expect(messagesWhenCalled).toEqual([["worker started", "worker stopped"]]);
    expect(processSignals.exitCodes()).toEqual([0]);
  });

  test("calls onWorkerStopped on a signal that arrives while the boot waits for Redis, then exits with code 0", async () => {
    const { lifecycle, lines } = lifecycleScenarioOf({
      env: MANUAL_SIGNAL_HANDLING,
      prepareDriver: (fakeRedisDriver) => {
        fakeRedisDriver.infoReply = async () => infoReplyWith(VALKEY_AT_THE_FLOOR);
      },
    });
    const { onWorkerStopped, messagesWhenCalled } = onWorkerStoppedRecordingLinesOf(lines);
    await Promise.all([
      lifecycle.startWorker({ onWorkerStopped }),
      vi.advanceTimersByTimeAsync(GIVE_UP_AFTER_IN_MILLISECONDS),
    ]);

    processSignals.sendSignal("SIGTERM");
    await vi.advanceTimersByTimeAsync(CLOSE_BOUND_IN_MILLISECONDS);

    expect(messagesWhenCalled).toEqual([[]]);
    expect(processSignals.exitCodes()).toEqual([0]);
  });

  test.each([
    { case: "without NEXT_MANUAL_SIG_HANDLE", env: { NODE_ENV: "production" }, prepareDriver: readyAtTheFloor },
    {
      case: "when startWorker rejects",
      env: MANUAL_SIGNAL_HANDLING,
      prepareDriver: (fakeRedisDriver: FakeRedisDriver) => {
        readyAtTheFloor(fakeRedisDriver);
        fakeRedisDriver.infoReply = async () => infoReplyWith({ redisVersion: "7.2.4", maxmemoryPolicy: "noeviction" });
      },
    },
  ])("listens to no signal $case, so onWorkerStopped is never called", async ({ env, prepareDriver }) => {
    const { lifecycle } = lifecycleScenarioOf({ env, prepareDriver });
    const onWorkerStopped = vi.fn();

    await lifecycle.startWorker({ onWorkerStopped }).catch(() => undefined);
    await lifecycle.close();

    expect(processSignals.listenerCalls()).toEqual([]);
    expect(onWorkerStopped).not.toHaveBeenCalled();
    expect(processSignals.exitCodes()).toEqual([]);
  });

  test("a close the app makes removes the listeners, calls no onWorkerStopped and exits nothing", async () => {
    const { lifecycle } = lifecycleScenarioOf({ env: MANUAL_SIGNAL_HANDLING });
    const onWorkerStopped = vi.fn();
    await lifecycle.startWorker({ onWorkerStopped });

    const closing = lifecycle.close();
    await vi.advanceTimersByTimeAsync(CLOSE_BOUND_IN_MILLISECONDS);
    await closing;

    expect(processSignals.attachedListenersOf("SIGTERM")).toEqual([]);
    expect(processSignals.attachedListenersOf("SIGINT")).toEqual([]);
    expect(onWorkerStopped).not.toHaveBeenCalled();
    expect(processSignals.exitCodes()).toEqual([]);
  });
});

describe("the closed instance", () => {
  test("rejects every later call with InstanceClosedError without opening a client, and a second close returns the first one's promise", async () => {
    const { lifecycle, fakeRedisDrivers } = lifecycleScenarioOf();

    const firstClose = lifecycle.close();
    await firstClose;

    expect(lifecycle.close()).toBe(firstClose);
    await expect(lifecycle.callWhileOpen(async () => "answered")).rejects.toBeInstanceOf(InstanceClosedError);
    await expect(lifecycle.startWorker()).rejects.toBeInstanceOf(InstanceClosedError);
    expect(fakeRedisDrivers.createdDrivers()).toEqual([]);
  });

  test("a call held while the connection is not ready rejects at once when close is issued, and no timer is left", async () => {
    const { lifecycle } = lifecycleScenarioOf({ prepareDriver: () => undefined });
    const callOutcome = lifecycle
      .callWhileOpen(({ engine: openedEngine }) => openedEngine.enqueue({ runs: runsOf("send-email", 1) }))
      .catch((error: unknown) => error);
    await vi.advanceTimersByTimeAsync(100);
    const timerCountWhileHeld = vi.getTimerCount();

    const closing = lifecycle.close();
    const outcomeAtTheClose = await callOutcome;
    await closing;

    expect(timerCountWhileHeld).toBe(1);
    expect(outcomeAtTheClose).toBeInstanceOf(InstanceClosedError);
    expect(vi.getTimerCount()).toBe(0);
  });

  test("a call already sent gets its answer when it arrives before the close closes the command client", async () => {
    const { lifecycle, engine, commandDriver } = lifecycleScenarioOf({
      prepareDriver: (fakeRedisDriver) => {
        readyAtTheFloor(fakeRedisDriver);
        fakeRedisDriver.closeReply = "never answered";
      },
    });
    const heldEnqueues = engine.holdCalls("enqueue");
    const enqueueReply = lifecycle.callWhileOpen(({ engine: openedEngine }) =>
      openedEngine.enqueue({ runs: runsOf("send-email", 1) }),
    );
    await vi.advanceTimersByTimeAsync(0);

    const closing = lifecycle.close();
    await vi.advanceTimersByTimeAsync(100);
    heldEnqueues.release();
    await vi.advanceTimersByTimeAsync(0);
    const isDriverClosedAtTheAnswer = commandDriver().isClosed;
    await vi.advanceTimersByTimeAsync(DEFAULT_WORKER_TIMING.commandConnectionCloseBoundInMilliseconds);
    await closing;

    expect(isDriverClosedAtTheAnswer).toBe(false);
    await expect(enqueueReply).resolves.toMatchObject({
      kind: "enqueued",
      createdRuns: [{ task: "send-email", runId: "send-email-1" }],
    });
  });

  test("a call already sent and unanswered rejects with InstanceClosedError once the close closes the command connection", async () => {
    const { lifecycle, engine } = lifecycleScenarioOf();
    engine.holdCalls("enqueue");
    const callOutcome = lifecycle
      .callWhileOpen(({ engine: openedEngine }) => openedEngine.enqueue({ runs: runsOf("send-email", 1) }))
      .catch((error: unknown) => error);
    await vi.advanceTimersByTimeAsync(0);

    const closing = lifecycle.close();
    await vi.advanceTimersByTimeAsync(DEFAULT_WORKER_TIMING.commandConnectionCloseBoundInMilliseconds);
    await closing;

    expect(await callOutcome).toBeInstanceOf(InstanceClosedError);
  });
});

describe("the close of a running worker", () => {
  test("resolves at the step boundary where the attempt yields, well before the deadline, with both clients closed", async () => {
    const { lifecycle, engine, lines, handlerGate, commandDriver, blockingDrivers } =
      await scenarioOfARunningWorker(gatedBetweenTwoSteps);
    const closeIssuedAt = Date.now();

    const closed = settlementOf(lifecycle.close());
    await vi.advanceTimersByTimeAsync(100);
    const isBlockingDriverClosedAtOnce = blockingDrivers()[0]?.isClosed;
    handlerGate.release(1);
    await vi.advanceTimersByTimeAsync(0);

    expect(isBlockingDriverClosedAtOnce).toBe(true);
    expect(closed.isSettled()).toBe(true);
    expect((closed.settledAt() ?? Infinity) - closeIssuedAt).toBeLessThan(SHUTDOWN_TIMEOUT_IN_MILLISECONDS);
    expect(settleOutcomeKindsOf(engine)).toEqual(["yield"]);
    expect(engine.storedRunOf("send-email", "send-email-1")?.state).toBe("pending");
    expect(messagesOf(lines).slice(-2)).toEqual(["run yielded at shutdown", "worker stopped"]);
    expect(commandDriver().closeCount).toBe(1);
    expect(commandDriver().destroyCount).toBe(1);
  });

  test("completes the run of a handler that ignores the deadline abort and finishes inside the grace", async () => {
    const { lifecycle, engine, lines, handlerGate } = await scenarioOfARunningWorker(gatedWithNoStep);

    const closing = lifecycle.close();
    await vi.advanceTimersByTimeAsync(SHUTDOWN_TIMEOUT_IN_MILLISECONDS + 200);
    handlerGate.release(1);
    await vi.advanceTimersByTimeAsync(CLOSE_BOUND_IN_MILLISECONDS);
    await closing;

    expect(engine.storedRunOf("send-email", "send-email-1")?.state).toBe("completed");
    expect(messagesOf(lines)).toEqual([
      "worker started",
      "attempt started",
      "attempts aborted at shutdown deadline",
      "run completed",
      "worker stopped",
    ]);
  });

  test("aborts the attempts still running at the deadline and resolves within the deadline plus 2 s, and a handler ending afterwards sends nothing", async () => {
    const { lifecycle, engine, lines, handlerGate, commandDriver, blockingDrivers } =
      await scenarioOfARunningWorker(gatedWithNoStep);
    const closeIssuedAt = Date.now();

    const closed = settlementOf(lifecycle.close());
    await vi.advanceTimersByTimeAsync(SHUTDOWN_TIMEOUT_IN_MILLISECONDS - 1);
    const isClosedBeforeTheDeadline = closed.isSettled();
    await vi.advanceTimersByTimeAsync(CLOSE_BOUND_IN_MILLISECONDS);
    const callCountAtTheClose = engine.calls.length;
    const linesAtTheClose = messagesOf(lines);
    const unhandledRejections = await unhandledRejectionsDuring(async () => {
      handlerGate.release(1);
      await vi.advanceTimersByTimeAsync(10_000);
    });

    expect(isClosedBeforeTheDeadline).toBe(false);
    expect((closed.settledAt() ?? Infinity) - closeIssuedAt).toBeGreaterThanOrEqual(SHUTDOWN_TIMEOUT_IN_MILLISECONDS);
    expect((closed.settledAt() ?? Infinity) - closeIssuedAt).toBeLessThanOrEqual(CLOSE_BOUND_IN_MILLISECONDS);
    expect(linesAtTheClose).toEqual([
      "worker started",
      "attempt started",
      "attempts aborted at shutdown deadline",
      "worker stopped",
    ]);
    expect([commandDriver().destroyCount, blockingDrivers()[0]?.destroyCount]).toEqual([1, 1]);
    expect(engine.calls.length).toBe(callCountAtTheClose);
    expect(messagesOf(lines)).toEqual(linesAtTheClose);
    expect(unhandledRejections).toEqual([]);
  });

  test.each([
    {
      driverState: "reconnecting",
      breakDriver: (fakeRedisDriver: FakeRedisDriver) => {
        fakeRedisDriver.startReconnecting();
      },
    },
    {
      driverState: "ready, whose close is never answered",
      breakDriver: (fakeRedisDriver: FakeRedisDriver) => {
        fakeRedisDriver.closeReply = "never answered";
      },
    },
  ])("resolves within the bound while both drivers are $driverState, leaving no timer", async ({ breakDriver }) => {
    const { lifecycle, commandDriver, blockingDrivers } = await scenarioOfARunningWorker(gatedWithNoStep);
    breakDriver(commandDriver());
    for (const blockingDriver of blockingDrivers()) {
      breakDriver(blockingDriver);
    }
    await vi.advanceTimersByTimeAsync(3_000);
    const closeIssuedAt = Date.now();

    const closed = settlementOf(lifecycle.close());
    await vi.advanceTimersByTimeAsync(CLOSE_BOUND_IN_MILLISECONDS);

    expect(closed.isSettled()).toBe(true);
    expect((closed.settledAt() ?? Infinity) - closeIssuedAt).toBeLessThanOrEqual(CLOSE_BOUND_IN_MILLISECONDS);
    expect(vi.getTimerCount()).toBe(0);
  });

  test("during an outage with an attempt waiting on its settle, resolves within the bound with no timer or worker error left", async () => {
    const { lifecycle, lines, handlerGate, commandDriver } = await scenarioOfARunningWorker(gatedWithNoStep, {
      engineOn: (engine, client, driverOfTheCommandConnection) => {
        const engineOnTheClient = engineOnFakeDriver(engine, client);
        return {
          ...engineOnTheClient,
          settle: (request) =>
            driverOfTheCommandConnection().isReady
              ? engineOnTheClient.settle(request)
              : Promise.reject(new SocketClosedUnexpectedlyError()),
        };
      },
    });
    commandDriver().startReconnecting();
    handlerGate.release(1);
    await vi.advanceTimersByTimeAsync(3 * GIVE_UP_AFTER_IN_MILLISECONDS);
    const closeIssuedAt = Date.now();

    const closed = settlementOf(lifecycle.close());
    await vi.advanceTimersByTimeAsync(CLOSE_BOUND_IN_MILLISECONDS);

    expect((closed.settledAt() ?? Infinity) - closeIssuedAt).toBeLessThanOrEqual(CLOSE_BOUND_IN_MILLISECONDS);
    expect(vi.getTimerCount()).toBe(0);
    expect(messagesOf(lines)).not.toContain("worker error");
  });

  test("a fetch answered after the close began has its runs started and drained, none left executing", async () => {
    const engine = createInMemoryEngine();
    await engine.enqueue({ runs: runsOf("send-email", 3) });
    const heldFetches = engine.holdCalls("fetch");
    const sendEmailTask = defineTask("send-email", { schema: v.object({}), handler: async () => undefined });
    const { lifecycle } = lifecycleScenarioOf({ tasks: [sendEmailTask] as AnyTaskDefinition[], engine });
    await lifecycle.startWorker({ shutdownTimeout: "5s" });
    await vi.advanceTimersByTimeAsync(0);

    const closed = settlementOf(lifecycle.close());
    await vi.advanceTimersByTimeAsync(100);
    heldFetches.release();
    await vi.advanceTimersByTimeAsync(0);

    expect(closed.isSettled()).toBe(true);
    expect(engine.runIdsInState("completed")).toEqual(["send-email-1", "send-email-2", "send-email-3"]);
    expect(engine.runIdsInState("executing")).toEqual([]);
  });

  test("answers a handler's send already in flight during the drain, while an app call through callWhileOpen rejects", async () => {
    const sendGate = createHandlerGate();
    const engine = createInMemoryEngine();
    const { lifecycle, handlerGate } = await scenarioOfARunningWorker(
      (gate) =>
        async ({ dispatch }) => {
          await gate.enter();
          await (dispatch as unknown as (name: string) => Promise<DispatchedRun>)("send-receipt");
        },
      {
        engine,
        senderOn: (openedEngine) => ({
          ...NO_SENDER,
          dispatch: async (_fence, name, _payload, { runId = "receipt" }) => {
            await sendGate.enter();
            await openedEngine.enqueue({ runs: [{ runId, task: name, payload: "{}", retries: 0 }] });
            return { runId };
          },
        }),
      },
    );
    handlerGate.release(1);
    await vi.advanceTimersByTimeAsync(0);

    const closing = lifecycle.close();
    const appCallOutcome = await lifecycle
      .callWhileOpen(({ engine: openedEngine }) => openedEngine.enqueue({ runs: runsOf("send-email", 1) }))
      .catch((error: unknown) => error);
    sendGate.release(1);
    await vi.advanceTimersByTimeAsync(0);
    await closing;

    expect(appCallOutcome).toBeInstanceOf(InstanceClosedError);
    expect(engine.runIdsInState("pending")).toHaveLength(1);
    expect(engine.storedRunOf("send-receipt", engine.runIdsInState("pending")[0] ?? "")?.state).toBe("pending");
    expect(settleOutcomeKindsOf(engine)).toEqual(["complete"]);
  });

  test("a close with commands queued or in flight leaves no rejection unhandled", async () => {
    const unhandledRejections = await unhandledRejectionsDuring(async () => {
      const booting = lifecycleScenarioOf({
        prepareDriver: (fakeRedisDriver) => {
          fakeRedisDriver.infoReply = async () => infoReplyWith(VALKEY_AT_THE_FLOOR);
        },
      });
      await Promise.all([booting.lifecycle.startWorker(), vi.advanceTimersByTimeAsync(GIVE_UP_AFTER_IN_MILLISECONDS)]);
      const queuedCallOutcome = booting.lifecycle
        .callWhileOpen(({ engine: openedEngine }) => openedEngine.enqueue({ runs: runsOf("send-email", 1) }))
        .catch(() => undefined);
      await booting.lifecycle.close();
      await queuedCallOutcome;

      const engine = createInMemoryEngine();
      await engine.enqueue({ runs: runsOf("send-email", 1) });
      const sendEmailTask = defineTask("send-email", { schema: v.object({}), handler: async () => undefined });
      const running = lifecycleScenarioOf({
        tasks: [sendEmailTask] as AnyTaskDefinition[],
        engine,
        prepareDriver: (fakeRedisDriver) => {
          readyAtTheFloor(fakeRedisDriver);
        },
      });
      await running.lifecycle.startWorker({ shutdownTimeout: "5s" });
      engine.holdCalls("fetch");
      engine.holdCalls("enqueue");
      await vi.advanceTimersByTimeAsync(DEFAULT_WORKER_TIMING.tickIntervalInMilliseconds);
      const inFlightCallOutcome = running.lifecycle
        .callWhileOpen(({ engine: openedEngine }) => openedEngine.enqueue({ runs: runsOf("send-email", 1) }))
        .catch(() => undefined);
      await vi.advanceTimersByTimeAsync(0);
      const closing = running.lifecycle.close();
      await vi.advanceTimersByTimeAsync(CLOSE_BOUND_IN_MILLISECONDS);
      await closing;
      await inFlightCallOutcome;
    });

    expect(unhandledRejections).toEqual([]);
  });
});
