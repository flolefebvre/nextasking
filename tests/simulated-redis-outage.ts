import { ReplyError } from "ioredis";

import { createConnectionOutageTracker, createRedisOutage } from "../src/connection.js";
import type { Logger } from "../src/logger.js";
import { untilAborted } from "../src/redis-outage.js";
import type { RedisOutage } from "../src/redis-outage.js";
import type { WorkerLoopEngine } from "../src/worker-loop.js";

export const UNANSWERED_COMMAND_ERROR = new Error("Connection is closed.");

export const OUT_OF_MEMORY_REPLY = new ReplyError("OOM command not allowed when used memory > 'maxmemory'.");

export type SimulatedRedisOutage = {
  redisOutage: RedisOutage;
  isRedisAway: () => boolean;
  unansweredCallsRejected: () => Promise<void>;
  startOutage: () => void;
  endOutage: () => void;
  rejectUnansweredCalls: () => void;
};

export function createSimulatedRedisOutage(logger: Logger): SimulatedRedisOutage {
  const outageTracker = createConnectionOutageTracker(logger);
  const simulatedClient = {};
  let redisBack: Promise<void> = Promise.resolve();
  let markRedisBack: (() => void) | undefined;
  let markUnansweredCallsRejected = () => undefined as void;
  const unansweredCallsRejected = new Promise<void>((resolve) => {
    markUnansweredCallsRejected = resolve;
  });
  return {
    redisOutage: createRedisOutage({
      outageTracker,
      waitUntilReady: (stopSignal) => untilAborted(redisBack, stopSignal),
      outOfMemoryRetryDelayInMilliseconds: 1000,
    }),
    isRedisAway: () => markRedisBack !== undefined,
    unansweredCallsRejected: () => Promise.race([redisBack, unansweredCallsRejected]),
    startOutage: () => {
      redisBack = new Promise<void>((resolve) => {
        markRedisBack = resolve;
      });
      outageTracker.reportClientError(simulatedClient, UNANSWERED_COMMAND_ERROR);
    },
    endOutage: () => {
      markRedisBack?.();
      markRedisBack = undefined;
      outageTracker.reportClientReady(simulatedClient);
    },
    rejectUnansweredCalls: () => {
      markUnansweredCallsRejected();
    },
  };
}

export type AwayEngineCalls = "rejected" | "pending until the outage ends";

export function engineAwayDuringOutage(
  engine: WorkerLoopEngine,
  simulatedRedisOutage: SimulatedRedisOutage,
  awayEngineCalls: AwayEngineCalls,
): WorkerLoopEngine {
  const awayDuringOutage =
    <Request, Reply>(call: (request: Request) => Promise<Reply>) =>
    (request: Request): Promise<Reply> => {
      if (!simulatedRedisOutage.isRedisAway()) {
        return call(request);
      }
      if (awayEngineCalls === "rejected") {
        return Promise.reject(UNANSWERED_COMMAND_ERROR);
      }
      return simulatedRedisOutage.unansweredCallsRejected().then(() => Promise.reject(UNANSWERED_COMMAND_ERROR));
    };
  return {
    fetch: awayDuringOutage(engine.fetch),
    settle: awayDuringOutage(engine.settle),
    tick: awayDuringOutage(engine.tick),
    saveStep: awayDuringOutage(engine.saveStep),
    readStep: awayDuringOutage(engine.readStep),
    fire: awayDuringOutage(engine.fire),
  };
}
