import { ErrorReply, SocketClosedUnexpectedlyError } from "@redis/client";

import { createConnectionOutageTracker, createRedisOutage } from "../src/connection.js";
import type { Logger } from "../src/logger.js";
import { isOutOfMemoryReply } from "../src/node-redis-driver.js";
import { untilAborted } from "../src/redis-outage.js";
import type { RedisOutage } from "../src/redis-outage.js";
import { createRefusedWriteStretch } from "../src/refused-write-stretch.js";
import { failedCommandErrorsOf } from "../src/worker-loop.js";
import type { WorkerLoopEngine } from "../src/worker-loop.js";

export const UNANSWERED_COMMAND_ERROR = new SocketClosedUnexpectedlyError();

export const OUT_OF_MEMORY_REPLY = new ErrorReply("OOM command not allowed when used memory > 'maxmemory'.");

export type SimulatedRedisOutage = {
  redisOutage: RedisOutage;
  engineOnWorkerCalls: (engine: WorkerLoopEngine) => WorkerLoopEngine;
  isRedisAway: () => boolean;
  unansweredCallsRejected: () => Promise<void>;
  startOutage: () => void;
  endOutage: () => void;
  rejectUnansweredCalls: () => void;
};

function noRefusalIn(): undefined {
  return undefined;
}

export function createSimulatedRedisOutage(logger: Logger): SimulatedRedisOutage {
  const outageTracker = createConnectionOutageTracker(logger);
  const refusedWriteStretch = createRefusedWriteStretch({ outageTracker, giveUpAfterInMilliseconds: 1000 });
  const simulatedClient = {};
  let redisBack: Promise<void> = Promise.resolve();
  let markRedisBack: (() => void) | undefined;
  let markUnansweredCallsRejected = () => undefined as void;
  const unansweredCallsRejected = new Promise<void>((resolve) => {
    markUnansweredCallsRejected = resolve;
  });
  const sentAsAWorkerCall =
    <Request, Reply>(
      methodName: keyof WorkerLoopEngine,
      call: (request: Request) => Promise<Reply>,
      outOfMemoryReplyIn: (reply: Reply) => Error | undefined = noRefusalIn,
    ) =>
    async (request: Request): Promise<Reply> => {
      let reply: Reply;
      try {
        reply = await call(request);
      } catch (error) {
        if (isOutOfMemoryReply(error)) {
          refusedWriteStretch.noteAnsweredOutOfMemory([methodName], error as Error);
        }
        throw error;
      }
      const outOfMemoryReply = outOfMemoryReplyIn(reply);
      if (outOfMemoryReply === undefined) {
        refusedWriteStretch.noteAccepted([methodName]);
      } else {
        refusedWriteStretch.noteAnsweredOutOfMemory([methodName], outOfMemoryReply);
      }
      return reply;
    };
  return {
    redisOutage: createRedisOutage({
      waitUntilReady: (stopSignal) => untilAborted(redisBack, stopSignal),
      outOfMemoryRetryDelayInMilliseconds: 1000,
    }),
    engineOnWorkerCalls: (engine) => ({
      fetch: sentAsAWorkerCall("fetch", engine.fetch),
      settle: sentAsAWorkerCall("settle", engine.settle),
      tick: sentAsAWorkerCall("tick", engine.tick, (reply) => failedCommandErrorsOf(reply).find(isOutOfMemoryReply)),
      saveStep: sentAsAWorkerCall("saveStep", engine.saveStep),
      readStep: sentAsAWorkerCall("readStep", engine.readStep),
      fire: sentAsAWorkerCall("fire", engine.fire),
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
