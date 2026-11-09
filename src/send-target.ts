import type { AnyTaskDefinition } from "./definitions.js";
import type { Duration } from "./durations.js";
import type { DispatchOrEmitRequest, Engine, EnqueueReply, ReplayReply, ReplayRequest } from "./engine/engine.js";
import { sendFenced } from "./engine-step-store.js";
import type { DispatchByName, EmitByName, EnqueueRequests, ReplayedRun } from "./enqueue-requests.js";
import { RunNotReplayableError, UnknownTaskError } from "./errors.js";
import type { RedisCallIds, ReplayRefusal } from "./errors.js";
import type { RunId } from "./ids.js";
import { enqueueLinesOf, writeLifecycleLines } from "./lifecycle-logs.js";
import type { Logger } from "./logger.js";
import type { HandlerSender } from "./step-engine.js";

export type SendTarget = {
  enqueue: (request: DispatchOrEmitRequest, delay: Duration | undefined) => Promise<EnqueueReply>;
  replay: (request: ReplayRequest) => Promise<ReplayReply>;
};

export type CallOnEngine = <Result>(
  callIds: RedisCallIds,
  call: (engine: Pick<Engine, "enqueue" | "replay">) => Promise<Result>,
) => Promise<Result>;

export type AppSenders = {
  dispatch: DispatchByName;
  emit: EmitByName;
  replay: (name: string, runId: RunId) => Promise<ReplayedRun>;
};

export type AppSendersConfig = {
  requests: EnqueueRequests;
  tasksByName: ReadonlyMap<string, AnyTaskDefinition>;
  sendTarget: () => SendTarget;
  logger: Logger;
};

function callIdsOf({ runs, eventDedup }: DispatchOrEmitRequest): RedisCallIds {
  if (eventDedup !== undefined) {
    return { eventId: eventDedup.eventId };
  }
  const runId = runs[0]?.runId;
  return runId === undefined ? {} : { runId };
}

export function engineSendTargetOf(callOnEngine: CallOnEngine): SendTarget {
  return {
    enqueue: (request) => callOnEngine(callIdsOf(request), (engine) => engine.enqueue(request)),
    replay: (request) => callOnEngine({ runId: request.runId }, (engine) => engine.replay(request)),
  };
}

const REPLAY_REFUSAL_BY_REPLY_KIND: Readonly<Record<Exclude<ReplayReply["kind"], "replayed">, ReplayRefusal>> = {
  notFound: "not found",
  wrongState: "not failed",
  unknownVersion: "written by a newer release",
};

export function appSendersOn({ requests, tasksByName, sendTarget, logger }: AppSendersConfig): AppSenders {
  return {
    dispatch: async (name, payload, options) => {
      const { request, runId, delay } = await requests.dispatchRequestOf(name, payload, options);
      const reply = await sendTarget().enqueue(request, delay);
      writeLifecycleLines(logger, enqueueLinesOf(request, reply));
      return { runId };
    },

    emit: async (name, payload, options) => {
      const { request, eventId } = await requests.emitRequestOf(name, payload, options);
      const reply = request.runs.length === 0 ? undefined : await sendTarget().enqueue(request, undefined);
      writeLifecycleLines(logger, enqueueLinesOf(request, reply));
      return { eventId };
    },

    replay: async (name, runId) => {
      if (!tasksByName.has(name)) {
        throw new UnknownTaskError(name, [...tasksByName.keys()]);
      }
      if (runId === "") {
        throw new RunNotReplayableError(name, runId, "not found");
      }
      const reply = await sendTarget().replay({ task: name, runId });
      if (reply.kind !== "replayed") {
        throw new RunNotReplayableError(name, runId, REPLAY_REFUSAL_BY_REPLY_KIND[reply.kind]);
      }
      logger.info({ task: name, runId }, "run replayed");
      return { runId };
    },
  };
}

export function handlerSenderOn(
  engine: Pick<Engine, "enqueue">,
  requests: EnqueueRequests,
  logger: Logger,
): HandlerSender {
  const writeLinesOf = (request: DispatchOrEmitRequest, reply: EnqueueReply) => {
    if (reply.kind !== "alreadySent") {
      writeLifecycleLines(logger, enqueueLinesOf(request, reply));
    }
  };
  return {
    dispatch: async (fence, name, payload, options) => {
      const { request, runId } = await requests.dispatchRequestOf(name, payload, options);
      const { sentId, reply } = await sendFenced(engine, request, fence, runId);
      writeLinesOf(request, reply);
      return { runId: sentId };
    },
    emit: async (fence, name, payload, options) => {
      const { request, eventId } = await requests.emitRequestOf(name, payload, options);
      const { sentId, reply } = await sendFenced(engine, request, fence, eventId);
      writeLinesOf(request, reply);
      return { eventId: sentId };
    },
  };
}
