import { isBuiltInEventName } from "./built-in-events.js";
import { concurrencyKeyOf, retriesOf, validatePayload } from "./definitions.js";
import type { AnyTaskDefinition, EventDefinition } from "./definitions.js";
import { parseDuration } from "./durations.js";
import type { Duration, DurationInMilliseconds } from "./durations.js";
import { FanOutNotReplayableError, RunNotReplayableError, UnknownEventError, UnknownTaskError } from "./errors.js";
import type { DefinitionKind, RedisCallIds } from "./errors.js";
import type { SendBuiltInEvent } from "./failure-notices.js";
import { createRandomId } from "./ids.js";
import type { EventId, RunId } from "./ids.js";
import type { Logger } from "./logger.js";
import type { EnqueueOutcome, RunQueue } from "./run-queue.js";

export type DispatchOptions = {
  runId?: RunId;
  delay?: Duration;
};

export type EmitOptions = {
  eventId?: EventId;
};

export type DispatchedRun = {
  runId: RunId;
};

export type EmittedEvent = {
  eventId: EventId;
};

export type ReplayedRun = {
  runId: RunId;
};

export type ReplayedEvent = {
  eventId: EventId;
};

export type DispatchByName = (name: string, payload?: unknown, options?: DispatchOptions) => Promise<DispatchedRun>;

export type EmitByName = (name: string, payload: unknown, options?: EmitOptions) => Promise<EmittedEvent>;

export type RunQueueAccess = {
  openRunQueue: () => Promise<RunQueue>;
  callOnRedis: <Result>(callIds: RedisCallIds, call: () => Promise<Result>) => Promise<Result>;
};

export type EnqueuerConfig = {
  tasksByName: ReadonlyMap<string, AnyTaskDefinition>;
  eventsByName: ReadonlyMap<string, EventDefinition>;
  listenerNamesByEventName: ReadonlyMap<string, readonly string[]>;
  logger: Logger;
  runQueueAccess: RunQueueAccess;
};

export type Enqueuer = {
  dispatch: DispatchByName;
  emit: EmitByName;
  replay: (name: string, runId: RunId) => Promise<ReplayedRun>;
  replayEvent: (name: string, eventId: EventId) => Promise<ReplayedEvent>;
  sendBuiltInEvent: SendBuiltInEvent;
};

export const RESERVED_NAME_PREFIX = "nextasking/";

export function createEnqueuer({
  tasksByName,
  eventsByName,
  listenerNamesByEventName,
  logger,
  runQueueAccess: { openRunQueue, callOnRedis },
}: EnqueuerConfig): Enqueuer {
  const appEventNames = [...eventsByName.keys()].filter((eventName) => !isBuiltInEventName(eventName));

  const enqueueFanOutToListeners = async (
    eventName: string,
    eventId: EventId,
    validatedPayload: unknown,
  ): Promise<EnqueueOutcome | "no listeners"> => {
    const listenerNames = listenerNamesByEventName.get(eventName) ?? [];
    if (listenerNames.length === 0) {
      return "no listeners";
    }
    return callOnRedis({ eventId }, async () =>
      (await openRunQueue()).enqueueFanOut(eventName, { eventId, payload: validatedPayload, listenerNames }),
    );
  };

  return {
    dispatch: async (name, payload, options = {}) => {
      const task = tasksByName.get(name);
      if (task === undefined) {
        throw new UnknownTaskError(name, [...tasksByName.keys()]);
      }
      const runId = consumerFacingIdOrRandomId("task", task.name, "runId", options.runId);
      const delayInMilliseconds = delayInMillisecondsOf(task.name, options.delay);
      if (task.schedule !== undefined && payload !== undefined) {
        throw new Error(`Scheduled task "${task.name}" takes no payload: dispatch it by its name alone to run it now`);
      }
      const validatedPayload = task.schedule === undefined ? await validatePayload("task", task, payload) : undefined;
      const concurrencyKey = concurrencyKeyOf(task, validatedPayload);
      const { isDuplicate } = await callOnRedis({ runId }, async () =>
        (await openRunQueue()).enqueueRun(task.name, {
          runId,
          payload: validatedPayload,
          retries: retriesOf(task),
          ...(concurrencyKey === undefined ? {} : { concurrencyKey }),
          ...(delayInMilliseconds === undefined ? {} : { delayInMilliseconds }),
        }),
      );
      logger.debug({ task: task.name, runId }, isDuplicate ? "duplicate dispatch dropped" : "run dispatched");
      return { runId };
    },

    emit: async (name, payload, options = {}) => {
      if (isBuiltInEventName(name)) {
        throw new Error(
          `Event "${name}" is built in: names starting with "${RESERVED_NAME_PREFIX}" belong to the built-in events, which only the library emits`,
        );
      }
      const event = eventsByName.get(name);
      if (event === undefined) {
        throw new UnknownEventError(name, appEventNames);
      }
      const eventId = consumerFacingIdOrRandomId("event", event.name, "eventId", options.eventId);
      const validatedPayload = await validatePayload("event", event, payload);
      const fanOutEnqueueing = await enqueueFanOutToListeners(event.name, eventId, validatedPayload);
      if (fanOutEnqueueing === "no listeners") {
        logger.warn({ eventName: event.name, eventId }, "no listeners for event");
      } else if (fanOutEnqueueing.isDuplicate) {
        logger.debug({ eventName: event.name, eventId }, "duplicate dispatch dropped");
      }
      return { eventId };
    },

    replay: async (name, runId) => {
      if (!tasksByName.has(name)) {
        throw new UnknownTaskError(name, [...tasksByName.keys()]);
      }
      const replayOutcome = await callOnRedis({ runId }, async () => (await openRunQueue()).replayRun(name, runId));
      if (replayOutcome !== "replayed") {
        throw new RunNotReplayableError(name, runId, replayOutcome);
      }
      logger.info({ task: name, runId }, "run replayed");
      return { runId };
    },

    replayEvent: async (name, eventId) => {
      if (!eventsByName.has(name)) {
        throw new UnknownEventError(name, [...eventsByName.keys()]);
      }
      const replayOutcome = await callOnRedis({ eventId }, async () =>
        (await openRunQueue()).replayFanOut(name, eventId),
      );
      if (replayOutcome !== "replayed") {
        throw new FanOutNotReplayableError(name, eventId, replayOutcome);
      }
      return { eventId };
    },

    sendBuiltInEvent: async (eventName, eventId, payload) => {
      await enqueueFanOutToListeners(eventName, eventId, payload);
    },
  };
}

function consumerFacingIdOrRandomId(
  definitionKind: DefinitionKind,
  definitionName: string,
  idOption: "runId" | "eventId",
  consumerFacingId: unknown,
): string {
  if (consumerFacingId === undefined) {
    return createRandomId();
  }
  if (typeof consumerFacingId !== "string" || consumerFacingId === "") {
    const shownId = JSON.stringify(consumerFacingId) ?? String(consumerFacingId);
    throw new Error(
      `Invalid ${idOption} for ${definitionKind} "${definitionName}": ${shownId} is not a non-empty string`,
    );
  }
  return consumerFacingId;
}

function delayInMillisecondsOf(taskName: string, delay: Duration | undefined): DurationInMilliseconds | undefined {
  if (delay === undefined) {
    return undefined;
  }
  try {
    return parseDuration(delay);
  } catch (error) {
    throw new Error(`Invalid delay for task "${taskName}": ${(error as Error).message}`, { cause: error });
  }
}
