import { isBuiltInEventName } from "./built-in-events.js";
import { concurrencyKeyOf, retriesOf, validatePayload } from "./definitions.js";
import type { AnyTaskDefinition, EventDefinition } from "./definitions.js";
import { parseDuration } from "./durations.js";
import type { Duration, DurationInMilliseconds } from "./durations.js";
import { serializePayload } from "./engine/engine.js";
import type { DispatchOrEmitRequest, RunToCreate } from "./engine/engine.js";
import { UnknownEventError, UnknownTaskError } from "./errors.js";
import type { DefinitionKind } from "./errors.js";
import { createRandomId, deriveListenerRunId } from "./ids.js";
import type { EventId, RunId } from "./ids.js";

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

export type DispatchByName = (name: string, payload?: unknown, options?: DispatchOptions) => Promise<DispatchedRun>;

export type EmitByName = (name: string, payload: unknown, options?: EmitOptions) => Promise<EmittedEvent>;

export type InstanceDefinitions = {
  tasksByName: ReadonlyMap<string, AnyTaskDefinition>;
  eventsByName: ReadonlyMap<string, EventDefinition>;
  listenerNamesByEventName: ReadonlyMap<string, readonly string[]>;
};

export type DispatchRequest = { request: DispatchOrEmitRequest; runId: RunId; delay?: Duration };

export type EmitRequest = { request: DispatchOrEmitRequest; eventId: EventId };

export type EnqueueRequests = {
  dispatchRequestOf: (name: string, payload: unknown, options?: DispatchOptions) => Promise<DispatchRequest>;
  emitRequestOf: (name: string, payload: unknown, options?: EmitOptions) => Promise<EmitRequest>;
};

export const RESERVED_NAME_PREFIX = "nextasking/";

export function createEnqueueRequests({
  tasksByName,
  eventsByName,
  listenerNamesByEventName,
}: InstanceDefinitions): EnqueueRequests {
  const appEventNames = [...eventsByName.keys()].filter((eventName) => !isBuiltInEventName(eventName));
  const listenersOf = (eventName: string): AnyTaskDefinition[] =>
    (listenerNamesByEventName.get(eventName) ?? []).flatMap((listenerName) => {
      const listener = tasksByName.get(listenerName);
      return listener === undefined ? [] : [listener];
    });

  return {
    dispatchRequestOf: async (name, payload, options = {}) => {
      const task = tasksByName.get(name);
      if (task === undefined) {
        throw new UnknownTaskError(name, [...tasksByName.keys()]);
      }
      const runId = consumerFacingIdOrRandomId("task", task.name, "runId", options.runId);
      const delayInMilliseconds = delayInMillisecondsOf(task.name, options.delay);
      const validatedPayload = await validatedTaskPayloadOf(task, payload);
      const concurrencyKey = concurrencyKeyOf(task, validatedPayload);
      const run: RunToCreate = {
        runId,
        task: task.name,
        payload: serializePayload(validatedPayload),
        retries: retriesOf(task),
        ...(concurrencyKey === undefined ? {} : { concurrencyKey }),
        ...(delayInMilliseconds === undefined ? {} : { delayInMilliseconds }),
      };
      return { request: { runs: [run] }, runId, ...(options.delay === undefined ? {} : { delay: options.delay }) };
    },

    emitRequestOf: async (name, payload, options = {}) => {
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
      const sourceEvent = { eventName: event.name, eventId };
      const serializedPayload = serializePayload(validatedPayload);
      const runs = listenersOf(event.name).map((listener): RunToCreate => {
        const concurrencyKey = concurrencyKeyOf(listener, validatedPayload);
        return {
          runId: deriveListenerRunId(eventId, listener.name),
          task: listener.name,
          payload: serializedPayload,
          retries: retriesOf(listener),
          sourceEvent,
          ...(concurrencyKey === undefined ? {} : { concurrencyKey }),
        };
      });
      return { request: { runs, eventDedup: sourceEvent }, eventId };
    },
  };
}

export async function validatedTaskPayloadOf(task: AnyTaskDefinition, payload: unknown): Promise<unknown> {
  if (task.schedule === undefined) {
    return validatePayload("task", task, payload);
  }
  if (payload !== undefined) {
    throw new Error(`Scheduled task "${task.name}" takes no payload: dispatch it by its name alone to run it now`);
  }
  return undefined;
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
