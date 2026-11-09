import type { BuiltInEventName } from "./built-in-events.js";
import type { AnyTaskDefinition, EventDefinition, Payload, PayloadSchema } from "./definitions.js";
import type { Duration } from "./durations.js";

import type { RunIdentity } from "./engine/engine-keys.js";
import { deserializePayload } from "./engine/engine.js";
import type { EventId, RunId } from "./ids.js";
import { controlsOf } from "./registry.js";
import type { Nextasking, NextaskingControls } from "./registry.js";
import type { SendTarget } from "./send-target.js";

type PayloadOfTask<Task> = Task extends { schema: infer Schema extends PayloadSchema } ? Payload<Schema> : undefined;

export type RecordedDispatch<Task extends AnyTaskDefinition> = Task extends AnyTaskDefinition
  ? { task: Task["name"]; payload: PayloadOfTask<Task>; runId: RunId; delay?: Duration }
  : never;

export type RecordedEmit<Event extends EventDefinition> = Event extends EventDefinition
  ? { event: Event["name"]; payload: Payload<Event["schema"]>; eventId: EventId }
  : never;

export type RecordedReplay<Task extends AnyTaskDefinition> = { task: Task["name"]; runId: RunId };

export type NextaskingFake<Task extends AnyTaskDefinition, Event extends EventDefinition = never> = {
  readonly dispatches: readonly RecordedDispatch<Task>[];
  readonly emits: readonly RecordedEmit<Exclude<Event, { name: BuiltInEventName }>>[];
  readonly replays: readonly RecordedReplay<Task>[];
  readonly restore: () => void;
  readonly [Symbol.dispose]: () => void;
};

export type RecordingSendTarget<Task extends AnyTaskDefinition, Event extends EventDefinition> = Pick<
  NextaskingFake<Task, Event>,
  "dispatches" | "emits" | "replays"
> & { sendTarget: SendTarget };

export function controlsOfInstance(nextasking: object, callName: string): NextaskingControls {
  const controls = controlsOf(nextasking);
  if (controls === undefined) {
    throw new Error(`${callName} takes the instance createNextasking returns`);
  }
  return controls;
}

export function createRecordingSendTarget<
  Task extends AnyTaskDefinition,
  Event extends EventDefinition,
>(): RecordingSendTarget<Task, Event> {
  type Fake = NextaskingFake<Task, Event>;
  const dispatches: Fake["dispatches"][number][] = [];
  const emits: Fake["emits"][number][] = [];
  const replays: Fake["replays"][number][] = [];
  const createdRunKeys = new Set<string>();
  const emittedEventKeys = new Set<string>();
  const sendTarget: SendTarget = {
    enqueue: ({ runs, eventDedup }, delay) => {
      if (eventDedup !== undefined) {
        const eventKey = JSON.stringify([eventDedup.eventName, eventDedup.eventId]);
        if (emittedEventKeys.has(eventKey)) {
          return Promise.resolve({ kind: "duplicate" });
        }
        emittedEventKeys.add(eventKey);
      }
      const createdRuns: RunIdentity[] = [];
      const skippedRuns: RunIdentity[] = [];
      for (const { task, runId } of runs) {
        const runKey = JSON.stringify([task, runId]);
        (createdRunKeys.has(runKey) ? skippedRuns : createdRuns).push({ task, runId });
        createdRunKeys.add(runKey);
      }
      const [firstRun] = runs;
      if (eventDedup !== undefined) {
        emits.push({
          event: eventDedup.eventName,
          payload: firstRun === undefined ? undefined : deserializePayload(firstRun.payload),
          eventId: eventDedup.eventId,
        } as Fake["emits"][number]);
      } else if (firstRun !== undefined && createdRuns.length > 0) {
        dispatches.push({
          task: firstRun.task,
          payload: deserializePayload(firstRun.payload),
          runId: firstRun.runId,
          ...(delay === undefined ? {} : { delay }),
        } as Fake["dispatches"][number]);
      }
      return Promise.resolve({ kind: "enqueued", createdRuns, skippedRuns });
    },
    replay: ({ task, runId }) => {
      replays.push({ task, runId });
      return Promise.resolve({ kind: "replayed", task });
    },
  };
  return { sendTarget, dispatches, emits, replays };
}

export function fakeNextasking<Task extends AnyTaskDefinition, Event extends EventDefinition>(
  nextasking: Nextasking<Task, Event>,
): NextaskingFake<Task, Event> {
  const controls = controlsOfInstance(nextasking, "fakeNextasking");
  const { sendTarget, dispatches, emits, replays } = createRecordingSendTarget<Task, Event>();
  controls.startRecordingSends(sendTarget);
  let isRestored = false;
  const restore = () => {
    if (!isRestored) {
      isRestored = true;
      controls.stopRecordingSends();
    }
  };
  return { dispatches, emits, replays, restore, [Symbol.dispose]: restore };
}
