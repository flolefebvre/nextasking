import type { BuiltInEventName } from "./built-in-events.js";
import type { AnyTaskDefinition, EventDefinition, Payload, PayloadSchema } from "./definitions.js";
import type { Duration } from "./durations.js";
import type { RunWriter } from "./enqueuer.js";
import { deriveJobId } from "./ids.js";
import type { EventId, JobId, RunId } from "./ids.js";
import { controlsOf } from "./registry.js";
import type { Nextasking } from "./registry.js";

type PayloadOfTask<Task> = Task extends { schema: infer Schema extends PayloadSchema } ? Payload<Schema> : undefined;

export type RecordedDispatch<Task extends AnyTaskDefinition> = Task extends AnyTaskDefinition
  ? { task: Task["name"]; payload: PayloadOfTask<Task>; runId: RunId; delay?: Duration }
  : never;

export type RecordedEmit<Event extends EventDefinition> = Event extends EventDefinition
  ? { event: Event["name"]; payload: Payload<Event["schema"]>; eventId: EventId }
  : never;

export type RecordedReplay<Task extends AnyTaskDefinition, Event extends EventDefinition> =
  { task: Task["name"]; runId: RunId } | { event: Event["name"]; eventId: EventId };

export type NextaskingFake<Task extends AnyTaskDefinition, Event extends EventDefinition = never> = {
  readonly dispatches: readonly RecordedDispatch<Task>[];
  readonly emits: readonly RecordedEmit<Exclude<Event, { name: BuiltInEventName }>>[];
  readonly replays: readonly RecordedReplay<Task, Event>[];
  readonly restore: () => void;
  readonly [Symbol.dispose]: () => void;
};

export function fakeNextasking<Task extends AnyTaskDefinition, Event extends EventDefinition>(
  nextasking: Nextasking<Task, Event>,
): NextaskingFake<Task, Event> {
  const controls = controlsOf(nextasking);
  if (controls === undefined) {
    throw new Error("fakeNextasking takes the instance createNextasking returns");
  }
  type Fake = NextaskingFake<Task, Event>;
  const dispatches: Fake["dispatches"][number][] = [];
  const emits: Fake["emits"][number][] = [];
  const replays: Fake["replays"][number][] = [];
  const writtenJobIds = new Set<JobId>();
  const isWrittenAlready = (definitionName: string, consumerFacingId: RunId | EventId): boolean => {
    const jobId = deriveJobId(definitionName, consumerFacingId);
    if (writtenJobIds.has(jobId)) {
      return true;
    }
    writtenJobIds.add(jobId);
    return false;
  };
  const recordingRunWriter: RunWriter = {
    enqueueRun: (taskName, run) => {
      const isDuplicate = isWrittenAlready(taskName, run.runId);
      if (!isDuplicate) {
        dispatches.push({
          task: taskName,
          payload: run.payload,
          runId: run.runId,
          ...(run.delay === undefined ? {} : { delay: run.delay }),
        } as Fake["dispatches"][number]);
      }
      return Promise.resolve({ isDuplicate });
    },
    enqueueFanOut: (eventName, fanOut) => {
      const isDuplicate = isWrittenAlready(eventName, fanOut.eventId);
      if (!isDuplicate) {
        emits.push({ event: eventName, payload: fanOut.payload, eventId: fanOut.eventId } as Fake["emits"][number]);
      }
      return Promise.resolve({ isDuplicate });
    },
    replayRun: (taskName, runId) => {
      replays.push({ task: taskName, runId });
      return Promise.resolve("replayed");
    },
    replayFanOut: (eventName, eventId) => {
      replays.push({ event: eventName, eventId });
      return Promise.resolve("replayed");
    },
  };
  controls.startRecordingWrites(recordingRunWriter);
  let isRestored = false;
  const restore = () => {
    if (!isRestored) {
      isRestored = true;
      controls.stopRecordingWrites();
    }
  };
  return { dispatches, emits, replays, restore, [Symbol.dispose]: restore };
}
