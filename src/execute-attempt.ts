import type { AnyTaskDefinition, EventDefinition, PayloadInput, PayloadSchema } from "./definitions.js";
import { validatedTaskPayloadOf } from "./enqueuer.js";
import { UnknownTaskError } from "./errors.js";
import { controlsOfInstance, createRecordingRunWriter } from "./fake-nextasking.js";
import type { NextaskingFake } from "./fake-nextasking.js";
import { createRandomId } from "./ids.js";
import type { RunId, StepId } from "./ids.js";
import { loggerWithFields, silentLogger } from "./logger.js";
import type { Logger } from "./logger.js";
import type { Nextasking } from "./registry.js";
import { createInMemoryStepStore } from "./in-memory-step-store.js";
import {
  executeTaskHandler,
  openStepEngine,
  savedStepResultsOf,
  sendThenSave,
  serializedStepResultOf,
} from "./step-engine.js";
import type { SavedStepResult, StepStore } from "./step-engine.js";

export type RecordedSleep = {
  id: StepId;
  wakeAt: Date;
};

export type ExecuteAttemptOptions = {
  savedSteps?: readonly SavedStepResult[];
  runId?: RunId;
  attempt?: number;
  abortSignal?: AbortSignal;
  scheduledAt?: Date;
  logger?: Logger;
};

export type ExecutedAttempt<Task extends AnyTaskDefinition, Event extends EventDefinition = never> = Pick<
  NextaskingFake<Task, Event>,
  "dispatches" | "emits"
> & {
  readonly steps: readonly SavedStepResult[];
  readonly sleeps: readonly RecordedSleep[];
};

export type ExecuteAttemptArguments<Task> = Task extends { schema: infer Schema extends PayloadSchema }
  ? [payload: PayloadInput<Schema>, options?: ExecuteAttemptOptions]
  : [payload?: undefined, options?: ExecuteAttemptOptions];

const FIRST_ATTEMPT = 1;

export async function executeAttempt<
  Task extends AnyTaskDefinition,
  Event extends EventDefinition,
  Name extends Task["name"],
>(
  nextasking: Nextasking<Task, Event>,
  taskName: Name,
  ...[payload, options = {}]: ExecuteAttemptArguments<Extract<Task, { name: Name }>>
): Promise<ExecutedAttempt<Task, Event>> {
  const controls = controlsOfInstance(nextasking, "executeAttempt");
  const task = controls.tasksByName.get(taskName);
  if (task === undefined) {
    throw new UnknownTaskError(taskName, [...controls.tasksByName.keys()]);
  }
  const validatedPayload = await validatedTaskPayloadOf(task, payload);
  const { runWriter, dispatches, emits } = createRecordingRunWriter<Task, Event>();
  const { dispatch, emit } = controls.createEnqueuerWritingTo(runWriter);
  const inMemoryStore = createInMemoryStepStore(
    Object.fromEntries((options.savedSteps ?? []).map(({ id, result }) => [id, serializedStepResultOf(id, result)])),
  );
  const sleeps: RecordedSleep[] = [];
  const store: StepStore = {
    ...inMemoryStore,
    saveSleep: async (stepId, sleep) => {
      const resumesAt = await inMemoryStore.saveSleep(stepId, sleep);
      sleeps.push({ id: stepId, wakeAt: new Date(resumesAt) });
      return resumesAt;
    },
  };
  const runId = options.runId ?? createRandomId();
  const attempt = options.attempt ?? FIRST_ATTEMPT;
  const abortSignal = options.abortSignal ?? new AbortController().signal;
  const stepEngine = openStepEngine({
    taskName: task.name,
    runId,
    lockToken: createRandomId(),
    store,
    sender: sendThenSave(store, { dispatch, emit }),
    abortSignal,
    isWorkerDraining: () => false,
    futureResumeTime: "continue at once",
    markLockLost: () => undefined,
  });
  try {
    await executeTaskHandler(stepEngine, task, {
      runId,
      attempt,
      logger: loggerWithFields(options.logger ?? silentLogger, { task: task.name, runId, attempt }),
      abortSignal,
      payload: validatedPayload,
      scheduledAt: options.scheduledAt ?? new Date(),
    });
  } finally {
    stepEngine.throwRecordedMisuse();
  }
  return {
    steps: savedStepResultsOf(inMemoryStore.savedSteps()),
    sleeps,
    dispatches,
    emits,
  };
}
