import { AsyncLocalStorage } from "node:async_hooks";

import type { AnyTaskDefinition, ScheduledTaskContext, TaskContext } from "./definitions.js";
import { parseDuration } from "./durations.js";
import type { Duration, DurationInMilliseconds, EpochMilliseconds } from "./durations.js";
import type { LockToken } from "./engine/engine-keys.js";
import type { SerializedStepValue, StepToSave } from "./engine/engine.js";
import { StepMisuseError } from "./errors.js";
import { deriveHandlerSendId, deriveStepIdempotencyKey } from "./ids.js";
import type { EventId, HandlerSendId, IdempotencyKey, RunId, StepId } from "./ids.js";
import { LostRunLockError } from "./job-lock.js";
import type { JsonValue } from "./json.js";
import type { Logger } from "./logger.js";
import type {
  DispatchByName,
  DispatchedRun,
  DispatchOptions,
  EmitByName,
  EmitOptions,
  EmittedEvent,
} from "./enqueuer.js";

export type { SerializedStepValue } from "./engine/engine.js";

export type SleepToSave = Exclude<StepToSave, { kind: "value" }>;

export type StepStore = {
  readStep: (stepId: StepId) => Promise<SerializedStepValue | undefined>;
  saveStep: (stepId: StepId, value: SerializedStepValue) => Promise<void>;
  saveSleep: (stepId: StepId, sleep: SleepToSave) => Promise<ResumeTime>;
};

export type ResumeTime = {
  resumesAt: EpochMilliseconds;
  isDue: boolean;
};

export type SendFence = {
  parentTask: string;
  parentRunId: RunId;
  lockToken: LockToken;
  stepId: HandlerSendId;
};

export type HandlerSender = {
  dispatch: (fence: SendFence, name: string, payload: unknown, options: DispatchOptions) => Promise<DispatchedRun>;
  emit: (fence: SendFence, name: string, payload: unknown, options: EmitOptions) => Promise<EmittedEvent>;
};

export type StepContext = {
  idempotencyKey: IdempotencyKey;
};

export type StepTools = {
  run: <Result extends JsonValue | void>(
    stepId: StepId,
    fn: (context: StepContext) => Result | Promise<Result>,
  ) => Promise<Result>;
  sleep: (stepId: StepId, duration: Duration) => Promise<void>;
  sleepUntil: (stepId: StepId, time: Date | string) => Promise<void>;
};

export type StepEngine = {
  step: StepTools;
  dispatch: DispatchByName;
  emit: EmitByName;
  executeHandler: (handlerExecution: () => Promise<void>) => Promise<HandlerOutcome>;
  throwRecordedMisuse: () => void;
};

export type HandlerOutcome =
  { kind: "completed" } | { kind: "suspended"; resumesAt: EpochMilliseconds } | { kind: "yielded" };

export type StepEngineConfig = {
  taskName: string;
  runId: RunId;
  lockToken: LockToken;
  store: StepStore;
  sender: HandlerSender;
  abortSignal: AbortSignal;
  isWorkerDraining: () => boolean;
  futureResumeTime: FutureResumeTimeHandling;
  sleepsWokenAt?: EpochMilliseconds;
  markLockLost: () => void;
};

export type FutureResumeTimeHandling = "suspend the run" | "continue at once";

export type SavedStep = { kind: "value"; value: unknown } | { kind: "sleep"; resumesAt: EpochMilliseconds };

class RunSuspension extends Error {
  override readonly name = "RunSuspension";

  constructor(resumesAt: EpochMilliseconds) {
    super(
      `The run is suspended until ${new Date(resumesAt).toISOString()}. Let this error through: the run resumes at that time.`,
    );
  }
}

class RunYield extends Error {
  override readonly name = "RunYield";

  constructor() {
    super(
      "The worker is shutting down, so the run goes back to pending at this step. Let this error through: another worker resumes it from its saved steps.",
    );
  }
}

type SizeInBytes = number;

const MEBIBYTE_IN_BYTES: SizeInBytes = 1024 * 1024;

const STEP_RESULT_CEILING_IN_BYTES: SizeInBytes = MEBIBYTE_IN_BYTES;

const RUN_SAVED_STEPS_CEILING_IN_BYTES: SizeInBytes = 10 * MEBIBYTE_IN_BYTES;

const LARGE_DATA_ADVICE = "Store large data elsewhere, such as your database, and return its id.";

const ISO_DATE_WITH_ZONE_PATTERN = /^\d{4}-\d{2}-\d{2}T[^Z+-]+(Z|[+-]\d{2}:\d{2})$/;

const SAVED_SLEEP_PATTERN = /^\d+$/;

const enclosingStepIdScope = new AsyncLocalStorage<StepId>();

export function openStepEngine(config: StepEngineConfig): StepEngine {
  const stepIdsStartedInThisAttempt = new Set<StepId>();
  const inFlightStepSettlements = new Set<Promise<void>>();
  let savedStepsSizeInBytes: SizeInBytes = 0;
  let recordedMisuse: StepMisuseError | undefined;
  let recordedResumesAt: EpochMilliseconds | undefined;
  let isYieldRecorded = false;

  const recordMisuse = (message: string): StepMisuseError => {
    const misuse = new StepMisuseError(message);
    recordedMisuse ??= misuse;
    return misuse;
  };

  const throwRecordedMisuse = () => {
    if (recordedMisuse !== undefined) {
      throw recordedMisuse;
    }
  };

  const throwIfStretchIsOver = () => {
    config.abortSignal.throwIfAborted();
    throwRecordedMisuse();
    if (recordedResumesAt !== undefined) {
      throw new RunSuspension(recordedResumesAt);
    }
    if (isYieldRecorded || config.isWorkerDraining()) {
      isYieldRecorded = true;
      throw new RunYield();
    }
  };

  const checkStepCanStart = (stepId: StepId) => {
    throwIfStretchIsOver();
    const enclosingStepId = enclosingStepIdScope.getStore();
    if (enclosingStepId !== undefined) {
      throw recordMisuse(
        `Step "${stepId}" was started inside the fn of step "${enclosingStepId}". Start every step from the handler itself.`,
      );
    }
    if (stepIdsStartedInThisAttempt.has(stepId)) {
      throw recordMisuse(
        `Step id "${stepId}" is used twice in one run of task "${config.taskName}". Give each step its own id.`,
      );
    }
    stepIdsStartedInThisAttempt.add(stepId);
  };

  const trackInFlight = <Result>(stepExecution: Promise<Result>): Promise<Result> => {
    const settlement: Promise<void> = stepExecution.then(
      () => {
        inFlightStepSettlements.delete(settlement);
      },
      () => {
        inFlightStepSettlements.delete(settlement);
      },
    );
    inFlightStepSettlements.add(settlement);
    return stepExecution;
  };

  const waitForStepsInFlight = async () => {
    while (inFlightStepSettlements.size > 0) {
      await Promise.all(inFlightStepSettlements);
    }
  };

  const countSavedStep = (serializedStep: string) => {
    savedStepsSizeInBytes += sizeInBytesOf(serializedStep);
  };

  const noteLockLoss = async <Result>(storeCall: Promise<Result>): Promise<Result> => {
    try {
      return await storeCall;
    } catch (error) {
      if (error instanceof LostRunLockError) {
        config.markLockLost();
      }
      throw error;
    }
  };

  const serializedResultWithinCeilings = (stepId: StepId, result: unknown): SerializedStepValue => {
    const serializedResult = serializeStepResult(result);
    if (serializedResult === undefined) {
      throw recordMisuse(
        `The result of step "${stepId}" is not JSON: return only strings, numbers, booleans, null, arrays and plain objects from a step's fn.`,
      );
    }
    const resultSizeInBytes = sizeInBytesOf(serializedResult);
    if (resultSizeInBytes > STEP_RESULT_CEILING_IN_BYTES) {
      throw recordMisuse(
        `The result of step "${stepId}" is ${resultSizeInBytes} bytes once serialized, over the 1 MiB limit for one step. ${LARGE_DATA_ADVICE}`,
      );
    }
    const runSizeInBytes = savedStepsSizeInBytes + resultSizeInBytes;
    if (runSizeInBytes > RUN_SAVED_STEPS_CEILING_IN_BYTES) {
      throw recordMisuse(
        `Saving step "${stepId}" would bring the saved steps of this run of task "${config.taskName}" to ${runSizeInBytes} bytes, over the 10 MiB limit for one run. ${LARGE_DATA_ADVICE}`,
      );
    }
    return serializedResult;
  };

  const executeStep = async <Result extends JsonValue | void>(
    stepId: StepId,
    fn: (context: StepContext) => Result | Promise<Result>,
  ): Promise<Result> => {
    const savedValue = await noteLockLoss(config.store.readStep(stepId));
    if (savedValue !== undefined) {
      const savedStep = savedStepOf(stepId, savedValue);
      if (savedStep.kind === "value") {
        countSavedStep(savedValue);
        return savedStep.value as Result;
      }
    }
    config.abortSignal.throwIfAborted();
    const idempotencyKey = deriveStepIdempotencyKey(config.taskName, config.runId, stepId);
    const result = await enclosingStepIdScope.run(stepId, () => fn({ idempotencyKey }));
    const serializedResult = serializedResultWithinCeilings(stepId, result);
    await noteLockLoss(config.store.saveStep(stepId, serializedResult));
    countSavedStep(serializedResult);
    return deserializeStepValue(serializedResult) as Result;
  };

  const sleepUntilResumeTime = async (stepId: StepId, sleep: SleepToSave) => {
    const { resumesAt, isDue } = await noteLockLoss(config.store.saveSleep(stepId, sleep));
    countSavedStep(serializedSleepOf(resumesAt));
    if (isDue || config.futureResumeTime === "continue at once" || config.sleepsWokenAt !== undefined) {
      return;
    }
    recordedResumesAt = Math.min(recordedResumesAt ?? resumesAt, resumesAt);
    throw new RunSuspension(resumesAt);
  };

  const sendCountsByEnclosingStepId = new Map<StepId | undefined, number>();

  const sendOnce = async <Sent>(send: (fence: SendFence) => Promise<Sent>, sentIdOf: (sent: Sent) => string) => {
    throwIfStretchIsOver();
    const enclosingStepId = enclosingStepIdScope.getStore();
    const positionInScope = sendCountsByEnclosingStepId.get(enclosingStepId) ?? 0;
    sendCountsByEnclosingStepId.set(enclosingStepId, positionInScope + 1);
    const stepId = deriveHandlerSendId(config.taskName, config.runId, enclosingStepId, positionInScope);
    return trackInFlight(
      (async () => {
        const sent = await noteLockLoss(
          send({ parentTask: config.taskName, parentRunId: config.runId, lockToken: config.lockToken, stepId }),
        );
        countSavedStep(serializeStepValue(sentIdOf(sent)));
        return sent;
      })(),
    );
  };

  const step: StepTools = {
    run: async (stepId, fn) => {
      checkStepCanStart(stepId);
      return trackInFlight(executeStep(stepId, fn));
    },
    sleep: async (stepId, duration) => {
      checkStepCanStart(stepId);
      let durationInMilliseconds: DurationInMilliseconds;
      try {
        durationInMilliseconds = parseDuration(duration);
      } catch (error) {
        throw recordMisuse(`The duration of step "${stepId}" is invalid: ${(error as Error).message}`);
      }
      return trackInFlight(sleepUntilResumeTime(stepId, { kind: "sleep", durationInMilliseconds }));
    },
    sleepUntil: async (stepId, time) => {
      checkStepCanStart(stepId);
      const resumesAt = resumeTimeOf(time);
      if (resumesAt === undefined) {
        const shownTime = time instanceof Date ? String(time) : JSON.stringify(time);
        throw recordMisuse(
          `The resume time of step "${stepId}" is not a Date or an ISO date string with Z or an offset: ${shownTime}`,
        );
      }
      return trackInFlight(sleepUntilResumeTime(stepId, { kind: "sleepUntil", resumesAt }));
    },
  };

  return {
    step,
    dispatch: async (name, payload, options = {}) => {
      const { runId } = await sendOnce(
        (fence) => config.sender.dispatch(fence, name, payload, { ...options, runId: options.runId ?? fence.stepId }),
        (dispatchedRun) => dispatchedRun.runId,
      );
      return { runId };
    },
    emit: async (name, payload, options = {}) => {
      const { eventId } = await sendOnce(
        (fence) => config.sender.emit(fence, name, payload, { ...options, eventId: options.eventId ?? fence.stepId }),
        (emittedEvent) => emittedEvent.eventId,
      );
      return { eventId };
    },
    executeHandler: async (handlerExecution) => {
      let handlerFailure: { error: unknown } | undefined;
      try {
        await handlerExecution();
      } catch (error) {
        handlerFailure = { error };
      }
      if ((recordedResumesAt !== undefined || isYieldRecorded) && recordedMisuse === undefined) {
        await waitForStepsInFlight();
      }
      throwRecordedMisuse();
      if (recordedResumesAt !== undefined) {
        return { kind: "suspended", resumesAt: recordedResumesAt };
      }
      if (isYieldRecorded) {
        return { kind: "yielded" };
      }
      if (handlerFailure !== undefined) {
        throw handlerFailure.error;
      }
      return { kind: "completed" };
    },
    throwRecordedMisuse,
  };
}

export function sendThenSave(
  store: StepStore,
  { dispatch, emit }: { dispatch: DispatchByName; emit: EmitByName },
): HandlerSender {
  const sendUnlessSaved = async (fence: SendFence, send: () => Promise<RunId | EventId>): Promise<string> => {
    const savedValue = await store.readStep(fence.stepId);
    if (savedValue !== undefined) {
      return sentIdOf(fence.stepId, savedValue);
    }
    const sentId = await send();
    await store.saveStep(fence.stepId, serializeStepValue(sentId));
    return sentId;
  };
  return {
    dispatch: async (fence, name, payload, options) => ({
      runId: await sendUnlessSaved(fence, async () => (await dispatch(name, payload, options)).runId),
    }),
    emit: async (fence, name, payload, options) => ({
      eventId: await sendUnlessSaved(fence, async () => (await emit(name, payload, options)).eventId),
    }),
  };
}

type AnyTaskHandler = (context: TaskContext<unknown> | ScheduledTaskContext) => Promise<void>;

export type HandlerRunFields = {
  runId: RunId;
  attempt: number;
  logger: Logger;
  abortSignal: AbortSignal;
  payload: unknown;
  scheduledAt: Date | undefined;
};

export function executeTaskHandler(
  stepEngine: StepEngine,
  task: AnyTaskDefinition,
  { payload, scheduledAt, ...runFields }: HandlerRunFields,
): Promise<HandlerOutcome> {
  const { step, emit, dispatch } = stepEngine;
  const runContext = { step, emit, dispatch, ...runFields };
  const handler = task.handler as AnyTaskHandler;
  return stepEngine.executeHandler(() =>
    handler(task.schedule === undefined ? { ...runContext, payload } : { ...runContext, scheduledAt }),
  );
}

export function serializedStepResultOf(stepId: StepId, result: unknown): SerializedStepValue {
  const serializedResult = serializeStepResult(result);
  if (serializedResult === undefined) {
    throw new Error(`The saved result of step "${stepId}" is not JSON`);
  }
  return serializedResult;
}

export function serializeStepValue(value: RunId | EventId): SerializedStepValue {
  return JSON.stringify([value]);
}

export function resumeTimeFromNowOf(sleep: SleepToSave): EpochMilliseconds {
  return sleep.kind === "sleep" ? Date.now() + sleep.durationInMilliseconds : sleep.resumesAt;
}

export function resumeTimeByAppClockOf(resumesAt: EpochMilliseconds): ResumeTime {
  return { resumesAt, isDue: resumesAt <= Date.now() };
}

export function serializedSleepOf(resumesAt: EpochMilliseconds): SerializedStepValue {
  return String(resumesAt);
}

export function savedStepOf(stepId: StepId, serializedStep: SerializedStepValue): SavedStep {
  if (serializedStep.startsWith("[")) {
    return { kind: "value", value: deserializeStepValue(serializedStep) };
  }
  if (SAVED_SLEEP_PATTERN.test(serializedStep)) {
    return { kind: "sleep", resumesAt: Number(serializedStep) };
  }
  throw new Error(
    `Step "${stepId}" is saved as ${JSON.stringify(serializedStep)}, which is neither a value nor a sleep`,
  );
}

export function sentIdOf(stepId: StepId, serializedStep: SerializedStepValue): RunId | EventId {
  const savedStep = savedStepOf(stepId, serializedStep);
  if (savedStep.kind !== "value" || typeof savedStep.value !== "string") {
    throw new Error(`Step "${stepId}" is saved as ${JSON.stringify(serializedStep)}, which is not a sent id`);
  }
  return savedStep.value;
}

export type SavedStepResult = {
  id: StepId;
  result: unknown;
};

export function savedStepResultsOf(serializedStepsById: Record<StepId, SerializedStepValue>): SavedStepResult[] {
  return Object.entries(serializedStepsById).flatMap(([id, serializedStep]) => {
    const savedStep = savedStepOf(id, serializedStep);
    return savedStep.kind === "value" ? [{ id, result: savedStep.value }] : [];
  });
}

function resumeTimeOf(time: unknown): EpochMilliseconds | undefined {
  let resumesAt = Number.NaN;
  if (time instanceof Date) {
    resumesAt = time.getTime();
  } else if (typeof time === "string" && ISO_DATE_WITH_ZONE_PATTERN.test(time)) {
    resumesAt = Date.parse(time);
  }
  return Number.isFinite(resumesAt) ? resumesAt : undefined;
}

function serializeStepResult(result: unknown): SerializedStepValue | undefined {
  if (result === undefined) {
    return "[]";
  }
  let serializedValue: string | undefined;
  try {
    serializedValue = JSON.stringify(result);
  } catch {
    return undefined;
  }
  return serializedValue === undefined ? undefined : `[${serializedValue}]`;
}

function deserializeStepValue(serializedValue: SerializedStepValue): unknown {
  const wrappedValue = JSON.parse(serializedValue) as unknown[];
  return wrappedValue[0];
}

function sizeInBytesOf(serializedStep: string): SizeInBytes {
  return Buffer.byteLength(serializedStep, "utf8");
}
