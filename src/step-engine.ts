import { AsyncLocalStorage } from "node:async_hooks";

import { parseDuration } from "./durations.js";
import type { Duration } from "./durations.js";
import { StepMisuseError } from "./errors.js";
import { deriveHandlerSendId, deriveStepIdempotencyKey } from "./ids.js";
import type { HandlerSendId, IdempotencyKey, RunId, StepId } from "./ids.js";
import type { JsonValue } from "./json.js";
import type { DispatchByName, DispatchedRun, EmitByName, EmittedEvent } from "./registry.js";

export type SerializedSavedStep = string;

export type StepStore = {
  load: () => Promise<Record<StepId, SerializedSavedStep>>;
  save: (stepId: StepId, serializedSavedStep: SerializedSavedStep) => Promise<void>;
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

export type WakeTimeInEpochMilliseconds = number;

export type StepEngine = {
  step: StepTools;
  dispatch: DispatchByName;
  emit: EmitByName;
  isResumingAfterSleep: boolean;
  executeHandler: (handlerExecution: () => Promise<void>) => Promise<WakeTimeInEpochMilliseconds | undefined>;
  throwRecordedMisuse: () => void;
};

export type StepEngineConfig = {
  taskName: string;
  runId: RunId;
  attempt: number;
  store: StepStore;
  dispatch: DispatchByName;
  emit: EmitByName;
  abortSignal: AbortSignal;
};

type SavedSleep = {
  kind: "sleep";
  wakeAt: WakeTimeInEpochMilliseconds;
  attempt: number;
};

type SavedDispatch = { kind: "dispatch" } & DispatchedRun;

type SavedEmit = { kind: "emit" } & EmittedEvent;

type SavedStep = { kind: "result"; result: unknown } | SavedSleep | SavedDispatch | SavedEmit;

class RunSuspension extends Error {
  override readonly name = "RunSuspension";

  constructor(wakeAt: WakeTimeInEpochMilliseconds) {
    super(
      `The run is suspended until ${new Date(wakeAt).toISOString()}. Let this error through: the run resumes at that time.`,
    );
  }
}

type SizeInBytes = number;

const MEBIBYTE_IN_BYTES: SizeInBytes = 1024 * 1024;

const STEP_RESULT_CEILING_IN_BYTES: SizeInBytes = MEBIBYTE_IN_BYTES;

const RUN_SAVED_STEPS_CEILING_IN_BYTES: SizeInBytes = 10 * MEBIBYTE_IN_BYTES;

const LARGE_DATA_ADVICE = "Store large data elsewhere, such as your database, and return its id.";

const ISO_DATE_WITH_ZONE_PATTERN = /^\d{4}-\d{2}-\d{2}T[^Z+-]+(Z|[+-]\d{2}:\d{2})$/;

const enclosingStepIdScope = new AsyncLocalStorage<StepId>();

export async function openStepEngine(config: StepEngineConfig): Promise<StepEngine> {
  const serializedSavedStepsByStepId = await config.store.load();
  const savedStepsByStepId = new Map<StepId, SavedStep>(
    Object.entries(serializedSavedStepsByStepId).map(([stepId, serializedSavedStep]) => [
      stepId,
      deserializeSavedStep(serializedSavedStep),
    ]),
  );
  const stepIdsStartedInThisAttempt = new Set<StepId>();
  const inFlightStepSettlements = new Set<Promise<void>>();
  let savedStepsSizeInBytes = Object.values(serializedSavedStepsByStepId).reduce(
    (sizeInBytes, serializedSavedStep) => sizeInBytes + sizeInBytesOf(serializedSavedStep),
    0,
  );
  let recordedMisuse: StepMisuseError | undefined;
  let recordedWakeAt: WakeTimeInEpochMilliseconds | undefined;

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
    if (recordedWakeAt !== undefined) {
      throw new RunSuspension(recordedWakeAt);
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

  const serializedResultWithinCeilings = (stepId: StepId, result: unknown): SerializedSavedStep => {
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

  const saveStep = async (stepId: StepId, serializedSavedStep: SerializedSavedStep) => {
    savedStepsSizeInBytes += sizeInBytesOf(serializedSavedStep);
    await config.store.save(stepId, serializedSavedStep);
  };

  const executeStep = async <Result extends JsonValue | void>(
    stepId: StepId,
    fn: (context: StepContext) => Result | Promise<Result>,
  ): Promise<Result> => {
    const savedStep = savedStepsByStepId.get(stepId);
    if (savedStep?.kind === "result") {
      return savedStep.result as Result;
    }
    const idempotencyKey = deriveStepIdempotencyKey(config.taskName, config.runId, stepId);
    const result = await enclosingStepIdScope.run(stepId, () => fn({ idempotencyKey }));
    const serializedResult = serializedResultWithinCeilings(stepId, result);
    await saveStep(stepId, serializedResult);
    return deserializeStepResult(serializedResult) as Result;
  };

  const sleepUntilWakeTime = async (stepId: StepId, wakeTimeOfNewSleep: () => WakeTimeInEpochMilliseconds) => {
    const savedStep = savedStepsByStepId.get(stepId);
    let wakeAt: WakeTimeInEpochMilliseconds;
    if (savedStep?.kind === "sleep") {
      wakeAt = savedStep.wakeAt;
    } else {
      wakeAt = wakeTimeOfNewSleep();
      const savedSleep: SavedSleep = { kind: "sleep", wakeAt, attempt: config.attempt };
      await saveStep(stepId, JSON.stringify(savedSleep));
    }
    if (wakeAt <= Date.now()) {
      return;
    }
    recordedWakeAt = Math.min(recordedWakeAt ?? wakeAt, wakeAt);
    throw new RunSuspension(wakeAt);
  };

  const sendCountsByEnclosingStepId = new Map<StepId | undefined, number>();

  const sendOnce = async <Sent extends SavedDispatch | SavedEmit>(
    kind: Sent["kind"],
    send: (sendId: HandlerSendId) => Promise<Sent>,
  ): Promise<Sent> => {
    throwIfStretchIsOver();
    const enclosingStepId = enclosingStepIdScope.getStore();
    const positionInScope = sendCountsByEnclosingStepId.get(enclosingStepId) ?? 0;
    sendCountsByEnclosingStepId.set(enclosingStepId, positionInScope + 1);
    const sendId = deriveHandlerSendId(config.taskName, config.runId, enclosingStepId, positionInScope);
    const savedSend = savedStepsByStepId.get(sendId);
    if (savedSend?.kind === kind) {
      return savedSend as Sent;
    }
    return trackInFlight(
      (async () => {
        const sent = await send(sendId);
        await saveStep(sendId, JSON.stringify(sent));
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
      let durationInMilliseconds: number;
      try {
        durationInMilliseconds = parseDuration(duration);
      } catch (error) {
        throw recordMisuse(`The duration of step "${stepId}" is invalid: ${(error as Error).message}`);
      }
      return trackInFlight(sleepUntilWakeTime(stepId, () => Date.now() + durationInMilliseconds));
    },
    sleepUntil: async (stepId, time) => {
      checkStepCanStart(stepId);
      const wakeAt = wakeTimeOf(time);
      if (wakeAt === undefined) {
        const shownTime = time instanceof Date ? String(time) : JSON.stringify(time);
        throw recordMisuse(
          `The wake time of step "${stepId}" is not a Date or an ISO date string with Z or an offset: ${shownTime}`,
        );
      }
      return trackInFlight(sleepUntilWakeTime(stepId, () => wakeAt));
    },
  };

  return {
    step,
    dispatch: async (name, payload, options = {}) => {
      const { runId } = await sendOnce<SavedDispatch>("dispatch", async (sendId) => {
        const dispatchedRun = await config.dispatch(name, payload, { ...options, runId: options.runId ?? sendId });
        return { kind: "dispatch", runId: dispatchedRun.runId };
      });
      return { runId };
    },
    emit: async (name, payload, options = {}) => {
      const { eventId } = await sendOnce<SavedEmit>("emit", async (sendId) => {
        const emittedEvent = await config.emit(name, payload, { ...options, eventId: options.eventId ?? sendId });
        return { kind: "emit", eventId: emittedEvent.eventId };
      });
      return { eventId };
    },
    isResumingAfterSleep: [...savedStepsByStepId.values()].some(
      (savedStep) => savedStep.kind === "sleep" && savedStep.attempt === config.attempt,
    ),
    executeHandler: async (handlerExecution) => {
      let handlerFailure: { error: unknown } | undefined;
      try {
        await handlerExecution();
      } catch (error) {
        handlerFailure = { error };
      }
      if (recordedWakeAt !== undefined && recordedMisuse === undefined) {
        await waitForStepsInFlight();
      }
      throwRecordedMisuse();
      if (recordedWakeAt !== undefined) {
        return recordedWakeAt;
      }
      if (handlerFailure !== undefined) {
        throw handlerFailure.error;
      }
      return undefined;
    },
    throwRecordedMisuse,
  };
}

function wakeTimeOf(time: unknown): WakeTimeInEpochMilliseconds | undefined {
  let wakeAt = Number.NaN;
  if (time instanceof Date) {
    wakeAt = time.getTime();
  } else if (typeof time === "string" && ISO_DATE_WITH_ZONE_PATTERN.test(time)) {
    wakeAt = Date.parse(time);
  }
  return Number.isFinite(wakeAt) ? wakeAt : undefined;
}

function serializeStepResult(result: unknown): SerializedSavedStep | undefined {
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

function deserializeStepResult(serializedResult: SerializedSavedStep): unknown {
  const wrappedResult = JSON.parse(serializedResult) as unknown[];
  return wrappedResult[0];
}

function deserializeSavedStep(serializedSavedStep: SerializedSavedStep): SavedStep {
  const savedValue = JSON.parse(serializedSavedStep) as unknown;
  if (Array.isArray(savedValue)) {
    return { kind: "result", result: savedValue[0] };
  }
  return savedValue as SavedStep;
}

function sizeInBytesOf(serializedSavedStep: SerializedSavedStep): SizeInBytes {
  return Buffer.byteLength(serializedSavedStep, "utf8");
}
