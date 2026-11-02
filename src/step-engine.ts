import { AsyncLocalStorage } from "node:async_hooks";

import { StepMisuseError } from "./errors.js";
import { deriveStepIdempotencyKey } from "./ids.js";
import type { IdempotencyKey, RunId, StepId } from "./ids.js";
import type { JsonValue } from "./json.js";

export type SerializedStepResult = string;

export type StepStore = {
  load: () => Promise<Record<StepId, SerializedStepResult>>;
  save: (stepId: StepId, serializedResult: SerializedStepResult) => Promise<void>;
};

export type StepContext = {
  idempotencyKey: IdempotencyKey;
};

export type StepTools = {
  run: <Result extends JsonValue | void>(
    stepId: StepId,
    fn: (context: StepContext) => Result | Promise<Result>,
  ) => Promise<Result>;
};

export type StepEngine = {
  step: StepTools;
  throwRecordedMisuse: () => void;
};

export type StepEngineConfig = {
  taskName: string;
  runId: RunId;
  store: StepStore;
};

type SizeInBytes = number;

const MEBIBYTE_IN_BYTES: SizeInBytes = 1024 * 1024;

const STEP_RESULT_CEILING_IN_BYTES: SizeInBytes = MEBIBYTE_IN_BYTES;

const RUN_SAVED_STEPS_CEILING_IN_BYTES: SizeInBytes = 10 * MEBIBYTE_IN_BYTES;

const LARGE_DATA_ADVICE = "Store large data elsewhere, such as your database, and return its id.";

const enclosingStepIdScope = new AsyncLocalStorage<StepId>();

export async function openStepEngine(config: StepEngineConfig): Promise<StepEngine> {
  const savedResultsByStepId = await config.store.load();
  const stepIdsStartedInThisAttempt = new Set<StepId>();
  let savedStepsSizeInBytes = Object.values(savedResultsByStepId).reduce(
    (sizeInBytes, serializedResult) => sizeInBytes + sizeInBytesOf(serializedResult),
    0,
  );
  let recordedMisuse: StepMisuseError | undefined;

  const recordMisuse = (message: string): StepMisuseError => {
    const misuse = new StepMisuseError(message);
    recordedMisuse ??= misuse;
    return misuse;
  };

  const checkStepCanStart = (stepId: StepId) => {
    if (recordedMisuse !== undefined) {
      throw recordedMisuse;
    }
    const enclosingStepId = enclosingStepIdScope.getStore();
    if (enclosingStepId !== undefined) {
      throw recordMisuse(
        `Step "${stepId}" was started inside the fn of step "${enclosingStepId}". Start every step from the handler itself.`,
      );
    }
    if (stepIdsStartedInThisAttempt.has(stepId)) {
      throw recordMisuse(
        `Step id "${stepId}" is used twice in one run of task "${config.taskName}". Give each step.run call its own id.`,
      );
    }
    stepIdsStartedInThisAttempt.add(stepId);
  };

  const serializedResultWithinCeilings = (stepId: StepId, result: unknown): SerializedStepResult => {
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

  const step: StepTools = {
    run: async <Result extends JsonValue | void>(
      stepId: StepId,
      fn: (context: StepContext) => Result | Promise<Result>,
    ): Promise<Result> => {
      checkStepCanStart(stepId);
      const savedResult = savedResultsByStepId[stepId];
      if (savedResult !== undefined) {
        return deserializeStepResult(savedResult) as Result;
      }
      const idempotencyKey = deriveStepIdempotencyKey(config.taskName, config.runId, stepId);
      const result = await enclosingStepIdScope.run(stepId, () => fn({ idempotencyKey }));
      const serializedResult = serializedResultWithinCeilings(stepId, result);
      savedStepsSizeInBytes += sizeInBytesOf(serializedResult);
      await config.store.save(stepId, serializedResult);
      return deserializeStepResult(serializedResult) as Result;
    },
  };

  return {
    step,
    throwRecordedMisuse: () => {
      if (recordedMisuse !== undefined) {
        throw recordedMisuse;
      }
    },
  };
}

function serializeStepResult(result: unknown): SerializedStepResult | undefined {
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

function deserializeStepResult(serializedResult: SerializedStepResult): unknown {
  const wrappedResult = JSON.parse(serializedResult) as unknown[];
  return wrappedResult[0];
}

function sizeInBytesOf(serializedResult: SerializedStepResult): SizeInBytes {
  return Buffer.byteLength(serializedResult, "utf8");
}
