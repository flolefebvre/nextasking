import type { StepId } from "./ids.js";
import { resumeTimeByAppClockOf, resumeTimeFromNowOf, savedStepOf, serializedSleepOf } from "./step-engine.js";
import type { SerializedStepValue, StepStore } from "./step-engine.js";

export type InMemoryStepStore = StepStore & {
  savedSteps: () => Record<StepId, SerializedStepValue>;
};

export function createInMemoryStepStore(savedSteps: Record<StepId, SerializedStepValue> = {}): InMemoryStepStore {
  const serializedStepsById = new Map(Object.entries(savedSteps));
  return {
    readStep: async (stepId) => serializedStepsById.get(stepId),
    saveStep: async (stepId, value) => {
      if (!serializedStepsById.has(stepId)) {
        serializedStepsById.set(stepId, value);
      }
    },
    saveSleep: async (stepId, sleep) => {
      const savedStep = serializedStepsById.get(stepId);
      if (savedStep !== undefined) {
        const savedSleep = savedStepOf(stepId, savedStep);
        if (savedSleep.kind === "sleep") {
          return resumeTimeByAppClockOf(savedSleep.resumesAt);
        }
      }
      const resumesAt = resumeTimeFromNowOf(sleep);
      serializedStepsById.set(stepId, serializedSleepOf(resumesAt));
      return resumeTimeByAppClockOf(resumesAt);
    },
    savedSteps: () => Object.fromEntries(serializedStepsById),
  };
}
