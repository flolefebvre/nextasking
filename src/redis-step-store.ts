import type { Job, MinimalJob } from "bullmq";
import type { Redis } from "ioredis";

import type { RunId, StepId } from "./ids.js";
import { FAILED_JOB_RETENTION } from "./run-queue.js";
import type { SerializedSavedStep, StepStore } from "./step-engine.js";

class LostRunLockError extends Error {
  override readonly name = "LostRunLockError";

  constructor(stepId: StepId, runId: RunId) {
    super(
      `Step "${stepId}" was not saved: this attempt no longer holds the lock of run "${runId}", so another attempt owns the run now.`,
    );
  }
}

const SAVED_STEPS_TIME_TO_LIVE_IN_MILLISECONDS = FAILED_JOB_RETENTION.age * 1000;

const SAVE_STEP_IF_LOCK_IS_HELD = `
if redis.call("GET", KEYS[1]) ~= ARGV[1] then
  return 0
end
redis.call("HSET", KEYS[2], ARGV[3], ARGV[4])
redis.call("PEXPIRE", KEYS[2], ARGV[2])
return 1
`;

const STEP_SAVED = 1;

function jobKeyOf(job: MinimalJob): string {
  return `${job.queueQualifiedName}:${job.id}`;
}

function savedStepsKeyOf(job: MinimalJob): string {
  return `${jobKeyOf(job)}:steps`;
}

export function createRedisStepStore(client: Redis, job: Job<{ runId: RunId }>): StepStore {
  const savedStepsKey = savedStepsKeyOf(job);
  return {
    load: () => client.hgetall(savedStepsKey),
    save: async (stepId: StepId, serializedSavedStep: SerializedSavedStep) => {
      const outcome = await client.eval(
        SAVE_STEP_IF_LOCK_IS_HELD,
        2,
        `${jobKeyOf(job)}:lock`,
        savedStepsKey,
        job.token ?? "",
        SAVED_STEPS_TIME_TO_LIVE_IN_MILLISECONDS,
        stepId,
        serializedSavedStep,
      );
      if (outcome !== STEP_SAVED) {
        throw new LostRunLockError(stepId, job.data.runId);
      }
    },
  };
}

export async function removeSavedSteps(client: Redis, job: MinimalJob): Promise<void> {
  await client.unlink(savedStepsKeyOf(job));
}

export async function keepSavedStepsForFailedRunRetention(client: Redis, job: MinimalJob): Promise<void> {
  await client.pexpire(savedStepsKeyOf(job), SAVED_STEPS_TIME_TO_LIVE_IN_MILLISECONDS);
}
