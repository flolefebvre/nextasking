import type { Job, MinimalJob } from "bullmq";
import type { Redis } from "ioredis";

import type { RunId, StepId } from "./ids.js";
import { LostRunLockError, RETURN_0_UNLESS_LOCK_IS_HELD, jobKeyOf, lockKeyOf } from "./job-lock.js";
import { FAILED_JOB_RETENTION } from "./job-retention.js";
import { savedSleepWokenAt } from "./step-engine.js";
import type { SerializedSavedStep, StepStore } from "./step-engine.js";

const SAVED_STEPS_TIME_TO_LIVE_IN_MILLISECONDS = FAILED_JOB_RETENTION.age * 1000;

const SAVE_STEP_IF_LOCK_IS_HELD = `${RETURN_0_UNLESS_LOCK_IS_HELD}
redis.call("HSET", KEYS[3], "stc", 0)
redis.call("HSET", KEYS[2], ARGV[3], ARGV[4])
redis.call("PEXPIRE", KEYS[2], ARGV[2])
return 1
`;

const STEP_SAVED = 1;

function savedStepsKeyOfJobKey(jobKey: string): string {
  return `${jobKey}:steps`;
}

function savedStepsKeyOf(job: MinimalJob): string {
  return savedStepsKeyOfJobKey(jobKeyOf(job));
}

export async function loadSavedSteps(client: Redis, jobKey: string): Promise<Record<StepId, SerializedSavedStep>> {
  return client.hgetall(savedStepsKeyOfJobKey(jobKey));
}

export function createRedisStepStore(client: Redis, job: Job<{ runId: RunId }>, markLockLost: () => void): StepStore {
  const savedStepsKey = savedStepsKeyOf(job);
  return {
    load: () => loadSavedSteps(client, jobKeyOf(job)),
    save: async (stepId: StepId, serializedSavedStep: SerializedSavedStep) => {
      const outcome = await client.eval(
        SAVE_STEP_IF_LOCK_IS_HELD,
        3,
        lockKeyOf(job),
        savedStepsKey,
        jobKeyOf(job),
        job.token ?? "",
        SAVED_STEPS_TIME_TO_LIVE_IN_MILLISECONDS,
        stepId,
        serializedSavedStep,
      );
      if (outcome !== STEP_SAVED) {
        markLockLost();
        throw new LostRunLockError(`Step "${stepId}" of run "${job.data.runId}"`);
      }
    },
  };
}

export async function wakeSavedSleepsNow(client: Redis, jobKey: string): Promise<void> {
  const now = Date.now();
  const wokenSleepFields = Object.entries(await loadSavedSteps(client, jobKey)).flatMap(
    ([stepId, serializedSavedStep]) => {
      const wokenSleep = savedSleepWokenAt(serializedSavedStep, now);
      return wokenSleep === undefined ? [] : [stepId, wokenSleep];
    },
  );
  if (wokenSleepFields.length > 0) {
    await client.hset(savedStepsKeyOfJobKey(jobKey), ...wokenSleepFields);
  }
}

export async function removeSavedSteps(client: Redis, job: MinimalJob): Promise<void> {
  await client.unlink(savedStepsKeyOf(job));
}

export async function keepSavedStepsForFailedRunRetention(client: Redis, job: MinimalJob): Promise<void> {
  await client.pexpire(savedStepsKeyOf(job), SAVED_STEPS_TIME_TO_LIVE_IN_MILLISECONDS);
}
