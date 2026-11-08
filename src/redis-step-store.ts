import type { Job, MinimalJob } from "bullmq";
import type { Redis } from "ioredis";

import type { EpochMilliseconds } from "./durations.js";
import type { RunId, StepId } from "./ids.js";
import { LostRunLockError, RETURN_0_UNLESS_LOCK_IS_HELD, jobKeyOf, lockKeyOf } from "./job-lock.js";
import { FAILED_JOB_RETENTION } from "./job-retention.js";
import { resumeTimeFromNowOf, savedStepResultsOf } from "./step-engine.js";
import type { SavedStepResult, SerializedStepValue, StepStore } from "./step-engine.js";

const SAVED_STEPS_TIME_TO_LIVE_IN_MILLISECONDS = FAILED_JOB_RETENTION.age * 1000;

const SAVE_STEP_IF_LOCK_IS_HELD = `${RETURN_0_UNLESS_LOCK_IS_HELD}
redis.call("HSET", KEYS[3], "stc", 0)
redis.call("HSET", KEYS[2], ARGV[3], ARGV[4])
redis.call("PEXPIRE", KEYS[2], ARGV[2])
return 1
`;

const STEP_SAVED = 1;

type SavedSleep = {
  kind: "sleep";
  wakeAt: EpochMilliseconds;
  attempt: number;
};

function savedStepsKeyOfJobKey(jobKey: string): string {
  return `${jobKey}:steps`;
}

function savedStepsKeyOf(job: MinimalJob): string {
  return savedStepsKeyOfJobKey(jobKeyOf(job));
}

function savedSleepOf(serializedStep: SerializedStepValue): SavedSleep | undefined {
  if (!serializedStep.startsWith("{")) {
    return undefined;
  }
  const savedStep = JSON.parse(serializedStep) as Partial<SavedSleep>;
  return savedStep.kind === "sleep" ? (savedStep as SavedSleep) : undefined;
}

async function loadSavedSteps(client: Redis, jobKey: string): Promise<Record<StepId, SerializedStepValue>> {
  return client.hgetall(savedStepsKeyOfJobKey(jobKey));
}

export async function loadSavedStepResults(client: Redis, jobKey: string): Promise<SavedStepResult[]> {
  const serializedStepsById = await loadSavedSteps(client, jobKey);
  return savedStepResultsOf(
    Object.fromEntries(
      Object.entries(serializedStepsById).filter(([, serializedStep]) => savedSleepOf(serializedStep) === undefined),
    ),
  );
}

export function isResumingAfterSleep(
  serializedStepsById: Record<StepId, SerializedStepValue>,
  attempt: number,
): boolean {
  return Object.values(serializedStepsById).some((serializedStep) => savedSleepOf(serializedStep)?.attempt === attempt);
}

export async function isJobResumingAfterSleep(client: Redis, job: MinimalJob, attempt: number): Promise<boolean> {
  return isResumingAfterSleep(await loadSavedSteps(client, jobKeyOf(job)), attempt);
}

export function createRedisStepStore(client: Redis, job: Job<{ runId: RunId }>, attempt: number): StepStore {
  const savedStepsKey = savedStepsKeyOf(job);
  const saveStep = async (stepId: StepId, serializedStep: SerializedStepValue) => {
    const outcome = await client.eval(
      SAVE_STEP_IF_LOCK_IS_HELD,
      3,
      lockKeyOf(job),
      savedStepsKey,
      jobKeyOf(job),
      job.token ?? "",
      SAVED_STEPS_TIME_TO_LIVE_IN_MILLISECONDS,
      stepId,
      serializedStep,
    );
    if (outcome !== STEP_SAVED) {
      throw new LostRunLockError(`Step "${stepId}" of run "${job.data.runId}"`);
    }
  };
  const readStep = async (stepId: StepId) => (await client.hget(savedStepsKey, stepId)) ?? undefined;
  return {
    readStep,
    saveStep,
    saveSleep: async (stepId, sleep) => {
      const savedStep = await readStep(stepId);
      const savedSleep = savedStep === undefined ? undefined : savedSleepOf(savedStep);
      if (savedSleep !== undefined) {
        return savedSleep.wakeAt;
      }
      const wakeAt = resumeTimeFromNowOf(sleep);
      const newSleep: SavedSleep = { kind: "sleep", wakeAt, attempt };
      await saveStep(stepId, JSON.stringify(newSleep));
      return wakeAt;
    },
  };
}

export async function wakeSavedSleepsNow(client: Redis, jobKey: string): Promise<void> {
  const now = Date.now();
  const wokenSleepFields = Object.entries(await loadSavedSteps(client, jobKey)).flatMap(([stepId, serializedStep]) => {
    const savedSleep = savedSleepOf(serializedStep);
    if (savedSleep === undefined || savedSleep.wakeAt <= now) {
      return [];
    }
    const wokenSleep: SavedSleep = { ...savedSleep, wakeAt: now };
    return [stepId, JSON.stringify(wokenSleep)];
  });
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
