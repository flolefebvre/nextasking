import { Job } from "bullmq";

import type { AnyTaskDefinition, Payload, PayloadSchema } from "./definitions.js";
import { UnknownTaskError } from "./errors.js";
import { deriveJobId } from "./ids.js";
import type { RunId } from "./ids.js";
import { loadSavedSteps } from "./redis-step-store.js";
import type { ErrorRecord, RunQueue, StoredRun } from "./run-queue.js";
import { attemptOf, runStateFieldsOf } from "./run-states.js";
import type { BullmqJobState, RunStateFields } from "./run-states.js";
import { savedStepResultsOf } from "./step-engine.js";
import type { SavedStepResult } from "./step-engine.js";

export type RunError = {
  attempt: number;
  name: string;
  message: string;
  stack: string;
  failedAt: Date;
};

type PayloadOfTask<Task> = Task extends { schema: infer Schema extends PayloadSchema } ? Payload<Schema> : undefined;

type RunSummaryFields = {
  runId: RunId;
  attempt: number;
  dispatchedAt: Date;
  finishedAt?: Date;
};

export type RunOfTask<Task extends AnyTaskDefinition> = Task extends AnyTaskDefinition
  ? { task: Task["name"] } & RunSummaryFields &
      RunStateFields & {
        payload: PayloadOfTask<Task>;
        errors: RunError[];
        steps: SavedStepResult[];
      }
  : never;

export type QueueReaderConfig = {
  openRunQueue: () => RunQueue;
  tasksByName: ReadonlyMap<string, AnyTaskDefinition>;
};

export type QueueReader = {
  getRun: (taskName: string, runId: RunId) => Promise<RunOfTask<AnyTaskDefinition> | undefined>;
};

function runErrorOf(errorRecord: ErrorRecord): RunError {
  return { ...errorRecord, failedAt: new Date(errorRecord.failedAt) };
}

type RunJob = Job<StoredRun>;

function runSummaryOf(job: RunJob, bullmqState: BullmqJobState) {
  const stateFields = runStateFieldsOf({
    bullmqState,
    storedRun: job.data,
    dispatchedAt: job.timestamp,
    delayInMilliseconds: job.delay,
  });
  if (stateFields === undefined) {
    return undefined;
  }
  return {
    task: job.name,
    runId: job.data.runId,
    ...stateFields,
    attempt: attemptOf(job, stateFields.state),
    dispatchedAt: new Date(job.timestamp),
    ...(job.finishedOn === undefined ? {} : { finishedAt: new Date(job.finishedOn) }),
  };
}

export function createQueueReader(config: QueueReaderConfig): QueueReader {
  const knownTaskNamed = (taskName: string): AnyTaskDefinition => {
    const task = config.tasksByName.get(taskName);
    if (task === undefined) {
      throw new UnknownTaskError(taskName, [...config.tasksByName.keys()]);
    }
    return task;
  };

  return {
    getRun: async (taskName, runId) => {
      knownTaskNamed(taskName);
      const { bullmqQueue, client } = config.openRunQueue();
      const job = (await Job.fromId(bullmqQueue, deriveJobId(taskName, runId))) as RunJob | undefined;
      if (job === undefined) {
        return undefined;
      }
      const [bullmqState, serializedSavedStepsById] = await Promise.all([job.getState(), loadSavedSteps(client, job)]);
      const runSummary = runSummaryOf(job, bullmqState);
      if (runSummary === undefined) {
        return undefined;
      }
      return {
        ...runSummary,
        payload: job.data.payload,
        errors: job.data.errors.map(runErrorOf),
        steps: savedStepResultsOf(serializedSavedStepsById),
      } as RunOfTask<AnyTaskDefinition>;
    },
  };
}
