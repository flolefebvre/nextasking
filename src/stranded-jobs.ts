import type { EpochMilliseconds } from "./durations.js";
import type { Logger } from "./logger.js";

export type ActiveJob = {
  jobId: string;
  lockToken: string | undefined;
  processedOn: EpochMilliseconds | undefined;
};

const NEXT_JOB_HAND_BACK_FAILED = "next job hand-back failed";

export async function handBackStrandedJobs({
  workerId,
  restoredAt,
  isJobInHand,
  readActiveJobs,
  moveActiveJobToWait,
  logger,
}: {
  workerId: string;
  restoredAt: EpochMilliseconds;
  isJobInHand: (jobId: string) => boolean;
  readActiveJobs: () => Promise<ActiveJob[]>;
  moveActiveJobToWait: (jobId: string, lockToken: string) => Promise<void>;
  logger: Logger;
}): Promise<void> {
  let activeJobs: ActiveJob[];
  try {
    activeJobs = await readActiveJobs();
  } catch (readError) {
    logger.warn({ err: readError }, NEXT_JOB_HAND_BACK_FAILED);
    return;
  }
  const lockTokenPrefixOfThisWorker = `${workerId}:`;
  for (const { jobId, lockToken, processedOn } of activeJobs) {
    const isStranded =
      lockToken?.startsWith(lockTokenPrefixOfThisWorker) === true &&
      !isJobInHand(jobId) &&
      processedOn !== undefined &&
      processedOn < restoredAt;
    if (isStranded) {
      await moveActiveJobToWait(jobId, lockToken).catch((moveError: unknown) => {
        logger.warn({ jobId, err: moveError }, NEXT_JOB_HAND_BACK_FAILED);
      });
    }
  }
}
