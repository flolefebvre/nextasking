import type { Job } from "bullmq";
import type { Redis } from "ioredis";

import { fanOutFailed, isBuiltInEventName, runFailed } from "./built-in-events.js";
import type { BuiltInEventName, FailureNoticeError, FanOutFailedPayload, RunFailedPayload } from "./built-in-events.js";
import type { DurationInMilliseconds, EpochMilliseconds } from "./durations.js";
import { saveFailedJobDataIfUnchanged } from "./failed-job-data.js";
import { deriveFailureNoticeEventId } from "./ids.js";
import type { EventId, JobId } from "./ids.js";
import type { ErrorRecord, RunQueue, StoredFanOut, StoredRun } from "./run-queue.js";

export type FailureNotice = {
  eventName: BuiltInEventName;
  eventId: EventId;
  failureNumber: number;
  payload: RunFailedPayload | FanOutFailedPayload;
};

export type SendBuiltInEvent = (eventName: BuiltInEventName, eventId: EventId, payload: unknown) => Promise<void>;

const UNSENT_NOTICE_LOOKBACK_IN_MILLISECONDS: DurationInMilliseconds = 24 * 60 * 60 * 1000;

const FAILED_JOBS_READ_PER_BATCH = 200;

type FailureCounts = { failureCount?: number; notifiedFailureCount?: number };

function unsentFailureNumberOf({ failureCount, notifiedFailureCount }: FailureCounts): number | undefined {
  return failureCount !== undefined && failureCount > (notifiedFailureCount ?? 0) ? failureCount : undefined;
}

function noticeErrorOf({ name, message, stack }: ErrorRecord): FailureNoticeError {
  return { name, message, stack };
}

function isStoredFanOut(jobData: unknown): jobData is StoredFanOut {
  return typeof jobData === "object" && jobData !== null && "listenerNames" in jobData;
}

function isStoredRun(jobData: unknown): jobData is StoredRun {
  return typeof jobData === "object" && jobData !== null && "runId" in jobData && "errors" in jobData;
}

function unsentFanOutFailureNoticeOf(eventName: string, storedFanOut: StoredFanOut): FailureNotice | undefined {
  const failureNumber = unsentFailureNumberOf(storedFanOut);
  const { finalError, eventId } = storedFanOut;
  if (failureNumber === undefined || finalError === undefined || isBuiltInEventName(eventName)) {
    return undefined;
  }
  return {
    eventName: fanOutFailed.name,
    eventId: deriveFailureNoticeEventId(eventName, eventId, failureNumber),
    failureNumber,
    payload: { eventName, eventId, error: noticeErrorOf(finalError), failedAt: finalError.failedAt },
  };
}

function unsentRunFailureNoticeOf(taskName: string, storedRun: StoredRun): FailureNotice | undefined {
  const failureNumber = unsentFailureNumberOf(storedRun);
  const { runId, reason, sourceEvent } = storedRun;
  const lastError = storedRun.errors.at(-1);
  if (
    failureNumber === undefined ||
    reason === undefined ||
    lastError === undefined ||
    (sourceEvent !== undefined && isBuiltInEventName(sourceEvent.eventName))
  ) {
    return undefined;
  }
  return {
    eventName: runFailed.name,
    eventId: deriveFailureNoticeEventId(taskName, runId, failureNumber),
    failureNumber,
    payload: {
      task: taskName,
      runId,
      reason,
      error: noticeErrorOf(lastError),
      attempt: lastError.attempt,
      failedAt: lastError.failedAt,
      ...sourceEvent,
    },
  };
}

export function unsentFailureNoticeOf(jobName: string, jobData: unknown): FailureNotice | undefined {
  if (isStoredFanOut(jobData)) {
    return unsentFanOutFailureNoticeOf(jobName, jobData);
  }
  if (isStoredRun(jobData)) {
    return unsentRunFailureNoticeOf(jobName, jobData);
  }
  return undefined;
}

async function markFailedJobNotified(client: Redis, job: Job<FailureCounts>, failureNumber: number): Promise<void> {
  await saveFailedJobDataIfUnchanged(client, job, { ...job.data, notifiedFailureCount: failureNumber });
}

export async function sendFailureNotice(
  client: Redis,
  job: Job<FailureCounts>,
  sendBuiltInEvent: SendBuiltInEvent,
): Promise<void> {
  const notice = unsentFailureNoticeOf(job.name, job.data);
  if (notice === undefined) {
    return;
  }
  await sendBuiltInEvent(notice.eventName, notice.eventId, notice.payload);
  await markFailedJobNotified(client, job, notice.failureNumber);
}

export async function sendUnsentFailureNotices(
  { bullmqQueue, client }: RunQueue,
  sendBuiltInEvent: SendBuiltInEvent,
  now: EpochMilliseconds = Date.now(),
): Promise<void> {
  const recentlyFailedJobIds: JobId[] = await client.zrangebyscore(
    bullmqQueue.toKey("failed"),
    now - UNSENT_NOTICE_LOOKBACK_IN_MILLISECONDS,
    "+inf",
  );
  for (let offset = 0; offset < recentlyFailedJobIds.length; offset += FAILED_JOBS_READ_PER_BATCH) {
    const batchOfJobIds = recentlyFailedJobIds.slice(offset, offset + FAILED_JOBS_READ_PER_BATCH);
    const failedJobs = await Promise.all(batchOfJobIds.map((jobId) => bullmqQueue.getJob(jobId)));
    for (const failedJob of failedJobs) {
      if (failedJob !== undefined) {
        await sendFailureNotice(client, failedJob as Job<FailureCounts>, sendBuiltInEvent);
      }
    }
  }
}
