import type { Job } from "bullmq";
import type { Redis } from "ioredis";

import type { DurationInMilliseconds } from "./durations.js";
import { saveJobDataIfLockIsHeld } from "./job-lock.js";
import type { IsoTimestamp } from "./run-queue.js";

export type UnknownNameHandBack = "handed back" | "given up";

export type UnknownNameTiming = {
  unknownNameHandBackDelayInMilliseconds: DurationInMilliseconds;
  unknownNameGiveUpDelayInMilliseconds: DurationInMilliseconds;
};

export async function handBackUnlessGivenUp<Data extends { firstFoundUnknownAt?: IsoTimestamp }>(
  client: Redis,
  job: Job<Data>,
  token: string | undefined,
  timing: UnknownNameTiming,
  dataDescription: string,
  dataOnHandBack: Partial<Data> = {},
): Promise<UnknownNameHandBack> {
  const now = Date.now();
  const storedFirstFoundUnknownAt = job.data.firstFoundUnknownAt;
  if (
    storedFirstFoundUnknownAt !== undefined &&
    now - Date.parse(storedFirstFoundUnknownAt) >= timing.unknownNameGiveUpDelayInMilliseconds
  ) {
    return "given up";
  }
  const firstFoundUnknownAt = storedFirstFoundUnknownAt ?? new Date(now).toISOString();
  const dataToHandBack = { ...job.data, ...dataOnHandBack, firstFoundUnknownAt };
  if (JSON.stringify(dataToHandBack) !== JSON.stringify(job.data)) {
    await saveJobDataIfLockIsHeld(client, job, token, dataToHandBack, dataDescription);
  }
  await job.moveToDelayed(now + timing.unknownNameHandBackDelayInMilliseconds, token);
  return "handed back";
}
