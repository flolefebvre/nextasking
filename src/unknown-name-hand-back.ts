import type { Job } from "bullmq";

import type { DurationInMilliseconds } from "./durations.js";
import type { IsoTimestamp } from "./run-queue.js";

export type UnknownNameHandBack = "handed back" | "given up";

export type UnknownNameTiming = {
  unknownNameHandBackDelayInMilliseconds: DurationInMilliseconds;
  unknownNameGiveUpDelayInMilliseconds: DurationInMilliseconds;
};

export async function handBackUnlessGivenUp(
  job: Job<{ firstFoundUnknownAt?: IsoTimestamp }>,
  token: string | undefined,
  timing: UnknownNameTiming,
): Promise<UnknownNameHandBack> {
  const now = Date.now();
  const { firstFoundUnknownAt } = job.data;
  if (firstFoundUnknownAt === undefined) {
    await job.updateData({ ...job.data, firstFoundUnknownAt: new Date(now).toISOString() });
  } else if (now - Date.parse(firstFoundUnknownAt) >= timing.unknownNameGiveUpDelayInMilliseconds) {
    return "given up";
  }
  await job.moveToDelayed(now + timing.unknownNameHandBackDelayInMilliseconds, token);
  return "handed back";
}
