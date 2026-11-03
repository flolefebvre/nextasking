import type { MinimalJob } from "bullmq";
import type { Redis } from "ioredis";

import { jobKeyOf } from "./job-lock.js";

const SAVE_DATA_IF_FAILED_AND_UNCHANGED = `
if not redis.call("ZSCORE", KEYS[2], ARGV[1]) then
  return 0
end
if redis.call("HGET", KEYS[1], "data") ~= ARGV[2] then
  return 0
end
redis.call("HSET", KEYS[1], "data", ARGV[3])
return 1
`;

const DATA_SAVED = 1;

export type FailedJobDataSaving = "saved" | "no longer failed or changed";

export async function saveFailedJobDataIfUnchanged<Data>(
  client: Redis,
  job: MinimalJob<Data>,
  data: Data,
): Promise<FailedJobDataSaving> {
  const outcome = await client.eval(
    SAVE_DATA_IF_FAILED_AND_UNCHANGED,
    2,
    jobKeyOf(job),
    `${job.queueQualifiedName}:failed`,
    job.id ?? "",
    JSON.stringify(job.data),
    JSON.stringify(data),
  );
  if (outcome !== DATA_SAVED) {
    return "no longer failed or changed";
  }
  job.data = data;
  return "saved";
}
