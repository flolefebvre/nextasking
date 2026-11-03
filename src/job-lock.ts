import type { MinimalJob } from "bullmq";
import type { Redis } from "ioredis";

export class LostRunLockError extends Error {
  override readonly name = "LostRunLockError";

  constructor(unsavedWrite: string) {
    super(`${unsavedWrite} was not saved: this attempt no longer holds its lock, so another attempt owns it now.`);
  }
}

export const RETURN_0_UNLESS_LOCK_IS_HELD = `
if redis.call("GET", KEYS[1]) ~= ARGV[1] then
  return 0
end
`;

const SAVE_DATA_IF_LOCK_IS_HELD = `${RETURN_0_UNLESS_LOCK_IS_HELD}
redis.call("HSET", KEYS[2], "data", ARGV[2])
return 1
`;

const DATA_SAVED = 1;

export function jobKeyOf(job: MinimalJob): string {
  return `${job.queueQualifiedName}:${job.id}`;
}

export function lockKeyOf(job: MinimalJob): string {
  return `${jobKeyOf(job)}:lock`;
}

export async function saveJobDataIfLockIsHeld<Data>(
  client: Redis,
  job: MinimalJob<Data>,
  token: string | undefined,
  data: Data,
  dataDescription: string,
): Promise<void> {
  const outcome = await client.eval(
    SAVE_DATA_IF_LOCK_IS_HELD,
    2,
    lockKeyOf(job),
    jobKeyOf(job),
    token ?? "",
    JSON.stringify(data),
  );
  if (outcome !== DATA_SAVED) {
    throw new LostRunLockError(dataDescription);
  }
  job.data = data;
}
