import "server-only";
import { Redis } from "ioredis";

let recordsClient: Redis | undefined;

export async function writeRecord(taskName: string, record: unknown) {
  recordsClient ??= new Redis(process.env.REDIS_URL ?? "redis://localhost:6379");
  await recordsClient.rpush(
    `${process.env.NEXTASKING_PREFIX ?? "nextasking"}records:${taskName}`,
    JSON.stringify(record),
  );
}
