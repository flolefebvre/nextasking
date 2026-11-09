import "server-only";
import { createClient } from "@redis/client";

let connectedRecordsClient: ReturnType<typeof connectRecordsClient> | undefined;

function connectRecordsClient() {
  return createClient({ url: process.env.REDIS_URL ?? "redis://localhost:6379", RESP: 2 })
    .on("error", () => undefined)
    .connect();
}

export async function writeRecord(taskName: string, record: unknown) {
  connectedRecordsClient ??= connectRecordsClient();
  const recordsClient = await connectedRecordsClient;
  await recordsClient.rPush(
    `${process.env.NEXTASKING_PREFIX ?? "nextasking"}records:${taskName}`,
    JSON.stringify(record),
  );
}
