import { createNextasking, defineScheduledTask } from "@flefebvre/nextasking";
import type { DispatchedRun } from "@flefebvre/nextasking";

const purgeExpiredSessionsTask = defineScheduledTask("purge-expired-sessions", {
  schedule: { cron: "0 3 * * *" },
  handler: async ({ scheduledAt, abortSignal }) => {
    await Promise.resolve(abortSignal.aborted ? undefined : scheduledAt);
  },
});

export const nextasking = createNextasking({
  connection: { url: "redis://localhost:6379" },
  tasks: [purgeExpiredSessionsTask],
});

export async function purgeNow(): Promise<DispatchedRun> {
  return nextasking.dispatch("purge-expired-sessions");
}
