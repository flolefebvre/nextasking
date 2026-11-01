import { defineTask } from "@flefebvre/nextasking";
import { db } from "@/lib/fakes";

export const purgeExpiredSessions = defineTask("purge-expired-sessions", {
  schedule: { cron: "0 3 * * *", timezone: "Europe/Paris" },
  handler: async ({ logger }) => {
    const deletedCount = await db.session.deleteExpired();
    logger.info("expired sessions purged", { deletedCount });
  },
});
