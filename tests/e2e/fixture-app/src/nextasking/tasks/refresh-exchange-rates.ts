import { defineScheduledTask } from "@flefebvre/nextasking";
import { writeRecord } from "@/nextasking/records";

export const refreshExchangeRatesTask = defineScheduledTask("refresh-exchange-rates", {
  schedule: { cron: "*/5 * * * * *" },
  handler: async ({ runId, scheduledAt }) => {
    await writeRecord("refresh-exchange-rates", { runId, scheduledAt });
  },
});
