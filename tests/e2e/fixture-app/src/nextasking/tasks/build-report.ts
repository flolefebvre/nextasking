import { defineTask } from "@flefebvre/nextasking";
import { setTimeout as sleep } from "node:timers/promises";
import { z } from "zod";
import { writeRecord } from "@/nextasking/records";

export const buildReport = defineTask("build-report", {
  schema: z.object({ reportId: z.string() }),
  handler: async ({ payload, runId, step }) => {
    const rowCount = await step.run("query-rows", async () => {
      await sleep(2000);
      return 42;
    });
    await writeRecord("build-report", { runId, payload, rowCount });
  },
});
