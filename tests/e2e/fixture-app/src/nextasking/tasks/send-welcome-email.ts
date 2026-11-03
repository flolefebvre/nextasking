import { defineTask } from "@flefebvre/nextasking";
import { z } from "zod";
import { writeRecord } from "@/nextasking/records";

export const sendWelcomeEmail = defineTask("send-welcome-email", {
  schema: z.object({ email: z.email() }),
  handler: async ({ payload, runId }) => {
    await writeRecord("send-welcome-email", { runId, payload });
  },
});
