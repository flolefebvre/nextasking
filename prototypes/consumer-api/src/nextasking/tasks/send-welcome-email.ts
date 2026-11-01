import { defineListener } from "@flefebvre/nextasking";
import { db, email } from "@/lib/fakes";
import { userSignedUp } from "@/nextasking/events";

export const sendWelcomeEmail = defineListener("send-welcome-email", {
  on: userSignedUp,
  retries: 5,
  handler: async ({ payload, step, runId }) => {
    await email.send({ to: payload.email, template: "welcome", idempotencyKey: `${runId}:welcome` });

    await step.sleep("wait-before-tips", "3d");

    const user = await step.run("load-user", () => db.user.find(payload.userId));
    if (user.onboarded) return;
    await email.send({ to: payload.email, template: "onboarding-tips", idempotencyKey: `${runId}:tips` });
  },
});
