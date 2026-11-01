import { defineTask } from "@flefebvre/nextasking";
import { db, email } from "@/lib/fakes";
import { userSignedUp } from "@/jobs/events";

export const sendWelcomeEmail = defineTask("send-welcome-email", {
  on: userSignedUp,
  retries: 5,
  handler: async ({ payload, step, run }) => {
    await email.send({ to: payload.email, template: "welcome", idempotencyKey: `${run.id}:welcome` });

    await step.sleep("wait-before-tips", "3d");

    const user = await step.run("load-user", () => db.user.find(payload.userId));
    if (user.onboarded) return;
    await email.send({ to: payload.email, template: "onboarding-tips", idempotencyKey: `${run.id}:tips` });
  },
});
