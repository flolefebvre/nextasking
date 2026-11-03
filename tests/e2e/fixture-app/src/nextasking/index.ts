import { createNextasking } from "@flefebvre/nextasking";
import { sendWelcomeEmail } from "@/nextasking/tasks/send-welcome-email";

export const nextasking = createNextasking({
  connection: { url: process.env.REDIS_URL ?? "redis://localhost:6379" },
  prefix: process.env.NEXTASKING_PREFIX ?? "nextasking",
  tasks: [sendWelcomeEmail],
});

declare module "@flefebvre/nextasking" {
  interface Register {
    nextasking: typeof nextasking;
  }
}
