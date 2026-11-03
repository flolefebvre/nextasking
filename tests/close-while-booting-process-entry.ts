import { z } from "zod";

import { createNextasking, defineTask } from "../src/index.js";
import { closeNextasking } from "../src/registry.js";
import { CLOSE_WHILE_BOOTING_URL_VARIABLE } from "./worker-process-config.js";

const nextasking = createNextasking({
  connection: { url: process.env[CLOSE_WHILE_BOOTING_URL_VARIABLE] ?? "", giveUpAfter: "1s" },
  tasks: [
    defineTask("send-welcome-email", { schema: z.object({ email: z.string() }), handler: async () => undefined }),
  ],
});

await nextasking.startWorker();
await closeNextasking(nextasking);
