import { z } from "zod";

import { createNextasking, defineScheduledTask, defineTask } from "../src/index.js";
import type { Logger } from "../src/index.js";
import { closeWhenTestFinishes } from "./close-when-test-finishes.js";
import { createRecordingLogger } from "./recording-logger.js";
import { startTcpProxy } from "./tcp-proxy.js";

export async function giveUpAfterOneSecondNextaskingBehindACutProxyOf(redisUrl: string, keyPrefix: string) {
  const proxy = await startTcpProxy(redisUrl);
  await proxy.cut();
  const { logger, lines } = createRecordingLogger();
  const nextasking = createGiveUpAfterOneSecondNextasking(proxy.url, keyPrefix, { logger });
  closeWhenTestFinishes({ proxy, nextaskings: [nextasking], lines });
  return { proxy, nextasking, lines };
}

export function createGiveUpAfterOneSecondNextasking(
  url: string,
  keyPrefix: string,
  {
    logger = createRecordingLogger().logger,
    onWelcomeEmail = () => undefined,
  }: { logger?: Logger; onWelcomeEmail?: (email: string) => void } = {},
) {
  return createNextasking({
    connection: { url, giveUpAfter: "1s" },
    prefix: keyPrefix,
    logger,
    tasks: [
      defineTask("send-welcome-email", {
        schema: z.object({ email: z.string() }),
        handler: async ({ payload }) => {
          onWelcomeEmail(payload.email);
        },
      }),
      defineScheduledTask("purge-expired-sessions", {
        schedule: { cron: "0 3 * * *" },
        handler: async () => undefined,
      }),
    ],
  });
}
