import type { Logger } from "@flefebvre/nextasking";

async function writeAfterAFlush(write: (logger: Logger) => void) {
  const { logger } = await import("@/nextasking/logger");
  await new Promise((resolve) => setTimeout(resolve, 200));
  write(logger);
}

export async function register() {
  if (process.env.NEXT_RUNTIME !== "nodejs" || process.env.NEXTASKING_WORKER !== "on") return;
  const { nextasking } = await import("@/nextasking");
  await nextasking.startWorker({
    onWorkerNotStarted: (error) =>
      writeAfterAFlush((logger) => {
        logger.error({ reason: (error as Error).message }, "app notified: worker not started");
      }),
    onWorkerStopped: () =>
      writeAfterAFlush((logger) => {
        logger.info({}, "app notified: worker stopped");
      }),
  });
  const { logger } = await import("@/nextasking/logger");
  logger.info({}, "app registered");
}
