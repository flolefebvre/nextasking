export async function register() {
  if (process.env.NEXT_RUNTIME !== "nodejs" || process.env.NEXTASKING_WORKER !== "on") return;
  const { nextasking } = await import("@/nextasking");
  if (process.env.NEXTASKING_E2E_REGISTER === "awaits-before-start-worker") {
    const { logger } = await import("@/nextasking/logger");
    await new Promise((resolve) => setTimeout(resolve, 1_500));
    logger.info({}, "app waited before startWorker");
  }
  const startingWorker = nextasking.startWorker({
    ...(process.env.NEXTASKING_E2E_START_WORKER === "invalid-concurrency" ? { concurrency: 0 } : {}),
    onWorkerNotStarted: async (error) => {
      const { logger } = await import("@/nextasking/logger");
      await new Promise((resolve) => setTimeout(resolve, 200));
      logger.error({ reason: (error as Error).message }, "app notified: worker not started");
    },
  });
  if (process.env.NEXTASKING_E2E_REGISTER === "catches-the-rejection") {
    try {
      await startingWorker;
    } catch (error) {
      const { logger } = await import("@/nextasking/logger");
      await new Promise((resolve) => setTimeout(resolve, 500));
      logger.error({ reason: (error as Error).message }, "app caught: worker not started");
      return;
    }
  } else {
    await startingWorker;
  }
  const { logger } = await import("@/nextasking/logger");
  logger.info({}, "app registered");
}
