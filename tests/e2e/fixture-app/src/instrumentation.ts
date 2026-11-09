export async function register() {
  if (process.env.NEXT_RUNTIME !== "nodejs" || process.env.NEXTASKING_WORKER !== "on") return;
  const { nextasking } = await import("@/nextasking");
  await nextasking.startWorker({
    onWorkerNotStarted: async (error) => {
      const { logger } = await import("@/nextasking/logger");
      await new Promise((resolve) => setTimeout(resolve, 200));
      logger.error({ reason: (error as Error).message }, "app notified: worker not started");
    },
  });
  const { logger } = await import("@/nextasking/logger");
  logger.info({}, "app registered");
}
