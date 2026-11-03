export type JobsInHand = {
  isJobInHand: (jobId: string) => boolean;
  holdWhileDelivering: <Delivery>(jobId: string | undefined, deliver: () => Promise<Delivery>) => Promise<Delivery>;
  releaseWhenProcessed: <ProcessingResult>(
    jobId: string | undefined,
    process: () => Promise<ProcessingResult>,
  ) => Promise<ProcessingResult>;
};

export function createJobsInHand(): JobsInHand {
  const holdCountByJobId = new Map<string, number>();
  const hold = (jobId: string | undefined) => {
    if (jobId !== undefined) {
      holdCountByJobId.set(jobId, (holdCountByJobId.get(jobId) ?? 0) + 1);
    }
  };
  const release = (jobId: string | undefined) => {
    const holdCount = jobId === undefined ? undefined : holdCountByJobId.get(jobId);
    if (jobId === undefined || holdCount === undefined) {
      return;
    }
    if (holdCount > 1) {
      holdCountByJobId.set(jobId, holdCount - 1);
    } else {
      holdCountByJobId.delete(jobId);
    }
  };
  return {
    isJobInHand: (jobId) => holdCountByJobId.has(jobId),
    holdWhileDelivering: async (jobId, deliver) => {
      hold(jobId);
      let delivery: Awaited<ReturnType<typeof deliver>>;
      try {
        delivery = await deliver();
      } catch (deliveryError) {
        release(jobId);
        throw deliveryError;
      }
      if (delivery === undefined) {
        release(jobId);
      }
      return delivery;
    },
    releaseWhenProcessed: async (jobId, process) => {
      try {
        return await process();
      } finally {
        release(jobId);
      }
    },
  };
}
