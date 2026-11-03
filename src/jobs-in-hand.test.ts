import { describe, expect, test } from "vitest";

import { createJobsInHand } from "./jobs-in-hand.js";

const JOB_ID = "job-7";

function deferred<Value>() {
  let resolve!: (value: Value) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<Value>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

describe("the jobs in hand", () => {
  test("hold a job from the moment its delivery starts, before the delivery resolves", () => {
    const jobsInHand = createJobsInHand();

    void jobsInHand.holdWhileDelivering(JOB_ID, () => deferred<object>().promise);

    expect(jobsInHand.isJobInHand(JOB_ID)).toBe(true);
    expect(jobsInHand.isJobInHand("job-8")).toBe(false);
  });

  test.each([
    ["resolves", (processing: ReturnType<typeof deferred<void>>) => processing.resolve()],
    ["rejects", (processing: ReturnType<typeof deferred<void>>) => processing.reject(new Error("Missing lock"))],
  ])("keep a delivered job in hand until its processing settles, here when it %s", async (_case, settle) => {
    const jobsInHand = createJobsInHand();
    const processing = deferred<void>();

    await jobsInHand.holdWhileDelivering(JOB_ID, async () => ({ id: JOB_ID }));
    const processed = jobsInHand.releaseWhenProcessed(JOB_ID, () => processing.promise).catch(() => undefined);
    const isInHandWhileProcessing = jobsInHand.isJobInHand(JOB_ID);
    settle(processing);
    await processed;

    expect(isInHandWhileProcessing).toBe(true);
    expect(jobsInHand.isJobInHand(JOB_ID)).toBe(false);
  });

  test("release a job whose delivery resolves with no job, or rejects", async () => {
    const jobsInHand = createJobsInHand();
    const deliveryError = new Error("Failed to add repeatable job for next iteration");

    const emptyDelivery = await jobsInHand.holdWhileDelivering(JOB_ID, async () => undefined);
    const isInHandAfterAnEmptyDelivery = jobsInHand.isJobInHand(JOB_ID);
    const rejectedDelivery = jobsInHand.holdWhileDelivering("job-8", async () => {
      throw deliveryError;
    });

    expect(emptyDelivery).toBeUndefined();
    expect(isInHandAfterAnEmptyDelivery).toBe(false);
    await expect(rejectedDelivery).rejects.toBe(deliveryError);
    expect(jobsInHand.isJobInHand("job-8")).toBe(false);
  });

  test("keep a job delivered again while its first processing is unsettled in hand until both processings settle", async () => {
    const jobsInHand = createJobsInHand();
    const firstProcessing = deferred<void>();
    const secondProcessing = deferred<void>();

    await jobsInHand.holdWhileDelivering(JOB_ID, async () => ({ id: JOB_ID }));
    const firstProcessed = jobsInHand.releaseWhenProcessed(JOB_ID, () => firstProcessing.promise);
    await jobsInHand.holdWhileDelivering(JOB_ID, async () => ({ id: JOB_ID }));
    const secondProcessed = jobsInHand.releaseWhenProcessed(JOB_ID, () => secondProcessing.promise);
    firstProcessing.resolve();
    await firstProcessed;
    const isInHandAfterTheFirstProcessing = jobsInHand.isJobInHand(JOB_ID);
    secondProcessing.resolve();
    await secondProcessed;

    expect(isInHandAfterTheFirstProcessing).toBe(true);
    expect(jobsInHand.isJobInHand(JOB_ID)).toBe(false);
  });

  test("pass the delivered job and the processing's result through", async () => {
    const jobsInHand = createJobsInHand();
    const deliveredJob = { id: JOB_ID };

    const delivery = await jobsInHand.holdWhileDelivering(JOB_ID, async () => deliveredJob);
    const processingResult = await jobsInHand.releaseWhenProcessed(JOB_ID, async () => "next job");

    expect(delivery).toBe(deliveredJob);
    expect(processingResult).toBe("next job");
  });
});
