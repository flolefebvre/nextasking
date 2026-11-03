import type { Job } from "bullmq";
import type { Redis } from "ioredis";
import { describe, expect, test, vi } from "vitest";

import { handBackUnlessGivenUp } from "./unknown-name-hand-back.js";

const timing = { unknownNameHandBackDelayInMilliseconds: 10_000, unknownNameGiveUpDelayInMilliseconds: 1_800_000 };

function stubbedHandBack(lockCheckOutcome: number) {
  const evalScript = vi.fn(async () => lockCheckOutcome);
  const moveToDelayed = vi.fn(async () => undefined);
  const job = { id: "job-1", queueQualifiedName: "nextasking:runs", data: { eventId: "event-1" }, moveToDelayed };
  return {
    client: { eval: evalScript } as unknown as Redis,
    job: job as unknown as Job<{ eventId: string; firstFoundUnknownAt?: string }>,
    evalScript,
    moveToDelayed,
  };
}

describe("handBackUnlessGivenUp", () => {
  test("saves the first time the name was found unknown, then hands the job back", async () => {
    const { client, job, evalScript, moveToDelayed } = stubbedHandBack(1);

    const handBack = await handBackUnlessGivenUp(client, job, "token-1", timing, 'The data of "event-1"');

    expect(handBack).toBe("handed back");
    expect(evalScript).toHaveBeenCalledWith(
      expect.any(String),
      2,
      "nextasking:runs:job-1:lock",
      "nextasking:runs:job-1",
      "token-1",
      expect.stringContaining('"firstFoundUnknownAt"'),
    );
    expect(job.data.firstFoundUnknownAt).toEqual(expect.any(String));
    expect(moveToDelayed).toHaveBeenCalledOnce();
  });

  test("throws without handing back when the attempt no longer holds the lock", async () => {
    const { client, job, moveToDelayed } = stubbedHandBack(0);

    await expect(handBackUnlessGivenUp(client, job, "stale-token", timing, 'The data of "event-1"')).rejects.toThrow(
      'The data of "event-1" was not saved: this attempt no longer holds its lock, so another attempt owns it now.',
    );
    expect(job.data).toEqual({ eventId: "event-1" });
    expect(moveToDelayed).not.toHaveBeenCalled();
  });
});
