import { setTimeout as sleep } from "node:timers/promises";

import { expect, test } from "vitest";

import { measuredDurationOf, measuredRejectionOf } from "./measured-duration.js";

function blockTheEventLoopFor(durationInMilliseconds: number): void {
  const blockEndsAt = performance.now() + durationInMilliseconds;
  while (performance.now() < blockEndsAt) {
    continue;
  }
}

test("a call that waits on a timer is measured whole, with no stall to take off", async () => {
  const { result, elapsedInMilliseconds, elapsedNetOfStallInMilliseconds } = await measuredDurationOf(async () => {
    await sleep(200);
    return "waited";
  });

  expect(result).toBe("waited");
  expect(elapsedInMilliseconds).toBeGreaterThanOrEqual(195);
  expect(elapsedInMilliseconds - elapsedNetOfStallInMilliseconds).toBeLessThan(50);
});

test("a call whose process stalls has the stall taken off its duration", async () => {
  const { elapsedInMilliseconds, elapsedNetOfStallInMilliseconds } = await measuredDurationOf(async () => {
    await sleep(50);
    blockTheEventLoopFor(400);
    await sleep(50);
  });

  expect(elapsedInMilliseconds).toBeGreaterThanOrEqual(495);
  expect(elapsedNetOfStallInMilliseconds).toBeLessThan(200);
  expect(elapsedNetOfStallInMilliseconds).toBeGreaterThanOrEqual(0);
});

test("a stall that delays the call's own timer is taken off its duration", async () => {
  const blocking = sleep(100).then(() => blockTheEventLoopFor(400));

  const { elapsedInMilliseconds, elapsedNetOfStallInMilliseconds } = await measuredDurationOf(() => sleep(150));
  await blocking;

  expect(elapsedInMilliseconds).toBeGreaterThanOrEqual(495);
  expect(elapsedNetOfStallInMilliseconds).toBeLessThan(250);
});

test("a measured rejection is the call's error, or an error saying it resolved", async () => {
  const refused = await measuredRejectionOf(() => Promise.reject(new Error("refused")));
  const resolved = await measuredRejectionOf(() => Promise.resolve("answered"));

  expect(refused.result).toEqual(new Error("refused"));
  expect(resolved.result).toEqual(new Error("the call resolved"));
});

test("a call that rejects rejects the measurement with its error", async () => {
  const measuring = measuredDurationOf(() => Promise.reject(new Error("refused")));

  await expect(measuring).rejects.toThrow("refused");
});
