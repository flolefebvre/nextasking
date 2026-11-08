import { expect, test } from "vitest";

import { engineKeysOf } from "./engine-keys.js";

test("every key sits under the prefix's hash tag, disjoint from the keys of 0.1.x", () => {
  const keys = engineKeysOf("nextasking");

  const everyKey = [
    keys.runRecord("run-1"),
    keys.runPayload("run-1"),
    keys.runErrors("run-1"),
    keys.runSteps("run-1"),
    keys.pending,
    keys.due,
    keys.locks,
    keys.lock("token-1"),
    keys.waitingList("send-email", undefined),
    keys.slotSet("send-email", "account-7"),
    keys.runDedup("send-email"),
    keys.eventDedup("user.signed-up"),
    keys.schedules,
    keys.schedule("send-email"),
    keys.tasks,
    keys.wakeUpMarker,
    keys.order,
    keys.runIndex("send-email", "pending"),
  ];

  expect(everyKey.filter((key) => !key.startsWith("{nextasking}:"))).toEqual([]);
  expect(everyKey.filter((key) => key.startsWith("nextasking:runs"))).toEqual([]);
});

test("a run id, a task, a concurrency key and an event name are encoded so no two keys collide", () => {
  const keys = engineKeysOf("nextasking");

  const collidingCandidates = [
    keys.runRecord("a:payload"),
    keys.runPayload("a"),
    keys.waitingList("a:b", undefined),
    keys.waitingList("a", "b"),
  ];

  expect(new Set(collidingCandidates).size).toBe(collidingCandidates.length);
});
