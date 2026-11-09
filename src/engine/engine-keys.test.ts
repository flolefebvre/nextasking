import { expect, test } from "vitest";

import { engineKeysOf, runIdentityOf, storedRunIdOf } from "./engine-keys.js";

test("a stored run id reads back to the task and the run id it was built from", () => {
  const identities = [
    { task: "send-email", runId: "order-1" },
    { task: "a", runId: "b:c" },
    { task: "a:b", runId: "c" },
    { task: "100%", runId: "%3A" },
    { task: "tâche 🚀", runId: "réservé:🚀:" },
  ];

  const readBack = identities.map(({ task, runId }) => runIdentityOf(storedRunIdOf(task, runId)));

  expect(readBack).toEqual(identities);
});

test("a task holding the separator and a run id holding it never share a stored run id", () => {
  const storedRunIds = [storedRunIdOf("a", "b:c"), storedRunIdOf("a:b", "c"), storedRunIdOf("a%3Ab", "c")];

  expect(new Set(storedRunIds).size).toBe(storedRunIds.length);
  expect(storedRunIds).toEqual(["a:b:c", "a%3Ab:c", "a%253Ab:c"]);
});

test("a stored run id costs the task and one byte when the task needs no encoding", () => {
  expect(storedRunIdOf("send-order-receipt-1", "6f9619ff-8b86-d011-b42d-00cf4fc964ff")).toBe(
    "send-order-receipt-1:6f9619ff-8b86-d011-b42d-00cf4fc964ff",
  );
});

test.each([
  ["no separator", "order-1"],
  ["an empty task", ":order-1"],
  ["an empty run id", "send-email:"],
  ["a task that is not percent-encoded", "100%:order-1"],
  ["a task holding a malformed escape", "%E0%A4%A:order-1"],
  ["a task not encoded as storedRunIdOf encodes it", "send email:order-1"],
])("a member with %s is not a stored run id", (_description, member) => {
  expect(runIdentityOf(member)).toBeUndefined();
});

test.each([
  ["", "order-1"],
  ["send-email", ""],
])("a run of task %j with run id %j has no stored run id", (task, runId) => {
  expect(() => storedRunIdOf(task, runId)).toThrow("invalid argument: a run is identified by a task and a run id");
});

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

test("isScheduleKey holds for the schedules set and every schedule record, and for no other key", () => {
  const keys = engineKeysOf("e2e*[app]?");
  const otherKeys = engineKeysOf("e2e*[app]?:other");

  const scheduleKeys = [keys.schedules, keys.schedule("build-report"), keys.schedule("a:b"), keys.schedule("a*?[b]\\")];
  const otherKeysOfThePrefix = [
    keys.runRecord("schedule:build-report"),
    keys.runIndex("schedule", "pending"),
    keys.runDedup("schedules"),
    keys.tasks,
    keys.due,
    otherKeys.schedule("build-report"),
    otherKeys.schedules,
  ];

  expect(scheduleKeys.filter((key) => !keys.isScheduleKey(key))).toEqual([]);
  expect(otherKeysOfThePrefix.filter((key) => keys.isScheduleKey(key))).toEqual([]);
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
