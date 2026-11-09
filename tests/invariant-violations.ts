import type { Redis } from "ioredis";
import { expect } from "vitest";

import { engineKeysOf, hashTaggedPrefixOf } from "../src/engine/engine-keys.js";
import type { EngineKeys } from "../src/engine/engine-keys.js";
import { RUN_STATES } from "../src/run-states.js";
import type { RunState } from "../src/run-states.js";

type RunRecord = Record<string, string>;

type IndexMembership = { indexKey: string; score: string };

type StoredKeysByKind = {
  recordKeysByRunId: Map<string, string>;
  sideKeys: { runId: string; side: string }[];
  sortedIndexKeys: string[];
  slotSetKeys: string[];
  scheduleKeys: string[];
};

const SCORE_FIELD_BY_STATE: Readonly<Record<RunState, string>> = {
  delayed: "startsAt",
  pending: "order",
  waitingForConcurrencySlot: "order",
  executing: "startedAt",
  suspended: "resumesAt",
  backingOff: "nextAttemptAt",
  completed: "finishedAt",
  failed: "finishedAt",
};

const DUE_STATES: readonly RunState[] = ["delayed", "suspended", "backingOff"];

function isRunState(value: string | undefined): value is RunState {
  return RUN_STATES.some((state) => state === value);
}

async function storedKeysOf(redis: Redis, keyPrefix: string): Promise<string[]> {
  const escapedPrefix = hashTaggedPrefixOf(keyPrefix).replace(/[*?[\]\\]/g, "\\$&");
  const storedKeys: string[] = [];
  for await (const scannedKeys of redis.scanStream({ match: `${escapedPrefix}:*`, count: 1000 })) {
    storedKeys.push(...(scannedKeys as string[]));
  }
  return storedKeys;
}

function storedKeysByKindOf(storedKeys: readonly string[], keyPrefix: string): StoredKeysByKind {
  const prefixLength = hashTaggedPrefixOf(keyPrefix).length + 1;
  const storedKeysByKind: StoredKeysByKind = {
    recordKeysByRunId: new Map(),
    sideKeys: [],
    sortedIndexKeys: [],
    slotSetKeys: [],
    scheduleKeys: [],
  };
  for (const storedKey of storedKeys) {
    const [kind, ...segments] = storedKey.slice(prefixLength).split(":");
    if (kind === "run" && segments.length === 1) {
      storedKeysByKind.recordKeysByRunId.set(decodeURIComponent(segments[0]!), storedKey);
    } else if (kind === "run" && segments.length === 2) {
      storedKeysByKind.sideKeys.push({ runId: decodeURIComponent(segments[0]!), side: segments[1]! });
    } else if (kind === "runs" || kind === "waiting") {
      storedKeysByKind.sortedIndexKeys.push(storedKey);
    } else if (kind === "pending" || kind === "due" || kind === "locks") {
      storedKeysByKind.sortedIndexKeys.push(storedKey);
    } else if (kind === "slots") {
      storedKeysByKind.slotSetKeys.push(storedKey);
    } else if (kind === "schedule") {
      storedKeysByKind.scheduleKeys.push(storedKey);
    }
  }
  return storedKeysByKind;
}

function expectedIndexScoresOf(keys: EngineKeys, record: RunRecord): Map<string, string | undefined> {
  const state = record["state"] as RunState;
  const task = record["task"] ?? "";
  const stateScore = record[SCORE_FIELD_BY_STATE[state]];
  const expectedIndexScores = new Map([[keys.runIndex(task, state), stateScore]]);
  if (state === "pending") {
    expectedIndexScores.set(keys.pending, stateScore);
  } else if (state === "waitingForConcurrencySlot") {
    expectedIndexScores.set(keys.waitingList(task, record["concurrencyKey"]), stateScore);
  } else if (state === "executing") {
    expectedIndexScores.set(keys.locks, record["lockedUntil"]);
  } else if (DUE_STATES.includes(state)) {
    expectedIndexScores.set(keys.due, stateScore);
  }
  return expectedIndexScores;
}

function haveEqualScores(storedScore: string, expectedScore: string | undefined): boolean {
  return expectedScore !== undefined && Number(storedScore) === Number(expectedScore);
}

export async function invariantViolationsOf(redis: Redis, keyPrefix: string): Promise<string[]> {
  const keys = engineKeysOf(keyPrefix);
  const { recordKeysByRunId, sideKeys, sortedIndexKeys, slotSetKeys, scheduleKeys } = storedKeysByKindOf(
    await storedKeysOf(redis, keyPrefix),
    keyPrefix,
  );
  const recordsByRunId = new Map<string, RunRecord>();
  for (const [runId, recordKey] of recordKeysByRunId) {
    recordsByRunId.set(runId, await redis.hgetall(recordKey));
  }
  const membershipsByRunId = new Map<string, IndexMembership[]>();
  for (const indexKey of sortedIndexKeys) {
    const membersWithScores = await redis.zrange(indexKey, 0, -1, "WITHSCORES");
    for (let position = 0; position < membersWithScores.length; position += 2) {
      const runId = membersWithScores[position]!;
      const memberships = membershipsByRunId.get(runId) ?? [];
      memberships.push({ indexKey, score: membersWithScores[position + 1]! });
      membershipsByRunId.set(runId, memberships);
    }
  }
  const taskNames = new Set(await redis.smembers(keys.tasks));
  const violations: string[] = [];

  for (const [runId, memberships] of membershipsByRunId) {
    const record = recordsByRunId.get(runId);
    if (record === undefined || !isRunState(record["state"])) {
      violations.push(
        ...memberships.map(({ indexKey }) => `run "${runId}" sits in ${indexKey} but has no record with a known state`),
      );
      continue;
    }
    const expectedIndexScores = expectedIndexScoresOf(keys, record);
    for (const { indexKey, score } of memberships) {
      if (!expectedIndexScores.has(indexKey)) {
        violations.push(`run "${runId}" sits in ${indexKey} though its record is ${record["state"]}`);
      } else if (!haveEqualScores(score, expectedIndexScores.get(indexKey))) {
        violations.push(
          `run "${runId}" has score ${score} in ${indexKey} but ${String(expectedIndexScores.get(indexKey))} in its record`,
        );
      }
    }
  }

  for (const [runId, record] of recordsByRunId) {
    if (!isRunState(record["state"])) {
      violations.push(`run "${runId}" has a record with no known state: ${String(record["state"])}`);
      continue;
    }
    const indexKeysHoldingTheRun = new Set((membershipsByRunId.get(runId) ?? []).map(({ indexKey }) => indexKey));
    for (const expectedIndexKey of expectedIndexScoresOf(keys, record).keys()) {
      if (!indexKeysHoldingTheRun.has(expectedIndexKey)) {
        violations.push(`run "${runId}" is ${record["state"]} but missing from ${expectedIndexKey}`);
      }
    }
    if (!taskNames.has(record["task"] ?? "")) {
      violations.push(`run "${runId}" is of task "${String(record["task"])}", missing from ${keys.tasks}`);
    }
  }

  for (const { runId, side } of sideKeys) {
    if (!recordsByRunId.has(runId)) {
      violations.push(`run "${runId}" has its ${side} key but no record`);
    }
  }

  for (const [runId, record] of recordsByRunId) {
    const slotSetKey = keys.slotSet(record["task"] ?? "", record["concurrencyKey"]);
    if (record["state"] === "executing" && (await redis.sismember(slotSetKey, runId)) === 0) {
      violations.push(`run "${runId}" is executing but holds no slot in ${slotSetKey}`);
    }
  }

  for (const slotSetKey of slotSetKeys) {
    for (const runId of await redis.smembers(slotSetKey)) {
      const record = recordsByRunId.get(runId);
      const isHolderOfThisSlotSet =
        record?.["state"] === "executing" &&
        keys.slotSet(record["task"] ?? "", record["concurrencyKey"]) === slotSetKey;
      if (!isHolderOfThisSlotSet) {
        violations.push(`run "${runId}" holds a slot in ${slotSetKey} but is not an executing run of that key`);
      }
    }
  }

  const scheduledTasksWithScores = await redis.zrange(keys.schedules, 0, -1, "WITHSCORES");
  const scheduleKeysListedInSchedules = new Set<string>();
  for (let position = 0; position < scheduledTasksWithScores.length; position += 2) {
    const task = scheduledTasksWithScores[position]!;
    const score = scheduledTasksWithScores[position + 1]!;
    scheduleKeysListedInSchedules.add(keys.schedule(task));
    const nextFiringAt = await redis.hget(keys.schedule(task), "nextFiringAt");
    if (!haveEqualScores(score, nextFiringAt ?? undefined)) {
      violations.push(
        `task "${task}" has score ${score} in ${keys.schedules} but next firing ${String(nextFiringAt)} in ${keys.schedule(task)}`,
      );
    }
  }
  for (const scheduleKey of scheduleKeys) {
    if (!scheduleKeysListedInSchedules.has(scheduleKey)) {
      violations.push(`${scheduleKey} is missing from ${keys.schedules}`);
    }
  }

  return violations;
}

export async function expectNoInvariantViolations(redis: Redis, keyPrefix: string): Promise<void> {
  const violations = await invariantViolationsOf(redis, keyPrefix);
  expect(violations, `invariant violations:\n${violations.join("\n")}`).toEqual([]);
}
