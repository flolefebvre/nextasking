import type { Redis } from "ioredis";
import { expect } from "vitest";

import { engineKeysOf, hashTaggedPrefixOf, runIdentityOf } from "../src/engine/engine-keys.js";
import type { EngineKeys } from "../src/engine/engine-keys.js";
import type { EpochMilliseconds } from "../src/durations.js";
import { isRunState } from "../src/run-states.js";
import type { RunState } from "../src/run-states.js";

export type StoredValue =
  | { type: "hash"; fieldsByName: Record<string, string> }
  | { type: "zset"; scoresByMember: Map<string, string> }
  | { type: "set"; members: Set<string> }
  | { type: "string" | "list" | "stream" | "none" };

export type StoredKeysSnapshot = {
  serverInstantInMilliseconds: EpochMilliseconds;
  storedValuesByKey: Map<string, StoredValue>;
};

type RunRecord = Record<string, string>;

type IndexMembership = { indexKey: string; score: string };

type KeyKind = "record" | "side" | "sortedIndex" | "slotSet" | "schedule" | "other";

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

const EXPECTED_TYPE_BY_KEY_KIND: Readonly<Record<KeyKind, StoredValue["type"] | undefined>> = {
  record: "hash",
  side: undefined,
  sortedIndex: "zset",
  slotSet: "set",
  schedule: "hash",
  other: undefined,
};

const SNAPSHOT_SCRIPT = `
local serverTime = redis.call("TIME")
local storedValues = {}
for _, key in ipairs(redis.call("KEYS", ARGV[1])) do
  local keyType = redis.call("TYPE", key)["ok"]
  local content = {}
  if keyType == "hash" then
    content = redis.call("HGETALL", key)
  elseif keyType == "zset" then
    content = redis.call("ZRANGE", key, 0, -1, "WITHSCORES")
  elseif keyType == "set" then
    content = redis.call("SMEMBERS", key)
  end
  table.insert(storedValues, { key, keyType, content })
end
return { serverTime[1], serverTime[2], storedValues }
`;

function storedValueOf(type: string, content: string[]): StoredValue {
  if (type === "hash") {
    const fieldsByName: Record<string, string> = {};
    for (let position = 0; position < content.length; position += 2) {
      fieldsByName[content[position]!] = content[position + 1]!;
    }
    return { type, fieldsByName };
  }
  if (type === "zset") {
    const scoresByMember = new Map<string, string>();
    for (let position = 0; position < content.length; position += 2) {
      scoresByMember.set(content[position]!, content[position + 1]!);
    }
    return { type, scoresByMember };
  }
  if (type === "set") {
    return { type, members: new Set(content) };
  }
  return { type: type as "string" | "list" | "stream" | "none" };
}

export async function storedKeysSnapshotOf(redis: Redis, keyPrefix: string): Promise<StoredKeysSnapshot> {
  const escapedPrefix = hashTaggedPrefixOf(keyPrefix).replace(/[*?[\]\\]/g, "\\$&");
  const [seconds, microseconds, storedValues] = (await redis.eval(SNAPSHOT_SCRIPT, 0, `${escapedPrefix}:*`)) as [
    string,
    string,
    [string, string, string[]][],
  ];
  return {
    serverInstantInMilliseconds: Number(seconds) * 1000 + Math.floor(Number(microseconds) / 1000),
    storedValuesByKey: new Map(storedValues.map(([key, type, content]) => [key, storedValueOf(type, content)])),
  };
}

function keyKindOf(storedKey: string, keyPrefix: string): { kind: KeyKind; segments: string[] } {
  const [kind, ...segments] = storedKey.slice(hashTaggedPrefixOf(keyPrefix).length + 1).split(":");
  if (kind === "run" && segments.length === 1) {
    return { kind: "record", segments };
  }
  if (kind === "run" && segments.length === 2) {
    return { kind: "side", segments };
  }
  if (kind === "runs" || kind === "waiting" || kind === "pending" || kind === "due" || kind === "locks") {
    return { kind: "sortedIndex", segments };
  }
  if (kind === "slots") {
    return { kind: "slotSet", segments };
  }
  if (kind === "schedules") {
    return { kind: "sortedIndex", segments };
  }
  if (kind === "schedule") {
    return { kind: "schedule", segments };
  }
  return { kind: "other", segments };
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

function hashFieldsOf(storedValue: StoredValue | undefined): Record<string, string> | undefined {
  return storedValue?.type === "hash" ? storedValue.fieldsByName : undefined;
}

function scoresByMemberOf(storedValue: StoredValue | undefined): Map<string, string> {
  return storedValue?.type === "zset" ? storedValue.scoresByMember : new Map();
}

function membersOf(storedValue: StoredValue | undefined): Set<string> {
  return storedValue?.type === "set" ? storedValue.members : new Set();
}

export function storedKeyViolationsIn(snapshot: StoredKeysSnapshot, keyPrefix: string): string[] {
  const keys = engineKeysOf(keyPrefix);
  const { storedValuesByKey } = snapshot;
  const violations: string[] = [];
  const recordsByStoredRunId = new Map<string, RunRecord>();
  const sideKeys: { storedRunId: string; side: string }[] = [];
  const membershipsByStoredRunId = new Map<string, IndexMembership[]>();
  const slotSetKeys: string[] = [];
  const scheduleKeys: string[] = [];

  for (const [storedKey, storedValue] of storedValuesByKey) {
    const { kind, segments } = keyKindOf(storedKey, keyPrefix);
    const expectedType = EXPECTED_TYPE_BY_KEY_KIND[kind];
    if (expectedType !== undefined && storedValue.type !== expectedType) {
      violations.push(`${storedKey} is a ${storedValue.type}, not a ${expectedType}`);
      continue;
    }
    if (kind === "record" && storedValue.type === "hash") {
      recordsByStoredRunId.set(decodeURIComponent(segments[0]!), storedValue.fieldsByName);
    } else if (kind === "side") {
      sideKeys.push({ storedRunId: decodeURIComponent(segments[0]!), side: segments[1]! });
    } else if (kind === "sortedIndex" && storedKey !== keys.schedules) {
      for (const [storedRunId, score] of scoresByMemberOf(storedValue)) {
        const memberships = membershipsByStoredRunId.get(storedRunId) ?? [];
        memberships.push({ indexKey: storedKey, score });
        membershipsByStoredRunId.set(storedRunId, memberships);
      }
    } else if (kind === "slotSet") {
      slotSetKeys.push(storedKey);
    } else if (kind === "schedule") {
      scheduleKeys.push(storedKey);
    }
  }
  const taskNames = membersOf(storedValuesByKey.get(keys.tasks));

  for (const [storedRunId, memberships] of membershipsByStoredRunId) {
    const record = recordsByStoredRunId.get(storedRunId);
    if (record === undefined || !isRunState(record["state"])) {
      violations.push(
        ...memberships.map(
          ({ indexKey }) => `run "${storedRunId}" sits in ${indexKey} but has no record with a known state`,
        ),
      );
      continue;
    }
    const expectedIndexScores = expectedIndexScoresOf(keys, record);
    for (const { indexKey, score } of memberships) {
      if (!expectedIndexScores.has(indexKey)) {
        violations.push(`run "${storedRunId}" sits in ${indexKey} though its record is ${record["state"]}`);
      } else if (!haveEqualScores(score, expectedIndexScores.get(indexKey))) {
        violations.push(
          `run "${storedRunId}" has score ${score} in ${indexKey} but ${String(expectedIndexScores.get(indexKey))} in its record`,
        );
      }
    }
  }

  for (const [storedRunId, record] of recordsByStoredRunId) {
    const runIdentity = runIdentityOf(storedRunId);
    if (runIdentity === undefined) {
      violations.push(`run "${storedRunId}" has a record under an id that is not a stored run id`);
    } else if (runIdentity.task !== record["task"]) {
      violations.push(
        `run "${storedRunId}" has a record of task "${String(record["task"])}" under an id of task "${runIdentity.task}"`,
      );
    }
    if (!isRunState(record["state"])) {
      violations.push(`run "${storedRunId}" has a record with no known state: ${String(record["state"])}`);
      continue;
    }
    const indexKeysHoldingTheRun = new Set(
      (membershipsByStoredRunId.get(storedRunId) ?? []).map(({ indexKey }) => indexKey),
    );
    for (const expectedIndexKey of expectedIndexScoresOf(keys, record).keys()) {
      if (!indexKeysHoldingTheRun.has(expectedIndexKey)) {
        violations.push(`run "${storedRunId}" is ${record["state"]} but missing from ${expectedIndexKey}`);
      }
    }
    if (!taskNames.has(record["task"] ?? "")) {
      violations.push(`run "${storedRunId}" is of task "${String(record["task"])}", missing from ${keys.tasks}`);
    }
    const slotSetKey = keys.slotSet(record["task"] ?? "", record["concurrencyKey"]);
    if (record["state"] === "executing" && !membersOf(storedValuesByKey.get(slotSetKey)).has(storedRunId)) {
      violations.push(`run "${storedRunId}" is executing but holds no slot in ${slotSetKey}`);
    }
  }

  for (const { storedRunId, side } of sideKeys) {
    if (!recordsByStoredRunId.has(storedRunId)) {
      violations.push(`run "${storedRunId}" has its ${side} key but no record`);
    }
  }

  for (const slotSetKey of slotSetKeys) {
    for (const storedRunId of membersOf(storedValuesByKey.get(slotSetKey))) {
      const record = recordsByStoredRunId.get(storedRunId);
      const isHolderOfThisSlotSet =
        record?.["state"] === "executing" &&
        keys.slotSet(record["task"] ?? "", record["concurrencyKey"]) === slotSetKey;
      if (!isHolderOfThisSlotSet) {
        violations.push(`run "${storedRunId}" holds a slot in ${slotSetKey} but is not an executing run of that key`);
      }
    }
  }

  const scheduleKeysListedInSchedules = new Set<string>();
  for (const [task, score] of scoresByMemberOf(storedValuesByKey.get(keys.schedules))) {
    scheduleKeysListedInSchedules.add(keys.schedule(task));
    const nextFiringAt = hashFieldsOf(storedValuesByKey.get(keys.schedule(task)))?.["nextFiringAt"];
    if (!haveEqualScores(score, nextFiringAt)) {
      violations.push(
        `task "${task}" has score ${score} in ${keys.schedules} but next firing ${String(nextFiringAt ?? null)} in ${keys.schedule(task)}`,
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

export function runLeftExecutingSentenceOf(storedRunId: string): string {
  return `run "${storedRunId}" is still executing under a lock that has not lapsed`;
}

export function runsLeftExecutingIn(snapshot: StoredKeysSnapshot, keyPrefix: string): string[] {
  const runsLeftExecuting: string[] = [];
  for (const [storedKey, storedValue] of snapshot.storedValuesByKey) {
    const record = hashFieldsOf(storedValue);
    if (keyKindOf(storedKey, keyPrefix).kind !== "record" || record?.["state"] !== "executing") {
      continue;
    }
    if (Number(record["lockedUntil"]) > snapshot.serverInstantInMilliseconds) {
      runsLeftExecuting.push(runLeftExecutingSentenceOf(decodeURIComponent(storedKey.split(":").at(-1)!)));
    }
  }
  return runsLeftExecuting;
}

export async function storedKeyViolationsOf(redis: Redis, keyPrefix: string): Promise<string[]> {
  return storedKeyViolationsIn(await storedKeysSnapshotOf(redis, keyPrefix), keyPrefix);
}

export async function expectNoStoredKeyViolations(redis: Redis, keyPrefix: string): Promise<void> {
  const violations = await storedKeyViolationsOf(redis, keyPrefix);
  expect(violations, `stored key violations:\n${violations.join("\n")}`).toEqual([]);
}
