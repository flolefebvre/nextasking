import { expect } from "vitest";

import { integrationTest } from "../../tests/integration-test.js";
import { withRedis } from "../../tests/with-redis.js";
import { engineKeysOf, hashTaggedPrefixOf } from "./engine-keys.js";
import { LUA_LIBRARY } from "./lua-library.js";

const KEYS_BUILT_BY_THE_LUA_LIBRARY = `${LUA_LIBRARY}
local runId, task, concurrencyKey, eventName, lockToken = ARGV[2], ARGV[3], ARGV[4], ARGV[5], ARGV[6]
return {
  runRecordKey(runId),
  runPayloadKey(runId),
  runErrorsKey(runId),
  runStepsKey(runId),
  lockKey(lockToken),
  waitingListKey(task, ""),
  waitingListKey(task, concurrencyKey),
  slotSetKey(task, ""),
  slotSetKey(task, concurrencyKey),
  runDedupKey(task),
  eventDedupKey(eventName),
  scheduleKey(task),
  runIndexKey(task, "waitingForConcurrencySlot"),
}
`;

const SAMPLE_SEGMENTS_BY_CASE = {
  plain: {
    runId: "run-1",
    task: "send-email",
    concurrencyKey: "account-7",
    eventName: "user.signed-up",
    lockToken: "t",
  },
  separators: { runId: "a:payload", task: "reports:purge", concurrencyKey: "b:c", eventName: "x:y", lockToken: "1:2" },
  hashTagsAndPercent: { runId: "{x}", task: "100%", concurrencyKey: "%41", eventName: "}{", lockToken: "{" },
  nonAscii: { runId: "réservé-🚀", task: "tâche", concurrencyKey: "ключ", eventName: "évènement", lockToken: "日本" },
  reservedCharacters: {
    runId: "a b/c?d#e&f=g+h",
    task: "!~*'()-_.",
    concurrencyKey: "@$,;",
    eventName: '\\"',
    lockToken: "[]",
  },
};

for (const [caseName, sample] of Object.entries(SAMPLE_SEGMENTS_BY_CASE)) {
  integrationTest(
    `the Lua library builds the same id-scoped keys as the key layout: ${caseName}`,
    async ({ redisUrl, keyPrefix }) => {
      const keys = engineKeysOf(keyPrefix);
      const expectedKeys = [
        keys.runRecord(sample.runId),
        keys.runPayload(sample.runId),
        keys.runErrors(sample.runId),
        keys.runSteps(sample.runId),
        keys.lock(sample.lockToken),
        keys.waitingList(sample.task, undefined),
        keys.waitingList(sample.task, sample.concurrencyKey),
        keys.slotSet(sample.task, undefined),
        keys.slotSet(sample.task, sample.concurrencyKey),
        keys.runDedup(sample.task),
        keys.eventDedup(sample.eventName),
        keys.schedule(sample.task),
        keys.runIndex(sample.task, "waitingForConcurrencySlot"),
      ];

      const keysBuiltByLua = await withRedis(redisUrl, (redis) =>
        redis.eval(
          KEYS_BUILT_BY_THE_LUA_LIBRARY,
          0,
          hashTaggedPrefixOf(keyPrefix),
          sample.runId,
          sample.task,
          sample.concurrencyKey,
          sample.eventName,
          sample.lockToken,
        ),
      );

      expect(keysBuiltByLua).toEqual(expectedKeys);
    },
  );
}
