import type { Redis } from "ioredis";
import { expect } from "vitest";

import { integrationTest } from "../../tests/integration-test.js";
import { expectNoInvariantViolations } from "../../tests/invariant-violations.js";
import { serverInstantInMillisecondsOf } from "../../tests/server-instant.js";
import { withRedis } from "../../tests/with-redis.js";
import { engineKeysOf, hashTaggedPrefixOf } from "./engine-keys.js";
import { RECORD_VERSION } from "./engine.js";
import { ENQUEUE_KEYS_ASSIGNMENT, enqueueKeysOf } from "./enqueue-script.js";
import { LUA_LIBRARY } from "./lua-library.js";

const PROBE_SCRIPT = `${LUA_LIBRARY}
${ENQUEUE_KEYS_ASSIGNMENT}
local probe, runId = ARGV[3], ARGV[4]
if probe == "write after the version check" then
  local refusal = unknownVersionRefusalOf(runId)
  if refusal then
    return refusal
  end
  redis.call("HSET", runRecordKey(runId), "written", "yes")
  return { "written" }
elseif probe == "current instant" then
  return currentInstantInMilliseconds()
elseif probe == "create a delayed run" then
  createRun({
    runId = runId,
    task = "send-email",
    payload = "{}",
    retries = 3,
    concurrencyKey = "",
    eventName = "",
    eventId = "",
    delayInMilliseconds = 60000,
  }, currentInstantInMilliseconds())
  return { "created" }
elseif probe == "move the delayed run to pending" then
  leaveState(runId, "send-email", "delayed")
  enterPending(runId, "send-email", nil)
  return { "moved" }
end
`;

function runProbe(redis: Redis, keyPrefix: string, probe: string, runId = "run-1", supportedVersion = RECORD_VERSION) {
  const probeKeys = enqueueKeysOf(engineKeysOf(keyPrefix));
  return redis.eval(
    PROBE_SCRIPT,
    probeKeys.length,
    ...probeKeys,
    hashTaggedPrefixOf(keyPrefix),
    supportedVersion,
    probe,
    runId,
  );
}

const UNKNOWN_VERSIONS = [
  { name: "a newer version", storedVersion: "2", reportedVersion: "2" },
  { name: "no version", storedVersion: undefined, reportedVersion: null },
  { name: "a version that is not a number", storedVersion: "two", reportedVersion: "two" },
];

for (const { name, storedVersion, reportedVersion } of UNKNOWN_VERSIONS) {
  integrationTest(
    `a record of ${name} is refused with unknownVersion before any write`,
    async ({ redisUrl, keyPrefix }) => {
      await withRedis(redisUrl, async (redis) => {
        const recordKey = engineKeysOf(keyPrefix).runRecord("run-1");
        await runProbe(redis, keyPrefix, "create a delayed run");
        await (storedVersion === undefined
          ? redis.hdel(recordKey, "version")
          : redis.hset(recordKey, "version", storedVersion));
        const recordBefore = await redis.hgetall(recordKey);

        const reply = await runProbe(redis, keyPrefix, "write after the version check");

        expect(reply).toEqual(["unknownVersion", reportedVersion]);
        expect(await redis.hgetall(recordKey)).toEqual(recordBefore);
        await expectNoInvariantViolations(redis, keyPrefix);
      });
    },
  );
}

integrationTest(
  "a missing record reads as an unknown version, so a script replying notFound or stale checks the record or the lock token before the version",
  async ({ redisUrl, keyPrefix }) => {
    await withRedis(redisUrl, async (redis) => {
      const reply = await runProbe(redis, keyPrefix, "write after the version check", "missing-run");

      expect(reply).toEqual(["unknownVersion", null]);
      expect(await redis.exists(engineKeysOf(keyPrefix).runRecord("missing-run"))).toBe(0);
      await expectNoInvariantViolations(redis, keyPrefix);
    });
  },
);

integrationTest(
  "a record of the supported version or an older one passes the version check",
  async ({ redisUrl, keyPrefix }) => {
    await withRedis(redisUrl, async (redis) => {
      const keys = engineKeysOf(keyPrefix);
      await runProbe(redis, keyPrefix, "create a delayed run", "current-run");
      await runProbe(redis, keyPrefix, "create a delayed run", "older-run");

      const replyOnTheCurrentVersion = await runProbe(redis, keyPrefix, "write after the version check", "current-run");
      const replyOnAnOlderVersion = await runProbe(
        redis,
        keyPrefix,
        "write after the version check",
        "older-run",
        RECORD_VERSION + 1,
      );

      expect(replyOnTheCurrentVersion).toEqual(["written"]);
      expect(replyOnAnOlderVersion).toEqual(["written"]);
      expect(await redis.hget(keys.runRecord("older-run"), "version")).toBe(String(RECORD_VERSION));
      await expectNoInvariantViolations(redis, keyPrefix);
    });
  },
);

integrationTest("the library's clock is the server's TIME", async ({ redisUrl, keyPrefix }) => {
  await withRedis(redisUrl, async (redis) => {
    const instantBefore = await serverInstantInMillisecondsOf(redis);

    const instantInTheScript = await runProbe(redis, keyPrefix, "current instant");

    const instantAfter = await serverInstantInMillisecondsOf(redis);
    expect(instantInTheScript).toBeGreaterThanOrEqual(instantBefore);
    expect(instantInTheScript).toBeLessThanOrEqual(instantAfter);
    await expectNoInvariantViolations(redis, keyPrefix);
  });
});

integrationTest(
  "a run moved from delayed to pending leaves its record and indexes in agreement",
  async ({ redisUrl, keyPrefix }) => {
    await withRedis(redisUrl, async (redis) => {
      const keys = engineKeysOf(keyPrefix);
      await runProbe(redis, keyPrefix, "create a delayed run");

      await runProbe(redis, keyPrefix, "move the delayed run to pending");

      expect(await redis.hget(keys.runRecord("run-1"), "state")).toBe("pending");
      expect(await redis.zrange(keys.pending, 0, -1)).toEqual(["run-1"]);
      expect(await redis.zcard(keys.due)).toBe(0);
      expect(await redis.zcard(keys.runIndex("send-email", "delayed"))).toBe(0);
      expect(await redis.zcard(keys.wakeUpMarker)).toBe(1);
      await expectNoInvariantViolations(redis, keyPrefix);
    });
  },
);
