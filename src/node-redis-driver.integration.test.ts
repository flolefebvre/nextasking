import { expect } from "vitest";

import { integrationTest } from "../tests/integration-test.js";
import { startTcpProxy } from "../tests/tcp-proxy.js";
import { withRedisClient } from "../tests/with-redis.js";
import { builtInCommandOf, scriptCommandOf } from "./redis-client.js";
import type { RedisClient, RedisScript } from "./redis-client.js";

const ECHO_SCRIPT: RedisScript = { name: "nextaskingTestEcho", lua: "return { KEYS[1], ARGV[1] }", keyCount: 1 };

function pairsOf(flatReply: unknown): [unknown, unknown][] {
  if (!Array.isArray(flatReply)) {
    throw new Error(`expected a flat array, got ${JSON.stringify(flatReply)}`);
  }
  return Array.from({ length: flatReply.length / 2 }, (_, position): [unknown, unknown] => [
    flatReply[position * 2],
    flatReply[position * 2 + 1],
  ]);
}

const HASH_FIELDS: [string, string][] = [
  ["__proto__", "polluted"],
  ["10", "ten"],
  ["2", "two"],
  ["state", "pending"],
];

async function seedHash(client: RedisClient, hashKey: string): Promise<void> {
  await client.send(builtInCommandOf("HSET", hashKey, ...HASH_FIELDS.flat()));
}

integrationTest(
  "HGETALL answers the flat array of fields and values, alone and in a batch",
  async ({ redisUrl, keyPrefix }) => {
    await withRedisClient(redisUrl, async (client) => {
      const hashKey = `${keyPrefix}hash`;
      await seedHash(client, hashKey);

      const reply = await client.send(builtInCommandOf("HGETALL", hashKey));
      const [batchResult] = await client.sendBatch([builtInCommandOf("HGETALL", hashKey)]);

      expect(Array.isArray(reply)).toBe(true);
      expect(pairsOf(reply)).toEqual(expect.arrayContaining(HASH_FIELDS));
      expect(pairsOf(reply)).toHaveLength(HASH_FIELDS.length);
      expect(batchResult).toEqual({ kind: "replied", reply });
    });
  },
);

integrationTest("the commands the modules send answer their raw RESP2 replies", async ({ redisUrl, keyPrefix }) => {
  await withRedisClient(redisUrl, async (client) => {
    const hashKey = `${keyPrefix}hash`;
    const listKey = `${keyPrefix}list`;
    const sortedSetKey = `${keyPrefix}sorted`;
    const setKey = `${keyPrefix}set`;
    await seedHash(client, hashKey);
    await client.send(builtInCommandOf("RPUSH", listKey, "first", "last"));
    await client.send(builtInCommandOf("ZADD", sortedSetKey, 1, "member"));
    await client.send(builtInCommandOf("SADD", setKey, "task"));

    const results = await client.sendBatch([
      builtInCommandOf("HMGET", hashKey, "state", "missing"),
      builtInCommandOf("GET", `${keyPrefix}missing`),
      builtInCommandOf("LRANGE", listKey, 0, -1),
      builtInCommandOf("LINDEX", listKey, -1),
      builtInCommandOf("ZCARD", sortedSetKey),
      builtInCommandOf("SMEMBERS", setKey),
      builtInCommandOf("TIME"),
      builtInCommandOf("SCAN", "0", "MATCH", `${keyPrefix}set`, "COUNT", 1000),
      builtInCommandOf("CONFIG", "GET", "maxmemory-policy"),
      builtInCommandOf("INFO"),
      builtInCommandOf("UNLINK", setKey, `${keyPrefix}missing`),
    ]);

    expect(results).toEqual([
      { kind: "replied", reply: ["pending", null] },
      { kind: "replied", reply: null },
      { kind: "replied", reply: ["first", "last"] },
      { kind: "replied", reply: "last" },
      { kind: "replied", reply: 1 },
      { kind: "replied", reply: ["task"] },
      { kind: "replied", reply: [expect.stringMatching(/^\d+$/), expect.stringMatching(/^\d+$/)] },
      { kind: "replied", reply: [expect.stringMatching(/^\d+$/), expect.any(Array)] },
      { kind: "replied", reply: ["maxmemory-policy", "noeviction"] },
      { kind: "replied", reply: expect.stringContaining("maxmemory_policy:") },
      { kind: "replied", reply: 1 },
    ]);
  });
});

integrationTest(
  "a batch answers each command, the one failing command with its own error",
  async ({ redisUrl, keyPrefix }) => {
    await withRedisClient(redisUrl, async (client) => {
      const hashKey = `${keyPrefix}hash`;
      await seedHash(client, hashKey);

      const results = await client.sendBatch([
        builtInCommandOf("HMGET", hashKey, "state"),
        builtInCommandOf("LRANGE", hashKey, 0, -1),
        builtInCommandOf("HMGET", hashKey, "2"),
      ]);

      expect(results).toEqual([
        { kind: "replied", reply: ["pending"] },
        { kind: "failed", error: expect.objectContaining({ message: expect.stringMatching(/^WRONGTYPE/) }) },
        { kind: "replied", reply: ["two"] },
      ]);
    });
  },
);

integrationTest(
  "a batch of several commands, a script among them, reaches the server in one write",
  async ({ redisUrl, keyPrefix }) => {
    const proxy = await startTcpProxy(redisUrl);
    try {
      await withRedisClient(proxy.url, async (client) => {
        const echoKey = `${keyPrefix}echo`;
        await client.send(scriptCommandOf(ECHO_SCRIPT, [echoKey], ["loaded"]));
        const requestChunks = proxy.recordRequestChunks();

        await client.sendBatch([
          builtInCommandOf("ECHO", "first"),
          scriptCommandOf(ECHO_SCRIPT, [echoKey], ["in the batch"]),
          builtInCommandOf("GET", echoKey),
          builtInCommandOf("ECHO", "last"),
        ]);

        expect(requestChunks()).toEqual([
          expect.stringMatching(/ECHO[\s\S]*first[\s\S]*EVALSHA[\s\S]*in the batch[\s\S]*GET[\s\S]*ECHO[\s\S]*last/),
        ]);
      });
    } finally {
      await proxy.stop();
    }
  },
);

integrationTest("an empty batch answers no result", async ({ redisUrl }) => {
  await withRedisClient(redisUrl, async (client) => {
    expect(await client.sendBatch([])).toEqual([]);
  });
});

integrationTest("a script answers alone and in a batch", async ({ redisUrl, keyPrefix }) => {
  await withRedisClient(redisUrl, async (client) => {
    const echoKey = `${keyPrefix}echo`;

    const reply = await client.send(scriptCommandOf(ECHO_SCRIPT, [echoKey], ["alone"]));
    const results = await client.sendBatch([
      scriptCommandOf(ECHO_SCRIPT, [echoKey], ["one"]),
      builtInCommandOf("GET", echoKey),
      scriptCommandOf(ECHO_SCRIPT, [echoKey], [2]),
    ]);

    expect(reply).toEqual([echoKey, "alone"]);
    expect(results).toEqual([
      { kind: "replied", reply: [echoKey, "one"] },
      { kind: "replied", reply: null },
      { kind: "replied", reply: [echoKey, "2"] },
    ]);
  });
});

async function echoRunOnceThenFlushed(client: RedisClient, echoKey: string): Promise<void> {
  await client.send(scriptCommandOf(ECHO_SCRIPT, [echoKey], ["before the flush"]));
  await client.send(builtInCommandOf("SCRIPT", "FLUSH"));
}

integrationTest(
  "a script still answers alone after the server's script cache is flushed",
  async ({ redisUrl, keyPrefix }) => {
    await withRedisClient(redisUrl, async (client) => {
      const echoKey = `${keyPrefix}echo`;
      await echoRunOnceThenFlushed(client, echoKey);

      const reply = await client.send(scriptCommandOf(ECHO_SCRIPT, [echoKey], ["after the flush"]));

      expect(reply).toEqual([echoKey, "after the flush"]);
    });
  },
);

integrationTest(
  "a script still answers in a batch after the server's script cache is flushed",
  async ({ redisUrl, keyPrefix }) => {
    await withRedisClient(redisUrl, async (client) => {
      const echoKey = `${keyPrefix}echo`;
      await echoRunOnceThenFlushed(client, echoKey);

      const results = await client.sendBatch([
        scriptCommandOf(ECHO_SCRIPT, [echoKey], ["one"]),
        builtInCommandOf("GET", echoKey),
        scriptCommandOf(ECHO_SCRIPT, [echoKey], ["two"]),
      ]);

      expect(results).toEqual([
        { kind: "replied", reply: [echoKey, "one"] },
        { kind: "replied", reply: null },
        { kind: "replied", reply: [echoKey, "two"] },
      ]);
    });
  },
);

integrationTest(
  "BZPOPMIN answers the popped member at once, and nothing at its timeout",
  async ({ redisUrl, keyPrefix }) => {
    await withRedisClient(redisUrl, async (client) => {
      const markerKey = `${keyPrefix}marker`;
      await client.send(builtInCommandOf("ZADD", markerKey, 0, "wake-up"));

      const hit = await client.send(builtInCommandOf("BZPOPMIN", markerKey, 1));
      const timedOut = await client.send(builtInCommandOf("BZPOPMIN", markerKey, 0.1));

      expect(hit).toEqual([markerKey, "wake-up", "0"]);
      expect(timedOut).toBeNull();
    });
  },
);
