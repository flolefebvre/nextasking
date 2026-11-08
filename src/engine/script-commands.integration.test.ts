import type { Redis } from "ioredis";
import { expect } from "vitest";

import { integrationTest } from "../../tests/integration-test.js";
import { withRedis } from "../../tests/with-redis.js";
import { defineScriptCommand } from "./script-commands.js";
import type { ScriptCommand } from "./script-commands.js";

const ECHO_SCRIPT = `return { KEYS[1], ARGV[1] }`;

async function echoCommandRunOnceThenFlushed(redis: Redis, echoedKey: string): Promise<ScriptCommand> {
  const echo = defineScriptCommand(redis, "nextaskingTestEcho", ECHO_SCRIPT, 1);
  await echo.call([echoedKey], ["before the flush"]);
  await redis.script("FLUSH");
  return echo;
}

integrationTest(
  "a script command still runs after the server's script cache is flushed",
  async ({ redisUrl, keyPrefix }) => {
    await withRedis(redisUrl, async (redis) => {
      const echo = await echoCommandRunOnceThenFlushed(redis, `${keyPrefix}echo`);

      const reply = await echo.call([`${keyPrefix}echo`], ["after the flush"]);

      expect(reply).toEqual([`${keyPrefix}echo`, "after the flush"]);
    });
  },
);

integrationTest(
  "a script command appended to a pipeline still runs after the server's script cache is flushed",
  async ({ redisUrl, keyPrefix }) => {
    await withRedis(redisUrl, async (redis) => {
      const echo = await echoCommandRunOnceThenFlushed(redis, `${keyPrefix}echo`);
      const pipeline = redis.pipeline();
      echo.appendTo(pipeline, [`${keyPrefix}echo`], ["one"]);
      pipeline.ping();
      echo.appendTo(pipeline, [`${keyPrefix}echo`], ["two"]);

      const replies = await pipeline.exec();

      expect(replies).toEqual([
        [null, [`${keyPrefix}echo`, "one"]],
        [null, "PONG"],
        [null, [`${keyPrefix}echo`, "two"]],
      ]);
    });
  },
);

integrationTest("a script command refuses a call with the wrong number of keys", async ({ redisUrl }) => {
  await withRedis(redisUrl, async (redis) => {
    const echo = defineScriptCommand(redis, "nextaskingTestEcho", ECHO_SCRIPT, 1);

    const call = () => echo.call([], ["no key"]);

    expect(call).toThrow("The script command nextaskingTestEcho takes 1 keys, not 0");
  });
});
