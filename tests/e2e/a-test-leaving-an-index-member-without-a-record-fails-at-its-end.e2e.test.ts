import { engineKeysOf, storedRunIdOf } from "../../src/engine/engine-keys.js";
import { testFailingWith } from "../test-failing-with.js";
import { withRedis } from "../with-redis.js";
import { e2eTest } from "./e2e-test.js";

testFailingWith(
  e2eTest,
  "an e2e test leaving an index member without a record fails at its end with the checker's list",
  ({ keyPrefix }) => [
    `The test ended with invariant violations it did not declare:\n  stored keys: run "send-welcome-email:ghost-run" sits in {${keyPrefix}}:pending but has no record with a known state`,
  ],
  async ({ redisUrl, keyPrefix }) => {
    await withRedis(redisUrl, (redis) =>
      redis.zadd(engineKeysOf(keyPrefix).pending, 7, storedRunIdOf("send-welcome-email", "ghost-run")),
    );
  },
);
