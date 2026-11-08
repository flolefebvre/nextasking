import { defineEngineContractTests } from "./engine-contract.js";
import { engineTest, expireLock } from "./engine-test.js";
import { serverInstantInMillisecondsOf } from "./server-instant.js";
import type { RunState } from "../src/run-states.js";

defineEngineContractTests((name, body) => {
  engineTest(name, async (context) => {
    const { engine, redis, keys } = context;
    await body({
      engine,
      expireLock: (runId) => expireLock(context, runId),
      pastInstant: async () => (await serverInstantInMillisecondsOf(redis)) - 1000,
      stateOf: async (runId) =>
        ((await redis.hget(keys.runRecord(runId), "state")) ?? undefined) as RunState | undefined,
      isWakeUpMarkerArmed: async () => (await redis.zcard(keys.wakeUpMarker)) > 0,
      disarmWakeUpMarker: async () => {
        await redis.del(keys.wakeUpMarker);
      },
    });
  });
});
