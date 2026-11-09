import { defineEngineContractTests } from "./engine-contract.js";
import { backdateEndedRun, backdateFirstFoundUnknownAt, engineTest, expireLock } from "./engine-test.js";
import { serverInstantInMillisecondsOf } from "./server-instant.js";
import type { RunState } from "../src/run-states.js";

defineEngineContractTests((name, body) => {
  engineTest(name, async (context) => {
    const { engine, redis, keys } = context;
    await body({
      engine,
      expireLock: (task, runId) => expireLock(context, task, runId),
      backdateEndedRun: (task, runId, durationInMilliseconds) =>
        backdateEndedRun(context, task, runId, durationInMilliseconds),
      backdateFirstFoundUnknownAt: (task, runId, durationInMilliseconds) =>
        backdateFirstFoundUnknownAt(context, task, runId, durationInMilliseconds),
      pastInstant: async () => (await serverInstantInMillisecondsOf(redis)) - 1000,
      stateOf: async (task, runId) =>
        ((await redis.hGet(keys.runRecord(task, runId), "state")) ?? undefined) as RunState | undefined,
      isWakeUpMarkerArmed: async () => (await redis.zCard(keys.wakeUpMarker)) > 0,
      disarmWakeUpMarker: async () => {
        await redis.del(keys.wakeUpMarker);
      },
    });
  });
});
