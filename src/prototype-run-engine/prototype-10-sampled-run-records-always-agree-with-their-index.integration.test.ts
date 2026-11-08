import { setTimeout as sleep } from "node:timers/promises";

import { expect, vi } from "vitest";

import { integrationTest } from "../../tests/integration-test.js";
import { inconsistenciesOf, openEngine, startInProcessWorker } from "./prototype-harness.js";
import type { Handler } from "./prototype-worker.js";

const RUN_COUNT = 300;

integrationTest(
  "prototype: under load with parks, retries, sleeps and stalls, every sampled run record agrees with its index membership",
  { timeout: 60_000 },
  async ({ redisUrl, keyPrefix }) => {
    const engine = openEngine(redisUrl, keyPrefix);
    const handlers: Record<string, Handler> = {
      work: async ({ payload }) => {
        await sleep((payload as { work: number }).work);
      },
      flaky: async ({ attempt }) => {
        if (attempt === 1) {
          throw new Error("first attempt fails");
        }
      },
      sleeper: async ({ sleepUntil }) => {
        await sleepUntil("nap", Date.now() + 150);
      },
    };
    const options = { redisUrl, keyPrefix, concurrency: 8, leaseInMilliseconds: 600, backoffInMilliseconds: () => 100, handlers };
    const workers = [startInProcessWorker(options), startInProcessWorker(options), startInProcessWorker(options)];
    const runIds = Array.from({ length: RUN_COUNT }, (_, index) => `run-${String(index)}`);
    const found: string[] = [];
    let sampling = true;
    let samples = 0;
    try {
      const sampler = (async () => {
        while (sampling) {
          const runId = runIds[Math.floor(Math.random() * runIds.length)]!;
          found.push(...(await inconsistenciesOf(engine, runId)));
          samples += 1;
        }
      })();
      await Promise.all(
        runIds.map((runId, index) =>
          engine.enqueue({
            runId,
            task: ["work", "flaky", "sleeper"][index % 3]!,
            payload: { work: 5 + (index % 40) },
            concurrencyScope: `scope-${String(index % 4)}`,
            concurrencyLimit: 2,
            delayInMilliseconds: index % 7 === 0 ? 200 : 0,
          }),
        ),
      );
      await sleep(400);
      await workers[0]!.worker.close({ waitForInFlight: false });
      await vi.waitUntil(async () => (await engine.redis.zcard(engine.key("completed"))) === RUN_COUNT, {
        timeout: 40_000,
        interval: 200,
      });
      sampling = false;
      await sampler;
      const finalChecks = (await Promise.all(runIds.map((runId) => inconsistenciesOf(engine, runId)))).flat();

      expect(found).toEqual([]);
      expect(finalChecks).toEqual([]);
      expect(samples).toBeGreaterThan(500);
      expect(await engine.redis.zcard(engine.key("executing"))).toBe(0);
      for (const scope of [0, 1, 2, 3]) {
        expect(await engine.redis.scard(engine.key(`slots:scope-${String(scope)}`))).toBe(0);
        expect(await engine.redis.zcard(engine.key(`parked:scope-${String(scope)}`))).toBe(0);
      }
    } finally {
      sampling = false;
      await Promise.all(workers.map(({ worker }) => worker.close()));
      engine.redis.disconnect();
    }
  },
);
