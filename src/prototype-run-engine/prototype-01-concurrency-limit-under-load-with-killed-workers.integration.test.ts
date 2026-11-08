import { setTimeout as sleep } from "node:timers/promises";

import { expect, vi } from "vitest";

import { integrationTest } from "../../tests/integration-test.js";
import type { ExecutionRecord } from "./prototype-handlers.js";
import { executionRecordsOf, openEngine, startPrototypeWorkerProcess } from "./prototype-harness.js";
import type { PrototypeWorkerProcess } from "./prototype-harness.js";

const LIMIT_BY_SCOPE_PREFIX = { "one-per-key": 1, "two-per-key": 2, "three-in-all": 3 } as const;
const KEYS = ["account-1", "account-2", "account-3"];
const RUN_COUNT = 60;
const WORKER_COUNT = 3;
const KILL_COUNT = 2;
const LEASE = 1000;

function seededRandom(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let mixed = state;
    mixed = Math.imul(mixed ^ (mixed >>> 15), mixed | 1);
    mixed ^= mixed + Math.imul(mixed ^ (mixed >>> 7), mixed | 61);
    return ((mixed ^ (mixed >>> 14)) >>> 0) / 2 ** 32;
  };
}

function violationsIn(
  records: ExecutionRecord[],
  killTimeByPid: Map<number, number>,
  limitOf: (scope: string) => number,
): string[] {
  const endedAt = new Map(records.filter((r) => r.event === "ended").map((r) => [r.executionId, r.at]));
  const byScope = new Map<string, { at: number; change: number }[]>();
  for (const started of records.filter((r) => r.event === "started")) {
    const end = endedAt.get(started.executionId) ?? killTimeByPid.get(started.pid) ?? Number.POSITIVE_INFINITY;
    byScope.set(started.scope, [
      ...(byScope.get(started.scope) ?? []),
      { at: started.at, change: 1 },
      { at: end, change: -1 },
    ]);
  }
  return [...byScope.entries()].flatMap(([scope, boundaries]) => {
    boundaries.sort((first, second) => first.at - second.at || first.change - second.change);
    let executing = 0;
    const found: string[] = [];
    for (const boundary of boundaries) {
      executing += boundary.change;
      if (executing > limitOf(scope)) {
        found.push(`${scope}: ${String(executing)} at ${String(boundary.at)}`);
      }
    }
    return found;
  });
}

integrationTest(
  "prototype: a concurrency limit is never exceeded under randomized load with SIGKILLed workers, and every run completes",
  { timeout: 60_000 },
  async ({ redisUrl, keyPrefix }) => {
    const seed = Number(process.env.NEXTASKING_TEST_SEED ?? Math.floor(Math.random() * 2 ** 32));
    const random = seededRandom(seed);
    const pick = <Item>(items: readonly Item[]): Item => items[Math.floor(random() * items.length)] as Item;
    const seedMessage = `replay with NEXTASKING_TEST_SEED=${String(seed)}`;
    const engine = openEngine(redisUrl, keyPrefix);
    const startWorker = () =>
      startPrototypeWorkerProcess({ redisUrl, keyPrefix, concurrency: 5, leaseInMilliseconds: LEASE });
    const workers: PrototypeWorkerProcess[] = [];
    const killTimeByPid = new Map<number, number>();
    const limitOf = (scope: string) =>
      LIMIT_BY_SCOPE_PREFIX[scope.split(":")[0] as keyof typeof LIMIT_BY_SCOPE_PREFIX];

    try {
      for (let index = 0; index < WORKER_COUNT; index += 1) {
        workers.push(await startWorker());
      }
      const runIds = await Promise.all(
        Array.from({ length: RUN_COUNT }, async (_, index) => {
          const scopePrefix = pick(Object.keys(LIMIT_BY_SCOPE_PREFIX)) as keyof typeof LIMIT_BY_SCOPE_PREFIX;
          const scope = scopePrefix === "three-in-all" ? scopePrefix : `${scopePrefix}:${pick(KEYS)}`;
          const runId = `run-${String(index)}`;
          await engine.enqueue({
            runId,
            task: "limited",
            payload: { scope, workInMilliseconds: 50 + Math.floor(random() * 250) },
            concurrencyScope: scope,
            concurrencyLimit: LIMIT_BY_SCOPE_PREFIX[scopePrefix],
          });
          return runId;
        }),
      );
      const killDelays = Array.from({ length: KILL_COUNT }, () => 300 + Math.floor(random() * 1500));
      for (const killDelay of killDelays) {
        await sleep(killDelay);
        const killed = pick(workers);
        killTimeByPid.set(killed.pid, Date.now());
        killed.kill("SIGKILL");
        await killed.exited;
        workers.splice(workers.indexOf(killed), 1, await startWorker());
      }
      await vi.waitFor(
        async () => {
          const states = await Promise.all(runIds.map(async (runId) => (await engine.run(runId)).state));
          expect(
            runIds.filter((_, index) => states[index] !== "completed"),
            seedMessage,
          ).toEqual([]);
        },
        { timeout: 40_000, interval: 300 },
      );
      const records = await executionRecordsOf(engine, keyPrefix);
      const endedIds = new Set(records.filter((r) => r.event === "ended").map((r) => r.executionId));

      expect(violationsIn(records, killTimeByPid, limitOf), seedMessage).toEqual([]);
      expect(
        records.filter((r) => r.event === "started" && !endedIds.has(r.executionId) && !killTimeByPid.has(r.pid)),
        seedMessage,
      ).toEqual([]);
      expect(
        records.filter((r) => r.attempt !== 1),
        seedMessage,
      ).toEqual([]);
      expect(
        workers.flatMap((w) => w.events.filter((event) => event.type === "worker-error")),
        seedMessage,
      ).toEqual([]);
    } finally {
      await Promise.all(workers.map((worker) => worker.stop()));
      engine.redis.disconnect();
    }
  },
);
