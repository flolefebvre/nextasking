import { setTimeout as sleep } from "node:timers/promises";
import { expect, vi } from "vitest";

import { engineKeysOf } from "../src/engine/engine-keys.js";
import type { LockToken } from "../src/engine/engine-keys.js";
import { createEngine } from "../src/engine/engine.js";
import type { LockedRun, RunToCreate, SettleOutcome } from "../src/engine/engine.js";
import { RUN_STATES } from "../src/run-states.js";
import { integrationTest } from "./integration-test.js";
import { freshLockTokensOf } from "./lock-tokens.js";
import { seededRandomnessOfThisRun } from "./seeded-random.js";
import { withRedis } from "./with-redis.js";

const CONCURRENCY_LIMITS_BY_TASK: Readonly<Record<string, number>> = { "one-per-key": 1, "two-in-all": 2 };

const TASKS = ["one-per-key", "two-in-all", "unlimited"];

const CONCURRENCY_KEYS = ["account-1", "account-2", "account-3"];

const RUN_COUNT = 200;

const FETCHER_COUNT = 4;

const MOST_LOCK_TOKENS_PER_TEST_FETCH = 5;

const LOCK_DURATION_IN_MILLISECONDS = 120_000;

const COMPLETE: SettleOutcome = { kind: "complete" };

const YIELD: SettleOutcome = { kind: "yield" };

type LockHanding = { fetcher: number; runId: string };

integrationTest(
  "concurrent fetches and settles lock each run under one lock at a time, within its concurrency limit, until every run completes",
  { timeout: 60_000 },
  async ({ redisUrl, keyPrefix }) => {
    const { random, pick, seedMessage } = seededRandomnessOfThisRun();
    const keys = engineKeysOf(keyPrefix);
    const runsToCreate: RunToCreate[] = Array.from({ length: RUN_COUNT }, (_, position) => {
      const task = pick(TASKS);
      return {
        runId: `run-${String(position + 1)}`,
        task,
        payload: "{}",
        retries: 0,
        ...(task === "one-per-key" ? { concurrencyKey: pick(CONCURRENCY_KEYS) } : {}),
      };
    });
    const limitedSlotSets = [
      ...CONCURRENCY_KEYS.map((concurrencyKey) => ({
        slotSetKey: keys.slotSet("one-per-key", concurrencyKey),
        limit: 1,
      })),
      { slotSetKey: keys.slotSet("two-in-all", undefined), limit: 2 },
    ];
    const lockHandingsByToken = new Map<LockToken, LockHanding>();
    const holderTokenByRunId = new Map<string, LockToken>();
    const violations: string[] = [];
    const slotCountsAboveTheLimit: string[] = [];
    const completedRunIds = new Set<string>();
    let slotSampleCount = 0;
    let isLoadOver = false;

    await withRedis(redisUrl, async (redis) => {
      await createEngine(redis, keyPrefix).enqueue({ runs: runsToCreate });

      const settleOne = async (engine: ReturnType<typeof createEngine>, lockedRun: LockedRun) => {
        await sleep(Math.floor(random() * 3));
        const outcome = random() < 0.7 ? COMPLETE : YIELD;
        holderTokenByRunId.delete(lockedRun.runId);
        const reply = await engine.settle({
          task: lockedRun.task,
          runId: lockedRun.runId,
          lockToken: lockedRun.lockToken,
          outcome,
          ...(CONCURRENCY_LIMITS_BY_TASK[lockedRun.task] === undefined
            ? {}
            : { concurrencyLimit: CONCURRENCY_LIMITS_BY_TASK[lockedRun.task] }),
        });
        if (reply.kind === "completed") {
          completedRunIds.add(lockedRun.runId);
        } else if (reply.kind !== "yielded") {
          violations.push(`a settle of ${lockedRun.runId} replied ${reply.kind}`);
        }
      };

      const runFetcher = (fetcher: number) =>
        withRedis(redisUrl, async (fetcherRedis) => {
          const engine = createEngine(fetcherRedis, keyPrefix);
          while (!isLoadOver) {
            const lockTokens = freshLockTokensOf(1 + Math.floor(random() * MOST_LOCK_TOKENS_PER_TEST_FETCH));
            const reply = await engine.fetch({
              lockTokens,
              concurrencyLimitsByTask: CONCURRENCY_LIMITS_BY_TASK,
              lockDurationInMilliseconds: LOCK_DURATION_IN_MILLISECONDS,
            });
            if (reply.kind !== "fetched") {
              violations.push(`fetcher ${String(fetcher)} heard ${reply.kind}`);
              return;
            }
            for (const { runId, lockToken } of reply.lockedRuns) {
              const earlierHanding = lockHandingsByToken.get(lockToken);
              if (earlierHanding !== undefined || !lockTokens.includes(lockToken)) {
                violations.push(`lock ${lockToken} of ${runId} was handed to fetcher ${String(fetcher)} twice`);
              }
              const otherHolderToken = holderTokenByRunId.get(runId);
              if (otherHolderToken !== undefined) {
                violations.push(`${runId} was locked by ${lockToken} while ${otherHolderToken} still held it`);
              }
              lockHandingsByToken.set(lockToken, { fetcher, runId });
              holderTokenByRunId.set(runId, lockToken);
            }
            await Promise.all(reply.lockedRuns.map((lockedRun) => settleOne(engine, lockedRun)));
            if (reply.lockedRuns.length === 0) {
              await sleep(1);
            }
          }
        });

      const sampleSlotSets = async () => {
        while (!isLoadOver) {
          for (const { slotSetKey, limit } of limitedSlotSets) {
            const slotCount = await redis.scard(slotSetKey);
            slotSampleCount += 1;
            if (slotCount > limit) {
              slotCountsAboveTheLimit.push(
                `${slotSetKey} held ${String(slotCount)} slots, over its limit ${String(limit)}`,
              );
            }
          }
        }
      };

      const sampling = sampleSlotSets();
      const fetching = Promise.all(Array.from({ length: FETCHER_COUNT }, (_, fetcher) => runFetcher(fetcher)));
      try {
        await vi.waitUntil(() => completedRunIds.size === RUN_COUNT || violations.length > 0, {
          timeout: 50_000,
          interval: 50,
        });
      } finally {
        isLoadOver = true;
        await Promise.all([fetching, sampling]);
      }

      expect(violations, seedMessage).toEqual([]);
      expect(slotCountsAboveTheLimit, seedMessage).toEqual([]);
      expect(slotSampleCount, seedMessage).toBeGreaterThan(0);
      expect(
        new Set([...lockHandingsByToken.values()].map(({ fetcher }) => fetcher)).size,
        seedMessage,
      ).toBeGreaterThan(1);
      const runCountsByState = await Promise.all(
        RUN_STATES.map(async (state) => {
          let runCount = 0;
          for (const task of TASKS) {
            runCount += await redis.zcard(keys.runIndex(task, state));
          }
          return [state, runCount] as const;
        }),
      );
      expect(Object.fromEntries(runCountsByState.filter(([, runCount]) => runCount > 0)), seedMessage).toEqual({
        completed: RUN_COUNT,
      });
    });
  },
);
