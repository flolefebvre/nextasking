import { setTimeout as sleep } from "node:timers/promises";

import type { Redis } from "ioredis";

import type { Handler } from "./prototype-worker.js";

export type ExecutionRecord = {
  event: "started" | "ended";
  executionId: string;
  runId: string;
  epoch: string;
  attempt: number;
  pid: number;
  scope: string;
  at: number;
};

export function recordsKey(keyPrefix: string): string {
  return `{${keyPrefix}}:records`;
}

export function prototypeHandlers(redis: Redis, keyPrefix: string): Record<string, Handler> {
  const record = async (entry: Omit<ExecutionRecord, "pid" | "at">) => {
    await redis.rpush(recordsKey(keyPrefix), JSON.stringify({ ...entry, pid: process.pid, at: Date.now() }));
  };
  return {
    limited: async ({ runId, epoch, attempt, payload }) => {
      const { scope, workInMilliseconds } = payload as { scope: string; workInMilliseconds: number };
      const executionId = `${runId}|${epoch}`;
      await record({ event: "started", executionId, runId, epoch, attempt, scope });
      await sleep(workInMilliseconds);
      await record({ event: "ended", executionId, runId, epoch, attempt, scope });
    },
    "blocks-first-epoch": async ({ runId, epoch, attempt, payload, step }) => {
      const { blockInMilliseconds } = payload as { blockInMilliseconds: number };
      const executionId = `${runId}|${epoch}`;
      await record({ event: "started", executionId, runId, epoch, attempt, scope: "" });
      if (epoch === "1") {
        const blockedUntil = Date.now() + blockInMilliseconds;
        while (Date.now() < blockedUntil) {
          // blocks the event loop, so no renewal happens
        }
      }
      await step("after-block", async () => epoch);
      await record({ event: "ended", executionId, runId, epoch, attempt, scope: "" });
      return epoch;
    },
    noop: async () => undefined,
  };
}
