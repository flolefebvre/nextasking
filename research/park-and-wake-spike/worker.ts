import { randomUUID } from "node:crypto";
import { DelayedError, Job, Queue, Worker } from "bullmq";
import { connect, keys, LOCK_MS, SAFETY_MS, STALLED_INTERVAL_MS, WORKER_CONCURRENCY, type RunData } from "./lib.ts";

const url = process.env.REDIS_URL!;
const queueName = process.env.QUEUE!;
const limits: Record<string, number> = JSON.parse(process.env.LIMITS!);
const recheck = process.env.RECHECK === "1";
const parkPauseMs = Number(process.env.PARK_PAUSE_MS ?? "0");

const redis = connect(url);
const queue = new Queue(queueName, { connection: { url } });
const held = new Map<string, { slot: string; token: string }>();

const stat = (field: string) => redis.hincrby(keys.stats, field, 1);

async function wake(task: string, key: string, headId: string, reason: string) {
  const park = keys.park(task, key);
  let id = headId;
  for (let i = 0; i < 5 && id; i++) {
    const job = await Job.fromId(queue, id);
    if (!job) {
      await redis.lrem(park, 1, id);
      await stat("wake_missing");
      id = (await redis.lindex(park, 0)) ?? "";
      continue;
    }
    try {
      await job.promote();
      await stat(`wake_ok:${reason}`);
    } catch (error) {
      await stat(`wake_fail:${reason}:${(error as Error).message.replace(/job [^ ]+/i, "job <id>")}`);
    }
    return;
  }
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

const worker = new Worker<RunData>(
  queueName,
  async (job, token) => {
    const task = job.name;
    const key = job.data.key;
    const limit = limits[task];
    const keyed = key !== undefined && limit !== undefined;
    const slot = keyed ? keys.slot(task, key) : "";
    const park = keyed ? keys.park(task, key) : "";

    if (keyed) {
      const [ok, next] = await redis.acquireOrPark(slot, park, token!, LOCK_MS, limit, job.id!);
      if (!ok) {
        if (parkPauseMs) await sleep(parkPauseMs);
        await job.moveToDelayed(Date.now() + SAFETY_MS, token);
        await stat("parks");
        if (recheck) {
          const head = await redis.releaseAndPeek(slot, park, "", limit);
          if (head) await wake(task, key, head, "recheck");
        }
        throw new DelayedError();
      }
      if (next) await wake(task, key, next, "chain");
      held.set(job.id!, { slot, token: token! });
    }

    const execId = randomUUID();
    const k = key ?? "-";
    const start = Date.now();
    await redis.rpush(keys.log, `S|${task}|${k}|${job.id}|${execId}|${process.pid}|${start}|${job.attemptsMade}|${job.attemptsStarted}|${job.data.dispatchedAt}`);
    await redis.truthEnter(keys.exec(task, k), keys.execMax(task, k));
    try {
      if (job.data.busyMs) {
        const until = Date.now() + job.data.busyMs;
        while (Date.now() < until) {}
      } else {
        await sleep(job.data.ms ?? 5 + Math.random() * 15);
      }
      if (job.data.failFirst && job.attemptsMade === 0) throw new Error("planned first-attempt failure");
    } finally {
      await redis.decr(keys.exec(task, k));
      await redis.rpush(keys.log, `E|${task}|${k}|${job.id}|${execId}|${process.pid}|${Date.now()}`);
      if (keyed) {
        held.delete(job.id!);
        const head = await redis.releaseAndPeek(slot, park, token!, limit);
        if (head) await wake(task, key, head, "release");
      }
    }
  },
  {
    connection: { url },
    concurrency: WORKER_CONCURRENCY,
    lockDuration: LOCK_MS,
    stalledInterval: STALLED_INTERVAL_MS,
    maxStalledCount: 3,
  },
);

worker.on("locksRenewed", async ({ jobIds }) => {
  for (const id of jobIds) {
    const h = held.get(id);
    if (h && !(await redis.renewSlot(h.slot, h.token, LOCK_MS))) await stat("slot_renew_lost");
  }
});
worker.on("lockRenewalFailed", () => void stat("lock_renewal_failed"));
worker.on("error", (error) => void stat(`worker_error:${error.message.replace(/job [^ ]+/i, "job <id>").slice(0, 80)}`));

await worker.waitUntilReady();
await redis.incr(keys.ready);
