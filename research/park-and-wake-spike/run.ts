import { execFileSync, spawn, type ChildProcess } from "node:child_process";
import { mkdirSync, writeFileSync } from "node:fs";
import { DelayedError, Job, Queue, QueueEvents, Worker } from "bullmq";
import { connect, keys, LOCK_MS, SAFETY_MS, STALLED_INTERVAL_MS, WORKER_CONCURRENCY, type RunData } from "./lib.ts";

const only = process.argv.slice(2);
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

const containerId = execFileSync("docker", [
  "run", "--rm", "-d", "-p", "127.0.0.1::6379", "valkey/valkey:9.1.3",
  "valkey-server", "--maxmemory-policy", "noeviction", "--save", "", "--appendonly", "no",
]).toString().trim();
const port = execFileSync("docker", ["port", containerId, "6379/tcp"]).toString().trim().split(":").pop();
const url = `redis://127.0.0.1:${port}`;
const redis = connect(url);
for (let i = 0; i < 50; i++) {
  try { await redis.ping(); break; } catch { await sleep(200); }
}
const serverInfo = (await redis.info("server")).match(/valkey_version:(\S+)/)?.[1];
mkdirSync("results", { recursive: true });

type Dispatch = { name: string; data: Omit<RunData, "dispatchedAt">; opts?: Record<string, unknown> };
type Scenario = {
  id: string;
  limits: Record<string, number>;
  recheck: boolean;
  workers: number;
  batches: Dispatch[][];
  chaosEveryMs?: number;
  parkPauseMs?: number;
  timeoutMs?: number;
};

const percentile = (values: number[], p: number) => {
  if (values.length === 0) return null;
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))];
};
const dist = (values: number[]) => ({ n: values.length, p50: percentile(values, 50), p95: percentile(values, 95), p99: percentile(values, 99), max: percentile(values, 100) });

async function runScenario(s: Scenario) {
  await redis.flushall();
  const queueName = `q-${s.id}`;
  const queue = new Queue(queueName, { connection: { url } });
  const events = new QueueEvents(queueName, { connection: { url } });
  await events.waitUntilReady();
  let stalled = 0;
  let failed = 0;
  events.on("stalled", () => stalled++);
  events.on("failed", () => failed++);

  const kills: Record<number, number> = {};
  const procs: ChildProcess[] = [];
  const env = { ...process.env, REDIS_URL: url, QUEUE: queueName, LIMITS: JSON.stringify(s.limits), RECHECK: s.recheck ? "1" : "0", PARK_PAUSE_MS: String(s.parkPauseMs ?? 0) };
  const spawnWorker = () => {
    const p = spawn(process.execPath, ["--import", "tsx", "worker.ts"], { env, stdio: ["ignore", "ignore", "inherit"] });
    procs.push(p);
    return p;
  };
  for (let i = 0; i < s.workers; i++) spawnWorker();
  while (Number(await redis.get(keys.ready)) < s.workers) await sleep(50);

  const t0 = Date.now();
  let total = 0;
  for (const batch of s.batches) {
    const dispatchedAt = Date.now();
    await queue.addBulk(batch.map((d) => ({ name: d.name, data: { ...d.data, dispatchedAt }, opts: { removeOnComplete: false, removeOnFail: false, ...d.opts } })));
    total += batch.length;
  }

  let chaos: NodeJS.Timeout | undefined;
  let killCount = 0;
  if (s.chaosEveryMs) {
    chaos = setInterval(() => {
      const alive = procs.filter((p) => p.exitCode === null && p.signalCode === null);
      const victim = alive[Math.floor(Math.random() * alive.length)];
      if (!victim?.pid) return;
      kills[victim.pid] = Date.now();
      victim.kill("SIGKILL");
      killCount++;
      setTimeout(spawnWorker, 500);
    }, s.chaosEveryMs);
  }

  const deadline = t0 + (s.timeoutMs ?? 240_000);
  let counts: Record<string, number> = {};
  while (Date.now() < deadline) {
    counts = await queue.getJobCounts("completed", "failed", "delayed", "wait", "active", "prioritized");
    if (counts.completed + counts.failed >= total) break;
    await sleep(100);
  }
  const elapsedMs = Date.now() - t0;
  if (chaos) clearInterval(chaos);

  const testEnd = Date.now();
  const log = await redis.lrange(keys.log, 0, -1);
  const stats = await redis.hgetall(keys.stats);
  const execMax: Record<string, number> = {};
  for (const k of await redis.keys("spk:execmax:*")) execMax[k.replace("spk:execmax:", "")] = Number(await redis.get(k));

  type Exec = { task: string; key: string; jobId: string; pid: number; start: number; end?: number; killed?: boolean; attemptsMade: number; attemptsStarted: number; dispatchedAt: number };
  const execs = new Map<string, Exec>();
  for (const line of log) {
    const f = line.split("|");
    if (f[0] === "S") execs.set(f[4], { task: f[1], key: f[2], jobId: f[3], pid: Number(f[5]), start: Number(f[6]), attemptsMade: Number(f[7]), attemptsStarted: Number(f[8]), dispatchedAt: Number(f[9]) });
    else execs.get(f[4])!.end = Number(f[6]);
  }
  for (const e of execs.values()) {
    if (e.end === undefined) {
      e.end = kills[e.pid] ?? testEnd;
      e.killed = kills[e.pid] !== undefined;
    }
  }

  const byKey = new Map<string, Exec[]>();
  for (const e of execs.values()) {
    const k = `${e.task}:${e.key}`;
    if (!byKey.has(k)) byKey.set(k, []);
    byKey.get(k)!.push(e);
  }
  const perKey: Record<string, unknown> = {};
  for (const [k, list] of byKey) {
    const limit = s.limits[k.split(":")[0]];
    list.sort((a, b) => a.start - b.start);
    const points = list.flatMap((e) => [[e.start, 1] as const, [e.end!, -1] as const]).sort((a, b) => a[0] - b[0] || a[1] - b[1]);
    let cur = 0;
    let maxConcurrent = 0;
    let overSince = 0;
    const violations: { at: number; ms: number; concurrent: number }[] = [];
    for (const [t, d] of points) {
      const before = cur;
      cur += d;
      maxConcurrent = Math.max(maxConcurrent, cur);
      if (limit !== undefined && before <= limit && cur > limit) overSince = t;
      if (limit !== undefined && before > limit && cur <= limit) violations.push({ at: overSince - t0, ms: t - overSince, concurrent: before });
    }
    const starts = list.map((e) => e.start).sort((a, b) => a - b);
    const releaseToNextStart: number[] = [];
    for (const e of list) {
      const next = starts.find((t) => t >= e.end!);
      if (next !== undefined) releaseToNextStart.push(next - e.end!);
    }
    const startDelays = list.map((e) => e.start - e.dispatchedAt);
    const seenFirst = new Set<string>();
    const startOrder = list.filter((e) => /^\d+$/.test(e.jobId) && !seenFirst.has(e.jobId) && seenFirst.add(e.jobId)).map((e) => Number(e.jobId));
    let outOfOrder = 0;
    let maxRank = -Infinity;
    for (const id of startOrder) { if (id < maxRank) outOfOrder++; else maxRank = id; }
    perKey[k] = {
      limit,
      runsStartedBeforeAnOlderRun: outOfOrder,
      maxPositionsOvertaken: startOrder.reduce((m, id, i) => Math.max(m, startOrder.slice(0, i).filter((x) => x > id).length), 0),
      executions: list.length,
      killedExecutions: list.filter((e) => e.killed).length,
      maxConcurrentFromIntervals: maxConcurrent,
      maxConcurrentFromCounter: execMax[k],
      violations,
      releaseToNextStartMs: limit !== undefined ? dist(releaseToNextStart) : undefined,
      releasesFallingBackOver1s: limit !== undefined ? releaseToNextStart.filter((g) => g > 1000).length : undefined,
      startDelayFromDispatchMs: dist(startDelays),
      firstStartMs: list[0].start - t0,
      lastEndMs: Math.max(...list.map((e) => e.end!)) - t0,
    };
  }

  const retried = await Job.fromId(queue, "retried");
  const smallTimeline = execs.size <= 100 ? [...execs.values()].sort((a, b) => a.start - b.start).map((e) => ({ jobId: e.jobId, key: e.key, startMs: e.start - t0, endMs: e.end! - t0, attemptsMade: e.attemptsMade, attemptsStarted: e.attemptsStarted, killed: e.killed })) : undefined;
  const result = {
    id: s.id,
    retriedJob: retried ? { attemptsMade: retried.attemptsMade, attemptsStarted: retried.attemptsStarted, state: await retried.getState() } : undefined,
    timeline: smallTimeline,
    limits: s.limits,
    recheck: s.recheck,
    workers: s.workers,
    workerConcurrency: WORKER_CONCURRENCY,
    dispatched: total,
    elapsedMs,
    finalCounts: counts,
    stuck: total - (counts.completed + counts.failed),
    stalledEvents: stalled,
    failedEvents: failed,
    kills: killCount,
    stats,
    perKey,
  };
  writeFileSync(`results/${s.id}.json`, JSON.stringify(result, null, 2));
  console.log(JSON.stringify(result, null, 2));

  for (const p of procs) p.kill("SIGKILL");
  await events.close();
  await queue.close();
  return { result, execs: [...execs.values()], t0 };
}

const runs = (n: number, name: string, data: Omit<RunData, "dispatchedAt">) => Array.from({ length: n }, () => ({ name, data }));

const scenarios: Scenario[] = [
  { id: "s1-limit1", limits: { charge: 1 }, recheck: true, workers: 4, batches: [runs(1000, "charge", { key: "acc" })] },
  { id: "s2-limit3", limits: { charge: 3 }, recheck: true, workers: 4, batches: [runs(1000, "charge", { key: "acc" })] },
  { id: "s3a-fairness", limits: { charge: 1 }, recheck: true, workers: 4, batches: [runs(1000, "charge", { key: "A" }), runs(1, "charge", { key: "B" })] },
  { id: "s3b-fairness-with-backlog", limits: { charge: 1 }, recheck: true, workers: 4, batches: [runs(1000, "charge", { key: "A" }), runs(3000, "bulk", {}), runs(1, "charge", { key: "B" })] },
  { id: "s4a-limit1-no-recheck", limits: { charge: 1 }, recheck: false, workers: 4, batches: [runs(1000, "charge", { key: "acc" })] },
  { id: "s4b-limit3-no-recheck", limits: { charge: 3 }, recheck: false, workers: 4, batches: [runs(1000, "charge", { key: "acc" })] },
  { id: "s4c-pairs-no-recheck", limits: { charge: 1 }, recheck: false, workers: 4, timeoutMs: 60_000, batches: [Array.from({ length: 1000 }, (_, i) => ({ name: "charge", data: { key: `k${i >> 1}`, ms: 0 } }))] },
  { id: "s4d-pairs-recheck", limits: { charge: 1 }, recheck: true, workers: 4, timeoutMs: 60_000, batches: [Array.from({ length: 1000 }, (_, i) => ({ name: "charge", data: { key: `k${i >> 1}`, ms: 0 } }))] },
  { id: "s4e-pairs-pause-no-recheck", limits: { charge: 1 }, recheck: false, parkPauseMs: 30, workers: 4, timeoutMs: 60_000, batches: [Array.from({ length: 200 }, (_, i) => ({ name: "charge", data: { key: `k${i >> 1}`, ms: 5 } }))] },
  { id: "s4f-pairs-pause-recheck", limits: { charge: 1 }, recheck: true, parkPauseMs: 30, workers: 4, timeoutMs: 60_000, batches: [Array.from({ length: 200 }, (_, i) => ({ name: "charge", data: { key: `k${i >> 1}`, ms: 5 } }))] },
  { id: "s5a-chaos-limit3", limits: { charge: 3 }, recheck: true, workers: 4, chaosEveryMs: 4000, timeoutMs: 400_000, batches: [runs(1000, "charge", { key: "acc", ms: 50 })] },
  { id: "s5b-chaos-limit1", limits: { charge: 1 }, recheck: true, workers: 4, chaosEveryMs: 4000, timeoutMs: 400_000, batches: [runs(300, "charge", { key: "acc", ms: 50 })] },
  { id: "s5d-chaos-limit1-long", limits: { charge: 1 }, recheck: true, workers: 4, chaosEveryMs: 3000, timeoutMs: 400_000, batches: [runs(60, "charge", { key: "acc", ms: 2000 })] },
  { id: "s5e-chaos-limit1-long-no-recheck", limits: { charge: 1 }, recheck: false, workers: 4, chaosEveryMs: 3000, timeoutMs: 400_000, batches: [runs(60, "charge", { key: "acc", ms: 2000 })] },
  { id: "s5c-blocked-event-loop", limits: { charge: 1 }, recheck: true, workers: 4, timeoutMs: 120_000, batches: [runs(1, "charge", { key: "acc", busyMs: 12_000 }), runs(20, "charge", { key: "acc", ms: 200 })] },
  {
    id: "s6-retries",
    limits: { charge: 1 },
    recheck: true,
    workers: 4,
    timeoutMs: 60_000,
    batches: [[{ name: "charge", data: { key: "acc", ms: 200, failFirst: true }, opts: { jobId: "retried", attempts: 3, backoff: { type: "fixed", delay: 2000 } } }], runs(5, "charge", { key: "acc", ms: 500 })],
  },
];

async function probe() {
  await redis.flushall();
  const out: Record<string, unknown> = {};
  const queue = new Queue("q-probe", { connection: { url } });
  const errorOf = async (fn: () => Promise<unknown>) => {
    try { await fn(); return "no error"; } catch (error) { return (error as Error).message; }
  };

  const waiting = await queue.add("x", {});
  out.promoteWaiting = await errorOf(() => waiting.promote());

  let selfPromotionRuns = 0;
  let noThrowRuns = 0;
  const attemptsSeen: { attemptsMade: number; attemptsStarted: number }[] = [];
  const workerErrors: string[] = [];
  const failedReasons: string[] = [];
  const worker = new Worker(
    "q-probe",
    async (job, token) => {
      if (job.name === "active") {
        out.promoteActive = await errorOf(async () => (await Job.fromId(queue, job.id!))!.promote());
        return;
      }
      if (job.name === "self-promote") {
        selfPromotionRuns++;
        if (selfPromotionRuns === 1) {
          await job.moveToDelayed(Date.now() + 60_000, token);
          out.promoteDelayedSelfBeforeThrow = await errorOf(async () => (await Job.fromId(queue, job.id!))!.promote());
          await sleep(300);
          throw new DelayedError();
        }
        return "second run";
      }
      if (job.name === "no-throw") {
        noThrowRuns++;
        if (noThrowRuns === 1) {
          await job.moveToDelayed(Date.now() + 200, token);
          return "returned without DelayedError";
        }
        return "second run";
      }
      if (job.name === "parks") {
        attemptsSeen.push({ attemptsMade: job.attemptsMade, attemptsStarted: job.attemptsStarted });
        if (attemptsSeen.length < 4) {
          await job.moveToDelayed(Date.now() + 50, token);
          throw new DelayedError();
        }
        return "done";
      }
    },
    { connection: { url }, concurrency: 5 },
  );
  worker.on("error", (e) => workerErrors.push(e.message));
  worker.on("failed", (job, e) => failedReasons.push(`${job?.name}: ${e.message}`));
  await worker.waitUntilReady();
  await queue.add("active", {});
  const selfJob = await queue.add("self-promote", {});
  const noThrowJob = await queue.add("no-throw", {});
  const parksJob = await queue.add("parks", {}, { attempts: 1 });
  await sleep(3000);
  const completed = await Job.fromId(queue, waiting.id!);
  out.promoteCompleted = await errorOf(async () => (await Job.fromId(queue, (await queue.getCompleted())[0].id!))!.promote());
  out.waitingJobState = await completed?.getState();
  out.selfPromote = { runs: selfPromotionRuns, state: await (await Job.fromId(queue, selfJob.id!))?.getState(), returnvalue: (await Job.fromId(queue, selfJob.id!))?.returnvalue };
  out.noThrow = { runs: noThrowRuns, state: await (await Job.fromId(queue, noThrowJob.id!))?.getState(), returnvalue: (await Job.fromId(queue, noThrowJob.id!))?.returnvalue };
  const parks = await Job.fromId(queue, parksJob.id!);
  out.parks = { seenOnEachPickup: attemptsSeen, final: { attemptsMade: parks?.attemptsMade, attemptsStarted: parks?.attemptsStarted, state: await parks?.getState() } };
  out.workerErrors = workerErrors;
  out.failedReasons = failedReasons;
  await worker.close();
  await queue.close();
  writeFileSync("results/s7-probe.json", JSON.stringify(out, null, 2));
  console.log(JSON.stringify(out, null, 2));
}

try {
  console.log(JSON.stringify({ valkey: serverInfo, lockMs: LOCK_MS, stalledIntervalMs: STALLED_INTERVAL_MS, safetyMs: SAFETY_MS, workerConcurrency: WORKER_CONCURRENCY }));
  for (const s of scenarios) if (only.length === 0 || only.includes(s.id)) await runScenario(s);
  if (only.length === 0 || only.includes("s7-probe")) await probe();
} finally {
  redis.disconnect();
  execFileSync("docker", ["stop", containerId]);
}
process.exit(0);
