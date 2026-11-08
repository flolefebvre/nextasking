import { spawn } from "node:child_process";
import { createInterface } from "node:readline";
import { fileURLToPath } from "node:url";

import { Redis } from "ioredis";

import type { ExecutionRecord } from "./prototype-handlers.js";
import { recordsKey } from "./prototype-handlers.js";
import { RunEngine } from "./prototype-run-engine.js";
import { PrototypeWorker } from "./prototype-worker.js";
import type { PrototypeWorkerOptions, WorkerEvent } from "./prototype-worker.js";

export function startInProcessWorker(options: Omit<PrototypeWorkerOptions, "onEvent">): {
  worker: PrototypeWorker;
  events: WorkerEvent[];
} {
  const events: WorkerEvent[] = [];
  const worker = new PrototypeWorker({ ...options, onEvent: (event) => events.push(event) });
  worker.start();
  return { worker, events };
}

const REPO_ROOT_DIR = fileURLToPath(new URL("../../", import.meta.url));
const ENTRY_PATH = fileURLToPath(new URL("./prototype-worker-process-entry.ts", import.meta.url));

export type PrototypeWorkerProcess = {
  pid: number;
  events: WorkerEvent[];
  kill: (signal: NodeJS.Signals) => void;
  exited: Promise<void>;
  stop: () => Promise<void>;
};

export async function startPrototypeWorkerProcess(config: {
  redisUrl: string;
  keyPrefix: string;
  concurrency: number;
  leaseInMilliseconds: number;
}): Promise<PrototypeWorkerProcess> {
  const child = spawn(process.execPath, ["--import", "tsx", ENTRY_PATH], {
    cwd: REPO_ROOT_DIR,
    env: { ...process.env, PROTOTYPE_WORKER_CONFIG: JSON.stringify(config) },
    stdio: ["ignore", "pipe", "pipe", "ipc"],
  });
  const events: WorkerEvent[] = [];
  const stderr: string[] = [];
  createInterface({ input: child.stdout! }).on("line", (line) => {
    try {
      events.push(JSON.parse(line) as WorkerEvent);
    } catch {
      stderr.push(line);
    }
  });
  createInterface({ input: child.stderr! }).on("line", (line) => stderr.push(line));
  const exited = new Promise<void>((resolve) => child.once("exit", () => resolve()));
  const stop = async () => {
    if (child.exitCode === null && child.signalCode === null) {
      child.kill("SIGKILL");
    }
    await exited;
  };
  const ready = await new Promise<boolean>((resolve) => {
    const giveUp = setTimeout(() => resolve(false), 20_000);
    child.on("message", (message) => {
      if (message === "ready") {
        clearTimeout(giveUp);
        resolve(true);
      }
    });
    void exited.then(() => resolve(false));
  });
  if (!ready) {
    await stop();
    throw new Error(`prototype worker never got ready:\n${stderr.join("\n")}`);
  }
  return { pid: child.pid!, events, kill: (signal) => child.kill(signal), exited, stop };
}

export function openEngine(redisUrl: string, keyPrefix: string): RunEngine {
  const redis = new Redis(redisUrl, { maxRetriesPerRequest: null });
  redis.on("error", () => undefined);
  return new RunEngine(redis, keyPrefix);
}

export async function executionRecordsOf(engine: RunEngine, keyPrefix: string): Promise<ExecutionRecord[]> {
  return (await engine.redis.lrange(recordsKey(keyPrefix), 0, -1)).map(
    (line) => JSON.parse(line) as ExecutionRecord,
  );
}

const INDEX_OF_STATE: Record<string, string> = {
  pending: "pending",
  delayed: "delayed",
  executing: "executing",
  completed: "completed",
  failed: "failed",
};

export async function inconsistenciesOf(engine: RunEngine, runId: string): Promise<string[]> {
  const scope = (await engine.redis.hget(engine.key(`run:${runId}`), "scope")) ?? "";
  const indexes = ["pending", "delayed", "executing", "completed", "failed", ...(scope === "" ? [] : [`parked:${scope}`])];
  const transaction = engine.redis.multi().hget(engine.key(`run:${runId}`), "state");
  for (const index of indexes) {
    transaction.zscore(engine.key(index), runId);
  }
  const replies = (await transaction.exec())!.map(([, reply]) => reply as string | null);
  const [state, ...scores] = replies;
  const memberOf = indexes.filter((_, position) => scores[position] !== null);
  const expected = state === "parked" ? `parked:${scope}` : INDEX_OF_STATE[state ?? ""];
  if (memberOf.length === 1 && memberOf[0] === expected) {
    return [];
  }
  return [`${runId}: state ${String(state)} but in [${memberOf.join(", ")}]`];
}
