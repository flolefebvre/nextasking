import { spawn } from "node:child_process";
import { createInterface } from "node:readline";
import type { Readable } from "node:stream";
import { fileURLToPath } from "node:url";

import type { EpochMilliseconds } from "../src/durations.js";
import { workerOptionsFrom } from "../src/worker-options.js";
import {
  CLOSE_BOUND_PAST_THE_SHUTDOWN_TIMEOUT_IN_MILLISECONDS,
  invariantsOfCurrentTestOrThrow,
} from "./invariant-violations.js";
import { violationOfProcessOutputLine } from "./process-output-violations.js";
import { reportedWorkerErrorOf } from "./recording-logger.js";
import type { RecordedLogLine } from "./recording-logger.js";
import { WORKER_PROCESS_CONFIG_VARIABLE, WORKER_PROCESS_READY } from "./worker-process-config.js";
import type { WorkerProcessConfig } from "./worker-process-config.js";

export type WorkerProcessExit = { code: number | null; signal: NodeJS.Signals | null };

export type WorkerProcess = {
  pid: number | undefined;
  logLines: RecordedLogLine[];
  output: () => string;
  kill: (signal: NodeJS.Signals) => void;
  exited: Promise<WorkerProcessExit>;
  stop: () => Promise<WorkerProcessExit>;
};

export type WorkerProcessOptions = WorkerProcessConfig & {
  env?: Record<string, string | undefined>;
};

const REPO_ROOT_DIR = fileURLToPath(new URL("../", import.meta.url));

const ENTRY_PATH = fileURLToPath(new URL("./worker-process-entry.ts", import.meta.url));

const READY_TIMEOUT_IN_MILLISECONDS = 20_000;

const CLOSE_SIGNALS: readonly NodeJS.Signals[] = ["SIGTERM", "SIGINT"];

function outputStreamOf(stream: Readable | null): Readable {
  if (stream === null) {
    throw new Error("The worker process was spawned without a piped output");
  }
  return stream;
}

function logLineOf(outputLine: string): RecordedLogLine | undefined {
  try {
    const parsedLine = JSON.parse(outputLine) as Partial<RecordedLogLine>;
    return typeof parsedLine.message === "string" ? (parsedLine as RecordedLogLine) : undefined;
  } catch {
    return undefined;
  }
}

export async function startWorkerProcess({ env = {}, ...config }: WorkerProcessOptions): Promise<WorkerProcess> {
  const childEnv = Object.fromEntries(
    Object.entries({
      ...process.env,
      NEXT_MANUAL_SIG_HANDLE: "1",
      ...env,
      [WORKER_PROCESS_CONFIG_VARIABLE]: JSON.stringify(config),
    }).filter((entry): entry is [string, string] => entry[1] !== undefined),
  );
  const invariants = invariantsOfCurrentTestOrThrow("startWorkerProcess");
  const closeBoundInMilliseconds =
    workerOptionsFrom(config.startWorkerOptions ?? {}).shutdownTimeoutInMilliseconds +
    CLOSE_BOUND_PAST_THE_SHUTDOWN_TIMEOUT_IN_MILLISECONDS;
  const child = spawn(process.execPath, ["--import", "tsx", ENTRY_PATH], {
    cwd: REPO_ROOT_DIR,
    env: childEnv,
    stdio: ["ignore", "pipe", "pipe", "ipc"],
  });
  const logLines: RecordedLogLine[] = [];
  const outputLines: string[] = [];
  createInterface({ input: outputStreamOf(child.stdout) }).on("line", (outputLine) => {
    outputLines.push(outputLine);
    const logLine = logLineOf(outputLine);
    if (logLine !== undefined) {
      logLines.push(logLine);
      const workerError = reportedWorkerErrorOf(logLine);
      if (workerError !== undefined) {
        invariants.report(workerError);
      }
    }
  });
  createInterface({ input: outputStreamOf(child.stderr) }).on("line", (outputLine) => {
    outputLines.push(outputLine);
    const violation = violationOfProcessOutputLine(outputLine);
    if (violation !== undefined) {
      invariants.report(violation);
    }
  });
  const output = () => outputLines.join("\n");
  const closed = new Promise<void>((resolve) => {
    child.once("close", () => {
      resolve();
    });
  });
  let closeSignalSentAt: { signal: NodeJS.Signals; sentAt: EpochMilliseconds } | undefined;
  const exited = new Promise<WorkerProcessExit>((resolve) => {
    child.once("exit", (code, signal) => {
      invariants.waitBeforeJudging(closed);
      if (closeSignalSentAt !== undefined && Date.now() - closeSignalSentAt.sentAt > closeBoundInMilliseconds) {
        invariants.report({
          invariant: "slow close",
          message: `a worker process took longer than ${String(closeBoundInMilliseconds)} ms to exit after ${closeSignalSentAt.signal}`,
        });
      }
      resolve({ code, signal });
    });
  });
  const stop = async () => {
    if (child.exitCode === null && child.signalCode === null) {
      child.kill("SIGKILL");
    }
    return exited;
  };
  const workerProcess: WorkerProcess = {
    pid: child.pid,
    logLines,
    output,
    kill: (signal) => {
      if (CLOSE_SIGNALS.includes(signal) && closeSignalSentAt === undefined) {
        closeSignalSentAt = { signal, sentAt: Date.now() };
      }
      child.kill(signal);
    },
    exited,
    stop,
  };
  const isReady = await new Promise<boolean>((resolve) => {
    const giveUp = setTimeout(() => {
      resolve(false);
    }, READY_TIMEOUT_IN_MILLISECONDS);
    child.on("message", (message) => {
      if (message === WORKER_PROCESS_READY) {
        clearTimeout(giveUp);
        resolve(true);
      }
    });
    void exited.then(() => {
      clearTimeout(giveUp);
      resolve(false);
    });
  });
  if (!isReady) {
    await stop();
    throw new Error(`The worker process never got ready:\n${output()}`);
  }
  return workerProcess;
}
