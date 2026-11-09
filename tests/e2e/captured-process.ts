import { spawn } from "node:child_process";
import { createInterface } from "node:readline";

import { invariantsOfCurrentTestOrThrow } from "../invariant-violations.js";
import type { InvariantViolation } from "../invariant-violations.js";
import { violationOfProcessOutputLine } from "../process-output-violations.js";
import { NEXT_TELEMETRY_ENV } from "./fixture-app.js";

export type ProcessExit = { code: number | null; signal: NodeJS.Signals | null; wasForced: boolean };

export type CapturedProcess = {
  output: () => string;
  hasExited: () => boolean;
  exited: Promise<ProcessExit>;
  stop: () => Promise<ProcessExit>;
};

const STOP_TIMEOUT_IN_MILLISECONDS = 10_000;

export const FORCED_STOP_SENTENCE = `a Next server process was stopped with SIGKILL ${String(STOP_TIMEOUT_IN_MILLISECONDS / 1000)} s after SIGTERM`;

function workerErrorOfJsonLine(outputLine: string): InvariantViolation | undefined {
  try {
    const parsedLine = JSON.parse(outputLine) as { message?: unknown };
    return parsedLine.message === "worker error"
      ? { invariant: "worker error", message: JSON.stringify(parsedLine) }
      : undefined;
  } catch {
    return undefined;
  }
}

export function violationOfCapturedOutputLine(outputLine: string): InvariantViolation | undefined {
  return workerErrorOfJsonLine(outputLine) ?? violationOfProcessOutputLine(outputLine);
}

export function spawnCapturedProcess(
  command: string,
  args: string[],
  { cwd, env }: { cwd: string; env: Record<string, string> },
): CapturedProcess {
  const invariants = invariantsOfCurrentTestOrThrow("spawnCapturedProcess");
  const child = spawn(command, args, {
    cwd,
    env: { ...process.env, ...env, ...NEXT_TELEMETRY_ENV },
    stdio: ["ignore", "pipe", "pipe"],
  });
  const outputChunks: Buffer[] = [];
  for (const outputStream of [child.stdout, child.stderr]) {
    outputStream.on("data", (chunk: Buffer) => outputChunks.push(chunk));
    createInterface({ input: outputStream }).on("line", (outputLine) => {
      const violation = violationOfCapturedOutputLine(outputLine);
      if (violation !== undefined) {
        invariants.report(violation);
      }
    });
  }
  const closed = new Promise<void>((resolve) => {
    child.once("close", () => {
      resolve();
    });
  });
  let isExited = false;
  const exited = new Promise<ProcessExit>((resolve) => {
    child.once("exit", (code, signal) => {
      isExited = true;
      invariants.waitBeforeJudging(closed);
      resolve({ code, signal, wasForced: signal === "SIGKILL" });
    });
    child.once("error", (error) => {
      outputChunks.push(Buffer.from(`${command} failed to spawn: ${error.message}\n`));
      isExited = true;
      resolve({ code: null, signal: null, wasForced: false });
    });
  });
  const stop = async (): Promise<ProcessExit> => {
    if (isExited) return exited;
    child.kill("SIGTERM");
    const forceKill = setTimeout(() => child.kill("SIGKILL"), STOP_TIMEOUT_IN_MILLISECONDS);
    const exit = await exited;
    clearTimeout(forceKill);
    if (exit.wasForced) {
      invariants.report({ invariant: "slow close", message: FORCED_STOP_SENTENCE });
    }
    return exit;
  };
  return {
    output: () => Buffer.concat(outputChunks).toString(),
    hasExited: () => isExited,
    exited,
    stop,
  };
}
