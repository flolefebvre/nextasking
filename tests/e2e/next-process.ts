import { spawn } from "node:child_process";
import type { ChildProcess } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";

import { freePort } from "./free-port.js";
import { NEXT_TELEMETRY_ENV, nextBinOf } from "./fixture-app.js";

export type NextCommand = "start" | "dev";

export type NextExit = { code: number | null; signal: NodeJS.Signals | null; wasForced: boolean };

export type NextProcess = {
  url: string;
  output: () => string;
  stop: () => Promise<NextExit>;
};

const RESPONDING_TIMEOUT_IN_MILLISECONDS = 60_000;
const RESPONDING_POLL_IN_MILLISECONDS = 200;
const STOP_TIMEOUT_IN_MILLISECONDS = 10_000;

function hasExited(child: ChildProcess): boolean {
  return child.exitCode !== null || child.signalCode !== null;
}

async function stopChild(child: ChildProcess): Promise<NextExit> {
  if (hasExited(child)) {
    return { code: child.exitCode, signal: child.signalCode, wasForced: child.signalCode === "SIGKILL" };
  }
  const exited = new Promise<void>((resolve) => child.once("exit", () => resolve()));
  child.kill("SIGTERM");
  const forceKill = setTimeout(() => child.kill("SIGKILL"), STOP_TIMEOUT_IN_MILLISECONDS);
  await exited;
  clearTimeout(forceKill);
  return { code: child.exitCode, signal: child.signalCode, wasForced: child.signalCode === "SIGKILL" };
}

async function isResponding(url: string): Promise<boolean> {
  try {
    await fetch(url, { signal: AbortSignal.timeout(RESPONDING_POLL_IN_MILLISECONDS * 5) });
    return true;
  } catch {
    return false;
  }
}

async function waitUntilResponding(url: string, child: ChildProcess, output: () => string): Promise<void> {
  const deadline = Date.now() + RESPONDING_TIMEOUT_IN_MILLISECONDS;
  while (Date.now() < deadline) {
    if (hasExited(child)) {
      throw new Error(`next exited with code ${String(child.exitCode)} before responding:\n${output()}`);
    }
    if (await isResponding(url)) return;
    await sleep(RESPONDING_POLL_IN_MILLISECONDS);
  }
  throw new Error(
    `next did not respond on ${url} within ${String(RESPONDING_TIMEOUT_IN_MILLISECONDS)}ms:\n${output()}`,
  );
}

export async function startNext(
  fixtureAppDir: string,
  { command, env }: { command: NextCommand; env: Record<string, string> },
): Promise<NextProcess> {
  const port = await freePort();
  const child = spawn(nextBinOf(fixtureAppDir), [command, "--port", String(port)], {
    cwd: fixtureAppDir,
    env: { ...process.env, ...env, ...NEXT_TELEMETRY_ENV },
    stdio: ["ignore", "pipe", "pipe"],
  });
  const outputChunks: Buffer[] = [];
  child.stdout?.on("data", (chunk: Buffer) => outputChunks.push(chunk));
  child.stderr?.on("data", (chunk: Buffer) => outputChunks.push(chunk));
  const output = () => Buffer.concat(outputChunks).toString();
  const url = `http://127.0.0.1:${String(port)}`;
  const nextProcess: NextProcess = { url, output, stop: () => stopChild(child) };
  try {
    await waitUntilResponding(url, child, output);
  } catch (error) {
    await nextProcess.stop();
    throw error;
  }
  return nextProcess;
}
