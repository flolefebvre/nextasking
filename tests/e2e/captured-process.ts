import { spawn } from "node:child_process";

import { NEXT_TELEMETRY_ENV } from "./fixture-app.js";

export type ProcessExit = { code: number | null; signal: NodeJS.Signals | null; wasForced: boolean };

export type CapturedProcess = {
  output: () => string;
  hasExited: () => boolean;
  exited: Promise<ProcessExit>;
  stop: () => Promise<ProcessExit>;
};

const STOP_TIMEOUT_IN_MILLISECONDS = 10_000;

export function spawnCapturedProcess(
  command: string,
  args: string[],
  { cwd, env }: { cwd: string; env: Record<string, string> },
): CapturedProcess {
  const child = spawn(command, args, {
    cwd,
    env: { ...process.env, ...env, ...NEXT_TELEMETRY_ENV },
    stdio: ["ignore", "pipe", "pipe"],
  });
  const outputChunks: Buffer[] = [];
  for (const outputStream of [child.stdout, child.stderr]) {
    outputStream.on("data", (chunk: Buffer) => outputChunks.push(chunk));
  }
  let isExited = false;
  const exited = new Promise<ProcessExit>((resolve) => {
    child.once("exit", (code, signal) => {
      isExited = true;
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
    return exit;
  };
  return {
    output: () => Buffer.concat(outputChunks).toString(),
    hasExited: () => isExited,
    exited,
    stop,
  };
}
