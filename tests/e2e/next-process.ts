import { setTimeout as sleep } from "node:timers/promises";

import { spawnCapturedProcess } from "./captured-process.js";
import type { CapturedProcess } from "./captured-process.js";
import { freePort } from "./free-port.js";
import { nextBinOf } from "./fixture-app.js";

export type NextCommand = "start" | "dev";

export type NextProcess = CapturedProcess & { url: string };

const RESPONDING_TIMEOUT_IN_MILLISECONDS = 60_000;
const RESPONDING_POLL_IN_MILLISECONDS = 200;

async function isResponding(url: string): Promise<boolean> {
  try {
    await fetch(url, { signal: AbortSignal.timeout(RESPONDING_POLL_IN_MILLISECONDS * 5) });
    return true;
  } catch {
    return false;
  }
}

async function waitUntilResponding(nextProcess: NextProcess): Promise<void> {
  const deadline = Date.now() + RESPONDING_TIMEOUT_IN_MILLISECONDS;
  while (Date.now() < deadline) {
    if (nextProcess.hasExited()) {
      const { code } = await nextProcess.exited;
      throw new Error(`next exited with code ${String(code)} before responding:\n${nextProcess.output()}`);
    }
    if (await isResponding(nextProcess.url)) return;
    await sleep(RESPONDING_POLL_IN_MILLISECONDS);
  }
  throw new Error(
    `next did not respond on ${nextProcess.url} within ${String(RESPONDING_TIMEOUT_IN_MILLISECONDS)}ms:\n${nextProcess.output()}`,
  );
}

async function startRespondingProcess(port: number, spawnProcess: () => CapturedProcess): Promise<NextProcess> {
  const nextProcess: NextProcess = { ...spawnProcess(), url: `http://127.0.0.1:${String(port)}` };
  try {
    await waitUntilResponding(nextProcess);
  } catch (error) {
    await nextProcess.stop();
    throw error;
  }
  return nextProcess;
}

export async function startNext(
  fixtureAppDir: string,
  { command, env }: { command: NextCommand; env: Record<string, string> },
): Promise<NextProcess> {
  const port = await freePort();
  return startRespondingProcess(port, () =>
    spawnCapturedProcess(nextBinOf(fixtureAppDir), [command, "--port", String(port)], { cwd: fixtureAppDir, env }),
  );
}

export async function startStandaloneServer(
  standaloneAppDir: string,
  { env }: { env: Record<string, string> },
): Promise<NextProcess> {
  const port = await freePort();
  return startRespondingProcess(port, () =>
    spawnCapturedProcess("node", ["server.js"], {
      cwd: standaloneAppDir,
      env: { ...env, PORT: String(port), HOSTNAME: "127.0.0.1" },
    }),
  );
}
