import { join } from "node:path";
import { expect } from "vitest";

import { spawnCapturedProcess } from "./captured-process.js";
import type { CapturedProcess } from "./captured-process.js";

export type WorkerBinPlacement = "installed" | "standalone";

export const WORKER_BIN_PLACEMENTS: readonly WorkerBinPlacement[] = ["installed", "standalone"];

const WORKER_STARTED_TIMEOUT_IN_MILLISECONDS = 60_000;

export const STANDALONE_WORKER_BIN_PATH = "node_modules/@flefebvre/nextasking/dist/bin.js";

export function appDirOfPlacement(
  placement: WorkerBinPlacement,
  { fixtureAppDir, standaloneAppDir }: { fixtureAppDir: string; standaloneAppDir: string },
): string {
  return placement === "standalone" ? standaloneAppDir : fixtureAppDir;
}

export function spawnWorkerBin(
  appDir: string,
  { env, placement = "installed" }: { env: Record<string, string>; placement?: WorkerBinPlacement },
): CapturedProcess {
  if (placement === "standalone") {
    return spawnCapturedProcess("node", [STANDALONE_WORKER_BIN_PATH, "worker"], { cwd: appDir, env });
  }
  return spawnCapturedProcess(join(appDir, "node_modules", ".bin", "nextasking"), ["worker"], { cwd: appDir, env });
}

export async function startWorkerBin(
  appDir: string,
  options: { env: Record<string, string>; placement?: WorkerBinPlacement },
): Promise<CapturedProcess> {
  const workerBin = spawnWorkerBin(appDir, options);
  try {
    await expect
      .poll(() => workerBin.output(), { timeout: WORKER_STARTED_TIMEOUT_IN_MILLISECONDS })
      .toContain('"message":"worker started"');
  } catch (error) {
    await workerBin.stop();
    throw error;
  }
  return workerBin;
}
