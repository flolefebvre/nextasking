#!/usr/bin/env node
import { existsSync, readFileSync } from "node:fs";
import { createRequire } from "node:module";
import { join } from "node:path";
import { pathToFileURL } from "node:url";

type NextServerFactory = (options: { dev: boolean; dir: string }) => { prepare: () => Promise<void> };

type RequiredServerFiles = { config?: { output?: unknown } };

const START_WORKER_CALLED_MARK = Symbol.for("@flefebvre/nextasking/start-worker-called");

const WORKER_NOT_STARTED_LISTENER = Symbol.for("@flefebvre/nextasking/worker-not-started-listener");

export const WORKER_BIN_USAGE = "Usage: nextasking worker";

export const NO_WORKER_STARTED_ERROR =
  "nextasking worker: register() started no worker. Turn on the environment variable your instrumentation.ts checks before calling startWorker().";

export function isWorkerCommand(args: readonly string[]): boolean {
  return args.length === 1 && args[0] === "worker";
}

export function standaloneConfigOf(appDir: string): string | undefined {
  const requiredServerFilesPath = join(appDir, ".next", "required-server-files.json");
  if (!existsSync(requiredServerFilesPath)) return undefined;
  const { config } = JSON.parse(readFileSync(requiredServerFilesPath, "utf-8")) as RequiredServerFiles;
  return config?.output === "standalone" ? JSON.stringify(config) : undefined;
}

export function hasStartWorkerBeenCalled(globalScope: object = globalThis): boolean {
  return (globalScope as Record<symbol, unknown>)[START_WORKER_CALLED_MARK] === true;
}

export function listenForWorkerNotStarted(onWorkerNotStarted: () => void, globalScope: object = globalThis): void {
  Object.assign(globalScope, { [WORKER_NOT_STARTED_LISTENER]: onWorkerNotStarted });
}

async function loadNextServerFactory(appDir: string): Promise<NextServerFactory> {
  const nextPath = createRequire(join(appDir, "package.json")).resolve("next");
  const nextModule = (await import(pathToFileURL(nextPath).href)) as { default: NextServerFactory };
  return nextModule.default;
}

async function runWorker(appDir: string): Promise<void> {
  const standaloneConfig = standaloneConfigOf(appDir);
  if (standaloneConfig === undefined) {
    process.env.NODE_ENV ??= "production";
  } else {
    process.env.__NEXT_PRIVATE_STANDALONE_CONFIG = standaloneConfig;
    process.env.NODE_ENV = "production";
  }
  process.env.NEXT_MANUAL_SIG_HANDLE = "1";
  listenForWorkerNotStarted(() => process.exit(1));
  const next = await loadNextServerFactory(appDir);
  await next({ dev: false, dir: appDir }).prepare();
  if (!hasStartWorkerBeenCalled()) {
    console.error(NO_WORKER_STARTED_ERROR);
    process.exit(1);
  }
}

if (import.meta.main) {
  if (isWorkerCommand(process.argv.slice(2))) {
    await runWorker(process.cwd());
  } else {
    console.error(WORKER_BIN_USAGE);
    process.exitCode = 1;
  }
}
