#!/usr/bin/env node
import { existsSync, readFileSync } from "node:fs";
import { createRequire } from "node:module";
import { join } from "node:path";
import { pathToFileURL } from "node:url";

type NextServerFactory = (options: { dev: boolean; dir: string }) => { prepare: () => Promise<void> };

type RequiredServerFiles = { config?: { output?: unknown } };

type WorkerBinProcess = {
  exitCode?: number | string | null | undefined;
  exit: (code: number) => void;
  on: (event: "beforeExit", listener: () => void) => unknown;
};

export type RegisterWatch = { onPrepared: () => void };

const WORKER_BIN_LISTENER = Symbol.for("@flefebvre/nextasking/worker-bin-listener");

export const NO_WORKER_EXIT_DELAY_IN_MILLISECONDS = 60_000;

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

export function listenForLibraryNotifications(
  listener: (notification: unknown) => void,
  globalScope: object = globalThis,
): () => void {
  Object.assign(globalScope, { [WORKER_BIN_LISTENER]: listener });
  return () => {
    Reflect.deleteProperty(globalScope, WORKER_BIN_LISTENER);
  };
}

export function watchRegister(binProcess: WorkerBinProcess = process, globalScope: object = globalThis): RegisterWatch {
  let hasStartWorkerBeenCalled = false;
  let hasWrittenNoWorkerStarted = false;
  let noWorkerExitTimer: NodeJS.Timeout | undefined;
  const writeNoWorkerStartedOnce = () => {
    if (!hasWrittenNoWorkerStarted) {
      hasWrittenNoWorkerStarted = true;
      console.error(NO_WORKER_STARTED_ERROR);
    }
  };
  const exitWithNoWorker = () => {
    writeNoWorkerStartedOnce();
    binProcess.exit(1);
  };
  const stopNoWorkerExitTimer = () => {
    clearTimeout(noWorkerExitTimer);
    noWorkerExitTimer = undefined;
  };
  const startNoWorkerExitTimer = () => {
    stopNoWorkerExitTimer();
    noWorkerExitTimer = setTimeout(exitWithNoWorker, NO_WORKER_EXIT_DELAY_IN_MILLISECONDS).unref();
  };
  binProcess.on("beforeExit", () => {
    if (!hasStartWorkerBeenCalled) {
      writeNoWorkerStartedOnce();
      binProcess.exitCode = 1;
    }
  });
  const listenToTheLibrary = (notification: unknown) => {
    if (notification === "startWorkerCalled") {
      hasStartWorkerBeenCalled = true;
      stopNoWorkerExitTimer();
    } else if (notification === "workerNotStarted") {
      binProcess.exit(1);
    }
  };
  listenForLibraryNotifications(listenToTheLibrary, globalScope);
  return {
    onPrepared: () => {
      if (!hasStartWorkerBeenCalled) {
        startNoWorkerExitTimer();
      }
    },
  };
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
  const registerWatch = watchRegister();
  const next = await loadNextServerFactory(appDir);
  await next({ dev: false, dir: appDir }).prepare();
  registerWatch.onPrepared();
}

if (import.meta.main) {
  if (isWorkerCommand(process.argv.slice(2))) {
    await runWorker(process.cwd());
  } else {
    console.error(WORKER_BIN_USAGE);
    process.exitCode = 1;
  }
}
