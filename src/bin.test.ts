import { EventEmitter } from "node:events";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";

import {
  NO_WORKER_EXIT_DELAY_IN_MILLISECONDS,
  NO_WORKER_STARTED_ERROR,
  START_WORKER_REJECTED_ERROR,
  isWorkerCommand,
  standaloneConfigOf,
  watchRegister,
} from "./bin.js";
import { notifyWorkerBin } from "./worker-bin-handshake.js";
import type { WorkerBinNotification } from "./worker-bin-handshake.js";

describe("isWorkerCommand", () => {
  test("accepts worker alone", () => {
    expect(isWorkerCommand(["worker"])).toBe(true);
  });

  test.each([[[]], [["start"]], [["worker", "--dir", "app"]], [["--help"]]])("refuses %j", (args) => {
    expect(isWorkerCommand(args)).toBe(false);
  });
});

describe("standaloneConfigOf", () => {
  let appDir: string;

  beforeEach(async () => {
    appDir = await mkdtemp(join(tmpdir(), "nextasking-bin-"));
  });

  afterEach(async () => {
    await rm(appDir, { recursive: true, force: true });
  });

  async function writeRequiredServerFiles(config: Record<string, unknown>) {
    await mkdir(join(appDir, ".next"));
    await writeFile(join(appDir, ".next", "required-server-files.json"), JSON.stringify({ version: 1, config }));
  }

  test("is the build's config as JSON when the build has standalone output", async () => {
    await writeRequiredServerFiles({ output: "standalone", distDir: "./.next" });

    expect(standaloneConfigOf(appDir)).toBe('{"output":"standalone","distDir":"./.next"}');
  });

  test("is undefined when the build has no standalone output", async () => {
    await writeRequiredServerFiles({ distDir: ".next" });

    expect(standaloneConfigOf(appDir)).toBeUndefined();
  });

  test("is undefined when the directory holds no build", () => {
    expect(standaloneConfigOf(appDir)).toBeUndefined();
  });
});

describe("watchRegister", () => {
  let writtenErrors: unknown[];

  beforeEach(() => {
    vi.useFakeTimers();
    writtenErrors = [];
    vi.spyOn(console, "error").mockImplementation((line: unknown) => {
      writtenErrors.push(line);
    });
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  function watchedBin() {
    const binProcess = Object.assign(new EventEmitter(), {
      exitCode: undefined as number | string | null | undefined,
      exit: vi.fn(),
    });
    const globalScope = {};
    const registerWatch = watchRegister(binProcess, globalScope);
    return {
      binProcess,
      registerWatch,
      notify: (notification: WorkerBinNotification) => notifyWorkerBin(notification, globalScope),
    };
  }

  function runThroughTheBound(binProcess: { exit: ReturnType<typeof vi.fn> }) {
    vi.advanceTimersByTime(NO_WORKER_EXIT_DELAY_IN_MILLISECONDS - 1);
    const hasExitedBeforeTheBound = binProcess.exit.mock.calls.length > 0;
    vi.advanceTimersByTime(1);
    return { hasExitedBeforeTheBound };
  }

  test("sets exit code 1 and writes the no-worker line once when the loop drains before any startWorker() call", () => {
    const { binProcess, registerWatch } = watchedBin();
    registerWatch.onPrepared();

    binProcess.emit("beforeExit");
    binProcess.emit("beforeExit");

    expect(binProcess.exitCode).toBe(1);
    expect(writtenErrors).toEqual([NO_WORKER_STARTED_ERROR]);
    expect(binProcess.exit).not.toHaveBeenCalled();
  });

  test("leaves a drain alone once register() called startWorker()", () => {
    const { binProcess, registerWatch, notify } = watchedBin();
    registerWatch.onPrepared();

    notify("startWorkerCalled");
    binProcess.emit("beforeExit");

    expect(binProcess.exitCode).toBeUndefined();
    expect(writtenErrors).toEqual([]);
  });

  test("exits 1 with the no-worker line when no startWorker() call comes within the bound counted from prepare()", () => {
    const { binProcess, registerWatch } = watchedBin();
    vi.advanceTimersByTime(2 * NO_WORKER_EXIT_DELAY_IN_MILLISECONDS);
    registerWatch.onPrepared();

    const { hasExitedBeforeTheBound } = runThroughTheBound(binProcess);

    expect(hasExitedBeforeTheBound).toBe(false);
    expect(binProcess.exit).toHaveBeenCalledExactlyOnceWith(1);
    expect(writtenErrors).toEqual([NO_WORKER_STARTED_ERROR]);
  });

  test("writes the no-worker line once when a drain already wrote it and the bound then ends a revived loop", () => {
    const { binProcess, registerWatch } = watchedBin();
    registerWatch.onPrepared();

    binProcess.emit("beforeExit");
    vi.advanceTimersByTime(NO_WORKER_EXIT_DELAY_IN_MILLISECONDS);

    expect(binProcess.exit).toHaveBeenCalledExactlyOnceWith(1);
    expect(writtenErrors).toEqual([NO_WORKER_STARTED_ERROR]);
  });

  test.each([
    ["before prepare() resolves", true],
    ["after prepare() resolves", false],
  ])("never gives up once startWorker() is called %s", (_, isCalledBeforePrepared) => {
    const { binProcess, registerWatch, notify } = watchedBin();

    if (isCalledBeforePrepared) notify("startWorkerCalled");
    registerWatch.onPrepared();
    if (!isCalledBeforePrepared) notify("startWorkerCalled");
    vi.advanceTimersByTime(10 * NO_WORKER_EXIT_DELAY_IN_MILLISECONDS);

    expect(binProcess.exit).not.toHaveBeenCalled();
    expect(writtenErrors).toEqual([]);
  });

  test("exits 1 when the library notifies a worker not started in the background", () => {
    const { binProcess, registerWatch, notify } = watchedBin();
    registerWatch.onPrepared();
    notify("startWorkerCalled");

    notify("workerNotStarted");

    expect(binProcess.exit).toHaveBeenCalledExactlyOnceWith(1);
  });

  test("sets exit code 1 without exiting when startWorker() rejects with no worker, so the app's catch runs to the end", () => {
    const { binProcess, registerWatch, notify } = watchedBin();
    registerWatch.onPrepared();
    notify("startWorkerCalled");

    notify("startWorkerRejectedWithNoWorker");
    binProcess.emit("beforeExit");

    expect(binProcess.exitCode).toBe(1);
    expect(binProcess.exit).not.toHaveBeenCalled();
    expect(writtenErrors).toEqual([]);
  });

  test("exits 1 with the rejection line when no worker runs within the bound counted from the rejection", () => {
    const { binProcess, registerWatch, notify } = watchedBin();
    registerWatch.onPrepared();
    vi.advanceTimersByTime(NO_WORKER_EXIT_DELAY_IN_MILLISECONDS / 2);
    notify("startWorkerCalled");
    notify("startWorkerRejectedWithNoWorker");

    const { hasExitedBeforeTheBound } = runThroughTheBound(binProcess);

    expect(hasExitedBeforeTheBound).toBe(false);
    expect(binProcess.exit).toHaveBeenCalledExactlyOnceWith(1);
    expect(writtenErrors).toEqual([START_WORKER_REJECTED_ERROR]);
    expect(START_WORKER_REJECTED_ERROR).toBe(
      "nextasking worker: startWorker() rejected and no worker started within 60 seconds. Exiting with code 1.",
    );
  });

  test.each([
    ["a rejected startWorker() the app retries", "rejection"],
    ["a drain whose loop a listener revives", "drain"],
  ])("clears the exit code it set on %s, and never gives up, once startWorker() resolves", (_, cause) => {
    const { binProcess, registerWatch, notify } = watchedBin();
    registerWatch.onPrepared();
    if (cause === "rejection") {
      notify("startWorkerCalled");
      notify("startWorkerRejectedWithNoWorker");
    } else {
      binProcess.emit("beforeExit");
    }

    notify("startWorkerCalled");
    notify("startWorkerResolved");
    vi.advanceTimersByTime(10 * NO_WORKER_EXIT_DELAY_IN_MILLISECONDS);

    expect(binProcess.exitCode).toBeUndefined();
    expect(binProcess.exit).not.toHaveBeenCalled();
  });

  test("keeps an exit code the app set itself after the rejection", () => {
    const { binProcess, registerWatch, notify } = watchedBin();
    registerWatch.onPrepared();
    notify("startWorkerCalled");
    notify("startWorkerRejectedWithNoWorker");

    binProcess.exitCode = 3;
    notify("startWorkerCalled");
    notify("startWorkerResolved");

    expect(binProcess.exitCode).toBe(3);
  });

  test("changes nothing when a second startWorker() call is rejected while a worker runs", () => {
    const { binProcess, registerWatch, notify } = watchedBin();
    registerWatch.onPrepared();
    notify("startWorkerCalled");
    notify("startWorkerResolved");

    notify("startWorkerCalled");
    vi.advanceTimersByTime(10 * NO_WORKER_EXIT_DELAY_IN_MILLISECONDS);

    expect(binProcess.exitCode).toBeUndefined();
    expect(binProcess.exit).not.toHaveBeenCalled();
  });

  test("ignores a notification it doesn't know, as from a newer library", () => {
    const globalScope = {};
    const binProcess = Object.assign(new EventEmitter(), { exitCode: undefined, exit: vi.fn() });
    watchRegister(binProcess, globalScope).onPrepared();

    notifyWorkerBin("aNotificationFromTheFuture" as WorkerBinNotification, globalScope);

    expect(binProcess.exit).not.toHaveBeenCalled();
    expect(binProcess.exitCode).toBeUndefined();
  });
});

describe("notifyWorkerBin", () => {
  test("does nothing on a global scope no bin listens on", () => {
    expect(() => notifyWorkerBin("workerNotStarted", {})).not.toThrow();
  });
});
