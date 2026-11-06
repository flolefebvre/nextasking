import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";

import { hasStartWorkerBeenCalled, isWorkerCommand, listenForWorkerNotStarted, standaloneConfigOf } from "./bin.js";
import { markStartWorkerCalled, notifyWorkerBinOfWorkerNotStarted } from "./worker-bin-handshake.js";

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

describe("hasStartWorkerBeenCalled", () => {
  test("is false until the library marks a startWorker call on the same global scope", () => {
    const globalScope = {};
    const isCalledBeforeTheMark = hasStartWorkerBeenCalled(globalScope);

    markStartWorkerCalled(globalScope);

    expect(isCalledBeforeTheMark).toBe(false);
    expect(hasStartWorkerBeenCalled(globalScope)).toBe(true);
  });
});

describe("listenForWorkerNotStarted", () => {
  test("hears the library notify the worker bin on the same global scope", () => {
    const globalScope = {};
    const onWorkerNotStarted = vi.fn();

    listenForWorkerNotStarted(onWorkerNotStarted, globalScope);
    notifyWorkerBinOfWorkerNotStarted(globalScope);

    expect(onWorkerNotStarted).toHaveBeenCalledOnce();
  });

  test("lets the library notify a global scope no bin listens on", () => {
    expect(() => notifyWorkerBinOfWorkerNotStarted({})).not.toThrow();
  });
});
