import { spawn } from "node:child_process";
import { once } from "node:events";
import { fileURLToPath } from "node:url";
import { expect, vi } from "vitest";

import { closeWhenTestFinishes } from "./close-when-test-finishes.js";
import { createGiveUpAfterOneSecondNextasking } from "./give-up-after-one-second-nextasking.js";
import { integrationTest } from "./integration-test.js";
import { createRecordingLogger, waitForLogMessage } from "./recording-logger.js";
import { startTcpProxy } from "./tcp-proxy.js";
import { startWorkerProcess } from "./worker-process.js";
import { CLOSE_WHILE_BOOTING_URL_VARIABLE } from "./worker-process-config.js";

const SLOWEST_RESOLVING_START_IN_MILLISECONDS = 4_000;

const NATURAL_EXIT_TIMEOUT_IN_MILLISECONDS = 15_000;

const REPO_ROOT_DIR = fileURLToPath(new URL("../", import.meta.url));

const CLOSE_WHILE_BOOTING_ENTRY_PATH = fileURLToPath(
  new URL("./close-while-booting-process-entry.ts", import.meta.url),
);

async function exitOfAProcessClosingAWorkerWhileItBoots(url: string): Promise<{ code: number | null; output: string }> {
  const child = spawn(process.execPath, ["--import", "tsx", CLOSE_WHILE_BOOTING_ENTRY_PATH], {
    cwd: REPO_ROOT_DIR,
    env: { ...process.env, NEXT_MANUAL_SIG_HANDLE: "", [CLOSE_WHILE_BOOTING_URL_VARIABLE]: url },
    stdio: ["ignore", "pipe", "pipe"],
  });
  const outputChunks: Buffer[] = [];
  child.stdout.on("data", (chunk: Buffer) => outputChunks.push(chunk));
  child.stderr.on("data", (chunk: Buffer) => outputChunks.push(chunk));
  const killIfStillRunning = setTimeout(() => child.kill("SIGKILL"), NATURAL_EXIT_TIMEOUT_IN_MILLISECONDS);
  const [code] = (await once(child, "exit")) as [number | null];
  clearTimeout(killIfStillRunning);
  return { code, output: Buffer.concat(outputChunks).toString() };
}

integrationTest(
  "startWorker() resolves while Redis is unreachable, and the worker syncs its schedules and starts once Redis is back",
  async ({ redisUrl, keyPrefix }) => {
    const proxy = await startTcpProxy(redisUrl);
    await proxy.cut();
    const { logger, lines } = createRecordingLogger();
    const nextasking = createGiveUpAfterOneSecondNextasking(proxy.url, keyPrefix, { logger });
    const onWorkerNotStarted = vi.fn();

    closeWhenTestFinishes({ proxy, nextaskings: [nextasking], lines });

    const startedAt = performance.now();
    await nextasking.startWorker({ onWorkerNotStarted });

    expect(performance.now() - startedAt).toBeLessThan(SLOWEST_RESOLVING_START_IN_MILLISECONDS);
    expect(lines.map((line) => line.message)).toContain("connection error");
    expect(lines.map((line) => line.message)).not.toContain("worker started");

    await proxy.restore();
    await waitForLogMessage(lines, "worker started", { timeout: 15_000 });
    const { runId } = await nextasking.dispatch("send-welcome-email", { email: "ada@example.com" });
    await vi.waitFor(
      async () => {
        expect(await nextasking.getRun("send-welcome-email", runId)).toMatchObject({ state: "completed" });
      },
      { timeout: 10_000 },
    );

    const messages = lines.map((line) => line.message);
    expect(messages.indexOf("schedule added")).toBeGreaterThan(-1);
    expect(messages.indexOf("schedule added")).toBeLessThan(messages.indexOf("worker started"));
    expect(messages.indexOf("connection restored")).toBeLessThan(messages.indexOf("worker started"));
    expect(onWorkerNotStarted).not.toHaveBeenCalled();
  },
  30_000,
);

integrationTest(
  "a process that closes its instance while the worker still waits for Redis at boot exits on its own",
  async ({ redisUrl }) => {
    const proxy = await startTcpProxy(redisUrl);
    await proxy.cut();

    closeWhenTestFinishes({ proxy });

    expect(await exitOfAProcessClosingAWorkerWhileItBoots(proxy.url)).toEqual({ code: 0, output: "" });
  },
  30_000,
);

integrationTest(
  "a worker process still waiting for Redis at boot exits 0 on SIGTERM",
  async ({ redisUrl, keyPrefix }) => {
    const proxy = await startTcpProxy(redisUrl);
    await proxy.cut();
    const workerProcess = await startWorkerProcess({ redisUrl: proxy.url, keyPrefix });

    try {
      workerProcess.kill("SIGTERM");

      expect(await workerProcess.exited).toEqual({ code: 0, signal: null });
      expect(workerProcess.logLines.map((line) => line.message)).toContain("connection error");
      expect(workerProcess.logLines.map((line) => line.message)).not.toContain("worker started");
    } finally {
      await workerProcess.stop();
      await proxy.stop();
    }
  },
  60_000,
);
