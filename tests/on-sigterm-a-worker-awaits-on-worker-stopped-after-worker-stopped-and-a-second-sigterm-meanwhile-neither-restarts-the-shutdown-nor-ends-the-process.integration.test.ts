import { expect } from "vitest";

import { parseDuration } from "../src/durations.js";
import type { Duration } from "../src/durations.js";
import { TEST_TIMEOUT_PAST_ITS_WAITS_IN_MILLISECONDS, integrationTest } from "./integration-test.js";
import { CLOSE_BOUND_PAST_THE_SHUTDOWN_TIMEOUT_IN_MILLISECONDS } from "./invariant-violations.js";
import { waitForLogMessage } from "./recording-logger.js";
import { WORKER_PROCESS_READY_TIMEOUT_IN_MILLISECONDS, startWorkerProcess } from "./worker-process.js";
import { ON_WORKER_STOPPED_CALLED, ON_WORKER_STOPPED_SETTLED } from "./worker-process-config.js";

const SHUTDOWN_TIMEOUT: Duration = "5s";

const ON_WORKER_STOPPED_DELAY_IN_MILLISECONDS = 1000;

const ON_WORKER_STOPPED_CALL_WAIT_IN_MILLISECONDS =
  parseDuration(SHUTDOWN_TIMEOUT) + CLOSE_BOUND_PAST_THE_SHUTDOWN_TIMEOUT_IN_MILLISECONDS;

integrationTest(
  "on SIGTERM, a worker process awaits onWorkerStopped after worker stopped, and a second SIGTERM meanwhile neither restarts the shutdown nor ends the process before it settled",
  {
    timeout:
      WORKER_PROCESS_READY_TIMEOUT_IN_MILLISECONDS +
      ON_WORKER_STOPPED_CALL_WAIT_IN_MILLISECONDS +
      ON_WORKER_STOPPED_DELAY_IN_MILLISECONDS +
      TEST_TIMEOUT_PAST_ITS_WAITS_IN_MILLISECONDS,
  },
  async ({ redisUrl, keyPrefix }) => {
    const workerProcess = await startWorkerProcess({
      redisUrl,
      keyPrefix,
      startWorkerOptions: { shutdownTimeout: SHUTDOWN_TIMEOUT },
      onWorkerStoppedDelayInMilliseconds: ON_WORKER_STOPPED_DELAY_IN_MILLISECONDS,
    });

    try {
      workerProcess.kill("SIGTERM");
      await waitForLogMessage(workerProcess.logLines, ON_WORKER_STOPPED_CALLED, {
        timeout: ON_WORKER_STOPPED_CALL_WAIT_IN_MILLISECONDS,
      });
      workerProcess.kill("SIGTERM");
      const exit = await workerProcess.exited;

      expect(exit).toEqual({ code: 0, signal: null });
      expect(workerProcess.logLines.map(({ message }) => message).slice(-3)).toEqual([
        "worker stopped",
        ON_WORKER_STOPPED_CALLED,
        ON_WORKER_STOPPED_SETTLED,
      ]);
      expect(workerProcess.logLines.filter(({ message }) => message === ON_WORKER_STOPPED_CALLED)).toHaveLength(1);
    } finally {
      await workerProcess.stop();
    }
  },
);
