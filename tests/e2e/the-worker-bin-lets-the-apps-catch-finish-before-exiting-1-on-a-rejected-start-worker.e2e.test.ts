import { expect } from "vitest";

import { e2eTest } from "./e2e-test.js";
import { spawnWorkerBin } from "./worker-bin-process.js";

e2eTest(
  "the nextasking worker bin lets a register() that catches the rejection of startWorker() write its line after an await, then exits 1",
  async ({ fixtureAppDir, fixtureAppEnv }) => {
    const workerBin = spawnWorkerBin(fixtureAppDir, {
      env: {
        ...fixtureAppEnv,
        NEXTASKING_E2E_START_WORKER: "invalid-concurrency",
        NEXTASKING_E2E_REGISTER: "catches-the-rejection",
      },
    });

    try {
      const exit = await workerBin.exited;

      expect(exit).toEqual({ code: 1, signal: null, wasForced: false });
      expect(workerBin.output()).toContain(
        JSON.stringify({
          level: "error",
          message: "app caught: worker not started",
          reason: "Invalid startWorker concurrency: 0 is not a positive integer",
        }),
      );
    } finally {
      await workerBin.stop();
    }
  },
);
