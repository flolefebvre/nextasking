import { expect } from "vitest";

import { NO_WORKER_EXIT_DELAY_IN_MILLISECONDS } from "../../src/bin.js";
import { e2eTest } from "./e2e-test.js";
import { WORKER_BIN_PLACEMENTS, appDirOfPlacement, spawnWorkerBin } from "./worker-bin-process.js";

e2eTest.for(WORKER_BIN_PLACEMENTS)(
  "the %s nextasking worker bin exits 1 with one line on stderr when register() starts no worker, once its event loop drains, well before the bound",
  async (placement, { fixtureAppDir, standaloneAppDir, fixtureAppEnvWithWorkerOff }) => {
    const spawnedAt = Date.now();
    const workerBin = spawnWorkerBin(appDirOfPlacement(placement, { fixtureAppDir, standaloneAppDir }), {
      env: fixtureAppEnvWithWorkerOff,
      placement,
    });

    try {
      const exit = await workerBin.exited;

      expect(exit).toEqual({ code: 1, signal: null, wasForced: false });
      expect(Date.now() - spawnedAt).toBeLessThan(NO_WORKER_EXIT_DELAY_IN_MILLISECONDS / 2);
      expect(workerBin.output()).toContain(
        "nextasking worker: register() started no worker. If an error is printed above, register() failed before it called startWorker(). If not, turn on the environment variable your instrumentation.ts checks before calling startWorker().",
      );
      expect(workerBin.output()).not.toContain('"message":"worker started"');
    } finally {
      await workerBin.stop();
    }
  },
);
