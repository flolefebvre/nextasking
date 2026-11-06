import { expect } from "vitest";

import { NO_WORKER_STARTED_ERROR } from "../../src/bin.js";
import { e2eTest } from "./e2e-test.js";
import { WORKER_BIN_PLACEMENTS, appDirOfPlacement, startWorkerBin } from "./worker-bin-process.js";

e2eTest.for(WORKER_BIN_PLACEMENTS)(
  "the %s nextasking worker bin starts the worker of a register() that awaits an import and a 1.5 s timer before startWorker()",
  async (placement, { fixtureAppDir, standaloneAppDir, fixtureAppEnv }) => {
    const appDir = appDirOfPlacement(placement, { fixtureAppDir, standaloneAppDir });

    const workerBin = await startWorkerBin(appDir, {
      env: { ...fixtureAppEnv, NEXTASKING_E2E_REGISTER: "awaits-before-start-worker" },
      placement,
    });

    try {
      const output = workerBin.output();
      expect(output.indexOf('"message":"app waited before startWorker"')).toBeGreaterThan(-1);
      expect(output.indexOf('"message":"app waited before startWorker"')).toBeLessThan(
        output.indexOf('"message":"worker started"'),
      );
      expect(output).not.toContain(NO_WORKER_STARTED_ERROR);
      expect(workerBin.hasExited()).toBe(false);
    } finally {
      await workerBin.stop();
    }
  },
);
