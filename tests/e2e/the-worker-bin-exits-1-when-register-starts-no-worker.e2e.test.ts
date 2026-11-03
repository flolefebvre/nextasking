import { expect } from "vitest";

import { e2eTest } from "./e2e-test.js";
import { spawnWorkerBin } from "./worker-bin-process.js";

e2eTest(
  "the nextasking worker bin exits 1 with one line on stderr when register() starts no worker",
  async ({ fixtureAppDir, fixtureAppEnvWithWorkerOff }) => {
    const workerBin = spawnWorkerBin(fixtureAppDir, { env: fixtureAppEnvWithWorkerOff });

    try {
      const exit = await workerBin.exited;

      expect(exit).toEqual({ code: 1, signal: null, wasForced: false });
      expect(workerBin.output()).toContain(
        "nextasking worker: register() started no worker. Turn on the environment variable your instrumentation.ts checks before calling startWorker().",
      );
      expect(workerBin.output()).not.toContain('"message":"worker started"');
    } finally {
      await workerBin.stop();
    }
  },
);
