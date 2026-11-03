import { expect } from "vitest";

import { e2eTest } from "./e2e-test.js";
import { startNext } from "./next-process.js";
import { expectAWelcomeEmailDispatchToRun } from "./welcome-email-route.js";
import { startWorkerBin } from "./worker-bin-process.js";

e2eTest(
  "the nextasking worker bin, on the same build, runs what next start without a worker dispatches",
  async ({ redisUrl, keyPrefix, fixtureAppDir, fixtureAppEnv, fixtureAppEnvWithWorkerOff }) => {
    const nextProcess = await startNext(fixtureAppDir, { command: "start", env: fixtureAppEnvWithWorkerOff });
    try {
      const workerBin = await startWorkerBin(fixtureAppDir, { env: fixtureAppEnv });
      try {
        await expectAWelcomeEmailDispatchToRun({ appUrl: nextProcess.url, redisUrl, keyPrefix });

        expect(workerBin.output()).toContain('"message":"worker started"');
        expect(nextProcess.output()).not.toContain('"message":"worker started"');
      } finally {
        await workerBin.stop();
      }
    } finally {
      await nextProcess.stop();
    }
  },
);
