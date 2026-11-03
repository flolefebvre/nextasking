import { expect } from "vitest";

import { e2eTest } from "./e2e-test.js";
import { startStandaloneServer } from "./next-process.js";
import { expectAWelcomeEmailDispatchToRun } from "./welcome-email-route.js";
import { startWorkerBin } from "./worker-bin-process.js";

e2eTest(
  "with output standalone, the worker bin traced into the standalone output runs there, moved out of the project, what its server.js dispatches",
  async ({ redisUrl, keyPrefix, standaloneAppDir, fixtureAppEnv, fixtureAppEnvWithWorkerOff }) => {
    const standaloneServer = await startStandaloneServer(standaloneAppDir, { env: fixtureAppEnvWithWorkerOff });
    try {
      const workerBin = await startWorkerBin(standaloneAppDir, { env: fixtureAppEnv, placement: "standalone" });
      try {
        await expectAWelcomeEmailDispatchToRun({ appUrl: standaloneServer.url, redisUrl, keyPrefix });

        expect(workerBin.output()).toContain('"message":"worker started"');
        expect(standaloneServer.output()).not.toContain('"message":"worker started"');
      } finally {
        await workerBin.stop();
      }
    } finally {
      await standaloneServer.stop();
    }
  },
);
