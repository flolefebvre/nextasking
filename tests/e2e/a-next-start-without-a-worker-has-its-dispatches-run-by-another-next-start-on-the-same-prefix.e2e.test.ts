import { expect } from "vitest";

import { e2eTest } from "./e2e-test.js";
import { startNext } from "./next-process.js";
import { expectAWelcomeEmailDispatchToRun } from "./welcome-email-route.js";

e2eTest(
  "a next start with the worker gate off has its dispatches run by another next start with the worker on, on the same prefix",
  async ({ redisUrl, keyPrefix, fixtureAppDir, fixtureAppEnv, fixtureAppEnvWithWorkerOff }) => {
    const webServerOnly = await startNext(fixtureAppDir, { command: "start", env: fixtureAppEnvWithWorkerOff });
    try {
      const webServerWithTheWorker = await startNext(fixtureAppDir, { command: "start", env: fixtureAppEnv });
      try {
        await expect
          .poll(() => webServerWithTheWorker.output(), { timeout: 15_000 })
          .toContain('"message":"worker started"');

        await expectAWelcomeEmailDispatchToRun({ appUrl: webServerOnly.url, redisUrl, keyPrefix });

        expect(webServerOnly.output()).not.toContain('"message":"worker started"');
      } finally {
        await webServerWithTheWorker.stop();
      }
    } finally {
      await webServerOnly.stop();
    }
  },
);
