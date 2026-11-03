import { e2eTest } from "./e2e-test.js";
import { startNext } from "./next-process.js";
import { expectAWelcomeEmailDispatchToRun } from "./welcome-email-route.js";

e2eTest(
  "a dispatch from a route handler runs in the worker embedded in next start",
  async ({ redisUrl, keyPrefix, fixtureAppDir, fixtureAppEnv }) => {
    const nextProcess = await startNext(fixtureAppDir, { command: "start", env: fixtureAppEnv });

    try {
      await expectAWelcomeEmailDispatchToRun({ appUrl: nextProcess.url, redisUrl, keyPrefix });
    } finally {
      await nextProcess.stop();
    }
  },
);
