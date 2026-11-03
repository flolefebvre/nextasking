import { e2eTest } from "./e2e-test.js";
import { startNext } from "./next-process.js";
import { expectAWelcomeEmailDispatchToRun } from "./welcome-email-route.js";

e2eTest(
  "the worker started from instrumentation runs a dispatch under next dev",
  async ({ redisUrl, keyPrefix, fixtureAppDir, fixtureAppEnv }) => {
    const nextProcess = await startNext(fixtureAppDir, { command: "dev", env: fixtureAppEnv });

    try {
      await expectAWelcomeEmailDispatchToRun({ appUrl: nextProcess.url, redisUrl, keyPrefix });
    } finally {
      await nextProcess.stop();
    }
  },
);
