import { expect } from "vitest";

import { e2eTest } from "./e2e-test.js";
import { startStandaloneServer } from "./next-process.js";
import { expectAWelcomeEmailDispatchToRun } from "./welcome-email-route.js";

e2eTest(
  "with output standalone and no other config, the worker embedded in the standalone server.js, moved out of the project, runs what it dispatches",
  async ({ redisUrl, keyPrefix, standaloneAppDir, fixtureAppEnv }) => {
    const standaloneServer = await startStandaloneServer(standaloneAppDir, { env: fixtureAppEnv });

    try {
      await expectAWelcomeEmailDispatchToRun({ appUrl: standaloneServer.url, redisUrl, keyPrefix });

      expect(standaloneServer.output()).toContain('"message":"worker started"');
    } finally {
      await standaloneServer.stop();
    }
  },
);
