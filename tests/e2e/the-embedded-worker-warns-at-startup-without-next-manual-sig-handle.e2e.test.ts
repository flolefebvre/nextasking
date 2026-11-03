import { expect } from "vitest";

import { e2eTest } from "./e2e-test.js";
import { startNext } from "./next-process.js";

e2eTest(
  "without NEXT_MANUAL_SIG_HANDLE, the worker embedded in next start warns at startup that a deploy cuts its attempts",
  async ({ fixtureAppDir, fixtureAppEnv }) => {
    const nextProcess = await startNext(fixtureAppDir, { command: "start", env: fixtureAppEnv });

    try {
      await expect
        .poll(() => nextProcess.output(), { timeout: 10_000 })
        .toContain(
          '{"level":"warn","message":"NEXT_MANUAL_SIG_HANDLE is not set: on SIGTERM Next exits at once and cuts the attempts running in this worker"}',
        );
    } finally {
      await nextProcess.stop();
    }
  },
);
