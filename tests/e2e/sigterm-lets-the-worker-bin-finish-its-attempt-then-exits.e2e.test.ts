import { expect } from "vitest";

import { e2eTest } from "./e2e-test.js";
import { startNext } from "./next-process.js";
import { dispatchABuildReportAndWaitForItsAttempt, expectTheBuildReportRecorded } from "./report-route.js";
import { startWorkerBin } from "./worker-bin-process.js";

e2eTest(
  "without NEXT_MANUAL_SIG_HANDLE in its environment, SIGTERM lets the worker bin finish the attempt in flight, then it exits 0",
  async ({ redisUrl, keyPrefix, fixtureAppDir, fixtureAppEnv, fixtureAppEnvWithWorkerOff }) => {
    const nextProcess = await startNext(fixtureAppDir, { command: "start", env: fixtureAppEnvWithWorkerOff });
    try {
      const workerBin = await startWorkerBin(fixtureAppDir, {
        env: { ...fixtureAppEnv, NEXT_MANUAL_SIG_HANDLE: "" },
      });
      try {
        const runId = await dispatchABuildReportAndWaitForItsAttempt(nextProcess.url, workerBin.output);
        const exit = await workerBin.stop();

        expect(exit).toEqual({ code: 0, signal: null, wasForced: false });
        await expectTheBuildReportRecorded(redisUrl, keyPrefix, runId);
        expect(workerBin.output()).toContain('"message":"worker stopped"');
        expect(workerBin.output()).not.toContain("NEXT_MANUAL_SIG_HANDLE is not set");
      } finally {
        await workerBin.stop();
      }
    } finally {
      await nextProcess.stop();
    }
  },
);
