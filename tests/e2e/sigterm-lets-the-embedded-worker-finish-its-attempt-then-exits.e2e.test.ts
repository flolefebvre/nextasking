import { expect } from "vitest";

import { e2eTest } from "./e2e-test.js";
import { startNext } from "./next-process.js";
import { dispatchABuildReportAndWaitForItsAttempt, expectTheBuildReportRecorded } from "./report-route.js";

e2eTest(
  "with NEXT_MANUAL_SIG_HANDLE, SIGTERM lets the embedded worker finish the attempt in flight, then awaits onWorkerStopped after worker stopped, and next start exits 0",
  async ({ redisUrl, keyPrefix, fixtureAppDir, fixtureAppEnv }) => {
    const nextProcess = await startNext(fixtureAppDir, {
      command: "start",
      env: { ...fixtureAppEnv, NEXT_MANUAL_SIG_HANDLE: "1" },
    });

    try {
      const runId = await dispatchABuildReportAndWaitForItsAttempt(nextProcess.url, nextProcess.output);
      const exit = await nextProcess.stop();

      expect(exit).toEqual({ code: 0, signal: null, wasForced: false });
      await expectTheBuildReportRecorded(redisUrl, keyPrefix, runId);
      const workerStoppedAt = nextProcess.output().indexOf('"message":"worker stopped"');
      const appNotifiedAt = nextProcess.output().indexOf('"message":"app notified: worker stopped"');
      expect(workerStoppedAt).toBeGreaterThan(-1);
      expect(appNotifiedAt).toBeGreaterThan(workerStoppedAt);
      expect(nextProcess.output()).not.toContain("NEXT_MANUAL_SIG_HANDLE is not set");
    } finally {
      await nextProcess.stop();
    }
  },
);
