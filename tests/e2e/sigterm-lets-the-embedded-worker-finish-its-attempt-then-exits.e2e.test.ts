import { expect } from "vitest";

import { fixtureRecordsOf } from "../fixture-records.js";
import { e2eTest } from "./e2e-test.js";
import { startNext } from "./next-process.js";

e2eTest(
  "with NEXT_MANUAL_SIG_HANDLE, SIGTERM lets the embedded worker finish the attempt in flight, then next start exits 0",
  async ({ redisUrl, keyPrefix, fixtureAppDir, fixtureAppEnv }) => {
    const nextProcess = await startNext(fixtureAppDir, {
      command: "start",
      env: { ...fixtureAppEnv, NEXT_MANUAL_SIG_HANDLE: "1" },
    });

    try {
      const response = await fetch(`${nextProcess.url}/api/reports`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ reportId: "report-7" }),
      });
      const { runId } = (await response.json()) as { runId: string };
      await expect
        .poll(() => nextProcess.output(), { timeout: 15_000 })
        .toContain(`"message":"attempt started","task":"build-report","runId":"${runId}"`);
      const exit = await nextProcess.stop();

      expect(exit).toEqual({ code: 0, signal: null, wasForced: false });
      expect(await fixtureRecordsOf(redisUrl, keyPrefix, "build-report")).toEqual([
        { runId, payload: { reportId: "report-7" }, rowCount: 42 },
      ]);
      expect(nextProcess.output()).toContain('"message":"worker stopped"');
      expect(nextProcess.output()).not.toContain("NEXT_MANUAL_SIG_HANDLE is not set");
    } finally {
      await nextProcess.stop();
    }
  },
);
