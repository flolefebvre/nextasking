import { expect } from "vitest";

import { e2eTest } from "./e2e-test.js";
import { startNext } from "./next-process.js";
import { dispatchABuildReportAndWaitForItsAttempt } from "./report-route.js";
import { fixtureRecordsOf } from "../fixture-records.js";
import { runLeftExecutingSentenceOf } from "../stored-key-violations.js";
import { withRedis } from "../with-redis.js";
import { engineKeysOf } from "../../src/engine/engine-keys.js";

e2eTest(
  "stopping next dev cuts the attempt in flight where it stands, even with NEXT_MANUAL_SIG_HANDLE: no drain, no step boundary, the run left executing under its lock",
  async ({ redisUrl, keyPrefix, fixtureAppDir, fixtureAppEnv, expectInvariantViolation }) => {
    const nextProcess = await startNext(fixtureAppDir, {
      command: "dev",
      env: { ...fixtureAppEnv, NEXT_MANUAL_SIG_HANDLE: "1" },
    });

    try {
      const runId = await dispatchABuildReportAndWaitForItsAttempt(nextProcess.url, nextProcess.output);
      expectInvariantViolation("run left executing", runLeftExecutingSentenceOf("build-report", runId));
      const exit = await nextProcess.stop();

      expect(exit.wasForced).toBe(false);
      expect(nextProcess.output()).not.toContain('"message":"worker stopped"');
      expect(await fixtureRecordsOf(redisUrl, keyPrefix, "build-report")).toEqual([]);
      const state = await withRedis(redisUrl, (redis) =>
        redis.hGet(engineKeysOf(keyPrefix).runRecord("build-report", runId), "state"),
      );
      expect(state).toBe("executing");
    } finally {
      await nextProcess.stop();
    }
  },
);
