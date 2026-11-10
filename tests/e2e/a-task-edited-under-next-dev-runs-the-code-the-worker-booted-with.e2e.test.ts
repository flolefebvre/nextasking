import { readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { expect, vi } from "vitest";

import { e2eTest } from "./e2e-test.js";
import { startNext } from "./next-process.js";
import { dispatchABuildReportAndWaitForItsAttempt, expectTheBuildReportRecorded } from "./report-route.js";

const BOOTED_RESULT = "return 42;";

const EDITED_RESULT = "return 43;";

e2eTest(
  "a task edited under next dev after the worker booted runs the code the worker booted with",
  async ({ redisUrl, keyPrefix, fixtureAppDir, fixtureAppEnv }) => {
    const taskPath = join(fixtureAppDir, "src", "nextasking", "tasks", "build-report.ts");
    const bootedTaskSource = await readFile(taskPath, "utf-8");
    expect(bootedTaskSource).toContain(BOOTED_RESULT);
    const nextProcess = await startNext(fixtureAppDir, { command: "dev", env: fixtureAppEnv });

    try {
      await expect
        .poll(nextProcess.output, { timeout: 15_000 })
        .toContain('{"level":"info","message":"app registered"}');
      await writeFile(taskPath, bootedTaskSource.replace(BOOTED_RESULT, EDITED_RESULT));

      const runId = await dispatchABuildReportAndWaitForItsAttempt(nextProcess.url, nextProcess.output);

      await vi.waitFor(() => expectTheBuildReportRecorded(redisUrl, keyPrefix, runId), { timeout: 15_000 });
    } finally {
      await nextProcess.stop();
      await writeFile(taskPath, bootedTaskSource);
    }
  },
);
