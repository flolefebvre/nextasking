import { expect } from "vitest";

import { fixtureRecordsOf } from "../fixture-records.js";

export const REPORT_ID = "report-7";

export async function dispatchABuildReportAndWaitForItsAttempt(
  appUrl: string,
  workerOutput: () => string,
): Promise<string> {
  const response = await fetch(`${appUrl}/api/reports`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ reportId: REPORT_ID }),
  });
  const { runId } = (await response.json()) as { runId: string };
  await expect
    .poll(workerOutput, { timeout: 15_000 })
    .toContain(`"message":"attempt started","task":"build-report","runId":"${runId}"`);
  return runId;
}

export async function expectTheBuildReportRecorded(redisUrl: string, keyPrefix: string, runId: string): Promise<void> {
  expect(await fixtureRecordsOf(redisUrl, keyPrefix, "build-report")).toEqual([
    { runId, payload: { reportId: REPORT_ID }, rowCount: 42 },
  ]);
}
