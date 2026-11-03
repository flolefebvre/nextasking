import { expect, vi } from "vitest";

import { fixtureRecordsOf } from "./fixture-records.js";

export async function postWelcomeEmail(appUrl: string, email: string): Promise<Response> {
  return fetch(`${appUrl}/api/welcome-emails`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ email }),
  });
}

export async function expectAWelcomeEmailDispatchToRun({
  appUrl,
  redisUrl,
  keyPrefix,
}: {
  appUrl: string;
  redisUrl: string;
  keyPrefix: string;
}): Promise<void> {
  const response = await postWelcomeEmail(appUrl, "ada@example.com");

  expect(response.status).toBe(202);
  const { runId } = (await response.json()) as { runId: string };
  await vi.waitFor(
    async () => {
      expect(await fixtureRecordsOf(redisUrl, keyPrefix, "send-welcome-email")).toEqual([
        { runId, payload: { email: "ada@example.com" } },
      ]);
    },
    { timeout: 15_000 },
  );
}
