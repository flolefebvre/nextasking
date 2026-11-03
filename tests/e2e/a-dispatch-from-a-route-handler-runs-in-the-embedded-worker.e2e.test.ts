import { expect, vi } from "vitest";

import { e2eTest } from "./e2e-test.js";
import { fixtureRecordsOf } from "./fixture-records.js";
import { startNext } from "./next-process.js";

e2eTest(
  "a dispatch from a route handler runs in the worker embedded in next start",
  async ({ redisUrl, keyPrefix, fixtureAppDir, fixtureAppEnv }) => {
    const nextProcess = await startNext(fixtureAppDir, { command: "start", env: fixtureAppEnv });

    try {
      const response = await fetch(`${nextProcess.url}/api/welcome-emails`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ email: "ada@example.com" }),
      });

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
    } finally {
      await nextProcess.stop();
    }
  },
);
