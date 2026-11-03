import { expect, vi } from "vitest";

import { e2eTest } from "./e2e-test.js";
import { fixtureRecordsOf } from "./fixture-records.js";
import { startNext } from "./next-process.js";
import { submitServerActionForm } from "./server-action-form.js";

e2eTest(
  "a server action's emit runs each of the event's two listeners once",
  async ({ redisUrl, keyPrefix, fixtureAppDir, fixtureAppEnv }) => {
    const nextProcess = await startNext(fixtureAppDir, { command: "start", env: fixtureAppEnv });

    try {
      const response = await submitServerActionForm(`${nextProcess.url}/invoices`, { invoiceId: "inv-1" });

      expect(response.status).toBe(200);
      await vi.waitFor(
        async () => {
          for (const listenerName of ["record-invoice-payment", "send-receipt"]) {
            expect(await fixtureRecordsOf(redisUrl, keyPrefix, listenerName)).toEqual([
              { runId: expect.any(String), payload: { invoiceId: "inv-1" } },
            ]);
          }
        },
        { timeout: 15_000 },
      );
    } finally {
      await nextProcess.stop();
    }
  },
);
