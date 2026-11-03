import { expect, vi } from "vitest";

import { e2eTest } from "./e2e-test.js";
import { startNext } from "./next-process.js";
import { postWelcomeEmail } from "./welcome-email-route.js";

const STARTUP_ERROR =
  'An error occurred while loading instrumentation hook: Listener "record-invoice-payment" listens to event "invoice.paid", which is not listed in events';

e2eTest(
  "a config error fails loudly at boot: next start logs it and answers 500 to every request",
  async ({ fixtureAppDir, fixtureAppEnv }) => {
    const nextProcess = await startNext(fixtureAppDir, {
      command: "start",
      env: { ...fixtureAppEnv, NEXTASKING_E2E_CONFIG_ERROR: "unlisted-event" },
    });

    try {
      const response = await postWelcomeEmail(nextProcess.url, "ada@example.com");

      expect(response.status).toBe(500);
      await vi.waitFor(
        () => {
          expect(nextProcess.output()).toContain(STARTUP_ERROR);
        },
        { timeout: 5_000 },
      );
    } finally {
      await nextProcess.stop();
    }
  },
);
