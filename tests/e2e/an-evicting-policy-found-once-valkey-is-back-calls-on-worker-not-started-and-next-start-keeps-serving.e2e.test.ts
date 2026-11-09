import { expect } from "vitest";

import { EVICTING_POLICY_REJECTION, startEvictingServer } from "../evicting-server.js";
import { providedRedisUrl } from "../valkey-container.js";
import { e2eTest } from "./e2e-test.js";
import { startNext } from "./next-process.js";

e2eTest.skipIf(providedRedisUrl() !== undefined)(
  "next start booted with Valkey down writes worker not started, then calls the app's onWorkerNotStarted, once Valkey is back with an evicting maxmemory-policy, and keeps serving",
  async ({ fixtureAppDir, fixtureAppEnv }) => {
    const evictingServer = await startEvictingServer();
    await evictingServer.proxy.cut();
    try {
      const nextProcess = await startNext(fixtureAppDir, {
        command: "start",
        env: { ...fixtureAppEnv, REDIS_URL: evictingServer.proxy.url },
      });
      try {
        await expect
          .poll(() => nextProcess.output(), { timeout: 60_000 })
          .toContain('{"level":"info","message":"app registered"}');
        expect(nextProcess.output()).toContain('"level":"error","message":"connection error"');

        await evictingServer.proxy.restore();
        await expect
          .poll(() => nextProcess.output(), { timeout: 60_000 })
          .toContain('"message":"app notified: worker not started"');

        const output = nextProcess.output();
        expect(output).toContain('"level":"error","message":"worker not started"');
        expect(output).toContain(
          JSON.stringify({
            level: "error",
            message: "app notified: worker not started",
            reason: EVICTING_POLICY_REJECTION,
          }),
        );
        expect(output.indexOf('"message":"worker not started"')).toBeLessThan(
          output.indexOf('"message":"app notified: worker not started"'),
        );
        expect(output).not.toContain('"message":"worker started"');
        expect(nextProcess.hasExited()).toBe(false);
        expect((await fetch(`${nextProcess.url}/invoices`)).status).toBe(200);
      } finally {
        await nextProcess.stop();
      }
    } finally {
      await evictingServer.stop();
    }
  },
  180_000,
);
