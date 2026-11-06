import { expect } from "vitest";

import { EVICTING_POLICY_REJECTION, startEvictingServer } from "../evicting-server.js";
import { providedRedisUrl } from "../valkey-container.js";
import { e2eTest } from "./e2e-test.js";
import { spawnWorkerBin } from "./worker-bin-process.js";

e2eTest.skipIf(providedRedisUrl() !== undefined)(
  "the nextasking worker bin started with Valkey down exits 1, after the app's onWorkerNotStarted, once Valkey is back with an evicting maxmemory-policy",
  async ({ fixtureAppDir, fixtureAppEnv }) => {
    const evictingServer = await startEvictingServer();
    await evictingServer.proxy.cut();
    const workerBin = spawnWorkerBin(fixtureAppDir, { env: { ...fixtureAppEnv, REDIS_URL: evictingServer.proxy.url } });

    try {
      await expect
        .poll(() => workerBin.output(), { timeout: 60_000 })
        .toContain('{"level":"info","message":"app registered"}');
      expect(workerBin.output()).toContain('"level":"error","message":"connection error"');
      await evictingServer.proxy.restore();
      const exit = await workerBin.exited;

      expect(exit).toEqual({ code: 1, signal: null, wasForced: false });
      const output = workerBin.output();
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
    } finally {
      await workerBin.stop();
      await evictingServer.stop();
    }
  },
  120_000,
);
