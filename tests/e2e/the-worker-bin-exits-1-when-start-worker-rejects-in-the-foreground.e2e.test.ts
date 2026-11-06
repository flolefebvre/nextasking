import { expect } from "vitest";

import { EVICTING_POLICY_REJECTION, startEvictingServer } from "../evicting-server.js";
import { providedRedisUrl } from "../valkey-container.js";
import { e2eTest } from "./e2e-test.js";
import { spawnWorkerBin } from "./worker-bin-process.js";

e2eTest.skipIf(providedRedisUrl() !== undefined)(
  "the nextasking worker bin exits 1 when startWorker() rejects on a reachable server with an evicting maxmemory-policy",
  async ({ fixtureAppDir, fixtureAppEnv }) => {
    const evictingServer = await startEvictingServer();
    const workerBin = spawnWorkerBin(fixtureAppDir, { env: { ...fixtureAppEnv, REDIS_URL: evictingServer.url } });

    try {
      const exit = await workerBin.exited;

      expect(exit).toEqual({ code: 1, signal: null, wasForced: false });
      expect(workerBin.output()).toContain(EVICTING_POLICY_REJECTION);
      expect(workerBin.output()).not.toContain('"message":"worker started"');
    } finally {
      await workerBin.stop();
      await evictingServer.stop();
    }
  },
);

e2eTest(
  "the nextasking worker bin exits 1 when startWorker() rejects an invalid option",
  async ({ fixtureAppDir, fixtureAppEnv }) => {
    const workerBin = spawnWorkerBin(fixtureAppDir, {
      env: { ...fixtureAppEnv, NEXTASKING_E2E_START_WORKER: "invalid-concurrency" },
    });

    try {
      const exit = await workerBin.exited;

      expect(exit).toEqual({ code: 1, signal: null, wasForced: false });
      expect(workerBin.output()).toContain("Invalid startWorker concurrency: 0 is not a positive integer");
    } finally {
      await workerBin.stop();
    }
  },
);
