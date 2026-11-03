import { expect } from "vitest";

import { startTcpProxy } from "../tcp-proxy.js";
import { e2eTest } from "./e2e-test.js";
import { startNext } from "./next-process.js";
import { expectAWelcomeEmailDispatchToRun, postWelcomeEmail } from "./welcome-email-route.js";

e2eTest(
  "with Valkey down at boot, next start serves pages and answers 503 on dispatch, then runs the dispatch once Valkey is back",
  async ({ redisUrl, keyPrefix, fixtureAppDir, fixtureAppEnv }) => {
    const proxy = await startTcpProxy(redisUrl);
    await proxy.cut();
    const nextProcess = await startNext(fixtureAppDir, {
      command: "start",
      env: { ...fixtureAppEnv, REDIS_URL: proxy.url },
    });

    try {
      const pageResponse = await fetch(`${nextProcess.url}/invoices`);
      const dispatchResponse = await postWelcomeEmail(nextProcess.url, "ada@example.com");

      expect(pageResponse.status).toBe(200);
      expect(dispatchResponse.status).toBe(503);
      expect(await dispatchResponse.json()).toEqual({ runId: expect.any(String) });
      expect(nextProcess.output()).toContain('"level":"error","message":"connection error"');
      expect(nextProcess.output()).not.toContain('"message":"worker started"');

      await proxy.restore();
      await expect
        .poll(() => nextProcess.output(), { timeout: 30_000 })
        .toContain('{"level":"info","message":"worker started"}');
      await expectAWelcomeEmailDispatchToRun({ appUrl: nextProcess.url, redisUrl, keyPrefix });
    } finally {
      await nextProcess.stop();
      await proxy.stop();
    }
  },
);
