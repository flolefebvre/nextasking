import { expect, vi } from "vitest";
import { z } from "zod";

import { engineKeysOf } from "../src/engine/engine-keys.js";
import { createNextasking, defineTask } from "../src/index.js";
import { closeWhenTestFinishes } from "./close-when-test-finishes.js";
import { integrationTest } from "./integration-test.js";
import { createRecordingLogger, waitForLogMessage } from "./recording-logger.js";
import { startTcpProxy } from "./tcp-proxy.js";
import { withRedis } from "./with-redis.js";

integrationTest(
  "a run whose lock is taken over during an outage, after its attempt failed and before its failure is stored, logs its outcome discarded and no attempt failed",
  { timeout: 20_000 },
  async ({ redisUrl, keyPrefix }) => {
    const proxy = await startTcpProxy(redisUrl);
    let isConnectionCut = false;
    const { logger, lines } = createRecordingLogger();
    const chargeInvoice = defineTask("charge-invoice", {
      schema: z.object({ invoiceId: z.string() }),
      retries: 1,
      backoff: "1s",
      handler: async () => {
        await proxy.cut();
        isConnectionCut = true;
        throw new Error("card declined");
      },
    });
    const workerNextasking = createNextasking({
      connection: { url: proxy.url, giveUpAfter: "1s" },
      prefix: keyPrefix,
      logger,
      tasks: [chargeInvoice],
    });
    const directNextasking = createNextasking({
      connection: { url: redisUrl },
      prefix: keyPrefix,
      tasks: [chargeInvoice],
    });
    closeWhenTestFinishes({ proxy, nextaskings: [workerNextasking, directNextasking], lines });

    await workerNextasking.startWorker();
    const { runId } = await directNextasking.dispatch("charge-invoice", { invoiceId: "invoice-7" });
    await vi.waitUntil(() => isConnectionCut, { timeout: 10_000 });
    await withRedis(redisUrl, (redis) =>
      redis.hset(engineKeysOf(keyPrefix).runRecord("charge-invoice", runId), "lockToken", "token-of-another-attempt"),
    );
    await proxy.restore();
    await waitForLogMessage(lines, "attempt outcome discarded: lock lost", { timeout: 15_000 });

    expect(lines.filter((line) => line.message === "attempt outcome discarded: lock lost")).toEqual([
      {
        level: "warn",
        fields: { task: "charge-invoice", runId, attempt: 1, outcome: "retry" },
        message: "attempt outcome discarded: lock lost",
      },
    ]);
    expect(lines.filter((line) => line.message === "attempt failed" || line.message === "worker error")).toEqual([]);
  },
);
