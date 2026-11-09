import { expect, vi } from "vitest";
import { z } from "zod";

import { createNextasking, defineTask } from "../src/index.js";
import { closeNextaskingAndCheckItsDuration } from "./close-nextasking-and-check-its-duration.js";
import { integrationTest } from "./integration-test.js";
import { listenerCountsByEventOfClientsFor } from "./recorded-redis-clients.js";
import { startTcpProxy } from "./tcp-proxy.js";

vi.mock("@redis/client", async (importOriginal) =>
  (await import("./recorded-redis-clients.js")).redisClientModuleRecordingClients(await importOriginal()),
);

const BURST_CALL_COUNT = 60;

const HANDSHAKE_ARRIVAL_BOUND_IN_MILLISECONDS = 10_000;

function mostListenersOnOneEventOf(listenerCountsByEvent: readonly Record<string, number>[]): number {
  return Math.max(...listenerCountsByEvent.flatMap((listenerCounts) => Object.values(listenerCounts)));
}

integrationTest(
  "a burst of 60 calls right after createNextasking, made before its client is ready, leaves at most one listener per event on its Redis client while they wait and once they are answered",
  { timeout: HANDSHAKE_ARRIVAL_BOUND_IN_MILLISECONDS + 10_000 + 5_000 },
  async ({ redisUrl, keyPrefix }) => {
    const proxy = await startTcpProxy(redisUrl);
    const handshake = proxy.holdRequest(() => true);
    const nextasking = createNextasking({
      connection: { url: proxy.url },
      prefix: keyPrefix,
      tasks: [defineTask("send-invoice", { schema: z.object({}), handler: async () => undefined })],
    });
    try {
      const calls = Array.from({ length: BURST_CALL_COUNT }, (_, position) =>
        nextasking.dispatch("send-invoice", {}, { runId: `invoice-${String(position)}` }),
      );
      const { release } = await vi.waitUntil(() => handshake, { timeout: HANDSHAKE_ARRIVAL_BOUND_IN_MILLISECONDS });
      const listenerCountsWhileTheCallsWait = listenerCountsByEventOfClientsFor(proxy.url);
      release();
      const answers = await Promise.all(calls);
      const listenerCountsOnceAnswered = listenerCountsByEventOfClientsFor(proxy.url);

      expect(answers).toHaveLength(BURST_CALL_COUNT);
      expect(listenerCountsWhileTheCallsWait).toHaveLength(1);
      expect(
        mostListenersOnOneEventOf(listenerCountsWhileTheCallsWait),
        JSON.stringify(listenerCountsWhileTheCallsWait),
      ).toBeLessThanOrEqual(1);
      expect(
        mostListenersOnOneEventOf(listenerCountsOnceAnswered),
        JSON.stringify(listenerCountsOnceAnswered),
      ).toBeLessThanOrEqual(1);
    } finally {
      await closeNextaskingAndCheckItsDuration(nextasking);
      await proxy.stop();
    }
  },
);
