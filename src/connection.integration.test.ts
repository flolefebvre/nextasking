import { setTimeout as sleep } from "node:timers/promises";

import { expect, vi } from "vitest";

import { integrationTest } from "../tests/integration-test.js";
import { createRecordingLogger } from "../tests/recording-logger.js";
import { startTcpProxy } from "../tests/tcp-proxy.js";
import { createRedisConnections } from "./connection.js";
import { builtInCommandOf } from "./redis-client.js";

const GIVE_UP_AFTER_IN_MILLISECONDS = 1000;

integrationTest(
  "a command in flight at a cut leaves no socket timeout on the new connection, and a command unanswered on it still times the socket out",
  { timeout: 20_000 },
  async ({ redisUrl, keyPrefix }) => {
    const proxy = await startTcpProxy(redisUrl);
    const connections = createRedisConnections({ url: proxy.url, giveUpAfter: "1s" }, createRecordingLogger().logger);
    const client = connections.createClient("instance");
    const clientErrors: Error[] = [];
    client.on("error", (error: Error) => clientErrors.push(error));
    const inFlightKey = `${keyPrefix}in-flight`;

    try {
      await connections.waitUntilReadyWithinGiveUpAfter(client);
      const replyCut = proxy.cutBeforeReplyTo((request) => request.includes(inFlightKey));
      const inFlightCommand = client.send(builtInCommandOf("GET", inFlightKey)).catch((error: unknown) => error);
      await replyCut;
      await proxy.restore();
      await vi.waitFor(() => expect(client.status).toBe("ready"), { timeout: 10_000 });
      await inFlightCommand;
      await sleep(GIVE_UP_AFTER_IN_MILLISECONDS * 1.5);
      const errorsAfterTheReconnect = clientErrors.length;
      const statusAfterTheWait = client.status;

      const unansweredCommand = client
        .send(builtInCommandOf("BLPOP", `${keyPrefix}unanswered`, (3 * GIVE_UP_AFTER_IN_MILLISECONDS) / 1000))
        .catch((error: unknown) => error);
      await vi.waitFor(() => expect(clientErrors.length).toBeGreaterThan(errorsAfterTheReconnect), { timeout: 5_000 });
      await unansweredCommand;

      expect(errorsAfterTheReconnect).toBe(1);
      expect(statusAfterTheWait).toBe("ready");
      expect(clientErrors.at(-1)?.message).toBe(
        `Socket timeout. Expecting data, but didn't receive any in ${String(GIVE_UP_AFTER_IN_MILLISECONDS)}ms.`,
      );
    } finally {
      await connections.closeClient(client);
      await proxy.stop();
    }
  },
);
