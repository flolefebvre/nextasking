import { describe, expect, test, vi } from "vitest";

import { createNodeRedisDriver } from "./node-redis-driver.js";
import { socketOptionsOfClientsFor } from "../tests/recorded-redis-clients.js";

vi.mock("@redis/client", async (importOriginal) =>
  (await import("../tests/recorded-redis-clients.js")).redisClientModuleRecordingClients(await importOriginal()),
);

const UNREACHABLE_PORT = 1;

describe("the clients the driver creates", () => {
  test.each([
    { scheme: "redis", url: `redis://127.0.0.1:${String(UNREACHABLE_PORT)}`, isTls: false },
    { scheme: "rediss", url: `rediss://127.0.0.1:${String(UNREACHABLE_PORT)}`, isTls: true },
  ] as const)(
    "on $scheme://, the first one and the fresh one of a forced reconnect, have TCP keepalive on with a 30 s initial delay",
    ({ url, isTls }) => {
      const driver = createNodeRedisDriver(url, {
        onConnect: () => undefined,
        onReady: () => undefined,
        onError: () => undefined,
      });
      try {
        driver.forceReconnect();

        expect(socketOptionsOfClientsFor(url)).toEqual([
          expect.objectContaining({ tls: isTls, keepAlive: true, keepAliveInitialDelay: 30_000 }),
          expect.objectContaining({ tls: isTls, keepAlive: true, keepAliveInitialDelay: 30_000 }),
        ]);
      } finally {
        driver.destroy();
      }
    },
  );
});
