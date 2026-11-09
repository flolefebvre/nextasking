import { connect } from "node:net";

import { expect } from "vitest";

import { integrationTest } from "./integration-test.js";
import { STORED_KEYS_READ_BOUND_IN_MILLISECONDS } from "./invariant-violations.js";
import { SERVER_CONTAINER_START_BOUND_IN_MILLISECONDS, startOwnServer } from "./own-server.js";
import { providedRedisUrl } from "./valkey-container.js";
import { withRedis } from "./with-redis.js";

async function connectErrorCodeOf(url: string): Promise<string | undefined> {
  const { hostname, port } = new URL(url);
  const socket = connect(Number(port), hostname);
  return new Promise((resolve) => {
    socket.once("connect", () => {
      socket.destroy();
      resolve(undefined);
    });
    socket.once("error", (error: NodeJS.ErrnoException) => resolve(error.code));
  });
}

integrationTest.skipIf(providedRedisUrl() !== undefined)(
  "an own server's stop() on a server that died ends, records that its stored keys could not be read, and stops the proxy",
  { timeout: SERVER_CONTAINER_START_BOUND_IN_MILLISECONDS + STORED_KEYS_READ_BOUND_IN_MILLISECONDS + 5_000 },
  async ({ expectInvariantViolation }) => {
    const ownServer = await startOwnServer();
    expectInvariantViolation("stored keys", `the stored keys on ${ownServer.url} could not be read`);
    await withRedis(ownServer.url, (redis) => redis.sendCommand(["SHUTDOWN", "NOSAVE"]).catch(() => undefined));

    await ownServer.stop();

    expect(await connectErrorCodeOf(ownServer.proxy.url)).toBe("ECONNREFUSED");
  },
);
