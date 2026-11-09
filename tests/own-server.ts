import { GenericContainer, Wait } from "testcontainers";

import { invariantsOfCurrentTestOrThrow } from "./invariant-violations.js";
import { startTcpProxy } from "./tcp-proxy.js";
import type { TcpProxy } from "./tcp-proxy.js";
import { testImage } from "./valkey-container.js";

const REDIS_CONTAINER_PORT = 6379;

export type OwnServer = {
  url: string;
  proxy: TcpProxy;
  stop: () => Promise<void>;
};

export async function startOwnServer({
  serverArguments = [],
}: { serverArguments?: readonly string[] } = {}): Promise<OwnServer> {
  const invariants = invariantsOfCurrentTestOrThrow("startOwnServer");
  const container = await new GenericContainer(testImage())
    .withCommand([...serverArguments])
    .withExposedPorts(REDIS_CONTAINER_PORT)
    .withWaitStrategy(Wait.forLogMessage(/Ready to accept connections/))
    .start();
  const url = `redis://${container.getHost()}:${String(container.getMappedPort(REDIS_CONTAINER_PORT))}`;
  const proxy = await startTcpProxy(url);
  return {
    url,
    proxy,
    stop: async () => {
      await invariants.checkStoredKeysOn(url);
      await proxy.stop();
      await container.stop();
    },
  };
}
