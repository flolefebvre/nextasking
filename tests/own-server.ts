import { GenericContainer, Wait } from "testcontainers";
import type { StartedNetwork, StartedTestContainer } from "testcontainers";

import { invariantsOfCurrentTestOrThrow } from "./invariant-violations.js";
import { startTcpProxy } from "./tcp-proxy.js";
import type { TcpProxy } from "./tcp-proxy.js";
import { testImage } from "./valkey-container.js";

export const REDIS_CONTAINER_PORT = 6379;

export type OwnServer = {
  url: string;
  proxy: TcpProxy;
  stop: () => Promise<void>;
};

export type ServerContainer = {
  url: string;
  container: StartedTestContainer;
};

export type ServerNetworkPlace = {
  network: StartedNetwork;
  networkAlias: string;
};

export async function startServerContainer({
  serverArguments = [],
  networkPlace,
}: {
  serverArguments?: readonly string[];
  networkPlace?: ServerNetworkPlace;
} = {}): Promise<ServerContainer> {
  const containerToStart = new GenericContainer(testImage())
    .withCommand([...serverArguments])
    .withExposedPorts(REDIS_CONTAINER_PORT)
    .withWaitStrategy(Wait.forLogMessage(/Ready to accept connections/));
  const container = await (
    networkPlace === undefined
      ? containerToStart
      : containerToStart.withNetwork(networkPlace.network).withNetworkAliases(networkPlace.networkAlias)
  ).start();
  return {
    url: `redis://${container.getHost()}:${String(container.getMappedPort(REDIS_CONTAINER_PORT))}`,
    container,
  };
}

export async function startOwnServer({
  serverArguments = [],
}: { serverArguments?: readonly string[] } = {}): Promise<OwnServer> {
  const invariants = invariantsOfCurrentTestOrThrow("startOwnServer");
  const { url, container } = await startServerContainer({ serverArguments });
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
