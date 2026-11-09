import { GenericContainer, getContainerRuntimeClient, Wait } from "testcontainers";
import type { StartedNetwork, StartedTestContainer } from "testcontainers";

import { invariantsOfCurrentTestOrThrow } from "./invariant-violations.js";
import { startTcpProxy } from "./tcp-proxy.js";
import type { TcpProxy } from "./tcp-proxy.js";
import { testImage } from "./valkey-container.js";

export const REDIS_CONTAINER_PORT = 6379;

export const SERVER_CONTAINER_STARTUP_TIMEOUT_IN_MILLISECONDS = 60_000;

const TESTCONTAINERS_PORT_BINDING_WAIT_IN_MILLISECONDS = 30_000;

const PORT_BINDING_TIMEOUT_MESSAGE = `Timed out after ${String(TESTCONTAINERS_PORT_BINDING_WAIT_IN_MILLISECONDS)}ms while waiting for container ports to be bound to the host`;

export const SERVER_CONTAINER_START_BOUND_IN_MILLISECONDS =
  2 * TESTCONTAINERS_PORT_BINDING_WAIT_IN_MILLISECONDS + SERVER_CONTAINER_STARTUP_TIMEOUT_IN_MILLISECONDS;

function isPortBindingTimeout(error: unknown): boolean {
  return error instanceof Error && error.message === PORT_BINDING_TIMEOUT_MESSAGE;
}

async function removedAfterAPortBindingTimeout(
  portBindingTimeout: unknown,
  removeAbandonedStart: () => Promise<void>,
): Promise<void> {
  try {
    await removeAbandonedStart();
  } catch (removalFailure) {
    throw new AggregateError(
      [portBindingTimeout, removalFailure],
      "a container start timed out waiting for its ports to be bound, and the container it left could not be removed",
    );
  }
}

export async function startRetryingOnceAPortBindingTimeout<Started>(
  start: () => Promise<Started>,
  removeAbandonedStart: () => Promise<void>,
): Promise<Started> {
  try {
    return await start();
  } catch (firstFailure) {
    if (!isPortBindingTimeout(firstFailure)) {
      throw firstFailure;
    }
    await removedAfterAPortBindingTimeout(firstFailure, removeAbandonedStart);
  }
  try {
    return await start();
  } catch (secondFailure) {
    if (isPortBindingTimeout(secondFailure)) {
      await removedAfterAPortBindingTimeout(secondFailure, removeAbandonedStart);
    }
    throw secondFailure;
  }
}

export class ContainerRecordingItsLastCreation extends GenericContainer {
  lastCreatedContainerId: string | undefined;

  protected override containerCreated(containerId: string): Promise<void> {
    this.lastCreatedContainerId = containerId;
    return Promise.resolve();
  }
}

export async function removeLastCreatedContainerOf(containerToStart: ContainerRecordingItsLastCreation): Promise<void> {
  if (containerToStart.lastCreatedContainerId === undefined) {
    return;
  }
  const runtimeClient = await getContainerRuntimeClient();
  await runtimeClient.container.getById(containerToStart.lastCreatedContainerId).remove({ force: true, v: true });
}

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

type ServerContainerPlacing = {
  serverArguments?: readonly string[];
  networkPlace?: ServerNetworkPlace;
};

export function serverContainerToStart({
  serverArguments = [],
  networkPlace,
}: ServerContainerPlacing = {}): ContainerRecordingItsLastCreation {
  const containerToStart = new ContainerRecordingItsLastCreation(testImage())
    .withCommand([...serverArguments])
    .withExposedPorts(REDIS_CONTAINER_PORT)
    .withWaitStrategy(Wait.forLogMessage(/Ready to accept connections/))
    .withStartupTimeout(SERVER_CONTAINER_STARTUP_TIMEOUT_IN_MILLISECONDS);
  return networkPlace === undefined
    ? containerToStart
    : containerToStart.withNetwork(networkPlace.network).withNetworkAliases(networkPlace.networkAlias);
}

export async function startServerContainer(placing: ServerContainerPlacing = {}): Promise<ServerContainer> {
  const containerToStart = serverContainerToStart(placing);
  const container = await startRetryingOnceAPortBindingTimeout(
    () => containerToStart.start(),
    () => removeLastCreatedContainerOf(containerToStart),
  );
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
