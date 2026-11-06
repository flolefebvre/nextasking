import { GenericContainer, Wait } from "testcontainers";

import { startTcpProxy } from "./tcp-proxy.js";
import type { TcpProxy } from "./tcp-proxy.js";
import { testImage } from "./valkey-container.js";

const REDIS_CONTAINER_PORT = 6379;

export const EVICTING_POLICY_REJECTION =
  'maxmemory-policy is "allkeys-lru": the Valkey or Redis holding the queues must use "noeviction", or it evicts locks and runs';

export type EvictingServer = {
  url: string;
  proxy: TcpProxy;
  stop: () => Promise<void>;
};

export async function startEvictingServer({
  isConfigDisabled = false,
}: { isConfigDisabled?: boolean } = {}): Promise<EvictingServer> {
  const container = await new GenericContainer(testImage())
    .withCommand(["--maxmemory-policy", "allkeys-lru", ...(isConfigDisabled ? ["--rename-command", "CONFIG", ""] : [])])
    .withExposedPorts(REDIS_CONTAINER_PORT)
    .withWaitStrategy(Wait.forLogMessage(/Ready to accept connections/))
    .start();
  const url = `redis://${container.getHost()}:${String(container.getMappedPort(REDIS_CONTAINER_PORT))}`;
  const proxy = await startTcpProxy(url);
  return {
    url,
    proxy,
    stop: async () => {
      await proxy.stop();
      await container.stop();
    },
  };
}
