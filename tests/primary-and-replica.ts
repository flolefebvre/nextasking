import { Network } from "testcontainers";
import { vi } from "vitest";

import { invariantsOfCurrentTestOrThrow } from "./invariant-violations.js";
import {
  REDIS_CONTAINER_PORT,
  SERVER_CONTAINER_START_BOUND_IN_MILLISECONDS,
  startServerContainer,
} from "./own-server.js";
import { startTcpProxy } from "./tcp-proxy.js";
import type { TcpProxy } from "./tcp-proxy.js";
import { withRedis } from "./with-redis.js";
import type { TestClient } from "./with-redis.js";

export type PrimaryAndReplica = {
  primaryUrl: string;
  replicaUrl: string;
  proxy: TcpProxy;
  waitUntilReplicaHoldsEveryWriteSoFar: () => Promise<void>;
  cutReplication: () => Promise<void>;
  promoteReplica: () => Promise<void>;
  stop: () => Promise<void>;
};

const PRIMARY_NETWORK_ALIAS = "primary";

const REPLICA_NETWORK_ALIAS = "replica";

const UNREACHABLE_PRIMARY_HOST = "127.0.0.1";

const UNREACHABLE_PRIMARY_PORT = "1";

const REPLICATION_LINK_UP_TIMEOUT_IN_MILLISECONDS = 10_000;

export const REPLICA_CATCH_UP_TIMEOUT_IN_MILLISECONDS = 5_000;

export const PRIMARY_AND_REPLICA_STARTUP_BOUND_IN_MILLISECONDS =
  2 * SERVER_CONTAINER_START_BOUND_IN_MILLISECONDS + REPLICATION_LINK_UP_TIMEOUT_IN_MILLISECONDS;

async function replicationInfoOf(redis: TestClient): Promise<string> {
  return String(await redis.sendCommand(["INFO", "replication"]));
}

function replicationOffsetIn(replicationInfo: string, offsetField: string): number {
  const offset = new RegExp(`^${offsetField}:(\\d+)\\r?$`, "m").exec(replicationInfo)?.[1];
  if (offset === undefined) {
    throw new Error(`INFO replication has no ${offsetField}:\n${replicationInfo}`);
  }
  return Number(offset);
}

async function waitUntilReplicaHoldsEveryWriteOf(primaryUrl: string, replicaUrl: string): Promise<void> {
  const primaryOffset = await withRedis(primaryUrl, async (primary) =>
    replicationOffsetIn(await replicationInfoOf(primary), "master_repl_offset"),
  );
  await withRedis(replicaUrl, async (replica) => {
    await vi.waitFor(
      async () => {
        const replicaOffset = replicationOffsetIn(await replicationInfoOf(replica), "slave_repl_offset");
        if (replicaOffset < primaryOffset) {
          throw new Error(
            `the replica has processed the replication stream up to offset ${String(replicaOffset)}, short of the primary's ${String(primaryOffset)}`,
          );
        }
      },
      { timeout: REPLICA_CATCH_UP_TIMEOUT_IN_MILLISECONDS, interval: 50 },
    );
  });
}

async function waitForReplicationLinkUp(replicaUrl: string): Promise<void> {
  await withRedis(replicaUrl, async (replica) => {
    await vi.waitFor(
      async () => {
        const replicationInfo = await replicationInfoOf(replica);
        if (!replicationInfo.includes("master_link_status:up")) {
          throw new Error(`the replica has not finished its first sync:\n${replicationInfo}`);
        }
      },
      { timeout: REPLICATION_LINK_UP_TIMEOUT_IN_MILLISECONDS, interval: 50 },
    );
  });
}

export async function startPrimaryAndReplica(): Promise<PrimaryAndReplica> {
  const invariants = invariantsOfCurrentTestOrThrow("startPrimaryAndReplica");
  const network = await new Network().start();
  const primary = await startServerContainer({
    serverArguments: ["--repl-diskless-sync-delay", "0"],
    networkPlace: { network, networkAlias: PRIMARY_NETWORK_ALIAS },
  });
  const replica = await startServerContainer({
    serverArguments: ["--replicaof", PRIMARY_NETWORK_ALIAS, String(REDIS_CONTAINER_PORT)],
    networkPlace: { network, networkAlias: REPLICA_NETWORK_ALIAS },
  });
  await waitForReplicationLinkUp(replica.url);
  const proxy = await startTcpProxy(primary.url);
  return {
    primaryUrl: primary.url,
    replicaUrl: replica.url,
    proxy,
    waitUntilReplicaHoldsEveryWriteSoFar: () => waitUntilReplicaHoldsEveryWriteOf(primary.url, replica.url),
    cutReplication: () =>
      withRedis(replica.url, async (redis) => {
        await redis.sendCommand(["REPLICAOF", UNREACHABLE_PRIMARY_HOST, UNREACHABLE_PRIMARY_PORT]);
      }),
    promoteReplica: async () => {
      await primary.container.stop();
      await withRedis(replica.url, async (redis) => {
        await redis.sendCommand(["REPLICAOF", "NO", "ONE"]);
      });
      proxy.retarget(replica.url);
    },
    stop: async () => {
      await invariants.checkStoredKeysOn(proxy.targetUrl());
      await proxy.stop();
      await primary.container.stop();
      await replica.container.stop();
      await network.stop();
    },
  };
}
