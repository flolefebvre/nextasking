import { expect } from "vitest";

import { integrationTest } from "./integration-test.js";
import { startPrimaryAndReplica } from "./primary-and-replica.js";
import { providedRedisUrl } from "./valkey-container.js";
import { withRedis } from "./with-redis.js";

const REPLICA_ACKNOWLEDGEMENT_TIMEOUT_IN_MILLISECONDS = 5_000;

const WINDOW_FOR_A_LOST_WRITE_TO_BE_ACKNOWLEDGED_IN_MILLISECONDS = 300;

integrationTest.skipIf(providedRedisUrl() !== undefined)(
  "a promotion keeps the writes the replica acknowledged before the cut, loses the ones the primary took after it, and the proxy reaches the promoted replica",
  { timeout: 60_000 },
  async ({ keyPrefix }) => {
    const primaryAndReplica = await startPrimaryAndReplica();
    try {
      const acknowledgedReplicaCountsByWrite = await withRedis(primaryAndReplica.primaryUrl, async (primary) => {
        await primary.set(`${keyPrefix}before-the-cut`, "kept");
        const beforeTheCut = await primary.sendCommand([
          "WAIT",
          "1",
          String(REPLICA_ACKNOWLEDGEMENT_TIMEOUT_IN_MILLISECONDS),
        ]);
        await primaryAndReplica.cutReplication();
        await primary.set(`${keyPrefix}after-the-cut`, "lost");
        const afterTheCut = await primary.sendCommand([
          "WAIT",
          "1",
          String(WINDOW_FOR_A_LOST_WRITE_TO_BE_ACKNOWLEDGED_IN_MILLISECONDS),
        ]);
        return { beforeTheCut, afterTheCut };
      });

      await primaryAndReplica.promoteReplica();
      const readThroughTheProxy = await withRedis(primaryAndReplica.proxy.url, async (promoted) => {
        await promoted.set(`${keyPrefix}after-the-promotion`, "accepted");
        return promoted.mGet([
          `${keyPrefix}before-the-cut`,
          `${keyPrefix}after-the-cut`,
          `${keyPrefix}after-the-promotion`,
        ]);
      });

      expect(acknowledgedReplicaCountsByWrite).toEqual({ beforeTheCut: 1, afterTheCut: 0 });
      expect(readThroughTheProxy).toEqual(["kept", null, "accepted"]);
      expect(primaryAndReplica.proxy.targetUrl()).toBe(primaryAndReplica.replicaUrl);
    } finally {
      await primaryAndReplica.stop();
    }
  },
);
