import { expect } from "vitest";

import { integrationTest } from "./integration-test.js";
import {
  PRIMARY_AND_REPLICA_STARTUP_BOUND_IN_MILLISECONDS,
  REPLICA_CATCH_UP_TIMEOUT_IN_MILLISECONDS,
  startPrimaryAndReplica,
} from "./primary-and-replica.js";
import { providedRedisUrl } from "./valkey-container.js";
import { withRedis } from "./with-redis.js";

const REPLICA_HELD_BACK_DURATION_IN_MILLISECONDS = 1_500;

const WAIT_TIMEOUT_IN_MILLISECONDS = 1_000;

const WINDOW_FOR_A_LOST_WRITE_TO_BE_ACKNOWLEDGED_IN_MILLISECONDS = 300;

integrationTest.skipIf(providedRedisUrl() !== undefined)(
  "a promotion keeps the writes the replica held once the starter waited for it, even held back, loses the ones the primary took after the cut, and the proxy reaches the promoted replica",
  {
    timeout:
      PRIMARY_AND_REPLICA_STARTUP_BOUND_IN_MILLISECONDS +
      WAIT_TIMEOUT_IN_MILLISECONDS +
      REPLICA_CATCH_UP_TIMEOUT_IN_MILLISECONDS +
      WINDOW_FOR_A_LOST_WRITE_TO_BE_ACKNOWLEDGED_IN_MILLISECONDS +
      5_000,
  },
  async ({ keyPrefix }) => {
    const primaryAndReplica = await startPrimaryAndReplica();
    try {
      const { replicaCountByWaitFromAConnectionThatWroteNothing, keptValueOnTheReplica, lostWriteReplicaCount } =
        await withRedis(primaryAndReplica.primaryUrl, async (primary) =>
          withRedis(primaryAndReplica.replicaUrl, async (replica) => {
            await replica.sendCommand(["CLIENT", "PAUSE", String(REPLICA_HELD_BACK_DURATION_IN_MILLISECONDS), "WRITE"]);
            await primary.set(`${keyPrefix}before-the-cut`, "kept");
            const replicaCountByWait = await withRedis(primaryAndReplica.primaryUrl, (connectionThatWroteNothing) =>
              connectionThatWroteNothing.sendCommand(["WAIT", "1", String(WAIT_TIMEOUT_IN_MILLISECONDS)]),
            );
            await primaryAndReplica.waitUntilReplicaHoldsEveryWriteSoFar();
            const keptValue = await replica.get(`${keyPrefix}before-the-cut`);
            await primaryAndReplica.cutReplication();
            await primary.set(`${keyPrefix}after-the-cut`, "lost");
            const lostWriteCount = await primary.sendCommand([
              "WAIT",
              "1",
              String(WINDOW_FOR_A_LOST_WRITE_TO_BE_ACKNOWLEDGED_IN_MILLISECONDS),
            ]);
            return {
              replicaCountByWaitFromAConnectionThatWroteNothing: replicaCountByWait,
              keptValueOnTheReplica: keptValue,
              lostWriteReplicaCount: lostWriteCount,
            };
          }),
        );

      await primaryAndReplica.promoteReplica();
      const readThroughTheProxy = await withRedis(primaryAndReplica.proxy.url, async (promoted) => {
        await promoted.set(`${keyPrefix}after-the-promotion`, "accepted");
        return promoted.mGet([
          `${keyPrefix}before-the-cut`,
          `${keyPrefix}after-the-cut`,
          `${keyPrefix}after-the-promotion`,
        ]);
      });

      expect(keptValueOnTheReplica).toBe("kept");
      expect(replicaCountByWaitFromAConnectionThatWroteNothing).toBe(1);
      expect(lostWriteReplicaCount).toBe(0);
      expect(readThroughTheProxy).toEqual(["kept", null, "accepted"]);
      expect(primaryAndReplica.proxy.targetUrl()).toBe(primaryAndReplica.replicaUrl);
    } finally {
      await primaryAndReplica.stop();
    }
  },
);
