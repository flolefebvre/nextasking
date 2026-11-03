import { describe, expect, test } from "vitest";

import { maxmemoryPolicyRejectionOf } from "./worker-boot.js";

describe("maxmemoryPolicyRejectionOf", () => {
  test("accepts noeviction", () => {
    expect(maxmemoryPolicyRejectionOf("noeviction")).toBeUndefined();
  });

  test.each(["allkeys-lru", "volatile-ttl", "allkeys-lfu"])("rejects %s in one line naming it", (policy) => {
    expect(maxmemoryPolicyRejectionOf(policy)?.message).toBe(
      `maxmemory-policy is "${policy}": the Valkey or Redis holding the queues must use "noeviction", or it evicts locks and runs`,
    );
  });
});
