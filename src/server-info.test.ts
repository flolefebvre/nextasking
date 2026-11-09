import { describe, expect, test } from "vitest";

import { maxmemoryPolicyRejectionOf, readMaxmemoryPolicy } from "./server-info.js";
import {
  CONFIG_DISABLED,
  INFO_DENIED,
  INFO_MEMORY_REPLY_WITHOUT_POLICY,
  infoMemoryReplyWith,
  maxmemoryPolicySourceAnswering,
} from "../tests/info-replies.js";

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

describe("readMaxmemoryPolicy", () => {
  test("reads the policy from INFO memory, without CONFIG", async () => {
    const source = maxmemoryPolicySourceAnswering({
      info: async () => infoMemoryReplyWith("allkeys-lru"),
      config: async () => {
        throw CONFIG_DISABLED();
      },
    });

    const reading = await readMaxmemoryPolicy(source);

    expect(reading).toEqual({ kind: "read", policy: "allkeys-lru" });
    expect(source.calledCommands).toEqual(["INFO memory"]);
  });

  test("falls back to CONFIG GET when INFO memory has no maxmemory_policy line", async () => {
    const source = maxmemoryPolicySourceAnswering({
      info: async () => INFO_MEMORY_REPLY_WITHOUT_POLICY,
      config: async () => ["maxmemory-policy", "noeviction"],
    });

    const reading = await readMaxmemoryPolicy(source);

    expect(reading).toEqual({ kind: "read", policy: "noeviction" });
    expect(source.calledCommands).toEqual(["INFO memory", "CONFIG GET maxmemory-policy"]);
  });

  test("falls back to CONFIG GET when INFO fails with a reply error", async () => {
    const source = maxmemoryPolicySourceAnswering({
      info: async () => {
        throw INFO_DENIED();
      },
      config: async () => ["maxmemory-policy", "volatile-ttl"],
    });

    expect(await readMaxmemoryPolicy(source)).toEqual({ kind: "read", policy: "volatile-ttl" });
  });

  test("is unreadable when neither command yields the policy, its error carrying both failures as thrown", async () => {
    const infoFailure = INFO_DENIED();
    const configFailure = CONFIG_DISABLED();
    const source = maxmemoryPolicySourceAnswering({
      info: async () => {
        throw infoFailure;
      },
      config: async () => {
        throw configFailure;
      },
    });

    const reading = await readMaxmemoryPolicy(source);

    expect(reading).toEqual({ kind: "unreadable", error: expect.any(AggregateError) });
    const { error } = reading as { error: AggregateError };
    expect(error.message).toBe("maxmemory-policy is readable from neither INFO memory nor CONFIG GET");
    expect(error.errors).toEqual([infoFailure, configFailure]);
    expect(infoFailure).not.toHaveProperty("cause");
    expect(configFailure).not.toHaveProperty("cause");
  });

  test("names the missing line and the odd CONFIG reply when neither yields the policy", async () => {
    const source = maxmemoryPolicySourceAnswering({
      info: async () => INFO_MEMORY_REPLY_WITHOUT_POLICY,
      config: async () => [],
    });

    const reading = await readMaxmemoryPolicy(source);

    expect((reading as { error: AggregateError }).error.errors.map((error: Error) => error.message)).toEqual([
      "INFO memory has no maxmemory_policy line",
      "CONFIG GET maxmemory-policy returned []",
    ]);
  });

  test("lets an error that is not a reply error through", async () => {
    const connectionClosed = new Error("Connection is closed.");
    const source = maxmemoryPolicySourceAnswering({
      info: async () => {
        throw connectionClosed;
      },
      config: async () => ["maxmemory-policy", "noeviction"],
    });

    await expect(readMaxmemoryPolicy(source)).rejects.toBe(connectionClosed);
  });
});
