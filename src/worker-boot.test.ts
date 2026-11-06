import { ReplyError } from "ioredis";
import { describe, expect, test } from "vitest";

import type { Logger } from "./logger.js";
import type { RunQueue } from "./run-queue.js";
import { bootWorker, maxmemoryPolicyRejectionOf, readMaxmemoryPolicy } from "./worker-boot.js";
import type { MaxmemoryPolicySource } from "./worker-boot.js";

const infoMemoryReplyWith = (policy: string) =>
  ["# Memory", "used_memory:1048576", "maxmemory:0", `maxmemory_policy:${policy}`, "mem_fragmentation_ratio:1.10"].join(
    "\r\n",
  );

const INFO_MEMORY_REPLY_WITHOUT_POLICY = ["# Memory", "used_memory:1048576", "maxmemory:0"].join("\r\n");

const CONFIG_DISABLED = () => new ReplyError("ERR unknown command 'CONFIG', with args beginning with: 'GET'");

const INFO_DENIED = () => new ReplyError("NOPERM User default has no permissions to run the 'info' command");

function maxmemoryPolicySourceAnswering({
  info,
  config,
}: {
  info: () => Promise<string>;
  config: () => Promise<unknown>;
}): MaxmemoryPolicySource & { calledCommands: string[] } {
  const calledCommands: string[] = [];
  return {
    calledCommands,
    info: async (...args: unknown[]) => {
      calledCommands.push(["INFO", ...args].join(" "));
      return info();
    },
    config: async (...args: unknown[]) => {
      calledCommands.push(["CONFIG", ...args].join(" "));
      return config();
    },
  } as MaxmemoryPolicySource & { calledCommands: string[] };
}

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

describe("bootWorker", () => {
  test("writes maxmemory-policy unreadable at warn with err, and boots on, when neither command yields the policy", async () => {
    const warnLines: { fields: Record<string, unknown>; message: string }[] = [];
    const logger: Logger = {
      debug: () => undefined,
      info: () => undefined,
      warn: (fields, message) => warnLines.push({ fields, message }),
      error: () => undefined,
    };
    const runQueue = {
      client: {
        ...maxmemoryPolicySourceAnswering({
          info: async () => {
            throw INFO_DENIED();
          },
          config: async () => {
            throw CONFIG_DISABLED();
          },
        }),
        zrangebyscore: async () => [],
      },
      bullmqQueue: { toKey: (type: string) => `bull:runs:${type}` },
      syncSchedules: async () => [],
    } as unknown as RunQueue;

    const { isFirstRoundTripAnswered, completion } = await bootWorker({
      runQueue,
      declaredSchedules: [],
      logger,
      sendBuiltInEvent: async () => undefined,
      giveUpAfterInMilliseconds: 1_000,
    });

    await expect(completion).resolves.toBeUndefined();
    expect(isFirstRoundTripAnswered).toBe(true);
    expect(warnLines).toEqual([
      { fields: { err: expect.any(AggregateError) }, message: "maxmemory-policy unreadable" },
    ]);
  });
});
