import { describe, expect, test } from "vitest";

import {
  checkServerInfoReading,
  maxmemoryPolicyRejectionOf,
  readMaxmemoryPolicy,
  readServerInfo,
  serverFloorRejectionOf,
  serverVersionReadingOf,
} from "./server-info.js";
import type { MaxmemoryPolicyReading } from "./server-info.js";
import {
  CONFIG_DISABLED,
  INFO_DENIED,
  INFO_MEMORY_REPLY_WITHOUT_POLICY,
  infoMemoryReplyWith,
  infoReplyWith,
  maxmemoryPolicySourceAnswering,
} from "../tests/info-replies.js";
import { createRecordingLogger } from "../tests/recording-logger.js";

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

describe("serverVersionReadingOf", () => {
  test("reads valkey_version before the compat redis_version", () => {
    expect(serverVersionReadingOf(infoReplyWith({ valkeyVersion: "8.1.2", redisVersion: "7.2.4" }))).toEqual({
      kind: "read",
      serverName: "Valkey",
      version: "8.1.2",
    });
  });

  test("reads redis_version when there is no valkey_version", () => {
    expect(serverVersionReadingOf(infoReplyWith({ redisVersion: "7.4.1" }))).toEqual({
      kind: "read",
      serverName: "Redis",
      version: "7.4.1",
    });
  });

  test("is unreadable when neither line is in the reply", () => {
    expect(serverVersionReadingOf(infoReplyWith({}))).toEqual({
      kind: "unreadable",
      error: new Error("INFO has neither a valkey_version nor a redis_version line"),
    });
  });

  test("is unreadable when the line holds no version", () => {
    expect(serverVersionReadingOf(infoReplyWith({ redisVersion: "unknown" }))).toEqual({
      kind: "unreadable",
      error: new Error('INFO reports redis_version "unknown", which is not a version'),
    });
  });
});

describe("serverFloorRejectionOf", () => {
  test.each([
    ["Valkey", "8.0.0"],
    ["Valkey", "9.0.1"],
    ["Redis", "7.4.0"],
    ["Redis", "8.2.1"],
  ] as const)("accepts %s %s", (serverName, version) => {
    expect(serverFloorRejectionOf({ kind: "read", serverName, version })).toBeUndefined();
  });

  test.each([
    ["Redis", "7.2.4"],
    ["Redis", "6.2.14"],
    ["Valkey", "7.2.5"],
  ] as const)("refuses %s %s in one line naming the floor", (serverName, version) => {
    expect(serverFloorRejectionOf({ kind: "read", serverName, version })?.message).toBe(
      `${serverName} ${version} is below the floor: Valkey 8 or Redis 7.4`,
    );
  });
});

describe("readServerInfo", () => {
  test("reads the policy and the version from one INFO with no section", async () => {
    const source = maxmemoryPolicySourceAnswering({
      info: async () => infoReplyWith({ valkeyVersion: "8.0.1", maxmemoryPolicy: "noeviction" }),
      config: async () => {
        throw CONFIG_DISABLED();
      },
    });

    const reading = await readServerInfo(source);

    expect(reading).toEqual({
      maxmemoryPolicyReading: { kind: "read", policy: "noeviction" },
      serverVersionReading: { kind: "read", serverName: "Valkey", version: "8.0.1" },
    });
    expect(source.calledCommands).toEqual(["INFO"]);
  });

  test("falls back to CONFIG GET for the policy and has no version when INFO is denied", async () => {
    const infoDenied = INFO_DENIED();
    const source = maxmemoryPolicySourceAnswering({
      info: async () => {
        throw infoDenied;
      },
      config: async () => ["maxmemory-policy", "noeviction"],
    });

    const reading = await readServerInfo(source);

    expect(reading).toEqual({
      maxmemoryPolicyReading: { kind: "read", policy: "noeviction" },
      serverVersionReading: { kind: "unreadable", error: infoDenied },
    });
    expect(source.calledCommands).toEqual(["INFO", "CONFIG GET maxmemory-policy"]);
  });

  test("rejects with an unanswered INFO, without sending CONFIG", async () => {
    const commandTimedOut = new Error("Command timed out");
    const source = maxmemoryPolicySourceAnswering({
      info: () => Promise.reject(commandTimedOut),
      config: async () => ["maxmemory-policy", "noeviction"],
    });

    await expect(readServerInfo(source)).rejects.toBe(commandTimedOut);
    expect(source.calledCommands).toEqual(["INFO"]);
  });
});

describe("checkServerInfoReading", () => {
  const NOEVICTION: MaxmemoryPolicyReading = { kind: "read", policy: "noeviction" };

  test("writes nothing for a server at the floor with noeviction", () => {
    const { logger, lines } = createRecordingLogger();

    checkServerInfoReading(
      {
        maxmemoryPolicyReading: NOEVICTION,
        serverVersionReading: { kind: "read", serverName: "Valkey", version: "8.0.0" },
      },
      logger,
    );

    expect(lines).toEqual([]);
  });

  test("warns server version unreadable with err and goes on", () => {
    const { logger, lines } = createRecordingLogger();
    const versionError = new Error("INFO has neither a valkey_version nor a redis_version line");

    checkServerInfoReading(
      { maxmemoryPolicyReading: NOEVICTION, serverVersionReading: { kind: "unreadable", error: versionError } },
      logger,
    );

    expect(lines).toEqual([{ level: "warn", message: "server version unreadable", fields: { err: versionError } }]);
  });

  test("throws the floor rejection for a server below the floor", () => {
    const { logger } = createRecordingLogger();

    expect(() => {
      checkServerInfoReading(
        {
          maxmemoryPolicyReading: NOEVICTION,
          serverVersionReading: { kind: "read", serverName: "Redis", version: "7.2.4" },
        },
        logger,
      );
    }).toThrow(new Error("Redis 7.2.4 is below the floor: Valkey 8 or Redis 7.4"));
  });

  test("throws the policy rejection for an evicting server", () => {
    const { logger } = createRecordingLogger();

    expect(() => {
      checkServerInfoReading(
        {
          maxmemoryPolicyReading: { kind: "read", policy: "allkeys-lru" },
          serverVersionReading: { kind: "read", serverName: "Valkey", version: "9.0.0" },
        },
        logger,
      );
    }).toThrow(/maxmemory-policy is "allkeys-lru"/);
  });
});
