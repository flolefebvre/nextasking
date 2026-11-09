import { ReplyError } from "ioredis";

import type { RedisClient } from "../src/redis-client.js";

export const infoMemoryReplyWith = (policy: string) =>
  ["# Memory", "used_memory:1048576", "maxmemory:0", `maxmemory_policy:${policy}`, "mem_fragmentation_ratio:1.10"].join(
    "\r\n",
  );

export type InfoReplyFields = { valkeyVersion?: string; redisVersion?: string; maxmemoryPolicy?: string };

export function infoReplyWith({ valkeyVersion, redisVersion, maxmemoryPolicy }: InfoReplyFields): string {
  return [
    "# Server",
    ...(redisVersion === undefined ? [] : [`redis_version:${redisVersion}`]),
    "redis_mode:standalone",
    ...(valkeyVersion === undefined ? [] : [`server_name:valkey`, `valkey_version:${valkeyVersion}`]),
    "",
    "# Memory",
    "used_memory:1048576",
    ...(maxmemoryPolicy === undefined ? [] : [`maxmemory_policy:${maxmemoryPolicy}`]),
    "",
  ].join("\r\n");
}

export const INFO_MEMORY_REPLY_WITHOUT_POLICY = ["# Memory", "used_memory:1048576", "maxmemory:0"].join("\r\n");

export const CONFIG_DISABLED = () => new ReplyError("ERR unknown command 'CONFIG', with args beginning with: 'GET'");

export const INFO_DENIED = () => new ReplyError("NOPERM User default has no permissions to run the 'info' command");

export function maxmemoryPolicySourceAnswering({
  info,
  config,
}: {
  info: () => Promise<string>;
  config: () => Promise<unknown>;
}): RedisClient & { calledCommands: string[] } {
  const calledCommands: string[] = [];
  const repliesByCommandName: Record<string, () => Promise<unknown>> = { INFO: info, CONFIG: config };
  return {
    calledCommands,
    send: async (command) => {
      if (command.kind !== "builtIn") {
        throw new Error(`this source runs no script, asked for ${command.script.name}`);
      }
      calledCommands.push([command.name, ...command.commandArguments].join(" "));
      const reply = repliesByCommandName[command.name];
      if (reply === undefined) {
        throw new Error(`this source has no reply to ${command.name}`);
      }
      return reply();
    },
    sendBatch: () => Promise.reject(new Error("this source answers no batch")),
  };
}
