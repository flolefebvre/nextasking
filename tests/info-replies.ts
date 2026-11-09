import { ReplyError } from "ioredis";

import type { MaxmemoryPolicySource } from "../src/server-info.js";

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
