import { vi } from "vitest";

import type { RedisClient, RedisCommand } from "../src/redis-client.js";

export type SentCommand = { name: string; args: unknown[] };

export type ReadTraffic = { roundTripCount: number; sentCommands: SentCommand[] };

function sentCommandOf(command: RedisCommand): SentCommand {
  return command.kind === "builtIn"
    ? { name: command.name.toLowerCase(), args: [...command.commandArguments] }
    : { name: "evalsha", args: [...command.keys, ...command.scriptArguments] };
}

export async function readTrafficOf(client: RedisClient, read: () => Promise<unknown>): Promise<ReadTraffic> {
  const send = vi.spyOn(client, "send");
  const sendBatch = vi.spyOn(client, "sendBatch");
  try {
    await read();
    return {
      roundTripCount: send.mock.calls.length + sendBatch.mock.calls.length,
      sentCommands: [
        ...send.mock.calls.map(([command]) => sentCommandOf(command)),
        ...sendBatch.mock.calls.flatMap(([commands]) => commands.map(sentCommandOf)),
      ],
    };
  } finally {
    send.mockRestore();
    sendBatch.mockRestore();
  }
}
