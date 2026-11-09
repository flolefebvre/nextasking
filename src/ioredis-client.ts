import { Command, Pipeline } from "ioredis";
import type { Redis } from "ioredis";

import type { CommandResult, RedisClient, RedisCommand } from "./redis-client.js";

type IoredisReply = ReturnType<Command["transformReply"]>;

function stringsOf(reply: unknown): unknown {
  if (Buffer.isBuffer(reply)) {
    return reply.toString("utf8");
  }
  return Array.isArray(reply) ? reply.map(stringsOf) : reply;
}

class RawReplyCommand extends Command {
  override transformReply(result: Buffer | Buffer[]): IoredisReply {
    return stringsOf(result) as IoredisReply;
  }
}

type CommandSender = Redis | Pipeline;

function sentTo(sender: CommandSender, command: RedisCommand): unknown {
  if (command.kind === "builtIn") {
    return sender.sendCommand(new RawReplyCommand(command.name.toLowerCase(), [...command.commandArguments]));
  }
  const scriptCall: unknown = Reflect.get(sender, command.script.name);
  if (typeof scriptCall !== "function") {
    throw new Error(`The script command ${command.script.name} is not defined on this client`);
  }
  return Reflect.apply(scriptCall, sender, [...command.keys, ...command.scriptArguments]);
}

function commandResultOf([error, reply]: [error: Error | null, reply: unknown]): CommandResult {
  return error === null ? { kind: "replied", reply } : { kind: "failed", error };
}

export function ioredisClientOf(redis: Redis): RedisClient {
  const definedScriptNames = new Set<string>();
  const defineScriptsOf = (commands: readonly RedisCommand[]) => {
    for (const command of commands) {
      if (command.kind === "script" && !definedScriptNames.has(command.script.name)) {
        redis.defineCommand(command.script.name, { numberOfKeys: command.script.keyCount, lua: command.script.lua });
        definedScriptNames.add(command.script.name);
      }
    }
  };
  return {
    send: async (command) => {
      defineScriptsOf([command]);
      return sentTo(redis, command);
    },
    sendBatch: async (commands) => {
      defineScriptsOf(commands);
      const pipeline = redis.pipeline();
      if (!(pipeline instanceof Pipeline)) {
        throw new Error("The ioredis client handed out a pipeline that is not a Pipeline");
      }
      for (const command of commands) {
        sentTo(pipeline, command);
      }
      const results = await pipeline.exec();
      if (results === null) {
        throw new Error("The batch was discarded before it ran");
      }
      return results.map(commandResultOf);
    },
  };
}
