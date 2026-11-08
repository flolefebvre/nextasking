import type { ChainableCommander, Redis } from "ioredis";

export type ScriptArgument = string | number;

export type ScriptCommand = {
  call: (keys: readonly string[], scriptArguments: readonly ScriptArgument[]) => Promise<unknown>;
  appendTo: (pipeline: ChainableCommander, keys: readonly string[], scriptArguments: readonly ScriptArgument[]) => void;
};

export function defineScriptCommand(
  client: Redis,
  commandName: string,
  lua: string,
  numberOfKeys: number,
): ScriptCommand {
  client.defineCommand(commandName, { numberOfKeys, lua });
  const sendTo = (
    target: Redis | ChainableCommander,
    keys: readonly string[],
    scriptArguments: readonly ScriptArgument[],
  ): unknown => {
    if (keys.length !== numberOfKeys) {
      throw new Error(
        `The script command ${commandName} takes ${String(numberOfKeys)} keys, not ${String(keys.length)}`,
      );
    }
    const command: unknown = Reflect.get(target, commandName);
    if (typeof command !== "function") {
      throw new Error(`The script command ${commandName} is not defined on this client`);
    }
    return Reflect.apply(command, target, [...keys, ...scriptArguments]);
  };
  return {
    call: (keys, scriptArguments) => Promise.resolve(sendTo(client, keys, scriptArguments)),
    appendTo: (pipeline, keys, scriptArguments) => {
      sendTo(pipeline, keys, scriptArguments);
    },
  };
}
