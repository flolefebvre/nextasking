export type CommandArgument = string | number;

export type ScriptKeyCount = number | "one or more";

export type RedisScript = { readonly name: string; readonly lua: string; readonly keyCount: ScriptKeyCount };

export type RedisCommand =
  | { readonly kind: "builtIn"; readonly name: string; readonly commandArguments: readonly CommandArgument[] }
  | {
      readonly kind: "script";
      readonly script: RedisScript;
      readonly keys: readonly string[];
      readonly scriptArguments: readonly CommandArgument[];
    };

export type CommandResult =
  { readonly kind: "replied"; readonly reply: unknown } | { readonly kind: "failed"; readonly error: Error };

export type RedisClient = {
  send: (command: RedisCommand) => Promise<unknown>;
  sendBatch: (commands: readonly RedisCommand[]) => Promise<CommandResult[]>;
};

export function commandResultOf(reply: Promise<unknown>): Promise<CommandResult> {
  return reply.then(
    (repliedValue): CommandResult => ({ kind: "replied", reply: repliedValue }),
    (error: unknown): CommandResult => ({ kind: "failed", error: error as Error }),
  );
}

export function builtInCommandOf(name: string, ...commandArguments: CommandArgument[]): RedisCommand {
  return { kind: "builtIn", name, commandArguments };
}

export function scriptCommandOf(
  script: RedisScript,
  keys: readonly string[],
  scriptArguments: readonly CommandArgument[],
): RedisCommand {
  const isKeyCountTaken = script.keyCount === "one or more" ? keys.length >= 1 : keys.length === script.keyCount;
  if (!isKeyCountTaken) {
    throw new Error(
      `The script command ${script.name} takes ${String(script.keyCount)} keys, not ${String(keys.length)}`,
    );
  }
  return { kind: "script", script, keys, scriptArguments };
}
