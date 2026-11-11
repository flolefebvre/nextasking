import { describe, expect, test } from "vitest";

import { builtInCommandOf, scriptCommandOf } from "./redis-client.js";
import type { RedisScript } from "./redis-client.js";

const ECHO_SCRIPT: RedisScript = { name: "nextaskingTestEcho", lua: "return { KEYS[1], ARGV[1] }", keyCount: 1 };

const KEYS_ECHO_SCRIPT: RedisScript = { name: "nextaskingTestKeysEcho", lua: "return KEYS", keyCount: "one or more" };

describe("scriptCommandOf", () => {
  test("describes a call of the script with its keys and arguments", () => {
    expect(scriptCommandOf(ECHO_SCRIPT, ["echo"], ["one", 2])).toEqual({
      kind: "script",
      script: ECHO_SCRIPT,
      keys: ["echo"],
      scriptArguments: ["one", 2],
    });
  });

  test("refuses a call with the wrong number of keys", () => {
    const call = () => scriptCommandOf(ECHO_SCRIPT, [], ["no key"]);

    expect(call).toThrow("The script command nextaskingTestEcho takes 1 keys, not 0");
  });

  test("refuses a call with more keys than a script of a fixed key count takes", () => {
    const call = () => scriptCommandOf(ECHO_SCRIPT, ["echo", "other"], []);

    expect(call).toThrow("The script command nextaskingTestEcho takes 1 keys, not 2");
  });

  test("describes a call of a script taking one or more keys with every key it is given", () => {
    expect(scriptCommandOf(KEYS_ECHO_SCRIPT, ["first", "second", "third"], [])).toMatchObject({
      keys: ["first", "second", "third"],
    });
  });

  test("refuses a call without a key to a script taking one or more keys", () => {
    const call = () => scriptCommandOf(KEYS_ECHO_SCRIPT, [], []);

    expect(call).toThrow("The script command nextaskingTestKeysEcho takes one or more keys, not 0");
  });
});

describe("builtInCommandOf", () => {
  test("describes a built-in command with its arguments", () => {
    expect(builtInCommandOf("HMGET", "record", "state")).toEqual({
      kind: "builtIn",
      name: "HMGET",
      commandArguments: ["record", "state"],
    });
  });
});
