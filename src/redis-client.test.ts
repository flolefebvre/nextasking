import { describe, expect, test } from "vitest";

import { builtInCommandOf, scriptCommandOf } from "./redis-client.js";
import type { RedisScript } from "./redis-client.js";

const ECHO_SCRIPT: RedisScript = { name: "nextaskingTestEcho", lua: "return { KEYS[1], ARGV[1] }", keyCount: 1 };

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
