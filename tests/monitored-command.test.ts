import { expect, test } from "vitest";

import { monitoredCommandOf } from "./monitored-command.js";

test("a command from a client reads as its arguments, with the client's address as its source", () => {
  const monitorLine = '1794216403.668639 [0 172.17.0.1:46482] "SET" "{p}:plain" "hello"';

  expect(monitoredCommandOf(monitorLine)).toEqual({ source: "172.17.0.1:46482", args: ["SET", "{p}:plain", "hello"] });
});

test("a command a script runs reads with lua as its source", () => {
  const monitorLine = '1794216403.670452 [0 lua] "SET" "{p}:lua" "from\\tlua"';

  expect(monitoredCommandOf(monitorLine)).toEqual({ source: "lua", args: ["SET", "{p}:lua", "from\tlua"] });
});

test("an argument holding an escaped quote and backslash stays one argument", () => {
  const monitorLine = '1794216403.669103 [0 172.17.0.1:46482] "SET" "{p}:quote" "say \\"hi\\" \\\\ back"';

  expect(monitoredCommandOf(monitorLine).args).toEqual(["SET", "{p}:quote", 'say "hi" \\ back']);
});

test("hexadecimal and newline escapes read as the characters they stand for", () => {
  const binaryLine = '1794216403.669539 [0 172.17.0.1:46482] "SET" "{p}:binary" "\\x01\\xff\\nA"';
  const scriptLine =
    '1794216403.670424 [0 172.17.0.1:46482] "EVAL" "redis.call(\\"SET\\", KEYS[1], ARGV[1])\\nreturn 1" "1" "{p}:lua" "from\\tlua"';

  expect(monitoredCommandOf(binaryLine).args).toEqual(["SET", "{p}:binary", "\u0001ÿ\nA"]);
  expect(monitoredCommandOf(scriptLine).args).toEqual([
    "EVAL",
    'redis.call("SET", KEYS[1], ARGV[1])\nreturn 1',
    "1",
    "{p}:lua",
    "from\tlua",
  ]);
});

test("an empty argument reads as an empty string", () => {
  const monitorLine = '1794216403.669933 [0 172.17.0.1:46482] "SET" "{p}:empty" ""';

  expect(monitoredCommandOf(monitorLine).args).toEqual(["SET", "{p}:empty", ""]);
});

test("a line that is not a MONITOR line is refused rather than read as one argument", () => {
  expect(() => monitoredCommandOf("OK")).toThrow("not a MONITOR line: OK");
  expect(() => monitoredCommandOf('1794216403.669933 [0 lua] "SET" "unterminated')).toThrow("not a MONITOR line");
});
