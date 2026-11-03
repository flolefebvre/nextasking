import { expect, vi } from "vitest";

import type { Logger } from "../src/index.js";

export type LogLevel = keyof Logger;

export type RecordedLogLine = {
  level: LogLevel;
  fields: Record<string, unknown>;
  message: string;
};

export function createRecordingLogger(): { logger: Logger; lines: RecordedLogLine[] } {
  const lines: RecordedLogLine[] = [];
  const lineRecorderAt =
    (level: LogLevel): Logger[LogLevel] =>
    (fields, message) => {
      lines.push({ level, fields, message });
    };
  return {
    logger: {
      debug: lineRecorderAt("debug"),
      info: lineRecorderAt("info"),
      warn: lineRecorderAt("warn"),
      error: lineRecorderAt("error"),
    },
    lines,
  };
}

export function expectOneConnectionErrorAndOneConnectionRestored(lines: readonly RecordedLogLine[]): void {
  expect(lines.filter((line) => line.message.startsWith("connection "))).toEqual([
    { level: "error", fields: { err: expect.any(Error) }, message: "connection error" },
    { level: "info", fields: {}, message: "connection restored" },
  ]);
}

export async function waitForLogMessage(
  lines: readonly RecordedLogLine[],
  message: string,
  { timeout = 10_000 }: { timeout?: number } = {},
): Promise<void> {
  await vi.waitFor(
    () => {
      expect(lines.map((line) => line.message)).toContain(message);
    },
    { timeout },
  );
}
