import { inspect } from "node:util";
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

const awaitedMessageByLines = new WeakMap<readonly RecordedLogLine[], string>();

function shownRecordedLines(lines: readonly RecordedLogLine[]): string {
  if (lines.length === 0) {
    return "  (none)";
  }
  return lines
    .map(({ level, message, fields }) => {
      const shownFields = Object.fromEntries(
        Object.entries(fields).map(([name, value]) => [name, value instanceof Error ? String(value) : value]),
      );
      return `  ${level} ${message} ${inspect(shownFields, { breakLength: Infinity, depth: 4 })}`;
    })
    .join("\n");
}

export function reportOfRecordedLines(lines: readonly RecordedLogLine[]): string {
  const awaitedMessage = awaitedMessageByLines.get(lines);
  const awaited =
    awaitedMessage === undefined
      ? "No log line was awaited"
      : `Still waiting for the log line ${JSON.stringify(awaitedMessage)}`;
  return `${awaited}. The ${String(lines.length)} log lines recorded:\n${shownRecordedLines(lines)}`;
}

export async function waitForLogMessage(
  lines: readonly RecordedLogLine[],
  message: string,
  { timeout = 10_000 }: { timeout?: number } = {},
): Promise<void> {
  awaitedMessageByLines.set(lines, message);
  try {
    await vi.waitFor(
      () => {
        expect(
          lines.map((line) => line.message),
          `No log line ${JSON.stringify(message)} within ${String(timeout)} ms. The ${String(lines.length)} log lines recorded:\n${shownRecordedLines(lines)}\n`,
        ).toContain(message);
      },
      { timeout },
    );
  } finally {
    if (awaitedMessageByLines.get(lines) === message) {
      awaitedMessageByLines.delete(lines);
    }
  }
}
