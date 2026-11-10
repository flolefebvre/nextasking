import { inspect } from "node:util";
import { expect, vi } from "vitest";

import type { Logger } from "../src/index.js";
import { invariantsOfCurrentTestIfAny } from "./invariant-violations.js";
import type { InvariantViolation } from "./invariant-violations.js";

export type LogLevel = keyof Logger;

export type RecordedLogLine = {
  level: LogLevel;
  fields: Record<string, unknown>;
  message: string;
};

export function reportedWorkerErrorOf(line: RecordedLogLine): InvariantViolation | undefined {
  if (line.message !== "worker error") {
    return undefined;
  }
  const err = line.fields["err"];
  const isErrorLike = typeof err === "object" && err !== null && typeof (err as Error).message === "string";
  return {
    invariant: "worker error",
    message: isErrorLike ? (err as Error).message : inspect(err),
    detail: err instanceof Error ? err.stack : undefined,
  };
}

export function createRecordingLogger(): { logger: Logger; lines: RecordedLogLine[] } {
  const lines: RecordedLogLine[] = [];
  const invariants = invariantsOfCurrentTestIfAny();
  const resolversOfWorkersNotStopped: (() => void)[] = [];
  const lineRecorderAt =
    (level: LogLevel): Logger[LogLevel] =>
    (fields, message) => {
      const line = { level, fields, message };
      lines.push(line);
      const workerError = reportedWorkerErrorOf(line);
      if (workerError !== undefined) {
        invariants?.report(workerError);
      }
      if (message === "worker started") {
        invariants?.waitBeforeJudging(
          new Promise<void>((resolve) => {
            resolversOfWorkersNotStopped.push(resolve);
          }),
        );
      }
      if (message === "worker stopped") {
        resolversOfWorkersNotStopped.shift()?.();
      }
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

export function expectOneStallAndOneCompletedPickupOfTheRun(
  lines: readonly RecordedLogLine[],
  { runFields, stalls }: { runFields: Record<string, unknown> & { runId: string }; stalls: number },
): void {
  const linesOfTheRun = lines.filter((line) => line.fields["runId"] === runFields.runId);
  expect(linesOfTheRun.filter((line) => line.message === "attempt stalled")).toEqual([
    { level: "warn", fields: { ...runFields, stalls }, message: "attempt stalled" },
  ]);
  expect(linesOfTheRun.filter((line) => line.message !== "attempt stalled")).toEqual([
    { level: "debug", fields: runFields, message: "attempt started" },
    { level: "info", fields: expect.objectContaining(runFields), message: "run completed" },
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
