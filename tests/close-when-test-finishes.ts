import { expect, onTestFinished, vi } from "vitest";

import { closeNextasking } from "../src/registry.js";
import { reportOfRecordedLines } from "./recording-logger.js";
import type { RecordedLogLine } from "./recording-logger.js";
import type { TcpProxy } from "./tcp-proxy.js";

const RECONNECT_TIMEOUT_IN_MILLISECONDS = 5_000;

const CLOSE_TIMEOUT_IN_MILLISECONDS = 20_000;

function countOf(lines: readonly RecordedLogLine[], message: string): number {
  return lines.filter((line) => line.message === message).length;
}

async function waitUntilEveryOutageIsRestored(lines: readonly RecordedLogLine[]): Promise<void> {
  try {
    await vi.waitFor(
      () => {
        expect(countOf(lines, "connection restored")).toBeGreaterThanOrEqual(countOf(lines, "connection error"));
      },
      { timeout: RECONNECT_TIMEOUT_IN_MILLISECONDS },
    );
  } catch {
    throw new Error(
      `Left the nextasking instances open: no "connection restored" for each "connection error" within ${String(RECONNECT_TIMEOUT_IN_MILLISECONDS)} ms of restoring the proxy, and closing a worker whose clients are reconnecting never returns. ${reportOfRecordedLines(lines)}`,
    );
  }
}

export function closeWhenTestFinishes({
  proxy,
  nextaskings = [],
  lines,
}: {
  proxy: TcpProxy;
  nextaskings?: object[];
  lines?: readonly RecordedLogLine[];
}): void {
  onTestFinished(async () => {
    await proxy.restore();
    try {
      if (lines !== undefined) {
        await waitUntilEveryOutageIsRestored(lines);
      }
      for (const nextasking of nextaskings) {
        await closeNextasking(nextasking);
      }
    } finally {
      await proxy.stop();
    }
  }, CLOSE_TIMEOUT_IN_MILLISECONDS);
  if (lines !== undefined) {
    onTestFinished(({ task }) => {
      if (task.result?.state === "fail") {
        throw new Error(reportOfRecordedLines(lines));
      }
    });
  }
}
