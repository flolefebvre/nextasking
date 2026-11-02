import { deriveJobId } from "../src/ids.js";
import type { RunId } from "../src/ids.js";

export function runQueueKeyPrefix(keyPrefix: string): string {
  return `${keyPrefix}:runs:`;
}

export function runJobKey(keyPrefix: string, taskName: string, runId: RunId): string {
  return `${runQueueKeyPrefix(keyPrefix)}${deriveJobId(taskName, runId)}`;
}
