import { deriveJobId } from "../src/ids.js";
import type { RunId } from "../src/ids.js";

export function runJobKey(keyPrefix: string, taskName: string, runId: RunId): string {
  return `${keyPrefix}:runs:${deriveJobId(taskName, runId)}`;
}
