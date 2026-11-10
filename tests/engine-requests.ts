import { engineKeysOf, storedRunIdOf } from "../src/engine/engine-keys.js";
import type { SettleOutcome } from "../src/engine/engine.js";
import { fetchKeysOf } from "../src/engine/fetch-script.js";
import { fireKeysOf } from "../src/engine/fire-script.js";
import { settleKeysOf } from "../src/engine/settle-script.js";
import type { RunId } from "../src/ids.js";

function bulkStringOf(value: string): string {
  return `$${String(Buffer.byteLength(value))}\r\n${value}\r\n`;
}

function scriptKeysOf(scriptKeys: readonly string[]): string {
  return `${bulkStringOf(String(scriptKeys.length))}${scriptKeys.map(bulkStringOf).join("")}`;
}

export function isFetchRequest(request: string, keyPrefix: string): boolean {
  return request.includes(scriptKeysOf(fetchKeysOf(engineKeysOf(keyPrefix))));
}

export function isFireRequestOf(request: string, keyPrefix: string, task: string): boolean {
  const fireKeysAt = request.indexOf(scriptKeysOf(fireKeysOf(engineKeysOf(keyPrefix))));
  return fireKeysAt !== -1 && request.indexOf(bulkStringOf(task), fireKeysAt) !== -1;
}

export function isRunErrorsRequestOf(
  request: string,
  keyPrefix: string,
  { task, runId }: { task: string; runId: RunId },
): boolean {
  return request.includes(bulkStringOf(engineKeysOf(keyPrefix).runErrors(task, runId)));
}

export function isSettleRequestOf(
  request: string,
  keyPrefix: string,
  { task, runId, settleKind }: { task: string; runId: RunId; settleKind: SettleOutcome["kind"] },
): boolean {
  const storedRunId = bulkStringOf(storedRunIdOf(task, runId));
  const storedRunIdAt = request.indexOf(storedRunId);
  if (!request.includes(scriptKeysOf(settleKeysOf(engineKeysOf(keyPrefix)))) || storedRunIdAt === -1) {
    return false;
  }
  const lockTokenAndSettleKind = request.slice(storedRunIdAt + storedRunId.length);
  const lockTokenEnd = lockTokenAndSettleKind.indexOf("\r\n", lockTokenAndSettleKind.indexOf("\r\n") + 2) + 2;
  return lockTokenAndSettleKind.slice(lockTokenEnd).startsWith(bulkStringOf(settleKind));
}
