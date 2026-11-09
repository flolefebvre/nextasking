import { storedRunIdOf } from "./engine/engine-keys.js";
import type { RunIdentity } from "./engine/engine-keys.js";
import type { IndexScore } from "./engine/index-page-script.js";
import { isRunState } from "./run-states.js";
import type { RunState } from "./run-states.js";

export type RunListCursor = string;

export type ListedIndexes = { state: RunState; listedTask: string | null };

const INVALID_CURSOR_MESSAGE = "Invalid listRuns cursor: pass back a nextCursor that listRuns returned";

function otherStateCursorMessageOf(cursorState: RunState, calledState: RunState): string {
  return `Invalid listRuns cursor: it pages through state "${cursorState}", not "${calledState}"`;
}

export function cursorOfFields(fields: readonly unknown[]): RunListCursor {
  return Buffer.from(JSON.stringify(fields), "utf8").toString("base64url");
}

function cursorFieldsOf(cursor: unknown, formatVersion: number, fieldCount: number): unknown[] | undefined {
  if (typeof cursor !== "string" || cursor === "") {
    return undefined;
  }
  let decoded: unknown;
  try {
    decoded = JSON.parse(Buffer.from(cursor, "base64url").toString("utf8"));
  } catch {
    return undefined;
  }
  if (!Array.isArray(decoded) || decoded.length !== fieldCount || decoded[0] !== formatVersion) {
    return undefined;
  }
  return decoded as unknown[];
}

export type LastListedRun = RunIdentity & { score: IndexScore };

export type RunListPosition = ListedIndexes & { lastListedRun: LastListedRun };

const CURSOR_FORMAT_VERSION = 2;

export function encodeRunListCursor({ state, listedTask, lastListedRun }: RunListPosition): RunListCursor {
  const { score, task, runId } = lastListedRun;
  return cursorOfFields([CURSOR_FORMAT_VERSION, state, listedTask, score, task, runId]);
}

function shownTaskOf(listedTask: string | null): string {
  return listedTask === null ? "every task" : `task ${JSON.stringify(listedTask)}`;
}

export function decodeRunListCursor(cursor: unknown, listedIndexes: ListedIndexes): LastListedRun {
  const position = positionIn(cursor);
  if (position === undefined) {
    throw new Error(INVALID_CURSOR_MESSAGE);
  }
  if (position.state !== listedIndexes.state) {
    throw new Error(otherStateCursorMessageOf(position.state, listedIndexes.state));
  }
  if (position.listedTask !== listedIndexes.listedTask) {
    throw new Error(
      `Invalid listRuns cursor: it pages through ${shownTaskOf(position.listedTask)}, not ${shownTaskOf(listedIndexes.listedTask)}`,
    );
  }
  return position.lastListedRun;
}

function isScoreSentExactly(score: unknown): score is number {
  return typeof score === "number" && Number.isFinite(score) && Number(String(score)) === score;
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === "string" && value !== "";
}

function isStorableRun(task: string, runId: string): boolean {
  try {
    storedRunIdOf(task, runId);
    return true;
  } catch {
    return false;
  }
}

function positionIn(cursor: unknown): RunListPosition | undefined {
  const fields = cursorFieldsOf(cursor, CURSOR_FORMAT_VERSION, 6);
  if (fields === undefined) {
    return undefined;
  }
  const [, state, listedTask, score, task, runId] = fields;
  if (
    !isRunState(state) ||
    (listedTask !== null && typeof listedTask !== "string") ||
    !isScoreSentExactly(score) ||
    !isNonEmptyString(task) ||
    !isNonEmptyString(runId) ||
    !isStorableRun(task, runId) ||
    (listedTask !== null && task !== listedTask)
  ) {
    return undefined;
  }
  return { state, listedTask, lastListedRun: { score, task, runId } };
}
