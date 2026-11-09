import type { RunIdentity } from "./engine/engine-keys.js";
import type { IndexScore } from "./engine/index-page-script.js";
import { INVALID_CURSOR_MESSAGE, cursorFieldsOf, cursorOfFields, otherStateCursorMessageOf } from "./read-api.js";
import type { RunListCursor } from "./read-api.js";
import { isRunState } from "./run-states.js";
import type { RunState } from "./run-states.js";

export type ListedIndexes = { state: RunState; listedTask: string | null };

export type LastListedRun = RunIdentity & { score: IndexScore };

export type EngineRunListPosition = ListedIndexes & { lastListedRun: LastListedRun };

const CURSOR_FORMAT_VERSION = 2;

export function encodeEngineRunListCursor({ state, listedTask, lastListedRun }: EngineRunListPosition): RunListCursor {
  const { score, task, runId } = lastListedRun;
  return cursorOfFields([CURSOR_FORMAT_VERSION, state, listedTask, score, task, runId]);
}

function shownTaskOf(listedTask: string | null): string {
  return listedTask === null ? "every task" : `task ${JSON.stringify(listedTask)}`;
}

export function decodeEngineRunListCursor(cursor: unknown, listedIndexes: ListedIndexes): LastListedRun {
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

function positionIn(cursor: unknown): EngineRunListPosition | undefined {
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
    (listedTask !== null && task !== listedTask)
  ) {
    return undefined;
  }
  return { state, listedTask, lastListedRun: { score, task, runId } };
}
