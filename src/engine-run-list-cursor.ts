import type { IndexPosition } from "./engine/index-page-script.js";
import { INVALID_CURSOR_MESSAGE, cursorFieldsOf, cursorOfFields, otherStateCursorMessageOf } from "./read-api.js";
import type { RunListCursor } from "./read-api.js";
import { isRunState } from "./run-states.js";
import type { RunState } from "./run-states.js";

export type ListedIndexes = { state: RunState; task: string | null };

export type EngineRunListPosition = ListedIndexes & { indexPosition: IndexPosition };

const CURSOR_FORMAT_VERSION = 2;

export function encodeEngineRunListCursor({ state, task, indexPosition }: EngineRunListPosition): RunListCursor {
  return cursorOfFields([CURSOR_FORMAT_VERSION, state, task, indexPosition.score, indexPosition.runId]);
}

function shownTaskOf(task: string | null): string {
  return task === null ? "every task" : `task ${JSON.stringify(task)}`;
}

export function decodeEngineRunListCursor(cursor: unknown, listedIndexes: ListedIndexes): IndexPosition {
  const position = positionIn(cursor);
  if (position === undefined) {
    throw new Error(INVALID_CURSOR_MESSAGE);
  }
  if (position.state !== listedIndexes.state) {
    throw new Error(otherStateCursorMessageOf(position.state, listedIndexes.state));
  }
  if (position.task !== listedIndexes.task) {
    throw new Error(
      `Invalid listRuns cursor: it pages through ${shownTaskOf(position.task)}, not ${shownTaskOf(listedIndexes.task)}`,
    );
  }
  return position.indexPosition;
}

function isScoreSentExactly(score: unknown): score is number {
  return typeof score === "number" && Number.isFinite(score) && Number(String(score)) === score;
}

function positionIn(cursor: unknown): EngineRunListPosition | undefined {
  const fields = cursorFieldsOf(cursor, CURSOR_FORMAT_VERSION, 5);
  if (fields === undefined) {
    return undefined;
  }
  const [, state, task, score, runId] = fields;
  if (
    !isRunState(state) ||
    (task !== null && typeof task !== "string") ||
    !isScoreSentExactly(score) ||
    typeof runId !== "string" ||
    runId === ""
  ) {
    return undefined;
  }
  return { state, task, indexPosition: { score, runId } };
}
