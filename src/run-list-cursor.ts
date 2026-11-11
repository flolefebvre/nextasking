import { storedRunIdOf } from "./engine/engine-keys.js";
import type { RunIdentity } from "./engine/engine-keys.js";
import type { IndexScore } from "./engine/index-page-script.js";
import { isRunState } from "./run-states.js";
import type { RunState } from "./run-states.js";

export type RunListCursor = string;

export type ListedIndexes = { state: RunState; listedTask: string | null };

export type PagingDirection = "forward" | "backward";

export type RunListPlace = RunIdentity & { score: IndexScore };

export type PageBoundary = { direction: PagingDirection; place: RunListPlace | undefined };

export type RunListCursorContent = ListedIndexes & PageBoundary;

const INVALID_CURSOR_MESSAGE =
  "Invalid listRuns cursor: pass back a nextCursor or previousCursor that listRuns returned";

function otherStateCursorMessageOf(cursorState: RunState, calledState: RunState): string {
  return `Invalid listRuns cursor: it pages through state "${cursorState}", not "${calledState}"`;
}

export function cursorOfFields(fields: readonly unknown[]): RunListCursor {
  return Buffer.from(JSON.stringify(fields), "utf8").toString("base64url");
}

function cursorFieldsOf(cursor: unknown): unknown[] | undefined {
  if (typeof cursor !== "string" || cursor === "") {
    return undefined;
  }
  let decoded: unknown;
  try {
    decoded = JSON.parse(Buffer.from(cursor, "base64url").toString("utf8"));
  } catch {
    return undefined;
  }
  return Array.isArray(decoded) ? decoded : undefined;
}

const CURSOR_FORMAT_VERSION = 3;

const FORWARD_ONLY_CURSOR_FORMAT_VERSION = 2;

export function encodeRunListCursor({ state, listedTask, direction, place }: RunListCursorContent): RunListCursor {
  const placeFields = place === undefined ? null : [place.score, place.task, place.runId];
  return cursorOfFields([CURSOR_FORMAT_VERSION, direction, placeFields, state, listedTask]);
}

function shownTaskOf(listedTask: string | null): string {
  return listedTask === null ? "every task" : `task ${JSON.stringify(listedTask)}`;
}

export function decodeRunListCursor(cursor: unknown, listedIndexes: ListedIndexes): PageBoundary {
  const content = contentIn(cursor);
  if (content === undefined) {
    throw new Error(INVALID_CURSOR_MESSAGE);
  }
  if (content.state !== listedIndexes.state) {
    throw new Error(otherStateCursorMessageOf(content.state, listedIndexes.state));
  }
  if (content.listedTask !== listedIndexes.listedTask) {
    throw new Error(
      `Invalid listRuns cursor: it pages through ${shownTaskOf(content.listedTask)}, not ${shownTaskOf(listedIndexes.listedTask)}`,
    );
  }
  return { direction: content.direction, place: content.place };
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

function isPagingDirection(value: unknown): value is PagingDirection {
  return value === "forward" || value === "backward";
}

function placeOf(placeFields: unknown, listedTask: string | null): RunListPlace | undefined {
  if (!Array.isArray(placeFields) || placeFields.length !== 3) {
    return undefined;
  }
  const [score, task, runId] = placeFields as unknown[];
  if (
    !isScoreSentExactly(score) ||
    !isNonEmptyString(task) ||
    !isNonEmptyString(runId) ||
    !isStorableRun(task, runId) ||
    (listedTask !== null && task !== listedTask)
  ) {
    return undefined;
  }
  return { score, task, runId };
}

function contentOf(
  state: unknown,
  listedTask: unknown,
  direction: unknown,
  placeFields: unknown,
): RunListCursorContent | undefined {
  if (!isRunState(state) || (listedTask !== null && typeof listedTask !== "string") || !isPagingDirection(direction)) {
    return undefined;
  }
  if (placeFields === null) {
    return direction === "backward" ? { state, listedTask, direction, place: undefined } : undefined;
  }
  const place = placeOf(placeFields, listedTask);
  return place === undefined ? undefined : { state, listedTask, direction, place };
}

function contentIn(cursor: unknown): RunListCursorContent | undefined {
  const fields = cursorFieldsOf(cursor);
  if (fields?.[0] === CURSOR_FORMAT_VERSION && fields.length === 5) {
    const [, direction, placeFields, state, listedTask] = fields;
    return contentOf(state, listedTask, direction, placeFields);
  }
  if (fields?.[0] === FORWARD_ONLY_CURSOR_FORMAT_VERSION && fields.length === 6) {
    const [, state, listedTask, score, task, runId] = fields;
    return contentOf(state, listedTask, "forward", [score, task, runId]);
  }
  return undefined;
}
