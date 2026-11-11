import { storedRunIdOf } from "./engine/engine-keys.js";
import type { RunIdentity } from "./engine/engine-keys.js";
import type { IndexScore, PagingDirection } from "./engine/index-page-script.js";
import { isOrderGroup, isRunState, orderGroupOf } from "./run-states.js";
import type { OrderGroup, RunState } from "./run-states.js";

export type RunListCursor = string;

export type ListedIndexes = { listedStates: readonly RunState[] | null; listedTasks: readonly string[] | null };

export type RunListPlace = RunIdentity & { orderGroup: OrderGroup; score: IndexScore };

export type PageBoundary = { direction: PagingDirection; place: RunListPlace | undefined };

export type RunListCursorContent = ListedIndexes & PageBoundary;

const INVALID_CURSOR_MESSAGE =
  "Invalid listRuns cursor: pass back a nextCursor or previousCursor that listRuns returned";

function quotedListOf(names: readonly string[]): string {
  const quotedNames = names.map((name) => JSON.stringify(name));
  return quotedNames.length === 1
    ? quotedNames.join("")
    : `${quotedNames.slice(0, -1).join(", ")} and ${quotedNames.at(-1)!}`;
}

function shownCursorStatesOf(listedStates: readonly RunState[] | null): string {
  if (listedStates === null) {
    return "every state";
  }
  return `${listedStates.length === 1 ? "state" : "states"} ${quotedListOf(listedStates)}`;
}

function shownCalledStatesOf(listedStates: readonly RunState[] | null): string {
  return listedStates === null ? "every state" : quotedListOf(listedStates);
}

function shownTasksOf(listedTasks: readonly string[] | null): string {
  if (listedTasks === null) {
    return "every task";
  }
  return `${listedTasks.length === 1 ? "task" : "tasks"} ${quotedListOf(listedTasks)}`;
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

export function encodeRunListCursor({
  listedStates,
  listedTasks,
  direction,
  place,
}: RunListCursorContent): RunListCursor {
  const placeFields = place === undefined ? null : [place.orderGroup, place.score, place.task, place.runId];
  return cursorOfFields([CURSOR_FORMAT_VERSION, direction, placeFields, listedStates, listedTasks]);
}

function isSameList(left: readonly string[] | null, right: readonly string[] | null): boolean {
  return (
    left === right ||
    (left !== null && right !== null && left.length === right.length && left.every((name, at) => name === right[at]))
  );
}

export function decodeRunListCursor(cursor: unknown, listedIndexes: ListedIndexes): PageBoundary {
  const content = contentIn(cursor);
  if (content === undefined) {
    throw new Error(INVALID_CURSOR_MESSAGE);
  }
  if (!isSameList(content.listedStates, listedIndexes.listedStates)) {
    throw new Error(
      `Invalid listRuns cursor: it pages through ${shownCursorStatesOf(content.listedStates)}, not ${shownCalledStatesOf(listedIndexes.listedStates)}`,
    );
  }
  if (!isSameList(content.listedTasks, listedIndexes.listedTasks)) {
    throw new Error(
      `Invalid listRuns cursor: it pages through ${shownTasksOf(content.listedTasks)}, not ${shownTasksOf(listedIndexes.listedTasks)}`,
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

function isListOrEvery<Name>(value: unknown, isName: (element: unknown) => element is Name): value is Name[] | null {
  return value === null || (Array.isArray(value) && value.length > 0 && value.every(isName));
}

function placeOf(placeFields: unknown, { listedStates, listedTasks }: ListedIndexes): RunListPlace | undefined {
  if (!Array.isArray(placeFields) || placeFields.length !== 4) {
    return undefined;
  }
  const [orderGroup, score, task, runId] = placeFields as unknown[];
  if (
    !isOrderGroup(orderGroup) ||
    !isScoreSentExactly(score) ||
    !isNonEmptyString(task) ||
    !isNonEmptyString(runId) ||
    !isStorableRun(task, runId) ||
    (listedStates !== null && !listedStates.some((state) => orderGroupOf(state) === orderGroup)) ||
    (listedTasks !== null && !listedTasks.includes(task))
  ) {
    return undefined;
  }
  return { orderGroup, score, task, runId };
}

function contentOf(
  listedStates: unknown,
  listedTasks: unknown,
  direction: unknown,
  placeFields: unknown,
): RunListCursorContent | undefined {
  if (
    !isListOrEvery(listedStates, isRunState) ||
    !isListOrEvery(listedTasks, isNonEmptyString) ||
    !isPagingDirection(direction)
  ) {
    return undefined;
  }
  if (placeFields === null) {
    return direction === "backward" ? { listedStates, listedTasks, direction, place: undefined } : undefined;
  }
  const place = placeOf(placeFields, { listedStates, listedTasks });
  return place === undefined ? undefined : { listedStates, listedTasks, direction, place };
}

function contentIn(cursor: unknown): RunListCursorContent | undefined {
  const fields = cursorFieldsOf(cursor);
  if (fields?.[0] === CURSOR_FORMAT_VERSION && fields.length === 5) {
    const [, direction, placeFields, listedStates, listedTasks] = fields;
    return contentOf(listedStates, listedTasks, direction, placeFields);
  }
  if (fields?.[0] === FORWARD_ONLY_CURSOR_FORMAT_VERSION && fields.length === 6) {
    const [, state, listedTask, score, task, runId] = fields;
    return isRunState(state) && (listedTask === null || typeof listedTask === "string")
      ? contentOf([state], listedTask === null ? null : [listedTask], "forward", [
          orderGroupOf(state),
          score,
          task,
          runId,
        ])
      : undefined;
  }
  return undefined;
}
