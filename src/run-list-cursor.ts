import type { EpochMilliseconds } from "./durations.js";
import { storedRunIdOf } from "./engine/engine-keys.js";
import type { RunIdentity } from "./engine/engine-keys.js";
import type { IndexScore, ListOrder, PagingDirection } from "./engine/index-page-script.js";
import { isOrderGroup, isRunState, orderGroupOf } from "./run-states.js";
import type { OrderGroup, RunState } from "./run-states.js";

export type RunListCursor = string;

export type TimeRange = { from: EpochMilliseconds | undefined; until: EpochMilliseconds | undefined };

export const EVERY_TIME: TimeRange = { from: undefined, until: undefined };

export type RunListRequest = {
  listedStates: readonly RunState[] | null;
  listedTasks: readonly string[] | null;
  timeRange: TimeRange;
  order: ListOrder;
};

export type RunListPlace = RunIdentity & { orderGroup: OrderGroup; score: IndexScore };

export type PageBoundary = { direction: PagingDirection; place: RunListPlace | undefined };

export type RunListCursorContent = RunListRequest & PageBoundary;

const INVALID_CURSOR_MESSAGE =
  "Invalid listRuns cursor: pass back a nextCursor or previousCursor that listRuns returned";

export function sentenceListOf(words: readonly string[]): string {
  return words.length <= 1 ? words.join("") : `${words.slice(0, -1).join(", ")} and ${words.at(-1)!}`;
}

export function quotedListOf(names: readonly string[]): string {
  return sentenceListOf(names.map((name) => JSON.stringify(name)));
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

function shownTimeRangeOf({ from, until }: TimeRange): string {
  const shownBounds = [
    ...(from === undefined ? [] : [`from ${new Date(from).toISOString()}`]),
    ...(until === undefined ? [] : [`until ${new Date(until).toISOString()}`]),
  ];
  return shownBounds.length === 0 ? "runs at any time" : `runs ${shownBounds.join(" ")}`;
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
  timeRange,
  order,
  direction,
  place,
}: RunListCursorContent): RunListCursor {
  const placeFields = place === undefined ? null : [place.orderGroup, place.score, place.task, place.runId];
  return cursorOfFields([
    CURSOR_FORMAT_VERSION,
    direction,
    placeFields,
    listedStates,
    listedTasks,
    timeRange.from ?? null,
    timeRange.until ?? null,
    order,
  ]);
}

function isSameTimeRange(left: TimeRange, right: TimeRange): boolean {
  return left.from === right.from && left.until === right.until;
}

function isSameSetOfNames(left: readonly string[] | null, right: readonly string[] | null): boolean {
  if (left === null || right === null) {
    return left === right;
  }
  const [leftNames, rightNames] = [new Set(left), new Set(right)];
  return leftNames.size === rightNames.size && [...leftNames].every((name) => rightNames.has(name));
}

export function decodeRunListCursor(cursor: unknown, runListRequest: RunListRequest): PageBoundary {
  const content = contentIn(cursor);
  if (content === undefined) {
    throw new Error(INVALID_CURSOR_MESSAGE);
  }
  if (!isSameSetOfNames(content.listedStates, runListRequest.listedStates)) {
    throw new Error(
      `Invalid listRuns cursor: it pages through ${shownCursorStatesOf(content.listedStates)}, not ${shownCalledStatesOf(runListRequest.listedStates)}`,
    );
  }
  if (!isSameSetOfNames(content.listedTasks, runListRequest.listedTasks)) {
    throw new Error(
      `Invalid listRuns cursor: it pages through ${shownTasksOf(content.listedTasks)}, not ${shownTasksOf(runListRequest.listedTasks)}`,
    );
  }
  if (!isSameTimeRange(content.timeRange, runListRequest.timeRange)) {
    throw new Error(
      `Invalid listRuns cursor: it pages through ${shownTimeRangeOf(content.timeRange)}, not ${shownTimeRangeOf(runListRequest.timeRange)}`,
    );
  }
  if (content.order !== runListRequest.order) {
    throw new Error(
      `Invalid listRuns cursor: it pages through runs in ${content.order} order, not in ${runListRequest.order} order`,
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

function isListOrder(value: unknown): value is ListOrder {
  return value === "descending" || value === "ascending";
}

function isListOrEvery<Name>(value: unknown, isName: (element: unknown) => element is Name): value is Name[] | null {
  return value === null || (Array.isArray(value) && value.length > 0 && value.every(isName));
}

function isScoreInside(score: IndexScore, { from, until }: TimeRange): boolean {
  return (from === undefined || score >= from) && (until === undefined || score <= until);
}

function placeOf(
  placeFields: unknown,
  { listedStates, listedTasks, timeRange }: RunListRequest,
): RunListPlace | undefined {
  if (!Array.isArray(placeFields) || placeFields.length !== 4) {
    return undefined;
  }
  const [orderGroup, score, task, runId] = placeFields as unknown[];
  if (
    !isOrderGroup(orderGroup) ||
    !isScoreSentExactly(score) ||
    !isScoreInside(score, timeRange) ||
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

function isTimeBound(value: unknown): value is EpochMilliseconds | null {
  return value === null || Number.isSafeInteger(value);
}

function timeRangeOf(from: unknown, until: unknown): TimeRange | undefined {
  if (!isTimeBound(from) || !isTimeBound(until) || (from !== null && until !== null && from > until)) {
    return undefined;
  }
  return { from: from ?? undefined, until: until ?? undefined };
}

type CursorFields = {
  direction: unknown;
  placeFields: unknown;
  listedStates: unknown;
  listedTasks: unknown;
  timeRange: TimeRange | undefined;
  order: unknown;
};

function contentOf({
  direction,
  placeFields,
  listedStates,
  listedTasks,
  timeRange,
  order,
}: CursorFields): RunListCursorContent | undefined {
  if (
    !isListOrEvery(listedStates, isRunState) ||
    !isListOrEvery(listedTasks, isNonEmptyString) ||
    timeRange === undefined ||
    !isListOrder(order) ||
    !isPagingDirection(direction)
  ) {
    return undefined;
  }
  const runListRequest = { listedStates, listedTasks, timeRange, order };
  if (placeFields === null) {
    return direction === "backward" ? { ...runListRequest, direction, place: undefined } : undefined;
  }
  const place = placeOf(placeFields, runListRequest);
  return place === undefined ? undefined : { ...runListRequest, direction, place };
}

function contentIn(cursor: unknown): RunListCursorContent | undefined {
  const fields = cursorFieldsOf(cursor);
  if (fields?.[0] === CURSOR_FORMAT_VERSION && fields.length === 8) {
    const [, direction, placeFields, listedStates, listedTasks, from, until, order] = fields;
    return contentOf({
      direction,
      placeFields,
      listedStates,
      listedTasks,
      timeRange: timeRangeOf(from, until),
      order,
    });
  }
  if (fields?.[0] === FORWARD_ONLY_CURSOR_FORMAT_VERSION && fields.length === 6) {
    const [, state, listedTask, score, task, runId] = fields;
    return isRunState(state) && (listedTask === null || typeof listedTask === "string")
      ? contentOf({
          direction: "forward",
          placeFields: [orderGroupOf(state), score, task, runId],
          listedStates: [state],
          listedTasks: listedTask === null ? null : [listedTask],
          timeRange: EVERY_TIME,
          order: "descending",
        })
      : undefined;
  }
  return undefined;
}
