import { INVALID_CURSOR_MESSAGE, cursorFieldsOf, cursorOfFields, otherStateCursorMessageOf } from "./read-api.js";
import type { RunListCursor } from "./read-api.js";
import { BULLMQ_SETS_BY_RUN_STATE, RUN_STATES } from "./run-states.js";
import type { RunState } from "./run-states.js";

export type RunListPosition = {
  state: RunState;
  setIndex: number;
  offset: number;
};

const CURSOR_FORMAT_VERSION = 1;

export function encodeRunListCursor({ state, setIndex, offset }: RunListPosition): RunListCursor {
  return cursorOfFields([CURSOR_FORMAT_VERSION, state, setIndex, offset]);
}

export function decodeRunListCursor(cursor: unknown, expectedState: RunState): RunListPosition {
  const position = positionIn(cursor);
  if (position === undefined) {
    throw new Error(INVALID_CURSOR_MESSAGE);
  }
  if (position.state !== expectedState) {
    throw new Error(otherStateCursorMessageOf(position.state, expectedState));
  }
  return position;
}

function isNonNegativeInteger(value: unknown): value is number {
  return typeof value === "number" && Number.isInteger(value) && value >= 0;
}

function positionIn(cursor: unknown): RunListPosition | undefined {
  const fields = cursorFieldsOf(cursor, CURSOR_FORMAT_VERSION, 4);
  if (fields === undefined) {
    return undefined;
  }
  const [, state, setIndex, offset] = fields;
  if (!RUN_STATES.includes(state as RunState) || !isNonNegativeInteger(setIndex) || !isNonNegativeInteger(offset)) {
    return undefined;
  }
  if (setIndex >= BULLMQ_SETS_BY_RUN_STATE[state as RunState].length) {
    return undefined;
  }
  return { state: state as RunState, setIndex, offset };
}
