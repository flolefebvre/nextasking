import type { RunListCursor } from "./read-api.js";
import { BULLMQ_SETS_BY_RUN_STATE, RUN_STATES } from "./run-states.js";
import type { RunState } from "./run-states.js";

export type RunListPosition = {
  state: RunState;
  setIndex: number;
  offset: number;
};

const CURSOR_FORMAT_VERSION = 1;

const INVALID_CURSOR_MESSAGE = "Invalid listRuns cursor: pass back a nextCursor that listRuns returned";

export function encodeRunListCursor({ state, setIndex, offset }: RunListPosition): RunListCursor {
  return Buffer.from(JSON.stringify([CURSOR_FORMAT_VERSION, state, setIndex, offset]), "utf8").toString("base64url");
}

export function decodeRunListCursor(cursor: unknown, expectedState: RunState): RunListPosition {
  const position = positionIn(cursor);
  if (position === undefined) {
    throw new Error(INVALID_CURSOR_MESSAGE);
  }
  if (position.state !== expectedState) {
    throw new Error(`Invalid listRuns cursor: it pages through state "${position.state}", not "${expectedState}"`);
  }
  return position;
}

function isNonNegativeInteger(value: unknown): value is number {
  return typeof value === "number" && Number.isInteger(value) && value >= 0;
}

function positionIn(cursor: unknown): RunListPosition | undefined {
  if (typeof cursor !== "string" || cursor === "") {
    return undefined;
  }
  let decoded: unknown;
  try {
    decoded = JSON.parse(Buffer.from(cursor, "base64url").toString("utf8"));
  } catch {
    return undefined;
  }
  if (!Array.isArray(decoded) || decoded.length !== 4 || decoded[0] !== CURSOR_FORMAT_VERSION) {
    return undefined;
  }
  const [, state, setIndex, offset] = decoded as [unknown, unknown, unknown, unknown];
  if (!RUN_STATES.includes(state as RunState) || !isNonNegativeInteger(setIndex) || !isNonNegativeInteger(offset)) {
    return undefined;
  }
  if (setIndex >= BULLMQ_SETS_BY_RUN_STATE[state as RunState].length) {
    return undefined;
  }
  return { state: state as RunState, setIndex, offset };
}
