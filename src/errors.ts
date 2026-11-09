import { shownDurationOf } from "./durations.js";
import type { DurationInMilliseconds } from "./durations.js";
import type { EventId, RunId } from "./ids.js";
import type { StandardSchemaV1Issue } from "./standard-schema.js";

export type DefinitionKind = "task" | "event";

export class UnknownTaskError extends Error {
  override readonly name = "UnknownTaskError";

  constructor(taskName: string, validTaskNames: readonly string[]) {
    super(`Unknown task "${taskName}". Valid task names: ${quotedList(validTaskNames)}.`);
  }
}

export class UnknownEventError extends Error {
  override readonly name = "UnknownEventError";

  constructor(eventName: string, validEventNames: readonly string[]) {
    super(`Unknown event "${eventName}". Valid event names: ${quotedList(validEventNames)}.`);
  }
}

export type ReplayRefusal = "not found" | "not failed";

function shownReplayRefusalOf(replayRefusal: ReplayRefusal, replayedKind: "run" | "fan-out", idName: string): string {
  return replayRefusal === "not found"
    ? `no such ${replayedKind} is kept, its ${idName} is unknown or the ${replayedKind} was removed`
    : "it is not failed";
}

export class RunNotReplayableError extends Error {
  override readonly name = "RunNotReplayableError";

  constructor(taskName: string, runId: string, replayRefusal: ReplayRefusal) {
    super(
      `Run "${runId}" of task "${taskName}" can't be replayed: ${shownReplayRefusalOf(replayRefusal, "run", "id")}.`,
    );
  }
}

export class FanOutNotReplayableError extends Error {
  override readonly name = "FanOutNotReplayableError";

  constructor(eventName: string, eventId: string, replayRefusal: ReplayRefusal) {
    super(
      `Fan-out "${eventId}" of event "${eventName}" can't be replayed: ${shownReplayRefusalOf(replayRefusal, "fan-out", "event id")}.`,
    );
  }
}

export type RedisCallIds = {
  runId?: RunId;
  eventId?: EventId;
};

export class RedisUnavailableError extends Error {
  override readonly name = "RedisUnavailableError";
  readonly runId?: RunId;
  readonly eventId?: EventId;

  constructor(giveUpAfterInMilliseconds: DurationInMilliseconds, callIds: RedisCallIds, unansweredError: unknown) {
    super(`Redis didn't answer within ${shownDurationOf(giveUpAfterInMilliseconds)}`, { cause: unansweredError });
    if (callIds.runId !== undefined) {
      this.runId = callIds.runId;
    }
    if (callIds.eventId !== undefined) {
      this.eventId = callIds.eventId;
    }
  }
}

export class InstanceClosedError extends Error {
  override readonly name = "InstanceClosedError";

  constructor() {
    super("this nextasking instance was closed");
  }
}

export class NonRetriableError extends Error {
  override readonly name = "NonRetriableError";
}

export class StepMisuseError extends Error {
  override readonly name = "StepMisuseError";
}

export type PayloadIssue = {
  readonly path: string;
  readonly message: string;
};

export class InvalidPayloadError extends Error {
  override readonly name = "InvalidPayloadError";
  readonly issues: readonly PayloadIssue[];

  constructor(
    definitionKind: DefinitionKind,
    definitionName: string,
    schemaIssues: readonly StandardSchemaV1Issue[],
    payload: unknown,
  ) {
    const issues = schemaIssues.map((schemaIssue) => ({
      path: issuePath(schemaIssue),
      message: redactPayloadValues(schemaIssue.message, payload),
    }));
    super(
      `Invalid payload for ${definitionKind} "${definitionName}": ${issues.map((issue) => `${issue.path}: ${issue.message}`).join("; ")}`,
    );
    this.issues = issues;
  }
}

const REDACTED = "[redacted]";

function redactPayloadValues(message: string, payload: unknown): string {
  const redactedTexts = primitiveLeaves(payload)
    .flatMap((leaf) => (typeof leaf === "string" ? [JSON.stringify(leaf), leaf] : [String(leaf)]))
    .filter((text) => text !== "")
    .sort((first, second) => second.length - first.length);
  return redactedTexts.reduce(
    (redactedMessage, text) => redactedMessage.replace(wholeTokenPattern(text), REDACTED),
    message,
  );
}

type PrimitiveLeaf = string | number | bigint | boolean | null;

function primitiveLeaves(value: unknown): PrimitiveLeaf[] {
  if (value === null || ["string", "number", "bigint", "boolean"].includes(typeof value)) {
    return [value as PrimitiveLeaf];
  }
  if (typeof value === "object") {
    return Object.values(value).flatMap(primitiveLeaves);
  }
  return [];
}

function wholeTokenPattern(text: string): RegExp {
  const escapedText = text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  return new RegExp(`(?<![\\p{L}\\p{N}_])${escapedText}(?![\\p{L}\\p{N}_])`, "gu");
}

function issuePath(schemaIssue: StandardSchemaV1Issue): string {
  const keys = (schemaIssue.path ?? []).map((segment) =>
    typeof segment === "object" && segment !== null ? segment.key : segment,
  );
  return keys.length === 0 ? "(root)" : keys.map((key) => String(key)).join(".");
}

function quotedList(names: readonly string[]): string {
  return names.length === 0 ? "none" : names.map((name) => `"${name}"`).join(", ");
}
