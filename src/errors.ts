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

const SHOWN_REPLAY_REFUSAL_BY_REFUSAL: Readonly<Record<ReplayRefusal, string>> = {
  "not found": "no such run is kept, its id is unknown or the run was removed",
  "not failed": "it is not failed",
};

export class RunNotReplayableError extends Error {
  override readonly name = "RunNotReplayableError";

  constructor(taskName: string, runId: string, replayRefusal: ReplayRefusal) {
    super(`Run "${runId}" of task "${taskName}" can't be replayed: ${SHOWN_REPLAY_REFUSAL_BY_REFUSAL[replayRefusal]}.`);
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
