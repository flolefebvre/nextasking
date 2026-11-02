import type { StandardSchemaV1Issue } from "./standard-schema.js";

export class UnknownTaskError extends Error {
  override readonly name = "UnknownTaskError";

  constructor(taskName: string, validTaskNames: readonly string[]) {
    super(`Unknown task "${taskName}". Valid task names: ${quotedList(validTaskNames)}.`);
  }
}

export type PayloadIssue = {
  readonly path: string;
  readonly message: string;
};

export class InvalidPayloadError extends Error {
  override readonly name = "InvalidPayloadError";
  readonly issues: readonly PayloadIssue[];

  constructor(taskName: string, schemaIssues: readonly StandardSchemaV1Issue[], payload: unknown) {
    const issues = schemaIssues.map((schemaIssue) => ({
      path: issuePath(schemaIssue),
      message: redactPayloadValues(schemaIssue.message, payload),
    }));
    super(
      `Invalid payload for task "${taskName}": ${issues.map((issue) => `${issue.path}: ${issue.message}`).join("; ")}`,
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
