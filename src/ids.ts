import { randomUUID } from "node:crypto";

export type RunId = string;

export function createRunId(): RunId {
  return randomUUID();
}
