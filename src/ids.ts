import { createHash, randomUUID } from "node:crypto";

export type RunId = string;

export type EventId = string;

export type JobId = string;

type Uuid = string;

const NEXTASKING_NAMESPACE: Uuid = "8f6c1f0e-4a5d-4b8e-9c3a-2d7e5b1a6f40";

export function createRandomId(): Uuid {
  return randomUUID();
}

export function nameBasedUuid(namespace: Uuid, name: string): Uuid {
  const namespaceBytes = Buffer.from(namespace.replaceAll("-", ""), "hex");
  const hashBytes = createHash("sha1").update(namespaceBytes).update(name, "utf8").digest().subarray(0, 16);
  hashBytes[6] = (hashBytes[6]! & 0x0f) | 0x50;
  hashBytes[8] = (hashBytes[8]! & 0x3f) | 0x80;
  const hex = hashBytes.toString("hex");
  return [hex.slice(0, 8), hex.slice(8, 12), hex.slice(12, 16), hex.slice(16, 20), hex.slice(20)].join("-");
}

function uuidFromNameParts(nameParts: readonly string[]): Uuid {
  return nameBasedUuid(NEXTASKING_NAMESPACE, JSON.stringify(nameParts));
}

export function deriveListenerRunId(eventId: EventId, listenerName: string): RunId {
  return uuidFromNameParts(["listener-run", eventId, listenerName]);
}

export function deriveJobId(definitionName: string, consumerFacingId: RunId | EventId): JobId {
  return uuidFromNameParts(["job", definitionName, consumerFacingId]);
}
