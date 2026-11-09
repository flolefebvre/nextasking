import { defineEvent } from "./definitions.js";
import type { EventId, RunId } from "./ids.js";
import type { IsoTimestamp } from "./durations.js";
import type { FailureReason } from "./run-states.js";
import type { StandardSchemaV1 } from "./standard-schema.js";

export type FailureNoticeError = {
  name: string;
  message: string;
  stack: string;
};

export type RunFailedPayload = {
  task: string;
  runId: RunId;
  reason: FailureReason;
  error: FailureNoticeError;
  attempt: number;
  failedAt: IsoTimestamp;
  eventName?: string;
  eventId?: EventId;
};

export type FanOutFailedPayload = {
  eventName: string;
  eventId: EventId;
  error: FailureNoticeError;
  failedAt: IsoTimestamp;
};

function payloadSchemaBuiltInto<BuiltInPayload>(): StandardSchemaV1<BuiltInPayload, BuiltInPayload> {
  return {
    "~standard": {
      version: 1,
      vendor: "nextasking",
      validate: (value) => ({ value: value as BuiltInPayload }),
    },
  };
}

export const runFailed = defineEvent("nextasking/run.failed", { schema: payloadSchemaBuiltInto<RunFailedPayload>() });

export const fanOutFailed = defineEvent("nextasking/fan-out.failed", {
  schema: payloadSchemaBuiltInto<FanOutFailedPayload>(),
});

export const BUILT_IN_EVENTS = [runFailed, fanOutFailed] as const;

export type BuiltInEvent = (typeof BUILT_IN_EVENTS)[number];

export type BuiltInEventName = BuiltInEvent["name"];

const BUILT_IN_EVENT_NAMES: ReadonlySet<string> = new Set(BUILT_IN_EVENTS.map((event) => event.name));

export function isBuiltInEventName(eventName: string): eventName is BuiltInEventName {
  return BUILT_IN_EVENT_NAMES.has(eventName);
}
