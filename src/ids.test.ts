import { describe, expect, test } from "vitest";

import {
  createRandomId,
  deriveFailureNoticeEventId,
  deriveFiringRunId,
  deriveHandlerSendId,
  deriveListenerRunId,
  deriveStepIdempotencyKey,
  nameBasedUuid,
} from "./ids.js";

const RANDOM_UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;

const NAME_BASED_UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-5[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;

describe("createRandomId", () => {
  test("returns a different random UUID on each call", () => {
    const firstId = createRandomId();
    const secondId = createRandomId();

    expect(firstId).toMatch(RANDOM_UUID);
    expect(secondId).not.toBe(firstId);
  });
});

describe("nameBasedUuid", () => {
  test("matches the RFC 4122 version 5 test vector", () => {
    const dnsNamespace = "6ba7b810-9dad-11d1-80b4-00c04fd430c8";

    expect(nameBasedUuid(dnsNamespace, "www.example.com")).toBe("2ed6657d-e927-568b-95e1-2665a8aea6a2");
  });
});

describe("deriveListenerRunId", () => {
  test("derives the same version 5 UUID from the same event id and listener name", () => {
    const runId = deriveListenerRunId("evt-1", "record-invoice-payment");

    expect(runId).toBe("4955431b-3249-5b7b-b944-d2567f5d94a6");
    expect(runId).toMatch(NAME_BASED_UUID);
  });

  test("derives a different run id for another listener of the same event", () => {
    expect(deriveListenerRunId("evt-1", "send-receipt")).not.toBe(
      deriveListenerRunId("evt-1", "record-invoice-payment"),
    );
  });
});

describe("deriveFiringRunId", () => {
  test("derives the same version 5 UUID from the same task and planned instant", () => {
    const plannedInstant = Date.UTC(2026, 10, 3, 3, 0, 0);

    const runId = deriveFiringRunId("purge-expired-sessions", plannedInstant);

    expect(runId).toBe(deriveFiringRunId("purge-expired-sessions", plannedInstant));
    expect(runId).toMatch(NAME_BASED_UUID);
  });

  test("derives a different run id for another planned instant, another task or a listener run", () => {
    const plannedInstant = Date.UTC(2026, 10, 3, 3, 0, 0);
    const runId = deriveFiringRunId("purge-expired-sessions", plannedInstant);

    expect(deriveFiringRunId("purge-expired-sessions", plannedInstant + 1000)).not.toBe(runId);
    expect(deriveFiringRunId("send-digest", plannedInstant)).not.toBe(runId);
    expect(deriveListenerRunId(String(plannedInstant), "purge-expired-sessions")).not.toBe(runId);
  });
});

describe("deriveFailureNoticeEventId", () => {
  test("derives the same version 5 UUID from the same definition name, id and failure number", () => {
    const eventId = deriveFailureNoticeEventId("sync-account", "run-1", 1);

    expect(eventId).toBe(deriveFailureNoticeEventId("sync-account", "run-1", 1));
    expect(eventId).toMatch(NAME_BASED_UUID);
  });

  test("derives a different event id for another failure, another run or another definition", () => {
    const eventId = deriveFailureNoticeEventId("sync-account", "run-1", 1);

    expect(deriveFailureNoticeEventId("sync-account", "run-1", 2)).not.toBe(eventId);
    expect(deriveFailureNoticeEventId("sync-account", "run-2", 1)).not.toBe(eventId);
    expect(deriveFailureNoticeEventId("invoice.paid", "run-1", 1)).not.toBe(eventId);
  });
});

describe("deriveStepIdempotencyKey", () => {
  test("derives the same version 5 UUID from the same task, run id and step id", () => {
    const idempotencyKey = deriveStepIdempotencyKey("charge-card", "run-1", "create-charge");

    expect(idempotencyKey).toBe("9c24dd31-8fba-5cc1-a4f2-c683ca4c3657");
    expect(idempotencyKey).toMatch(NAME_BASED_UUID);
  });

  test("derives a different key for another run of the same step", () => {
    expect(deriveStepIdempotencyKey("charge-card", "run-2", "create-charge")).not.toBe(
      deriveStepIdempotencyKey("charge-card", "run-1", "create-charge"),
    );
  });
});

describe("deriveHandlerSendId", () => {
  test("derives the same version 5 UUID from the same task, run id, scope and position", () => {
    const sendId = deriveHandlerSendId("charge-card", "run-1", undefined, 0);

    expect(sendId).toBe(deriveHandlerSendId("charge-card", "run-1", undefined, 0));
    expect(sendId).toMatch(NAME_BASED_UUID);
  });

  test("derives a different id for each part that differs", () => {
    const sendId = deriveHandlerSendId("charge-card", "run-1", undefined, 0);

    expect(
      new Set([
        sendId,
        deriveHandlerSendId("refund-card", "run-1", undefined, 0),
        deriveHandlerSendId("charge-card", "run-2", undefined, 0),
        deriveHandlerSendId("charge-card", "run-1", "create-charge", 0),
        deriveHandlerSendId("charge-card", "run-1", undefined, 1),
      ]).size,
    ).toBe(5);
  });

  test("keeps the top level apart from a step whatever its id", () => {
    expect(deriveHandlerSendId("charge-card", "run-1", "top-level", 0)).not.toBe(
      deriveHandlerSendId("charge-card", "run-1", undefined, 0),
    );
  });
});
