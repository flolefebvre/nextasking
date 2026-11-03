import { describe, expect, test } from "vitest";

import { unsentFailureNoticeOf } from "./failure-notices.js";
import { deriveFailureNoticeEventId } from "./ids.js";
import type { ErrorRecord, StoredFanOut, StoredRun } from "./run-queue.js";

const FINAL_ERROR: ErrorRecord = {
  attempt: 3,
  name: "Error",
  message: "ledger unavailable",
  stack: "Error: ledger unavailable\n    at handler",
  failedAt: "2026-11-03T03:00:00.000Z",
};

const FAILED_RUN: StoredRun = {
  runId: "run-7",
  payload: { accountId: "account-7", email: "ada@example.com" },
  errors: [{ ...FINAL_ERROR, attempt: 2, message: "timeout" }, FINAL_ERROR],
  reason: "retries exhausted",
  failureCount: 1,
  writeToken: "write-token",
};

const FAILED_FAN_OUT: StoredFanOut = {
  eventId: "event-7",
  payload: { invoiceId: "inv-7" },
  listenerNames: ["record-payment"],
  finalError: FINAL_ERROR,
  failureCount: 1,
  writeToken: "write-token",
};

describe("unsentFailureNoticeOf", () => {
  test("shapes a failed run's notice from its last error, without its payload, under a derived event id", () => {
    const notice = unsentFailureNoticeOf("sync-account", FAILED_RUN);

    expect(notice).toEqual({
      eventName: "nextasking/run.failed",
      eventId: deriveFailureNoticeEventId("sync-account", "run-7", 1),
      failureNumber: 1,
      payload: {
        task: "sync-account",
        runId: "run-7",
        reason: "retries exhausted",
        error: { name: "Error", message: "ledger unavailable", stack: "Error: ledger unavailable\n    at handler" },
        attempt: 3,
        failedAt: "2026-11-03T03:00:00.000Z",
      },
    });
  });

  test("adds the source event to a failed listener run's notice", () => {
    const notice = unsentFailureNoticeOf("record-payment", {
      ...FAILED_RUN,
      sourceEvent: { eventName: "invoice.paid", eventId: "event-7" },
    });

    expect(notice?.payload).toMatchObject({ eventName: "invoice.paid", eventId: "event-7" });
  });

  test("shapes a failed fan-out's notice from its final error", () => {
    const notice = unsentFailureNoticeOf("invoice.paid", FAILED_FAN_OUT);

    expect(notice).toEqual({
      eventName: "nextasking/fan-out.failed",
      eventId: deriveFailureNoticeEventId("invoice.paid", "event-7", 1),
      failureNumber: 1,
      payload: {
        eventName: "invoice.paid",
        eventId: "event-7",
        error: { name: "Error", message: "ledger unavailable", stack: "Error: ledger unavailable\n    at handler" },
        failedAt: "2026-11-03T03:00:00.000Z",
      },
    });
  });

  test("numbers a later failure's notice after the failures already counted", () => {
    const notice = unsentFailureNoticeOf("sync-account", { ...FAILED_RUN, failureCount: 2, notifiedFailureCount: 1 });

    expect(notice).toMatchObject({ eventId: deriveFailureNoticeEventId("sync-account", "run-7", 2), failureNumber: 2 });
  });

  test("sends nothing for a failure already notified", () => {
    expect(unsentFailureNoticeOf("sync-account", { ...FAILED_RUN, notifiedFailureCount: 1 })).toBeUndefined();
    expect(unsentFailureNoticeOf("invoice.paid", { ...FAILED_FAN_OUT, notifiedFailureCount: 1 })).toBeUndefined();
  });

  test("sends nothing for a failed listener run or a failed fan-out of a built-in event", () => {
    const listenerRunOfBuiltInEvent = {
      ...FAILED_RUN,
      sourceEvent: { eventName: "nextasking/run.failed", eventId: "notice-1" },
    };

    expect(unsentFailureNoticeOf("alert-on-call", listenerRunOfBuiltInEvent)).toBeUndefined();
    expect(unsentFailureNoticeOf("nextasking/fan-out.failed", FAILED_FAN_OUT)).toBeUndefined();
  });

  test("sends nothing for a failure no final failure was counted for, nor for a firing", () => {
    expect(unsentFailureNoticeOf("sync-account", { ...FAILED_RUN, failureCount: undefined })).toBeUndefined();
    expect(unsentFailureNoticeOf("invoice.paid", { ...FAILED_FAN_OUT, finalError: undefined })).toBeUndefined();
    expect(unsentFailureNoticeOf("nextasking/firing", { taskName: "purge-expired-sessions" })).toBeUndefined();
  });
});
