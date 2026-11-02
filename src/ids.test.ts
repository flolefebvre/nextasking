import { describe, expect, test } from "vitest";

import { createRandomId, deriveJobId, deriveListenerRunId, nameBasedUuid } from "./ids.js";

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

describe("deriveJobId", () => {
  test("derives the same version 5 UUID from the same definition name and id", () => {
    expect(deriveJobId("send-welcome-email", "run-1")).toBe("dbb61551-a48e-5ff9-8dd1-68fc445a08ad");
  });

  test("keeps the definition name and the id apart, whatever characters they hold", () => {
    expect(deriveJobId("send-welcome", "email:run-1")).toBe("f169c714-cc3b-5c64-9ab2-77a43591ea43");
  });

  test("derives a job id that is not the listener run id of the same parts", () => {
    expect(deriveJobId("evt-1", "record-invoice-payment")).not.toBe(
      deriveListenerRunId("evt-1", "record-invoice-payment"),
    );
  });
});
