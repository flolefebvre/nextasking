import { describe, expect, onTestFailed } from "vitest";

import type { ExpectInvariantViolation } from "./invariant-violations.js";

export type FailingTestContext = {
  redisUrl: string;
  keyPrefix: string;
  expectInvariantViolation: ExpectInvariantViolation;
};

type FailingTestOptions = { timeout?: number };

type FailingTestChain = {
  (name: string, fn: () => void): void;
  fails: (name: string, options: FailingTestOptions, fn: (context: FailingTestContext) => Promise<void>) => void;
};

type RecordedFailure = { failureMessages: string[]; expectedFailureMessages: string[] };

const recordedFailuresByTestName = new Map<string, RecordedFailure>();

export function testFailingWith(
  chain: FailingTestChain,
  name: string,
  expectedFailureMessagesOf: (context: FailingTestContext) => string[],
  body: (context: FailingTestContext) => Promise<void>,
  options: FailingTestOptions = {},
): void {
  describe(name, () => {
    chain.fails("fails", options, async ({ redisUrl, keyPrefix, expectInvariantViolation }) => {
      const context = { redisUrl, keyPrefix, expectInvariantViolation };
      const expectedFailureMessages = expectedFailureMessagesOf(context);
      onTestFailed(({ task }) => {
        recordedFailuresByTestName.set(name, {
          failureMessages: (task.result?.errors ?? []).map((error) => error.message),
          expectedFailureMessages,
        });
      });
      await body(context);
    });
    chain("failed with exactly the expected messages", () => {
      const recordedFailure = recordedFailuresByTestName.get(name);
      if (recordedFailure === undefined) {
        throw new Error(
          `The test "${name} > fails" did not run, or did not fail, before this one: run the whole describe block, not a test filtered out of it`,
        );
      }
      expect(recordedFailure.failureMessages).toEqual(recordedFailure.expectedFailureMessages);
    });
  });
}
