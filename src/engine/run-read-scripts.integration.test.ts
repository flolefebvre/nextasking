import { expect } from "vitest";

import { engineTest, fetchedLockedRunsOf } from "../../tests/engine-test.js";
import type { EngineTestContext } from "../../tests/engine-test.js";
import type { RunIdentity } from "./engine-keys.js";
import type { SettleOutcome } from "./engine.js";
import { listedRunReadCommandOf, runReadCommandOf } from "./run-read-scripts.js";

const RECORD_FIELDS = ["task", "finishedAt", "state", "attempt"] as const;

const RUN_IDENTITY: RunIdentity = { task: "send-email", runId: "run-1" };

const RETRY_AT_ONCE: SettleOutcome = {
  kind: "retry",
  error: { attempt: 1, name: "Error", message: "boom", stack: "Error: boom" },
  backoffInMilliseconds: 0,
};

async function runExecutingItsThirdAttemptAfterTwoErrorsAndASavedStep(context: EngineTestContext): Promise<void> {
  const { engine } = context;
  await engine.enqueue({ runs: [{ ...RUN_IDENTITY, payload: '{"to":"ada"}', retries: 2 }] });
  for (let attempt = 1; attempt <= 2; attempt += 1) {
    const [lockedRun] = await fetchedLockedRunsOf(context, 1);
    if (attempt === 1) {
      await engine.saveStep({ ...lockedRun!, stepId: "charge", step: { kind: "value", value: '["paid"]' } });
    }
    await engine.settle({ ...lockedRun!, outcome: RETRY_AT_ONCE });
    await engine.promote();
  }
  await fetchedLockedRunsOf(context, 1);
}

async function storedErrorsOf({ redis, keys }: EngineTestContext): Promise<string[]> {
  return redis.lRange(keys.runErrors(RUN_IDENTITY.task, RUN_IDENTITY.runId), 0, -1);
}

async function sentRunRead({ client, keys }: EngineTestContext): Promise<unknown> {
  return client.send(runReadCommandOf(keys, RUN_IDENTITY, RECORD_FIELDS));
}

async function sentListedRunRead({ client, keys }: EngineTestContext): Promise<unknown> {
  return client.send(listedRunReadCommandOf(keys, RUN_IDENTITY, RECORD_FIELDS));
}

engineTest(
  "the run read returns the record fields, one missing in the middle as null, the payload, every error and the steps",
  async (context) => {
    await runExecutingItsThirdAttemptAfterTwoErrorsAndASavedStep(context);
    const storedErrors = await storedErrorsOf(context);

    const reply = await sentRunRead(context);

    expect(storedErrors).toHaveLength(2);
    expect(reply).toEqual([
      ["send-email", null, "executing", "3"],
      '{"to":"ada"}',
      storedErrors,
      ["charge", '["paid"]'],
    ]);
  },
);

engineTest(
  "the run read of a run never stored returns null fields, no payload, no errors and no steps",
  async (context) => {
    const reply = await sentRunRead(context);

    expect(reply).toEqual([[null, null, null, null], null, [], []]);
  },
);

engineTest(
  "the listed run read returns the record fields, one missing in the middle as null, and the last error alone",
  async (context) => {
    await runExecutingItsThirdAttemptAfterTwoErrorsAndASavedStep(context);
    const storedErrors = await storedErrorsOf(context);

    const reply = await sentListedRunRead(context);

    expect(reply).toEqual([["send-email", null, "executing", "3"], storedErrors.at(-1)]);
  },
);

engineTest("the listed run read of a run without errors returns null as its last error", async (context) => {
  await context.engine.enqueue({ runs: [{ ...RUN_IDENTITY, payload: "", retries: 0 }] });

  const reply = await sentListedRunRead(context);

  expect(reply).toEqual([["send-email", null, "pending", "1"], null]);
});
