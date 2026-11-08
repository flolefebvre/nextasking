import { expect } from "vitest";

import { engineTest, fetchedLockedRunsOf, storedValuesByKeyOf } from "../../tests/engine-test.js";

engineTest(
  "a read with the current token returns the saved value, or none for a step never saved, and writes nothing",
  async (context) => {
    const { engine, redis, keyPrefix } = context;
    await engine.enqueue({ runs: [{ runId: "run-1", task: "send-email", payload: "{}", retries: 0 }] });
    const [lockedRun] = await fetchedLockedRunsOf(context, 1);
    await engine.saveStep({
      ...lockedRun!,
      stepId: "charge-card",
      step: { kind: "value", value: '{"chargeId":"ch-1"}' },
    });
    const storedValuesBefore = await storedValuesByKeyOf(redis, keyPrefix);

    const savedReply = await engine.readStep({ ...lockedRun!, stepId: "charge-card" });
    const unsavedReply = await engine.readStep({ ...lockedRun!, stepId: "send-receipt" });

    expect(savedReply).toEqual({ kind: "read", savedValue: '{"chargeId":"ch-1"}' });
    expect(unsavedReply).toEqual({ kind: "read", savedValue: undefined });
    expect(await storedValuesByKeyOf(redis, keyPrefix)).toEqual(storedValuesBefore);
  },
);
