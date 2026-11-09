import { expect, vi } from "vitest";
import { z } from "zod";

import { createNextasking, defineTask } from "../src/index.js";
import { closeNextaskingAndCheckItsDuration } from "./close-nextasking-and-check-its-duration.js";
import { createHandlerGate } from "./handler-gate.js";
import { integrationTest } from "./integration-test.js";
import { PRIMARY_AND_REPLICA_STARTUP_BOUND_IN_MILLISECONDS, startPrimaryAndReplica } from "./primary-and-replica.js";
import { createRecordingLogger, waitForLogMessage } from "./recording-logger.js";
import { savedSteps } from "./saved-steps.js";
import { storedRun } from "./stored-run.js";
import { providedRedisUrl } from "./valkey-container.js";
import { withRedis } from "./with-redis.js";

type StepOutcome = { execution: number; result: string } | { execution: number; error: unknown };

const REPLICA_ACKNOWLEDGEMENT_TIMEOUT_IN_MILLISECONDS = 5_000;

const HANDLER_ENTRY_TIMEOUT_IN_MILLISECONDS = 10_000;

const STEP_OUTCOME_TIMEOUT_IN_MILLISECONDS = 5_000;

const RUN_COMPLETION_TIMEOUT_IN_MILLISECONDS = 10_000;

integrationTest.skipIf(providedRedisUrl() !== undefined)(
  "a step saved by an attempt whose lock a lossy promotion lost is refused by the lock token, and the run fetched again on the promoted replica completes once",
  {
    timeout:
      PRIMARY_AND_REPLICA_STARTUP_BOUND_IN_MILLISECONDS +
      REPLICA_ACKNOWLEDGEMENT_TIMEOUT_IN_MILLISECONDS +
      2 * HANDLER_ENTRY_TIMEOUT_IN_MILLISECONDS +
      STEP_OUTCOME_TIMEOUT_IN_MILLISECONDS +
      RUN_COMPLETION_TIMEOUT_IN_MILLISECONDS +
      5_000,
  },
  async ({ keyPrefix }) => {
    const primaryAndReplica = await startPrimaryAndReplica();
    const { logger, lines } = createRecordingLogger();
    const handlerGate = createHandlerGate();
    const stepOutcomes: StepOutcome[] = [];
    let handlerExecutionCount = 0;
    const sendNewsletter = defineTask("send-newsletter", {
      schema: z.object({ issueId: z.string() }),
      handler: async ({ step }) => {
        handlerExecutionCount += 1;
        const execution = handlerExecutionCount;
        await step
          .run("send", async () => {
            await handlerGate.enter();
            return `sent by execution ${String(execution)}`;
          })
          .then(
            (result) => {
              stepOutcomes.push({ execution, result });
            },
            (error: unknown) => {
              stepOutcomes.push({ execution, error });
              throw error;
            },
          );
      },
    });
    const nextasking = createNextasking({
      connection: { url: primaryAndReplica.proxy.url },
      prefix: keyPrefix,
      logger,
      tasks: [sendNewsletter],
    });

    try {
      const { runId } = await nextasking.dispatch("send-newsletter", { issueId: "issue-7" });
      const replicaCountHoldingTheDispatch = await withRedis(primaryAndReplica.primaryUrl, (primary) =>
        primary.sendCommand(["WAIT", "1", String(REPLICA_ACKNOWLEDGEMENT_TIMEOUT_IN_MILLISECONDS)]),
      );
      await primaryAndReplica.cutReplication();
      await nextasking.startWorker();
      await vi.waitFor(() => expect(handlerGate.inFlightCount()).toBe(1), {
        timeout: HANDLER_ENTRY_TIMEOUT_IN_MILLISECONDS,
      });
      const runOnThePrimary = await storedRun(primaryAndReplica.primaryUrl, keyPrefix, "send-newsletter", runId);
      const runOnTheReplica = await storedRun(primaryAndReplica.replicaUrl, keyPrefix, "send-newsletter", runId);
      await primaryAndReplica.promoteReplica();
      await vi.waitFor(() => expect(handlerGate.inFlightCount()).toBe(2), {
        timeout: HANDLER_ENTRY_TIMEOUT_IN_MILLISECONDS,
      });
      const runFetchedAgain = await storedRun(primaryAndReplica.replicaUrl, keyPrefix, "send-newsletter", runId);
      handlerGate.release(1);
      await vi.waitFor(() => expect(stepOutcomes).toHaveLength(1), { timeout: STEP_OUTCOME_TIMEOUT_IN_MILLISECONDS });
      const stepsOnceTheStaleAttemptSaved = await savedSteps(
        primaryAndReplica.replicaUrl,
        keyPrefix,
        "send-newsletter",
        runId,
      );
      handlerGate.release(1);
      await waitForLogMessage(lines, "run completed", { timeout: RUN_COMPLETION_TIMEOUT_IN_MILLISECONDS });
      const runAtTheEnd = await storedRun(primaryAndReplica.replicaUrl, keyPrefix, "send-newsletter", runId);

      expect({
        staleAttemptStepOutcome: stepOutcomes[0],
        savedStepsOnThePromotedReplica: stepsOnceTheStaleAttemptSaved.serializedResultsByStepId,
      }).toEqual({
        staleAttemptStepOutcome: {
          execution: 1,
          error: expect.objectContaining({
            name: "LostRunLockError",
            message: expect.stringMatching(new RegExp(`^Step "send" of run "${runId}" was not saved: `)) as unknown,
          }) as unknown,
        },
        savedStepsOnThePromotedReplica: {},
      });
      expect(replicaCountHoldingTheDispatch).toBe(1);
      expect(runOnThePrimary).toMatchObject({ state: "executing" });
      expect(runOnTheReplica).toMatchObject({ state: "pending" });
      expect(runOnTheReplica).not.toHaveProperty("lockToken");
      expect(runFetchedAgain).toMatchObject({ state: "executing" });
      expect(runFetchedAgain?.lockToken).not.toBe(runOnThePrimary?.lockToken);
      expect(stepOutcomes[1]).toEqual({ execution: 2, result: "sent by execution 2" });
      expect(runAtTheEnd).toMatchObject({ state: "completed" });
      expect(lines.filter(({ message }) => message === "run completed")).toEqual([
        expect.objectContaining({ fields: expect.objectContaining({ task: "send-newsletter", runId }) as unknown }),
      ]);
      expect(lines).toContainEqual({
        level: "debug",
        message: "attempt aborted: lock lost",
        fields: { task: "send-newsletter", runId, attempt: 1 },
      });
      expect(lines.map(({ message }) => message)).not.toContain("run failed");
    } finally {
      handlerGate.release(2);
      await closeNextaskingAndCheckItsDuration(nextasking);
      await primaryAndReplica.stop();
    }
  },
);
