import { expect, vi } from "vitest";
import { z } from "zod";

import { engineKeysOf } from "../src/engine/engine-keys.js";
import { deriveListenerRunId } from "../src/ids.js";
import { createNextasking, defineEvent, defineListener, defineTask } from "../src/index.js";
import { serializeStepValue } from "../src/step-engine.js";
import type { EventId, Logger, RunId, TaskHandler } from "../src/index.js";
import { closeNextaskingAndCheckItsDuration } from "./close-nextasking-and-check-its-duration.js";
import { DAY_IN_MILLISECONDS } from "./ended-runs.js";
import { backdateEndedRun, engineTestContextOf } from "./engine-test.js";
import { createHandlerGate } from "./handler-gate.js";
import type { HandlerGate } from "./handler-gate.js";
import { integrationTest } from "./integration-test.js";
import { createRecordingLogger } from "./recording-logger.js";
import type { RecordedLogLine } from "./recording-logger.js";
import { savedSteps } from "./saved-steps.js";
import { serverInstantInMillisecondsOf } from "./server-instant.js";
import { storedRun } from "./stored-run.js";
import { withRedis } from "./with-redis.js";

const userSignedUp = defineEvent("user.signed-up", {
  schema: z.object({ userId: z.string() }),
});

function createSignUpNextasking(
  redisUrl: string,
  keyPrefix: string,
  logger: Logger,
  signUpHandler: TaskHandler<{ userId: string }>,
  workspaceRunIds: RunId[],
  { isListenerListed }: { isListenerListed: boolean } = { isListenerListed: true },
) {
  const signUpUser = defineTask("sign-up-user", {
    schema: z.object({ userId: z.string() }),
    retries: 1,
    backoff: "0s",
    handler: signUpHandler,
  });
  const createDefaultWorkspace = defineListener("create-default-workspace", {
    on: userSignedUp,
    handler: async ({ runId }) => {
      workspaceRunIds.push(runId);
    },
  });
  return createNextasking({
    connection: { url: redisUrl },
    prefix: keyPrefix,
    logger,
    events: [userSignedUp],
    tasks: isListenerListed ? [signUpUser, createDefaultWorkspace] : [signUpUser],
  });
}

async function waitForCompletedRuns(lines: readonly RecordedLogLine[], runCount: number): Promise<void> {
  await vi.waitFor(
    () => {
      expect(lines.filter((line) => line.message === "run completed")).toHaveLength(runCount);
    },
    { timeout: 10_000 },
  );
}

function emitThenThrowOnTheFirstAttempt(
  emittedEventIdsByAttempt: EventId[],
  afterTheEmitOfALaterAttempt: () => Promise<void> = async () => undefined,
): TaskHandler<{ userId: string }> {
  return async ({ payload, emit, attempt }) => {
    const { eventId } = await emit("user.signed-up", { userId: payload.userId });
    emittedEventIdsByAttempt.push(eventId);
    if (attempt === 1) {
      throw new Error("welcome banner service unavailable");
    }
    await afterTheEmitOfALaterAttempt();
  };
}

async function waitForTheHandlerAtTheGate(handlerGate: HandlerGate): Promise<void> {
  await vi.waitFor(
    () => {
      expect(handlerGate.inFlightCount()).toBe(1);
    },
    { timeout: 10_000 },
  );
}

function expectOneEmitAcrossTwoStretches(
  emittedEventIdsByStretch: readonly EventId[],
  workspaceRunIds: readonly RunId[],
  lines: readonly RecordedLogLine[],
): void {
  expect(emittedEventIdsByStretch).toHaveLength(2);
  expect(emittedEventIdsByStretch[1]).toBe(emittedEventIdsByStretch[0]);
  expect(workspaceRunIds).toHaveLength(1);
  expect(lines.map((line) => line.message)).not.toContain("duplicate dispatch dropped");
}

integrationTest(
  "a handler that emits then throws on its first attempt emits once, so the listener runs once (names from the Register augmentation of src/index.test.ts)",
  async ({ redisUrl, keyPrefix }) => {
    const { logger, lines } = createRecordingLogger();
    const emittedEventIdsByAttempt: EventId[] = [];
    const workspaceRunIds: RunId[] = [];
    const nextasking = createSignUpNextasking(
      redisUrl,
      keyPrefix,
      logger,
      emitThenThrowOnTheFirstAttempt(emittedEventIdsByAttempt),
      workspaceRunIds,
    );

    try {
      await nextasking.startWorker();
      await nextasking.dispatch("sign-up-user", { userId: "user-1" });
      await waitForCompletedRuns(lines, 2);

      expect(lines.filter((line) => line.message === "attempt started")).toHaveLength(3);
      expectOneEmitAcrossTwoStretches(emittedEventIdsByAttempt, workspaceRunIds, lines);
    } finally {
      await closeNextaskingAndCheckItsDuration(nextasking);
    }
  },
);

integrationTest(
  "a handler that emits before a sleep emits once across the resume (names from the Register augmentation of src/index.test.ts)",
  async ({ redisUrl, keyPrefix }) => {
    const { logger, lines } = createRecordingLogger();
    const emittedEventIdsByStretch: EventId[] = [];
    const workspaceRunIds: RunId[] = [];
    let wakeAt = new Date();
    const nextasking = createSignUpNextasking(
      redisUrl,
      keyPrefix,
      logger,
      async ({ payload, emit, step }) => {
        const { eventId } = await emit("user.signed-up", { userId: payload.userId });
        emittedEventIdsByStretch.push(eventId);
        await step.sleepUntil("wait-for-onboarding", wakeAt);
      },
      workspaceRunIds,
    );

    try {
      await nextasking.startWorker();
      wakeAt = new Date(Date.now() + 1000);
      await nextasking.dispatch("sign-up-user", { userId: "user-1" });
      await waitForCompletedRuns(lines, 2);

      expect(lines.map((line) => line.message)).toContain("run resumed");
      expectOneEmitAcrossTwoStretches(emittedEventIdsByStretch, workspaceRunIds, lines);
    } finally {
      await closeNextaskingAndCheckItsDuration(nextasking);
    }
  },
);

integrationTest(
  "a handler that emits an event with no listener then throws on its first attempt completes on its second, warning once (names from the Register augmentation of src/index.test.ts)",
  { timeout: 30_000 },
  async ({ redisUrl, keyPrefix }) => {
    const { logger, lines } = createRecordingLogger();
    const emittedEventIdsByAttempt: EventId[] = [];
    const handlerGate = createHandlerGate();
    const nextasking = createSignUpNextasking(
      redisUrl,
      keyPrefix,
      logger,
      emitThenThrowOnTheFirstAttempt(emittedEventIdsByAttempt, handlerGate.enter),
      [],
      { isListenerListed: false },
    );

    try {
      await nextasking.startWorker();
      const { runId } = await nextasking.dispatch("sign-up-user", { userId: "user-1" });
      await waitForTheHandlerAtTheGate(handlerGate);
      const stepsOfTheSecondAttempt = await savedSteps(redisUrl, keyPrefix, "sign-up-user", runId);
      handlerGate.release(1);
      await waitForCompletedRuns(lines, 1);

      expect(emittedEventIdsByAttempt).toHaveLength(2);
      expect(emittedEventIdsByAttempt[1]).toBe(emittedEventIdsByAttempt[0]);
      expect(Object.values(stepsOfTheSecondAttempt.serializedResultsByStepId)).toEqual([
        serializeStepValue(emittedEventIdsByAttempt[0]!),
      ]);
      expect(lines.filter((line) => line.message === "no listeners for event")).toEqual([
        {
          level: "warn",
          fields: { eventName: "user.signed-up", eventId: emittedEventIdsByAttempt[0] },
          message: "no listeners for event",
        },
      ]);
      expect(lines.map((line) => line.message)).not.toContain("run failed");
    } finally {
      await closeNextaskingAndCheckItsDuration(nextasking);
    }
  },
);

integrationTest(
  "a handler that emits an event id already emitted, then sleeps past the event's window, starts no listener run on its resume (names from the Register augmentation of src/index.test.ts)",
  { timeout: 50_000 },
  async ({ redisUrl, keyPrefix }) => {
    const { logger, lines } = createRecordingLogger();
    const workspaceRunIds: RunId[] = [];
    const handlerGate = createHandlerGate();
    const eventId = "sign-up-of-user-1";
    const workspaceRunId = deriveListenerRunId(eventId, "create-default-workspace");
    let isFirstStretch = true;
    const nextasking = createSignUpNextasking(
      redisUrl,
      keyPrefix,
      logger,
      async ({ payload, emit, step }) => {
        await emit("user.signed-up", { userId: payload.userId }, { eventId });
        if (isFirstStretch) {
          isFirstStretch = false;
          await handlerGate.enter();
        }
        await step.sleepUntil("wait-for-onboarding", new Date(Date.now() + 500));
      },
      workspaceRunIds,
    );
    const keys = engineKeysOf(keyPrefix);

    try {
      await nextasking.startWorker();
      await nextasking.emit("user.signed-up", { userId: "user-1" }, { eventId });
      await vi.waitFor(
        async () => {
          expect(await storedRun(redisUrl, keyPrefix, "create-default-workspace", workspaceRunId)).toMatchObject({
            state: "completed",
          });
        },
        { timeout: 10_000 },
      );
      const { runId } = await nextasking.dispatch("sign-up-user", { userId: "user-1" });
      await waitForTheHandlerAtTheGate(handlerGate);
      await withRedis(redisUrl, async (redis, client) => {
        await redis.zAdd(
          keys.eventDedup("user.signed-up"),
          { score: (await serverInstantInMillisecondsOf(redis)) - 1, value: eventId },
          { condition: "XX" },
        );
        await backdateEndedRun(
          engineTestContextOf(redis, client, keyPrefix),
          "create-default-workspace",
          workspaceRunId,
          2 * DAY_IN_MILLISECONDS,
        );
      });
      await vi.waitFor(
        async () => {
          expect(await storedRun(redisUrl, keyPrefix, "create-default-workspace", workspaceRunId)).toBeUndefined();
        },
        { timeout: 10_000 },
      );

      handlerGate.release(1);
      await vi.waitFor(
        async () => {
          expect(await storedRun(redisUrl, keyPrefix, "sign-up-user", runId)).toMatchObject({ state: "completed" });
        },
        { timeout: 10_000 },
      );

      expect(lines.map((line) => line.message)).toContain("run resumed");
      expect(await storedRun(redisUrl, keyPrefix, "create-default-workspace", workspaceRunId)).toBeUndefined();
      expect(workspaceRunIds).toEqual([workspaceRunId]);
      expect(lines.filter((line) => line.message === "duplicate dispatch dropped")).toEqual([
        {
          level: "debug",
          fields: { eventName: "user.signed-up", eventId },
          message: "duplicate dispatch dropped",
        },
      ]);
    } finally {
      await closeNextaskingAndCheckItsDuration(nextasking);
    }
  },
);

integrationTest(
  "a handler dispatch with its own runId starts that run once across retries (names from the Register augmentation of src/index.test.ts)",
  async ({ redisUrl, keyPrefix }) => {
    const { logger, lines } = createRecordingLogger();
    const dispatchedRunIdsByAttempt: RunId[] = [];
    const welcomeEmailRunIds: RunId[] = [];
    const inviteUser = defineTask("invite-user", {
      schema: z.object({ email: z.email() }),
      retries: 1,
      backoff: "0s",
      handler: async ({ payload, dispatch, attempt }) => {
        const { runId } = await dispatch(
          "send-welcome-email",
          { email: payload.email },
          { runId: `welcome-${payload.email}` },
        );
        dispatchedRunIdsByAttempt.push(runId);
        if (attempt === 1) {
          throw new Error("invitation store unavailable");
        }
      },
    });
    const sendWelcomeEmail = defineTask("send-welcome-email", {
      schema: z.object({ email: z.email() }),
      handler: async ({ runId }) => {
        welcomeEmailRunIds.push(runId);
      },
    });
    const nextasking = createNextasking({
      connection: { url: redisUrl },
      prefix: keyPrefix,
      logger,
      tasks: [inviteUser, sendWelcomeEmail],
    });

    try {
      await nextasking.startWorker();
      await nextasking.dispatch("invite-user", { email: "ada@example.com" });
      await waitForCompletedRuns(lines, 2);

      expect(dispatchedRunIdsByAttempt).toEqual(["welcome-ada@example.com", "welcome-ada@example.com"]);
      expect(welcomeEmailRunIds).toEqual(["welcome-ada@example.com"]);
      expect(lines.map((line) => line.message)).not.toContain("duplicate dispatch dropped");
    } finally {
      await closeNextaskingAndCheckItsDuration(nextasking);
    }
  },
);
