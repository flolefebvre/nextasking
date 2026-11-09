import { expect, vi } from "vitest";
import { z } from "zod";

import { createNextasking, defineEvent, defineListener, defineTask } from "../src/index.js";
import type { EventId, Logger, RunId, TaskHandler } from "../src/index.js";
import { closeNextaskingAndCheckItsDuration } from "./close-nextasking-and-check-its-duration.js";
import { integrationTest } from "./integration-test.js";
import { createRecordingLogger } from "./recording-logger.js";
import type { RecordedLogLine } from "./recording-logger.js";

const userSignedUp = defineEvent("user.signed-up", {
  schema: z.object({ userId: z.string() }),
});

function createSignUpNextasking(
  redisUrl: string,
  keyPrefix: string,
  logger: Logger,
  signUpHandler: TaskHandler<{ userId: string }>,
  workspaceRunIds: RunId[],
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
    tasks: [signUpUser, createDefaultWorkspace],
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
      async ({ payload, emit, attempt }) => {
        const { eventId } = await emit("user.signed-up", { userId: payload.userId });
        emittedEventIdsByAttempt.push(eventId);
        if (attempt === 1) {
          throw new Error("welcome banner service unavailable");
        }
      },
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
