import { expect, vi } from "vitest";
import { z } from "zod";

import { createNextasking, defineEvent, defineListener, defineTask } from "../src/index.js";
import { closeNextaskingAndCheckItsDuration } from "./close-nextasking-and-check-its-duration.js";
import { integrationTest } from "./integration-test.js";
import { createRecordingLogger } from "./recording-logger.js";

const PAYLOAD_SENTINEL = "payload-sentinel-6f1c";

integrationTest(
  "a payload never reaches the logger, dispatched or emitted",
  { timeout: 15_000 },
  async ({ redisUrl, keyPrefix }) => {
    const { logger, lines } = createRecordingLogger();
    const sendWelcomeEmailTask = defineTask("send-welcome-email", {
      schema: z.object({ email: z.string(), profile: z.object({ nickname: z.string() }) }),
      handler: async (context) => {
        context.logger.info({ template: "welcome" }, "welcome email sent");
      },
    });
    const userSignedUpEvent = defineEvent("user.signed-up", {
      schema: z.object({ userId: z.string() }),
    });
    const userDeletedEvent = defineEvent("user.deleted", {
      schema: z.object({ userId: z.string() }),
    });
    const createDefaultWorkspaceTask = defineListener("create-default-workspace", {
      on: userSignedUpEvent,
      handler: async (context) => {
        context.logger.info({}, "workspace created");
      },
    });
    const nextasking = createNextasking({
      connection: { url: redisUrl },
      prefix: keyPrefix,
      logger,
      events: [userSignedUpEvent, userDeletedEvent],
      tasks: [sendWelcomeEmailTask, createDefaultWorkspaceTask],
    });

    try {
      await nextasking.startWorker();
      await nextasking.dispatch("send-welcome-email", {
        email: `${PAYLOAD_SENTINEL}@example.com`,
        profile: { nickname: PAYLOAD_SENTINEL },
      });
      await nextasking.emit("user.signed-up", { userId: PAYLOAD_SENTINEL });
      const { eventId: eventIdWithNoListener } = await nextasking.emit("user.deleted", { userId: PAYLOAD_SENTINEL });
      await vi.waitFor(
        () => {
          expect(lines.filter((line) => line.message === "run completed")).toHaveLength(2);
        },
        { timeout: 10_000 },
      );
      await closeNextaskingAndCheckItsDuration(nextasking);

      expect(lines.filter((line) => line.message === "no listeners for event")).toEqual([
        {
          level: "warn",
          fields: { eventName: "user.deleted", eventId: eventIdWithNoListener },
          message: "no listeners for event",
        },
      ]);
      for (const line of lines) {
        expect(JSON.stringify(line.fields)).not.toContain(PAYLOAD_SENTINEL);
        expect(line.message).not.toContain(PAYLOAD_SENTINEL);
      }
    } finally {
      await closeNextaskingAndCheckItsDuration(nextasking);
    }
  },
);
