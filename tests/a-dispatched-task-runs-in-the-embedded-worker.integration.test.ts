import { expect, vi } from "vitest";
import { z } from "zod";

import { createNextasking, defineTask } from "../src/index.js";
import type { TaskContext } from "../src/index.js";
import { closeNextasking } from "../src/registry.js";
import { integrationTest } from "./integration-test.js";

const RANDOM_UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;

integrationTest(
  "a dispatched task runs in the embedded worker with the schema's output",
  async ({ redisUrl, keyPrefix }) => {
    const recordedContexts: TaskContext<{ email: string }>[] = [];
    const sendWelcomeEmail = defineTask("send-welcome-email", {
      schema: z.object({ email: z.string().trim().toLowerCase() }),
      handler: async (context) => {
        recordedContexts.push(context);
      },
    });
    const nextasking = createNextasking({
      connection: { url: redisUrl },
      prefix: keyPrefix,
      tasks: [sendWelcomeEmail],
    });

    try {
      await nextasking.startWorker();

      const { runId } = await nextasking.dispatch("send-welcome-email", { email: "  Ada@Example.com " });

      expect(runId).toMatch(RANDOM_UUID);
      await vi.waitFor(
        () => {
          expect(recordedContexts).toEqual([
            {
              payload: { email: "ada@example.com" },
              step: { run: expect.any(Function), sleep: expect.any(Function), sleepUntil: expect.any(Function) },
              emit: expect.any(Function),
              dispatch: expect.any(Function),
              runId,
              attempt: 1,
              logger: expect.any(Object),
            },
          ]);
        },
        { timeout: 10_000 },
      );
    } finally {
      await closeNextasking(nextasking);
    }
  },
);
