import { expect, vi } from "vitest";
import { z } from "zod";

import { createNextasking, defineTask } from "../src/index.js";
import { closeNextasking } from "../src/registry.js";
import { integrationTest } from "./integration-test.js";
import { savedSteps } from "./saved-steps.js";
import type { SavedSteps } from "./saved-steps.js";

integrationTest(
  "a run's saved steps live in a hash next to its job while it runs, and the hash is removed when it completes",
  async ({ redisUrl, keyPrefix }) => {
    const savedStepsSeenByHandler: SavedSteps[] = [];
    const exportReport = defineTask("export-report", {
      schema: z.object({ reportId: z.string() }),
      handler: async ({ step, runId }) => {
        await step.run("render", () => ({ pages: 3 }));
        await step.run("upload", async () => {});
        savedStepsSeenByHandler.push(await savedSteps(redisUrl, keyPrefix, "export-report", runId));
      },
    });
    const nextasking = createNextasking({
      connection: { url: redisUrl },
      prefix: keyPrefix,
      tasks: [exportReport],
    });

    try {
      await nextasking.startWorker();
      const { runId } = await nextasking.dispatch("export-report", { reportId: "report-7" });
      await vi.waitFor(
        async () => {
          expect(savedStepsSeenByHandler).toHaveLength(1);
          expect(await savedSteps(redisUrl, keyPrefix, "export-report", runId)).toEqual({
            serializedResultsByStepId: {},
            timeToLiveInMilliseconds: -2,
          });
        },
        { timeout: 10_000 },
      );

      expect(savedStepsSeenByHandler[0]?.serializedResultsByStepId).toEqual({
        render: '[{"pages":3}]',
        upload: "[]",
      });
    } finally {
      await closeNextasking(nextasking);
    }
  },
);
