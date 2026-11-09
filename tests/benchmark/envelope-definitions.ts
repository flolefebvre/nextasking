import { setTimeout as sleep } from "node:timers/promises";
import { z } from "zod";

import { defineEvent, defineListener, defineScheduledTask, defineTask } from "../../src/index.js";
import type { RunId } from "../../src/index.js";
import type { WriteRecord } from "../worker-process-tasks.js";

export const HANDLED_RUNS_RECORD_NAME = "envelope-load-handled";

export type HandledRunRecord = { task: string; runId: RunId };

const PLAIN_TASK_COUNT = 48;

const PLAIN_TASK_WORK_IN_MILLISECONDS = 20;

export const LOAD_EVENT_NAME = "load.arrived";

export const SCHEDULED_TASK_NAME = "yearly-report";

export function envelopeDefinitionsOf(writeRecord: WriteRecord) {
  const recordHandled = (task: string, runId: RunId) =>
    writeRecord(HANDLED_RUNS_RECORD_NAME, { task, runId } satisfies HandledRunRecord);
  const plainTasks = Array.from({ length: PLAIN_TASK_COUNT }, (_, position) => {
    const task = `task-${String(position + 1).padStart(2, "0")}`;
    return defineTask(task, {
      schema: z.object({}),
      handler: async ({ runId }) => {
        await sleep(PLAIN_TASK_WORK_IN_MILLISECONDS);
        await recordHandled(task, runId);
      },
    });
  });
  const loadArrived = defineEvent(LOAD_EVENT_NAME, { schema: z.object({}) });
  const yearlyReport = defineScheduledTask(SCHEDULED_TASK_NAME, {
    schedule: { cron: "0 0 29 2 *" },
    handler: ({ runId }) => recordHandled(SCHEDULED_TASK_NAME, runId),
  });
  const recordLoadEvent = defineListener("record-load-event", {
    on: loadArrived,
    handler: ({ runId }) => recordHandled("record-load-event", runId),
  });
  return {
    plainTaskNames: plainTasks.map(({ name }) => name),
    tasks: [...plainTasks, yearlyReport, recordLoadEvent],
    events: [loadArrived],
  };
}
