import { expect } from "vitest";

import { TEST_TIMEOUT_PAST_ITS_WAITS_IN_MILLISECONDS, integrationTest } from "./integration-test.js";
import { WORKER_PROCESS_READY_TIMEOUT_IN_MILLISECONDS, startWorkerProcess } from "./worker-process.js";
import {
  DELAY_OF_THE_LINES_WRITTEN_AFTER_EXIT_IN_MILLISECONDS,
  EXIT_WITH_LINES_STILL_COMING_SIGNAL,
  LINE_COUNT_WRITTEN_AFTER_EXIT,
  LINE_COUNT_WRITTEN_BEFORE_EXIT,
  LINE_WRITTEN_AROUND_EXIT,
} from "./worker-process-config.js";

const LINE_COUNT_WRITTEN_AROUND_EXIT = LINE_COUNT_WRITTEN_BEFORE_EXIT + LINE_COUNT_WRITTEN_AFTER_EXIT;

integrationTest(
  "a worker process that exits while lines it wrote are still on their way has every one of them in its log lines and its output once its exit is awaited",
  {
    timeout:
      WORKER_PROCESS_READY_TIMEOUT_IN_MILLISECONDS +
      DELAY_OF_THE_LINES_WRITTEN_AFTER_EXIT_IN_MILLISECONDS +
      TEST_TIMEOUT_PAST_ITS_WAITS_IN_MILLISECONDS,
  },
  async ({ redisUrl, keyPrefix }) => {
    const workerProcess = await startWorkerProcess({ redisUrl, keyPrefix });

    try {
      workerProcess.kill(EXIT_WITH_LINES_STILL_COMING_SIGNAL);
      const exit = await workerProcess.exited;

      expect(exit).toEqual({ code: 0, signal: null });
      expect(
        workerProcess.logLines
          .filter((line) => line.message === LINE_WRITTEN_AROUND_EXIT)
          .map((line) => line.fields["position"]),
      ).toEqual(Array.from({ length: LINE_COUNT_WRITTEN_AROUND_EXIT }, (_, position) => position));
      expect(
        workerProcess
          .output()
          .split("\n")
          .filter((outputLine) => outputLine.includes(LINE_WRITTEN_AROUND_EXIT)),
      ).toHaveLength(LINE_COUNT_WRITTEN_AROUND_EXIT);
    } finally {
      await workerProcess.stop();
    }
  },
);
