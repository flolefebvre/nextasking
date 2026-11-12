import { spawn } from "node:child_process";

import { createNextasking, defineListener, runFailedEvent } from "../src/index.js";
import { envelopeDefinitionsOf } from "./benchmark/envelope-definitions.js";
import type { Logger } from "../src/index.js";
import { overrideWorkerTiming } from "../src/worker-timing.js";
import { fixtureRecordsKey } from "./fixture-records.js";
import { UNHANDLED_REJECTION_MARKER } from "./process-output-violations.js";
import type { LogLevel } from "./recording-logger.js";
import { connectedTestClientOf } from "./with-redis.js";
import {
  DELAY_OF_THE_LINES_WRITTEN_AFTER_EXIT_IN_MILLISECONDS,
  EXIT_WITH_LINES_STILL_COMING_SIGNAL,
  FAILURE_NOTICE_RECORDING_LISTENER_NAME,
  LINE_COUNT_WRITTEN_AFTER_EXIT,
  LINE_COUNT_WRITTEN_BEFORE_EXIT,
  LINE_WRITTEN_AROUND_EXIT,
  ON_WORKER_STOPPED_CALLED,
  ON_WORKER_STOPPED_SETTLED,
  WORKER_PROCESS_CONFIG_VARIABLE,
  WORKER_PROCESS_READY,
} from "./worker-process-config.js";
import type { WorkerProcessConfig } from "./worker-process-config.js";
import { workerProcessTasks } from "./worker-process-tasks.js";
import type { WriteRecord } from "./worker-process-tasks.js";

const config = JSON.parse(process.env[WORKER_PROCESS_CONFIG_VARIABLE] ?? "{}") as WorkerProcessConfig;

function errorsAsPlainObjects(_key: string, value: unknown): unknown {
  return value instanceof Error ? { name: value.name, message: value.message } : value;
}

const lineWriterAt =
  (level: LogLevel): Logger[LogLevel] =>
  (fields, message) => {
    process.stdout.write(`${JSON.stringify({ level, fields, message }, errorsAsPlainObjects)}\n`);
  };

const ndjsonLogger: Logger = {
  debug: lineWriterAt("debug"),
  info: lineWriterAt("info"),
  warn: lineWriterAt("warn"),
  error: lineWriterAt("error"),
};

process.on("unhandledRejection", (reason) => {
  const shownReason = reason instanceof Error ? (reason.stack ?? reason.message) : String(reason);
  process.stderr.write(`${UNHANDLED_REJECTION_MARKER}${shownReason}\n`);
});

const connectedRecordsClient = connectedTestClientOf(config.redisUrl);

overrideWorkerTiming(config.timing ?? {});

const writeRecord: WriteRecord = async (taskName, record) => {
  await (await connectedRecordsClient).rPush(fixtureRecordsKey(config.keyPrefix, taskName), JSON.stringify(record));
};

const failureNoticeRecordingListeners =
  config.isFailureNoticeRecorded === true
    ? [
        defineListener(FAILURE_NOTICE_RECORDING_LISTENER_NAME, {
          on: runFailedEvent,
          handler: async ({ payload: { task, runId, reason } }) => {
            await writeRecord(FAILURE_NOTICE_RECORDING_LISTENER_NAME, { task, runId, reason });
          },
        }),
      ]
    : [];

const commonConfig = {
  connection: config.connection ?? { url: config.redisUrl },
  prefix: config.keyPrefix,
  logger: ndjsonLogger,
};

function createEnvelopeLoadNextasking() {
  const { tasks, events } = envelopeDefinitionsOf(writeRecord);
  return createNextasking({ ...commonConfig, tasks, events });
}

const nextasking =
  config.definitions === "envelope load"
    ? createEnvelopeLoadNextasking()
    : createNextasking({
        ...commonConfig,
        tasks: [...workerProcessTasks(writeRecord), ...failureNoticeRecordingListeners],
      });

function lineWrittenAroundExitAt(position: number): string {
  return `${JSON.stringify({ level: "info", fields: { position }, message: LINE_WRITTEN_AROUND_EXIT })}\n`;
}

const linesWrittenAfterExitSource = `setTimeout(() => {
  for (let position = ${String(LINE_COUNT_WRITTEN_BEFORE_EXIT)}; position < ${String(LINE_COUNT_WRITTEN_BEFORE_EXIT + LINE_COUNT_WRITTEN_AFTER_EXIT)}; position++) {
    process.stdout.write(JSON.stringify({ level: "info", fields: { position }, message: ${JSON.stringify(LINE_WRITTEN_AROUND_EXIT)} }) + "\\n");
  }
}, ${String(DELAY_OF_THE_LINES_WRITTEN_AFTER_EXIT_IN_MILLISECONDS)});`;

process.on(EXIT_WITH_LINES_STILL_COMING_SIGNAL, () => {
  for (let position = 0; position < LINE_COUNT_WRITTEN_BEFORE_EXIT - 1; position++) {
    process.stdout.write(lineWrittenAroundExitAt(position));
  }
  process.stdout.write(lineWrittenAroundExitAt(LINE_COUNT_WRITTEN_BEFORE_EXIT - 1), () => {
    spawn(process.execPath, ["-e", linesWrittenAfterExitSource], { stdio: ["ignore", "inherit", "inherit"] });
    process.exit(0);
  });
});

const onWorkerStoppedDelayInMilliseconds = config.onWorkerStoppedDelayInMilliseconds;

const onWorkerStoppedOption =
  onWorkerStoppedDelayInMilliseconds === undefined
    ? {}
    : {
        onWorkerStopped: async () => {
          ndjsonLogger.info({}, ON_WORKER_STOPPED_CALLED);
          await new Promise((resolve) => setTimeout(resolve, onWorkerStoppedDelayInMilliseconds));
          ndjsonLogger.info({}, ON_WORKER_STOPPED_SETTLED);
        },
      };

await nextasking.startWorker({ ...config.startWorkerOptions, ...onWorkerStoppedOption });
process.send?.(WORKER_PROCESS_READY);
