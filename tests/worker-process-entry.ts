import { createNextasking, defineListener, runFailed } from "../src/index.js";
import type { Logger } from "../src/index.js";
import { overrideWorkerTiming } from "../src/worker-timing.js";
import { fixtureRecordsKey } from "./fixture-records.js";
import { UNHANDLED_REJECTION_MARKER } from "./process-output-violations.js";
import type { LogLevel } from "./recording-logger.js";
import { connectedTestClientOf } from "./with-redis.js";
import {
  FAILURE_NOTICE_RECORDING_LISTENER_NAME,
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
          on: runFailed,
          handler: async ({ payload: { task, runId, reason } }) => {
            await writeRecord(FAILURE_NOTICE_RECORDING_LISTENER_NAME, { task, runId, reason });
          },
        }),
      ]
    : [];

const nextasking = createNextasking({
  connection: config.connection ?? { url: config.redisUrl },
  prefix: config.keyPrefix,
  logger: ndjsonLogger,
  tasks: [...workerProcessTasks(writeRecord), ...failureNoticeRecordingListeners],
});

await nextasking.startWorker(config.startWorkerOptions);
process.send?.(WORKER_PROCESS_READY);
