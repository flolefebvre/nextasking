import { Redis } from "ioredis";

import { createNextasking } from "../src/index.js";
import type { Logger } from "../src/index.js";
import { overrideWorkerTiming } from "../src/worker-timing.js";
import { fixtureRecordsKey } from "./fixture-records.js";
import { UNHANDLED_REJECTION_MARKER } from "./process-output-violations.js";
import type { LogLevel } from "./recording-logger.js";
import { WORKER_PROCESS_CONFIG_VARIABLE, WORKER_PROCESS_READY } from "./worker-process-config.js";
import type { WorkerProcessConfig } from "./worker-process-config.js";
import { workerProcessTasks } from "./worker-process-tasks.js";

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

const recordsClient = new Redis(config.redisUrl);
recordsClient.on("error", () => undefined);

overrideWorkerTiming(config.timing ?? {});

const nextasking = createNextasking({
  connection: { url: config.redisUrl },
  prefix: config.keyPrefix,
  logger: ndjsonLogger,
  tasks: workerProcessTasks(async (taskName, record) => {
    await recordsClient.rpush(fixtureRecordsKey(config.keyPrefix, taskName), JSON.stringify(record));
  }),
});

await nextasking.startWorker(config.startWorkerOptions);
process.send?.(WORKER_PROCESS_READY);
