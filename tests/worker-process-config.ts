import type { ConnectionConfig, StartWorkerOptions } from "../src/index.js";
import type { WorkerTiming } from "../src/worker-timing.js";

export type WorkerProcessConfig = {
  redisUrl: string;
  keyPrefix: string;
  timing?: Partial<WorkerTiming>;
  connection?: ConnectionConfig;
  startWorkerOptions?: StartWorkerOptions;
  isFailureNoticeRecorded?: boolean;
  definitions?: WorkerProcessDefinitions;
};

export type WorkerProcessDefinitions = "fixture tasks" | "envelope load";

export const FAILURE_NOTICE_RECORDING_LISTENER_NAME = "record-failure-notice";

export const WORKER_PROCESS_CONFIG_VARIABLE = "NEXTASKING_WORKER_PROCESS_CONFIG";

export const WORKER_PROCESS_READY = "worker process ready";

export const CLOSE_WHILE_BOOTING_URL_VARIABLE = "NEXTASKING_CLOSE_WHILE_BOOTING_URL";
