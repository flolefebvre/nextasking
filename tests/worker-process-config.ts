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
  onWorkerStoppedDelayInMilliseconds?: number;
};

export type WorkerProcessDefinitions = "fixture tasks" | "envelope load";

export const FAILURE_NOTICE_RECORDING_LISTENER_NAME = "record-failure-notice";

export const WORKER_PROCESS_CONFIG_VARIABLE = "NEXTASKING_WORKER_PROCESS_CONFIG";

export const WORKER_PROCESS_READY = "worker process ready";

export const EXIT_WITH_LINES_STILL_COMING_SIGNAL = "SIGUSR2";

export const LINE_WRITTEN_AROUND_EXIT = "line written around the exit";

export const LINE_COUNT_WRITTEN_BEFORE_EXIT = 1000;

export const LINE_COUNT_WRITTEN_AFTER_EXIT = 1000;

export const DELAY_OF_THE_LINES_WRITTEN_AFTER_EXIT_IN_MILLISECONDS = 200;

export const CLOSE_WHILE_BOOTING_URL_VARIABLE = "NEXTASKING_CLOSE_WHILE_BOOTING_URL";

export const ON_WORKER_STOPPED_CALLED = "onWorkerStopped called";

export const ON_WORKER_STOPPED_SETTLED = "onWorkerStopped settled";
