import type { StartWorkerOptions } from "../src/index.js";
import type { WorkerTiming } from "../src/worker-timing.js";

export type WorkerProcessConfig = {
  redisUrl: string;
  keyPrefix: string;
  timing?: Partial<WorkerTiming>;
  startWorkerOptions?: StartWorkerOptions;
};

export const WORKER_PROCESS_CONFIG_VARIABLE = "NEXTASKING_WORKER_PROCESS_CONFIG";

export const WORKER_PROCESS_READY = "worker process ready";
