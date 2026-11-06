const WORKER_BIN_LISTENER = Symbol.for("@flefebvre/nextasking/worker-bin-listener");

const START_WORKER_CALLED_MARK = Symbol.for("@flefebvre/nextasking/start-worker-called");

export type WorkerBinNotification =
  "startWorkerCalled" | "startWorkerResolved" | "startWorkerRejectedWithNoWorker" | "workerNotStarted";

export function notifyWorkerBin(notification: WorkerBinNotification, globalScope: object = globalThis): void {
  if (notification === "startWorkerCalled") {
    Object.assign(globalScope, { [START_WORKER_CALLED_MARK]: true });
  }
  const workerBinListener = (globalScope as Record<symbol, unknown>)[WORKER_BIN_LISTENER];
  if (typeof workerBinListener === "function") {
    workerBinListener(notification);
  }
}
