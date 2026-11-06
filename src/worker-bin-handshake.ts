const WORKER_BIN_LISTENER = Symbol.for("@flefebvre/nextasking/worker-bin-listener");

export type WorkerBinNotification = "startWorkerCalled" | "workerNotStarted";

export function notifyWorkerBin(notification: WorkerBinNotification, globalScope: object = globalThis): void {
  const workerBinListener = (globalScope as Record<symbol, unknown>)[WORKER_BIN_LISTENER];
  if (typeof workerBinListener === "function") {
    workerBinListener(notification);
  }
}
