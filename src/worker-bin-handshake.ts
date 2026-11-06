const START_WORKER_CALLED_MARK = Symbol.for("@flefebvre/nextasking/start-worker-called");

const WORKER_NOT_STARTED_LISTENER = Symbol.for("@flefebvre/nextasking/worker-not-started-listener");

export function markStartWorkerCalled(globalScope: object = globalThis): void {
  Object.assign(globalScope, { [START_WORKER_CALLED_MARK]: true });
}

export function notifyWorkerBinOfWorkerNotStarted(globalScope: object = globalThis): void {
  const workerNotStartedListener = (globalScope as Record<symbol, unknown>)[WORKER_NOT_STARTED_LISTENER];
  if (typeof workerNotStartedListener === "function") {
    workerNotStartedListener();
  }
}
