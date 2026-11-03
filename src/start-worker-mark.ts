const START_WORKER_CALLED_MARK = Symbol.for("@flefebvre/nextasking/start-worker-called");

export function markStartWorkerCalled(globalScope: object = globalThis): void {
  Object.assign(globalScope, { [START_WORKER_CALLED_MARK]: true });
}
