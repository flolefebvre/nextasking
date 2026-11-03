import { createNextasking } from "../src/index.js";
import { closeNextasking } from "../src/registry.js";
import { workerProcessTasks } from "./worker-process-tasks.js";

function createDispatchingNextasking(redisUrl: string, keyPrefix: string) {
  return createNextasking({
    connection: { url: redisUrl },
    prefix: keyPrefix,
    tasks: workerProcessTasks(async () => {
      throw new Error("The test process only dispatches: the worker process runs the tasks");
    }),
  });
}

export function createWorkerProcessNextasking(redisUrl: string, keyPrefix: string) {
  const nextasking = createDispatchingNextasking(redisUrl, keyPrefix);
  return {
    dispatch: nextasking.dispatch,
    close: () => closeNextasking(nextasking),
  };
}
