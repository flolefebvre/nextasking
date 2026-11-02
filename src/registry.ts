import type { ConnectionConfig } from "./connection.js";
import { validatePayload } from "./definitions.js";
import type { AnyTaskDefinition, PayloadInput } from "./definitions.js";
import { UnknownTaskError } from "./errors.js";
import type { RunId } from "./ids.js";
import { silentLogger } from "./logger.js";
import type { Logger } from "./logger.js";
import { createRunQueue } from "./run-queue.js";
import type { RunQueue } from "./run-queue.js";
import { startRunWorker } from "./worker-runtime.js";
import type { RunningWorker } from "./worker-runtime.js";

// eslint-disable-next-line @typescript-eslint/no-empty-object-type
export interface Register {}

export type DispatchedRun = {
  runId: RunId;
};

export type Nextasking<Task extends AnyTaskDefinition> = {
  readonly dispatch: <Name extends Task["name"]>(
    name: Name,
    payload: PayloadInput<Extract<Task, { name: Name }>["schema"]>,
  ) => Promise<DispatchedRun>;
  readonly startWorker: () => Promise<void>;
};

export type NextaskingConfig<Tasks extends readonly AnyTaskDefinition[]> = {
  connection: ConnectionConfig;
  prefix?: string;
  logger?: Logger;
  tasks: Tasks;
};

const DEFAULT_PREFIX = "nextasking";

const closeByNextasking = new WeakMap<object, () => Promise<void>>();

export function createNextasking<const Tasks extends readonly AnyTaskDefinition[]>(
  config: NextaskingConfig<Tasks>,
): Nextasking<Tasks[number]> {
  const prefix = config.prefix ?? DEFAULT_PREFIX;
  const logger = config.logger ?? silentLogger;
  const tasksByName = new Map<string, AnyTaskDefinition>(config.tasks.map((task) => [task.name, task]));
  let runQueue: RunQueue | undefined;
  let runningWorker: RunningWorker | undefined;

  const nextasking: Nextasking<Tasks[number]> = {
    dispatch: async (name, payload) => {
      const task = tasksByName.get(name);
      if (task === undefined) {
        throw new UnknownTaskError(name, [...tasksByName.keys()]);
      }
      const validatedPayload = await validatePayload(task, payload);
      runQueue ??= createRunQueue(config.connection, prefix);
      const runId = await runQueue.enqueueRun(task.name, validatedPayload);
      logger.debug({ task: task.name, runId }, "run dispatched");
      return { runId };
    },
    startWorker: async () => {
      runningWorker = startRunWorker({ connection: config.connection, prefix, logger, tasksByName });
    },
  };

  closeByNextasking.set(nextasking, async () => {
    await runningWorker?.close();
    runningWorker = undefined;
    await runQueue?.close();
    runQueue = undefined;
  });

  return nextasking;
}

export async function closeNextasking(nextasking: object): Promise<void> {
  await closeByNextasking.get(nextasking)?.();
}
