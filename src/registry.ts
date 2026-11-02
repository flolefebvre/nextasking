import { validatePayload } from "./definitions.js";
import type { AnyTaskDefinition, PayloadInput } from "./definitions.js";
import { UnknownTaskError } from "./errors.js";
import type { RunId } from "./ids.js";

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
};

export type NextaskingConfig<Tasks extends readonly AnyTaskDefinition[]> = {
  connection: { url: string };
  prefix?: string;
  tasks: Tasks;
};

export function createNextasking<const Tasks extends readonly AnyTaskDefinition[]>(
  config: NextaskingConfig<Tasks>,
): Nextasking<Tasks[number]> {
  const tasksByName = new Map(config.tasks.map((task) => [task.name, task]));

  return {
    dispatch: async (name, payload) => {
      const task = tasksByName.get(name);
      if (task === undefined) {
        throw new UnknownTaskError(name, [...tasksByName.keys()]);
      }
      await validatePayload(task, payload);
      throw new Error("Enqueueing a run is not implemented yet");
    },
  };
}
