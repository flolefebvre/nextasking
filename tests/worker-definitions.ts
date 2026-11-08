import type { AnyTaskDefinition } from "../src/definitions.js";
import type { WorkerDefinitions } from "../src/engine-failure-notice.js";

export function workerDefinitionsOf(tasks: readonly AnyTaskDefinition[]): WorkerDefinitions {
  const listenerNamesByEventName = new Map<string, string[]>();
  for (const task of tasks) {
    if (task.on !== undefined) {
      listenerNamesByEventName.set(task.on.name, [...(listenerNamesByEventName.get(task.on.name) ?? []), task.name]);
    }
  }
  return { tasksByName: new Map(tasks.map((task) => [task.name, task])), listenerNamesByEventName };
}
