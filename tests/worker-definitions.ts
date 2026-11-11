import { runFailedEvent } from "../src/built-in-events.js";
import { defineListener } from "../src/definitions.js";
import type { AnyTaskDefinition } from "../src/definitions.js";
import type { WorkerDefinitions } from "../src/definitions.js";

export const brokenKeyRunFailedListenerTask = defineListener("broken-key-listener", {
  on: runFailedEvent,
  concurrency: {
    key: () => {
      throw new Error("no tenant in this payload");
    },
    limit: 1,
  },
  handler: async () => undefined,
});

export function workerDefinitionsOf(tasks: readonly AnyTaskDefinition[]): WorkerDefinitions {
  const listenerNamesByEventName = new Map<string, string[]>();
  for (const task of tasks) {
    if (task.on !== undefined) {
      listenerNamesByEventName.set(task.on.name, [...(listenerNamesByEventName.get(task.on.name) ?? []), task.name]);
    }
  }
  return { tasksByName: new Map(tasks.map((task) => [task.name, task])), listenerNamesByEventName };
}
