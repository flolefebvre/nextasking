import type { EpochMilliseconds } from "../src/durations.js";
import { fixtureRecordsOf } from "./fixture-records.js";
import type { LimitedExecutionRecord } from "./worker-process-tasks.js";

export type LimitedExecution = {
  taskName: string;
  runId: string;
  pid: number;
  attempt: number;
  concurrencyKey: string;
  startedAt: EpochMilliseconds;
  endedAt: EpochMilliseconds | undefined;
  killedAt: EpochMilliseconds | undefined;
};

export type ConcurrencyViolation = {
  taskName: string;
  concurrencyKey: string;
  executingAtOnce: number;
  at: EpochMilliseconds;
};

export async function limitedExecutionsOf(
  redisUrl: string,
  keyPrefix: string,
  taskNames: readonly string[],
  killTimeByPid: ReadonlyMap<number, EpochMilliseconds>,
): Promise<LimitedExecution[]> {
  const recordsByTaskName = await Promise.all(
    taskNames.map(
      async (taskName) =>
        [taskName, (await fixtureRecordsOf(redisUrl, keyPrefix, taskName)) as LimitedExecutionRecord[]] as const,
    ),
  );
  return recordsByTaskName.flatMap(([taskName, records]) => {
    const endedAtByExecutionId = new Map(
      records.filter((record) => record.event === "ended").map((record) => [record.executionId, record.at]),
    );
    return records
      .filter((record) => record.event === "started")
      .map((record) => ({
        taskName,
        runId: record.runId,
        pid: record.pid,
        attempt: record.attempt,
        concurrencyKey: record.concurrencyKey,
        startedAt: record.at,
        endedAt: endedAtByExecutionId.get(record.executionId),
        killedAt: killTimeByPid.get(record.pid),
      }));
  });
}

export function concurrencyViolationsIn(
  executions: readonly LimitedExecution[],
  limitByTaskName: ReadonlyMap<string, number>,
): ConcurrencyViolation[] {
  const executionsByScope = new Map<string, LimitedExecution[]>();
  for (const execution of executions) {
    const scope = JSON.stringify([execution.taskName, execution.concurrencyKey]);
    executionsByScope.set(scope, [...(executionsByScope.get(scope) ?? []), execution]);
  }
  return [...executionsByScope.values()].flatMap((scopeExecutions) => {
    const [{ taskName, concurrencyKey }] = scopeExecutions as [LimitedExecution];
    const limit = limitByTaskName.get(taskName) ?? 0;
    const boundaries = scopeExecutions
      .flatMap((execution) => [
        { at: execution.startedAt, change: 1 },
        { at: execution.endedAt ?? execution.killedAt ?? Number.POSITIVE_INFINITY, change: -1 },
      ])
      .sort((first, second) => first.at - second.at || first.change - second.change);
    let executingAtOnce = 0;
    const violations: ConcurrencyViolation[] = [];
    for (const boundary of boundaries) {
      executingAtOnce += boundary.change;
      if (executingAtOnce > limit) {
        violations.push({ taskName, concurrencyKey, executingAtOnce, at: boundary.at });
      }
    }
    return violations;
  });
}
