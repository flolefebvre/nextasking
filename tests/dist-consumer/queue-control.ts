import type { ConnectionConfig, Duration, RunId } from "@flefebvre/nextasking";
import { createNextaskingControl } from "@flefebvre/nextasking/testing";
import type { NextaskingControl, NextaskingControlOptions } from "@flefebvre/nextasking/testing";

import type { Expect, IsEqual } from "./type-equality.js";

export async function skipTheSleepsAndFireTheSchedule(): Promise<RunId> {
  await using control = createNextaskingControl({ connection: { url: "redis://localhost:6379" }, prefix: "shop" });
  await control.reset({ timeout: "10s" });
  const runsMadeDue = await control.makeRunsDueNow({ task: "send-welcome-email" });
  await control.makeRunsDueNow();
  const { runId } = await control.fireSchedule("purge-expired-sessions");
  await control.waitForIdle({ timeout: "30s" });
  await control.waitForIdle();
  // @ts-expect-error a timeout is a Duration with a unit
  await control.waitForIdle({ timeout: 30 });
  await control.close();
  return runsMadeDue[0]?.runId ?? runId;
}

export async function refuseANameTheInstanceDoesNotList(): Promise<void> {
  await using control = createNextaskingControl({ connection: { url: "redis://localhost:6379" } });
  // @ts-expect-error the instance lists no such task
  await control.makeRunsDueNow({ task: "send-welcome-emial" });
  // @ts-expect-error the instance lists no such task
  await control.fireSchedule("purge-expired-sesions");
  // @ts-expect-error a task the instance lists, but not a scheduled one
  await control.fireSchedule("send-welcome-email");
}

export async function runTheManualsOfflineDevScript(): Promise<void> {
  const control = createNextaskingControl({
    connection: { url: process.env.REDIS_URL ?? "redis://localhost:6379" },
  });
  const [command, taskName] = process.argv.slice(2);

  try {
    if (command === "fire" && taskName !== undefined) {
      console.log(await control.fireSchedule(taskName));
    } else if (command === "skip") {
      console.log(await control.makeRunsDueNow(taskName === undefined ? {} : { task: taskName }));
    } else {
      console.log("usage: tsx scripts/nextasking-control.mts fire <scheduled-task> | skip [task]");
    }
    await control.waitForIdle();
  } finally {
    await control.close();
  }
}

export type FireScheduleTakesAString = Expect<
  IsEqual<Parameters<NextaskingControl["fireSchedule"]>, [taskName: string]>
>;

export type ControlOptionsTakeTheConnectionAndPrefix = Expect<
  IsEqual<NextaskingControlOptions, { connection: ConnectionConfig; prefix?: string }>
>;

export type MakeRunsDueNowResolvesWithTheRunsPromoted = Expect<
  IsEqual<Awaited<ReturnType<NextaskingControl["makeRunsDueNow"]>>, { task: string; runId: RunId }[]>
>;

export type FireScheduleResolvesWithTheRunId = Expect<
  IsEqual<Awaited<ReturnType<NextaskingControl["fireSchedule"]>>, { runId: RunId }>
>;

export type WaitForIdleTakesADuration = Expect<
  IsEqual<Parameters<NextaskingControl["waitForIdle"]>, [options?: { timeout?: Duration }]>
>;
