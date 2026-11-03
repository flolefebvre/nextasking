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
