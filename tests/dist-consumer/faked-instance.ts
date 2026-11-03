import type { Duration, EventId, Nextasking, RunId } from "@flefebvre/nextasking";
import { fakeNextasking } from "@flefebvre/nextasking/testing";
import type { NextaskingFake, RecordedDispatch, RecordedEmit, RecordedReplay } from "@flefebvre/nextasking/testing";

import { nextasking } from "./registered-instance.js";
import type { Expect, IsEqual } from "./type-equality.js";

type RegisteredTaskName = "send-welcome-email" | "create-default-workspace" | "purge-expired-sessions";

type WelcomeEmailPayload = { email: string; locale: string };

const fakeTheInstance = () => fakeNextasking(nextasking);

type InstanceFake = ReturnType<typeof fakeTheInstance>;

type InstanceDispatch = InstanceFake["dispatches"][number];

type InstanceEmit = InstanceFake["emits"][number];

type InstanceReplay = InstanceFake["replays"][number];

export async function dispatchWhileFaked(): Promise<readonly InstanceDispatch[]> {
  using fake = fakeTheInstance();
  await nextasking.dispatch("send-welcome-email", { email: "ada@example.com" }, { delay: "5m" });
  await nextasking.emit("user.signed-up", { userId: "user-1" });
  await nextasking.replay("send-welcome-email", "welcome-ada");
  return fake.dispatches;
}

export type FakeIsANextaskingFake = Expect<
  IsEqual<InstanceFake, NextaskingFake<InstanceTaskDefinition, InstanceEventDefinition>>
>;

export type DispatchesAreDiscriminatedByTask = Expect<IsEqual<InstanceDispatch["task"], RegisteredTaskName>>;

export type DispatchPayloadIsTheSchemaOutput = Expect<
  IsEqual<Extract<InstanceDispatch, { task: "send-welcome-email" }>["payload"], WelcomeEmailPayload>
>;

export type ScheduledTaskDispatchHasNoPayload = Expect<
  IsEqual<Extract<InstanceDispatch, { task: "purge-expired-sessions" }>["payload"], undefined>
>;

export type DispatchHoldsItsRunIdAndDelay = Expect<
  IsEqual<Pick<InstanceDispatch, "runId" | "delay">, { runId: RunId; delay?: Duration }>
>;

export type EmitsHoldOnlyAppEvents = Expect<
  IsEqual<InstanceEmit, { event: "user.signed-up"; payload: { userId: string }; eventId: EventId }>
>;

export type ReplaysHoldRunsAndFanOuts = Expect<
  IsEqual<
    InstanceReplay,
    | { task: RegisteredTaskName; runId: RunId }
    | { event: "user.signed-up" | "nextasking/run.failed" | "nextasking/fan-out.failed"; eventId: EventId }
  >
>;

export type RecordTypesAreExported = Expect<
  IsEqual<
    [InstanceDispatch, InstanceEmit, InstanceReplay],
    [
      RecordedDispatch<InstanceTaskDefinition>,
      RecordedEmit<InstanceEmittableEventDefinition>,
      RecordedReplay<InstanceTaskDefinition, InstanceEventDefinition>,
    ]
  >
>;

type InstanceDefinitions =
  typeof nextasking extends Nextasking<infer Task, infer Event> ? { task: Task; event: Event } : never;

export type InstanceTaskDefinition = InstanceDefinitions["task"];

export type InstanceEventDefinition = InstanceDefinitions["event"];

type InstanceEmittableEventDefinition = Extract<InstanceEventDefinition, { name: "user.signed-up" }>;

export const disposedByUsing: InstanceFake[typeof Symbol.dispose] = () => undefined;
