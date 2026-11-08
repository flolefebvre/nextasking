import { NonRetriableError } from "@flefebvre/nextasking";
import type { EventId, Logger, RunId, SavedStepResult } from "@flefebvre/nextasking";
import { executeAttempt } from "@flefebvre/nextasking/testing";
import type { ExecuteAttemptOptions, ExecutedAttempt, RecordedSleep } from "@flefebvre/nextasking/testing";

import type { InstanceEventDefinition, InstanceTaskDefinition } from "./faked-instance.js";
import { nextasking } from "./registered-instance.js";
import type { Expect, IsEqual } from "./type-equality.js";

type RegisteredTaskName = "send-welcome-email" | "create-default-workspace" | "purge-expired-sessions";

const executeTheWelcomeEmail = () => executeAttempt(nextasking, "send-welcome-email", { email: "ada@example.com" });

type WelcomeEmailAttempt = Awaited<ReturnType<typeof executeTheWelcomeEmail>>;

export async function retryTheWelcomeEmail(logger: Logger): Promise<WelcomeEmailAttempt> {
  const firstAttempt = await executeTheWelcomeEmail();
  const abortController = new AbortController();
  await executeAttempt(nextasking, "purge-expired-sessions", undefined, { scheduledAt: new Date() });
  await executeAttempt(nextasking, "create-default-workspace", { userId: "user-1" }, { runId: "workspace-1" });
  // @ts-expect-error the instance lists no such task
  await executeAttempt(nextasking, "send-goodbye-email", { email: "ada@example.com" });
  // @ts-expect-error a scheduled task takes no payload
  await executeAttempt(nextasking, "purge-expired-sessions", { olderThanDays: 30 });
  // @ts-expect-error the payload doesn't match the task's schema
  await executeAttempt(nextasking, "send-welcome-email", { email: 42 });
  await executeAttempt(nextasking, "send-welcome-email", { email: "ada@example.com" }).catch((error: unknown) => {
    if (!(error instanceof NonRetriableError)) {
      throw error;
    }
  });
  return executeAttempt(
    nextasking,
    "send-welcome-email",
    { email: "ada@example.com" },
    { savedSteps: firstAttempt.steps, attempt: 2, abortSignal: abortController.signal, logger },
  );
}

export type AttemptIsAnExecutedAttempt = Expect<
  IsEqual<WelcomeEmailAttempt, ExecutedAttempt<InstanceTaskDefinition, InstanceEventDefinition>>
>;

export type AttemptHoldsStepsAndSleeps = Expect<
  IsEqual<
    Pick<WelcomeEmailAttempt, "steps" | "sleeps">,
    { readonly steps: readonly SavedStepResult[]; readonly sleeps: readonly RecordedSleep[] }
  >
>;

export type RecordedSleepHoldsItsIdAndResumeTime = Expect<IsEqual<RecordedSleep, { id: string; resumesAt: Date }>>;

export type AttemptDispatchesAreDiscriminatedByTask = Expect<
  IsEqual<WelcomeEmailAttempt["dispatches"][number]["task"], RegisteredTaskName>
>;

export type AttemptEmitsHoldOnlyAppEvents = Expect<
  IsEqual<
    WelcomeEmailAttempt["emits"][number],
    { event: "user.signed-up"; payload: { userId: string }; eventId: EventId }
  >
>;

export type ExecuteAttemptOptionsAreTheDocumentedOnes = Expect<
  IsEqual<
    ExecuteAttemptOptions,
    {
      savedSteps?: readonly SavedStepResult[];
      runId?: RunId;
      attempt?: number;
      abortSignal?: AbortSignal;
      scheduledAt?: Date;
      logger?: Logger;
    }
  >
>;
