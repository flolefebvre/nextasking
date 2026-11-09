import { describe, expect, expectTypeOf, test } from "vitest";
import { z } from "zod";

import * as root from "./index.js";
import { createNextasking, defineEvent, defineListener, defineScheduledTask, defineTask } from "./index.js";
import type {
  EventName,
  EventWithRuns,
  FailureReason,
  ListedRun,
  ListedSchedule,
  ListRunsOptions,
  Register,
  Run,
  RunCounts,
  RunPage,
  RunState,
  ScheduledTaskContext,
  TaskContext,
  TaskName,
} from "./index.js";
import type { EventNameIn, TaskNameIn } from "./registry.js";

const sendWelcomeEmail = defineTask("send-welcome-email", {
  schema: z.object({ email: z.email() }),
  handler: async () => undefined,
});

const userSignedUp = defineEvent("user.signed-up", {
  schema: z.object({ userId: z.string() }),
});

const createDefaultWorkspace = defineListener("create-default-workspace", {
  on: userSignedUp,
  handler: async ({ payload, dispatch }) => {
    await dispatch("send-welcome-email", { email: `${payload.userId}@example.com` });
  },
});

const purgeExpiredSessions = defineScheduledTask("purge-expired-sessions", {
  schedule: { cron: "0 3 * * *" },
  handler: async () => undefined,
});

export const nextasking = createNextasking({
  connection: { url: "redis://nextasking.invalid:6379" },
  events: [userSignedUp],
  tasks: [sendWelcomeEmail, createDefaultWorkspace, purgeExpiredSessions],
});

declare module "./index.js" {
  interface Register {
    nextasking: typeof nextasking;
  }
}

type RegisteredTaskName = "send-welcome-email" | "create-default-workspace" | "purge-expired-sessions";

describe("the root export", () => {
  test("exports the definers, createNextasking, the errors and the built-in events, and nothing else at runtime", () => {
    const exportedNames = Object.keys(root).sort();

    expect(exportedNames).toEqual([
      "FanOutNotReplayableError",
      "InvalidPayloadError",
      "NonRetriableError",
      "RedisUnavailableError",
      "RunNotReplayableError",
      "StepMisuseError",
      "UnknownEventError",
      "UnknownTaskError",
      "createNextasking",
      "defineEvent",
      "defineListener",
      "defineScheduledTask",
      "defineTask",
      "fanOutFailed",
      "runFailed",
    ]);
  });

  test("lets an app register its instance through the Register augmentation", () => {
    expectTypeOf<Register["nextasking"]>().toEqualTypeOf<typeof nextasking>();
    expectTypeOf<Register["nextasking"]["dispatch"]>().parameter(0).toEqualTypeOf<RegisteredTaskName>();
    expectTypeOf<Register["nextasking"]["emit"]>().parameter(0).toEqualTypeOf<"user.signed-up">();
  });

  test("types a handler's emit and dispatch through the Register augmentation", () => {
    const sendFromAHandler = async ({ dispatch, emit }: TaskContext<unknown>) => {
      await dispatch("send-welcome-email", { email: "ada@example.com" }, { runId: "welcome-ada", delay: "5m" });
      await emit("user.signed-up", { userId: "user-1" }, { eventId: "signed-up-user-1" });
      // @ts-expect-error the payload doesn't match the event's schema
      await emit("user.signed-up", { email: "ada@example.com" });
      // @ts-expect-error the instance lists no such task
      await dispatch("send-goodbye-email", { email: "ada@example.com" });
    };

    expectTypeOf(sendFromAHandler).returns.resolves.toBeVoid();
    expectTypeOf<TaskContext<unknown>["dispatch"]>().parameter(0).toEqualTypeOf<RegisteredTaskName>();
    expectTypeOf<TaskContext<unknown>["emit"]>().parameter(0).toEqualTypeOf<"user.signed-up">();
  });

  test("types a handler's dispatch of a scheduled task with no payload", () => {
    const runScheduledTaskNow = async ({ dispatch }: ScheduledTaskContext) => {
      await dispatch("purge-expired-sessions");
      await dispatch("purge-expired-sessions", undefined, { runId: "purge-now" });
      // @ts-expect-error a scheduled task takes no payload
      await dispatch("purge-expired-sessions", { olderThanDays: 30 });
    };

    expectTypeOf(runScheduledTaskNow).returns.resolves.toBeVoid();
  });

  test("exports the registered task and event names", () => {
    expectTypeOf<TaskName>().toEqualTypeOf<RegisteredTaskName>();
    expectTypeOf<EventName>().toEqualTypeOf<"user.signed-up" | "nextasking/run.failed" | "nextasking/fan-out.failed">();
  });

  test("types a run by its task, then by its state", () => {
    const describeRun = (run: Run) => {
      if (run.task === "send-welcome-email") {
        expectTypeOf(run.payload).toEqualTypeOf<{ email: string }>();
      }
      if (run.task === "purge-expired-sessions") {
        expectTypeOf(run.payload).toBeUndefined();
      }
      switch (run.state) {
        case "delayed":
          return run.startsAt;
        case "suspended":
          return run.resumesAt;
        case "backingOff":
          return run.nextAttemptAt;
        case "failed":
          expectTypeOf(run.reason).toEqualTypeOf<FailureReason>();
          return run.finishedAt;
        default:
          // @ts-expect-error only a delayed run has a startsAt
          return run.startsAt;
      }
    };

    expectTypeOf(describeRun).returns.toEqualTypeOf<Date | undefined>();
    expectTypeOf<Run["task"]>().toEqualTypeOf<RegisteredTaskName>();
    expectTypeOf<Run["state"]>().toEqualTypeOf<RunState>();
    expectTypeOf<RunState>().toEqualTypeOf<
      | "delayed"
      | "pending"
      | "waitingForConcurrencySlot"
      | "executing"
      | "suspended"
      | "backingOff"
      | "completed"
      | "failed"
    >();
  });

  test("types getRun's answer by the task named", () => {
    expectTypeOf(nextasking.getRun<"send-welcome-email">)
      .returns.resolves.exclude<undefined>()
      .toHaveProperty("payload")
      .toEqualTypeOf<{ email: string }>();
    const readRunOfUnknownTask = () =>
      // @ts-expect-error the instance lists no such task
      nextasking.getRun("send-goodbye-email", "run-1");

    expectTypeOf(readRunOfUnknownTask).toBeFunction();
  });

  test("types listRuns' runs by the state and the task asked for, and countRuns by task and state", () => {
    type FailedWelcomeEmailRun = Awaited<
      ReturnType<typeof nextasking.listRuns<"failed", "send-welcome-email">>
    >["runs"][number];

    expectTypeOf<FailedWelcomeEmailRun["state"]>().toEqualTypeOf<"failed">();
    expectTypeOf<FailedWelcomeEmailRun["task"]>().toEqualTypeOf<"send-welcome-email">();
    expectTypeOf<FailedWelcomeEmailRun["reason"]>().toEqualTypeOf<FailureReason>();
    expectTypeOf<FailedWelcomeEmailRun["lastErrorMessage"]>().toEqualTypeOf<string | undefined>();
    expectTypeOf<ListRunsOptions["task"]>().toEqualTypeOf<RegisteredTaskName | undefined>();
    expectTypeOf<RunPage["runs"][number]>().toEqualTypeOf<ListedRun>();
    expectTypeOf<RunCounts["purge-expired-sessions"]["waitingForConcurrencySlot"]>().toEqualTypeOf<number>();
    expectTypeOf(nextasking.countRuns).returns.resolves.toEqualTypeOf<RunCounts>();
    // @ts-expect-error a listed run carries no payload
    expectTypeOf<ListedRun["payload"]>().toBeUnknown();
  });

  test("describes the tasks and events, with no schema and no handler, for a run-now picker", () => {
    const runNow = async (taskName: (typeof nextasking.taskDefinitions)[number]["name"]) => {
      if (taskName === "purge-expired-sessions") {
        await nextasking.dispatch(taskName);
      }
    };

    expect(nextasking.taskDefinitions).toEqual([
      { name: "send-welcome-email" },
      { name: "create-default-workspace", event: "user.signed-up" },
      { name: "purge-expired-sessions", schedule: { cron: "0 3 * * *", timezone: "UTC" } },
    ]);
    expect(nextasking.eventDefinitions).toEqual([
      { name: "user.signed-up", listenerNames: ["create-default-workspace"] },
      { name: "nextasking/run.failed", listenerNames: [] },
      { name: "nextasking/fan-out.failed", listenerNames: [] },
    ]);
    expect(Object.isFrozen(nextasking.taskDefinitions[2]?.schedule)).toBe(true);
    expectTypeOf(runNow).parameter(0).toEqualTypeOf<TaskName>();
    expectTypeOf(nextasking.taskDefinitions[0]!.event).toEqualTypeOf<EventName | undefined>();
    expectTypeOf(nextasking.eventDefinitions[0]!.name).toEqualTypeOf<EventName>();
    expectTypeOf(nextasking.eventDefinitions[0]!.listenerNames).toEqualTypeOf<readonly TaskName[]>();
  });

  test("types getEvent's listener runs by listener, and listSchedules by scheduled task", () => {
    type SignedUpEvent = Awaited<ReturnType<typeof nextasking.getEvent<"user.signed-up">>>;

    expectTypeOf<keyof SignedUpEvent["runsByListener"]>().toEqualTypeOf<"create-default-workspace">();
    expectTypeOf<NonNullable<SignedUpEvent["runsByListener"]["create-default-workspace"]>["payload"]>().toEqualTypeOf<{
      userId: string;
    }>();
    expectTypeOf<SignedUpEvent>().not.toHaveProperty("fanOut");
    expectTypeOf<SignedUpEvent>().toEqualTypeOf<Extract<EventWithRuns, { event: "user.signed-up" }>>();
    expectTypeOf<ListedSchedule["task"]>().toEqualTypeOf<"purge-expired-sessions">();
    expectTypeOf(nextasking.listSchedules).returns.resolves.toEqualTypeOf<ListedSchedule[]>();
    const readUnknownEvent = () =>
      // @ts-expect-error the instance lists no such event
      nextasking.getEvent("user.deleted", "event-1");

    expectTypeOf(readUnknownEvent).toBeFunction();
  });

  test("falls back to no name at all when Register is not augmented", () => {
    expectTypeOf<TaskNameIn<object>>().toBeNever();
    expectTypeOf<EventNameIn<object>>().toBeNever();
  });
});
