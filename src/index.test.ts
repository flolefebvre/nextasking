import { describe, expect, expectTypeOf, test } from "vitest";
import { z } from "zod";

import * as root from "./index.js";
import { createNextasking, defineEvent, defineListener, defineScheduledTask, defineTask } from "./index.js";
import type { EventName, Register, ScheduledTaskContext, TaskContext, TaskName } from "./index.js";
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
  test("exports the definers, createNextasking and the errors, and nothing else at runtime", () => {
    const exportedNames = Object.keys(root).sort();

    expect(exportedNames).toEqual([
      "InvalidPayloadError",
      "NonRetriableError",
      "StepMisuseError",
      "UnknownEventError",
      "UnknownTaskError",
      "createNextasking",
      "defineEvent",
      "defineListener",
      "defineScheduledTask",
      "defineTask",
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
    expectTypeOf<EventName>().toEqualTypeOf<"user.signed-up">();
  });

  test("falls back to no name at all when Register is not augmented", () => {
    expectTypeOf<TaskNameIn<object>>().toBeNever();
    expectTypeOf<EventNameIn<object>>().toBeNever();
  });
});
