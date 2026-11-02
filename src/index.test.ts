import { describe, expect, expectTypeOf, test } from "vitest";
import { z } from "zod";

import * as root from "./index.js";
import { createNextasking, defineEvent, defineListener, defineTask } from "./index.js";
import type { EventName, Register, TaskName } from "./index.js";
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
  handler: async () => undefined,
});

export const nextasking = createNextasking({
  connection: { url: "redis://nextasking.invalid:6379" },
  events: [userSignedUp],
  tasks: [sendWelcomeEmail, createDefaultWorkspace],
});

declare module "./index.js" {
  interface Register {
    nextasking: typeof nextasking;
  }
}

describe("the root export", () => {
  test("exports the definers, createNextasking and the errors, and nothing else at runtime", () => {
    const exportedNames = Object.keys(root).sort();

    expect(exportedNames).toEqual([
      "InvalidPayloadError",
      "NonRetriableError",
      "UnknownEventError",
      "UnknownTaskError",
      "createNextasking",
      "defineEvent",
      "defineListener",
      "defineTask",
    ]);
  });

  test("lets an app register its instance through the Register augmentation", () => {
    expectTypeOf<Register["nextasking"]>().toEqualTypeOf<typeof nextasking>();
    expectTypeOf<Register["nextasking"]["dispatch"]>()
      .parameter(0)
      .toEqualTypeOf<"send-welcome-email" | "create-default-workspace">();
    expectTypeOf<Register["nextasking"]["emit"]>().parameter(0).toEqualTypeOf<"user.signed-up">();
  });

  test("exports the registered task and event names", () => {
    expectTypeOf<TaskName>().toEqualTypeOf<"send-welcome-email" | "create-default-workspace">();
    expectTypeOf<EventName>().toEqualTypeOf<"user.signed-up">();
  });

  test("falls back to no name at all when Register is not augmented", () => {
    expectTypeOf<TaskNameIn<object>>().toBeNever();
    expectTypeOf<EventNameIn<object>>().toBeNever();
  });
});
