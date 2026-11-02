import { describe, expect, expectTypeOf, test } from "vitest";
import { z } from "zod";

import * as root from "./index.js";
import { createNextasking, defineTask } from "./index.js";
import type { Register } from "./index.js";

const sendWelcomeEmail = defineTask("send-welcome-email", {
  schema: z.object({ email: z.email() }),
  handler: async () => undefined,
});

export const nextasking = createNextasking({
  connection: { url: "redis://nextasking.invalid:6379" },
  tasks: [sendWelcomeEmail],
});

declare module "./index.js" {
  interface Register {
    nextasking: typeof nextasking;
  }
}

describe("the root export", () => {
  test("exports the definers, createNextasking and the dispatch errors, and nothing else at runtime", () => {
    const exportedNames = Object.keys(root).sort();

    expect(exportedNames).toEqual(["InvalidPayloadError", "UnknownTaskError", "createNextasking", "defineTask"]);
  });

  test("lets an app register its instance through the Register augmentation", () => {
    expectTypeOf<Register["nextasking"]>().toEqualTypeOf<typeof nextasking>();
    expectTypeOf<Register["nextasking"]["dispatch"]>().parameter(0).toEqualTypeOf<"send-welcome-email">();
  });
});
