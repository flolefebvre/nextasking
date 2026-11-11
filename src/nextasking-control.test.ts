import { describe, expect, expectTypeOf, test } from "vitest";

import { createNextaskingControl } from "./nextasking-control.js";
import type { NextaskingControl } from "./nextasking-control.js";
import type { ControlledScheduledTaskNameIn, ControlledTaskNameIn, TaskName } from "./registry.js";

declare const control: NextaskingControl;

describe("the control's task names", () => {
  test("take the registered task names and refuse a literal the instance doesn't list", () => {
    const registeredName: TaskName = "send-welcome-email";
    const makeDue = async () => {
      await control.makeRunsDueNow({ task: "send-welcome-email" });
      await control.makeRunsDueNow({ task: "purge-expired-sessions" });
      await control.makeRunsDueNow({ task: registeredName });
      await control.makeRunsDueNow({});
      await control.makeRunsDueNow();
      // @ts-expect-error the instance lists no such task
      await control.makeRunsDueNow({ task: "send-welcome-emial" });
    };

    expect(makeDue).toBeTypeOf("function");
  });

  test("fire only the registered scheduled tasks, refusing a literal that isn't one", () => {
    const scheduledName = "purge-expired-sessions" as const;
    const fire = async () => {
      await control.fireSchedule("purge-expired-sessions");
      await control.fireSchedule(scheduledName);
      // @ts-expect-error the instance lists no such task
      await control.fireSchedule("purge-expired-sesions");
      // @ts-expect-error a task the instance lists, but not a scheduled one
      await control.fireSchedule("send-welcome-email");
    };

    expect(fire).toBeTypeOf("function");
  });

  test("pass a name typed string unchecked, as a script reading its arguments does", () => {
    const [taskName] = process.argv.slice(2);
    const runCommand = async () => {
      if (taskName !== undefined) {
        await control.fireSchedule(taskName);
      }
      await control.makeRunsDueNow(taskName === undefined ? {} : { task: taskName });
    };

    expect(runCommand).toBeTypeOf("function");
    expectTypeOf<Parameters<NextaskingControl["fireSchedule"]>>().toEqualTypeOf<[taskName: string]>();
  });

  test("pass through a wrapper taking a string name", () => {
    const fireByName = (taskName: string) => control.fireSchedule(taskName);
    const makeDueByName = (taskName: string) => control.makeRunsDueNow({ task: taskName });

    expectTypeOf(fireByName).parameters.toEqualTypeOf<[taskName: string]>();
    expectTypeOf(makeDueByName).parameters.toEqualTypeOf<[taskName: string]>();
  });

  test("take any string without the Register augmentation", () => {
    expectTypeOf<ControlledTaskNameIn<object>>().toEqualTypeOf<string>();
    expectTypeOf<ControlledScheduledTaskNameIn<object>>().toEqualTypeOf<string>();
  });
});

describe("createNextaskingControl", () => {
  test.each([
    ["without a scheme", "localhost:6379", '"localhost:6379"'],
    ["of another scheme, hiding its password", "http://user:s3cr3t@cache.internal", '"http://user:***@cache.internal"'],
    ["that is not a URL", "not a url", '"not a url"'],
  ])("rejects a connection.url %s", (_, url, shownUrl) => {
    expect(() => createNextaskingControl({ connection: { url } })).toThrow(
      `Invalid connection.url: ${shownUrl} is not a valid redis:// or rediss:// URL, such as "redis://localhost:6379"`,
    );
  });

  test.each(["redis://localhost:6379", "rediss://default:s3cr3t@cache.example.com:6380/2"])("accepts %s", (url) => {
    expect(() => createNextaskingControl({ connection: { url } })).not.toThrow();
  });
});
