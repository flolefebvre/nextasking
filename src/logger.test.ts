import { describe, expect, expectTypeOf, test, vi } from "vitest";

import { loggerWithFields } from "./logger.js";
import type { Logger } from "./logger.js";

function createSpyLogger() {
  return { debug: vi.fn(), info: vi.fn(), warn: vi.fn(), error: vi.fn() } satisfies Logger;
}

describe("loggerWithFields", () => {
  test("adds the bound fields to every line, at every level", () => {
    const appLogger = createSpyLogger();
    const runLogger = loggerWithFields(appLogger, { task: "send-welcome-email", runId: "run-1" });

    runLogger.debug({ step: "render" }, "rendering");
    runLogger.info({}, "sent");
    runLogger.warn({ retryInSeconds: 5 }, "slow provider");
    runLogger.error({ code: 500 }, "provider down");

    expect(appLogger.debug).toHaveBeenCalledWith(
      { step: "render", task: "send-welcome-email", runId: "run-1" },
      "rendering",
    );
    expect(appLogger.info).toHaveBeenCalledWith({ task: "send-welcome-email", runId: "run-1" }, "sent");
    expect(appLogger.warn).toHaveBeenCalledWith(
      { retryInSeconds: 5, task: "send-welcome-email", runId: "run-1" },
      "slow provider",
    );
    expect(appLogger.error).toHaveBeenCalledWith(
      { code: 500, task: "send-welcome-email", runId: "run-1" },
      "provider down",
    );
  });

  test("keeps the bound fields when a line sets the same keys", () => {
    const appLogger = createSpyLogger();
    const runLogger = loggerWithFields(appLogger, { runId: "run-1" });

    runLogger.info({ runId: "spoofed", userId: "user-7" }, "done");

    expect(appLogger.info).toHaveBeenCalledWith({ runId: "run-1", userId: "user-7" }, "done");
  });
});

describe("Logger", () => {
  test("takes console as it is", () => {
    expectTypeOf(console).toExtend<Logger>();
  });

  test("rejects a logger missing a level", () => {
    expectTypeOf({ info: console.info, warn: console.warn, error: console.error }).not.toExtend<Logger>();
  });
});
