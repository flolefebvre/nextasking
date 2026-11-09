import * as v from "valibot";
import { expect, test } from "vitest";

import { runFailed } from "./built-in-events.js";
import { defineListener, defineScheduledTask, defineTask } from "./definitions.js";
import type { AnyTaskDefinition } from "./definitions.js";
import type { LockedRun, RunToCreate } from "./engine/engine.js";
import { NonRetriableError } from "./errors.js";
import { executeLockedRunAttempt, failingAttemptOf, settleRequestOf } from "./locked-run-attempt.js";
import type { AttemptResult } from "./locked-run-attempt.js";
import { silentLogger } from "./logger.js";
import type { Logger } from "./logger.js";
import type { HandlerSender } from "./step-engine.js";
import { createInMemoryEngine } from "../tests/in-memory-engine.js";
import type { InMemoryEngine } from "../tests/in-memory-engine.js";
import { freshLockTokensOf } from "../tests/lock-tokens.js";
import { createRecordingLogger } from "../tests/recording-logger.js";
import { createSimulatedRedisOutage } from "../tests/simulated-redis-outage.js";
import { brokenKeyRunFailedListener, workerDefinitionsOf } from "../tests/worker-definitions.js";

const NO_SENDER: HandlerSender = {
  dispatch: () => Promise.reject(new Error("this attempt sends nothing")),
  emit: () => Promise.reject(new Error("this attempt sends nothing")),
};

const alertOnCall = defineListener("alert-on-call", { on: runFailed, handler: async () => undefined });

async function lockedRunOf(engine: InMemoryEngine, run: Partial<RunToCreate> & { task: string }): Promise<LockedRun> {
  await engine.enqueue({ runs: [{ runId: "run-1", payload: '{"to":"ada@example.com"}', retries: 2, ...run }] });
  const reply = await engine.fetch({
    lockTokens: freshLockTokensOf(1),
    concurrencyLimitsByTask: {},
    lockDurationInMilliseconds: 120_000,
  });
  const lockedRun = reply.kind === "fetched" ? reply.lockedRuns[0] : undefined;
  if (lockedRun === undefined) {
    throw new Error(`expected one locked run, got ${JSON.stringify(reply)}`);
  }
  return lockedRun;
}

async function attemptResultOf(
  task: AnyTaskDefinition,
  {
    lockedRun,
    engine = createInMemoryEngine(),
    logger = silentLogger,
  }: { lockedRun?: LockedRun; engine?: InMemoryEngine; logger?: Logger } = {},
): Promise<AttemptResult> {
  const abortController = new AbortController();
  return executeLockedRunAttempt({
    engine,
    task,
    lockedRun: lockedRun ?? (await lockedRunOf(engine, { task: task.name })),
    definitions: workerDefinitionsOf([task, alertOnCall] as AnyTaskDefinition[]),
    sender: NO_SENDER,
    logger,
    redisOutage: createSimulatedRedisOutage(logger).redisOutage,
    abortSignal: abortController.signal,
    abortAttempt: () => {
      abortController.abort();
    },
    isWorkerDraining: () => false,
  });
}

test("a handler that returns completes the attempt, seeing the payload, the run id, the attempt and run fields on its logger", async () => {
  const { logger, lines } = createRecordingLogger();
  const seenContexts: unknown[] = [];
  const sendEmail = defineTask("send-email", {
    schema: v.object({ to: v.string() }),
    handler: async ({ payload, runId, attempt, logger: runLogger }) => {
      seenContexts.push({ payload, runId, attempt });
      runLogger.info({}, "sending");
    },
  });

  const { attemptOutcome } = await attemptResultOf(sendEmail, { logger });

  expect(attemptOutcome).toEqual({ kind: "complete" });
  expect(seenContexts).toEqual([{ payload: { to: "ada@example.com" }, runId: "run-1", attempt: 1 }]);
  expect(lines).toEqual([
    { level: "info", message: "sending", fields: { task: "send-email", runId: "run-1", attempt: 1 } },
  ]);
});

test("a scheduled task's handler sees its planned instant and no payload", async () => {
  const engine = createInMemoryEngine();
  const scheduledAt = Date.UTC(2030, 0, 1, 3, 30);
  const seenScheduledAts: unknown[] = [];
  const buildReport = defineScheduledTask("build-report", {
    schedule: { cron: "30 3 * * *" },
    handler: async (context) => {
      seenScheduledAts.push(context.scheduledAt);
    },
  });
  const lockedRun = await lockedRunOf(engine, { task: "build-report", payload: "" });

  await attemptResultOf(buildReport as AnyTaskDefinition, { engine, lockedRun: { ...lockedRun, scheduledAt } });

  expect(seenScheduledAts).toEqual([new Date(scheduledAt)]);
});

test("a handler that throws with retries left retries after the task's backoff doubled per failed attempt", async () => {
  const engine = createInMemoryEngine();
  const error = new Error("provider down");
  const sendEmail = defineTask("send-email", {
    schema: v.object({}),
    backoff: "10s",
    handler: async () => {
      throw error;
    },
  });
  const lockedRun = await lockedRunOf(engine, { task: "send-email" });

  const { attemptOutcome } = await attemptResultOf(sendEmail, { engine, lockedRun: { ...lockedRun, attempt: 2 } });

  expect(attemptOutcome).toEqual({
    kind: "retry",
    error,
    retriesLeft: 1,
    backoffInMilliseconds: expect.any(Number),
  });
  const backoffInMilliseconds = attemptOutcome.kind === "retry" ? attemptOutcome.backoffInMilliseconds : 0;
  expect(backoffInMilliseconds).toBeGreaterThanOrEqual(16_000);
  expect(backoffInMilliseconds).toBeLessThanOrEqual(20_000);
});

test("a handler that throws on its last attempt fails the run with the notice prepared for the release's listeners", async () => {
  const engine = createInMemoryEngine();
  const error = new Error("provider down");
  const sendEmail = defineTask("send-email", {
    schema: v.object({}),
    handler: async () => {
      throw error;
    },
  });
  const lockedRun = await lockedRunOf(engine, { task: "send-email" });

  const { attemptOutcome, listenersLeftOut } = await attemptResultOf(sendEmail, {
    engine,
    lockedRun: { ...lockedRun, attempt: 3 },
  });

  expect(attemptOutcome).toMatchObject({ kind: "fail", reason: "retries exhausted", error });
  expect(attemptOutcome.kind === "fail" ? attemptOutcome.failureNotice?.runs.map(({ task }) => task) : []).toEqual([
    "alert-on-call",
  ]);
  expect(listenersLeftOut).toEqual([]);
});

test("a non-retriable error fails the run on its first attempt", async () => {
  const sendEmail = defineTask("send-email", {
    schema: v.object({}),
    handler: async () => {
      throw new NonRetriableError("bad address");
    },
  });

  const { attemptOutcome } = await attemptResultOf(sendEmail);

  expect(attemptOutcome).toMatchObject({ kind: "fail", reason: "non-retriable" });
});

test("a failing listener of runFailed fails with an empty notice", async () => {
  const engine = createInMemoryEngine();
  const failingListener = defineListener("failing-listener", {
    on: runFailed,
    retries: 0,
    handler: async () => {
      throw new Error("pager down");
    },
  });
  const lockedRun = await lockedRunOf(engine, {
    task: "failing-listener",
    retries: 0,
    sourceEvent: { eventName: runFailed.name, eventId: "notice-1" },
  });

  const { attemptOutcome } = await attemptResultOf(failingListener as AnyTaskDefinition, { engine, lockedRun });

  expect(attemptOutcome).toMatchObject({ kind: "fail", reason: "retries exhausted" });
  expect(attemptOutcome.kind === "fail" ? attemptOutcome.failureNotice?.runs : undefined).toEqual([]);
});

test("a handler sleeping until a later time suspends the run until then", async () => {
  const resumesAt = new Date(Date.now() + 60 * 60 * 1000);
  const sendEmail = defineTask("send-email", {
    schema: v.object({}),
    handler: async ({ step }) => {
      await step.sleepUntil("wait-an-hour", resumesAt);
    },
  });

  const { attemptOutcome } = await attemptResultOf(sendEmail);

  expect(attemptOutcome).toEqual({ kind: "suspend", resumesAt: resumesAt.getTime() });
});

test("a handler running past its timeout retries with the timeout error", async () => {
  const sendEmail = defineTask("send-email", {
    schema: v.object({}),
    timeout: "1s",
    handler: async () => {
      await new Promise((resolve) => setTimeout(resolve, 5_000));
    },
  });

  const { attemptOutcome } = await attemptResultOf(sendEmail);

  expect(attemptOutcome).toMatchObject({ kind: "retry", error: { message: 'Task "send-email" timed out after 1s' } });
}, 10_000);

test("a failing attempt built for a third stall carries the notice and the listeners left out", () => {
  const error = new Error("stalled");

  const { attemptOutcome, listenersLeftOut } = failingAttemptOf(
    { runId: "run-1", task: "send-email", attempt: 1, replays: 0 },
    { error, reason: "stalled" },
    workerDefinitionsOf([alertOnCall, brokenKeyRunFailedListener] as AnyTaskDefinition[]),
  );

  expect(attemptOutcome).toMatchObject({ kind: "fail", reason: "stalled", error });
  expect(attemptOutcome.kind === "fail" ? attemptOutcome.failureNotice?.runs.map(({ task }) => task) : []).toEqual([
    "alert-on-call",
  ]);
  expect(listenersLeftOut.map(({ listenerName }) => listenerName)).toEqual(["broken-key-listener"]);
});

test("a settle request carries the run's fence, the outcome with its error recorded for the attempt, and the task's limit", async () => {
  const engine = createInMemoryEngine();
  const sendEmail = defineTask("send-email", {
    schema: v.object({}),
    concurrency: { limit: 3 },
    handler: async () => undefined,
  });
  const lockedRun = await lockedRunOf(engine, { task: "send-email" });
  const error = new Error("provider down");

  const request = settleRequestOf(sendEmail as AnyTaskDefinition, lockedRun, {
    kind: "retry",
    error,
    retriesLeft: 1,
    backoffInMilliseconds: 1000,
  });

  expect(request).toEqual({
    runId: "run-1",
    lockToken: lockedRun.lockToken,
    concurrencyLimit: 3,
    outcome: {
      kind: "retry",
      backoffInMilliseconds: 1000,
      error: { attempt: 1, name: "Error", message: "provider down", stack: error.stack, failedAt: expect.any(String) },
    },
  });
});
