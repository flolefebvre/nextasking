import * as v from "valibot";
import { describe, expect, test } from "vitest";

import { createInMemoryEngine } from "../tests/in-memory-engine.js";
import type { InMemoryEngine } from "../tests/in-memory-engine.js";
import { freshLockTokensOf } from "../tests/lock-tokens.js";
import { createRecordingLogger } from "../tests/recording-logger.js";
import { workerDefinitionsOf } from "../tests/worker-definitions.js";
import { defineEvent, defineListener, defineTask } from "./definitions.js";
import type { AnyTaskDefinition, EventDefinition } from "./definitions.js";
import type { LockedRun, ReplayReply } from "./engine/engine.js";
import { createEnqueueRequests } from "./enqueue-requests.js";
import type { EnqueueRequests } from "./enqueue-requests.js";
import { RunNotReplayableError } from "./errors.js";
import { deriveHandlerSendId, deriveListenerRunId } from "./ids.js";
import { appSendersOn, handlerSenderOn } from "./send-target.js";
import type { SendTarget } from "./send-target.js";
import type { SendFence } from "./step-engine.js";
import { DEFAULT_WORKER_TIMING } from "./worker-timing.js";

const userSignedUpEvent = defineEvent("user.signed-up", { schema: v.object({ userId: v.string() }) });

const sendWelcomeEmailTask = defineTask("send-welcome-email", {
  schema: v.object({ email: v.string() }),
  handler: async () => undefined,
});

const createWorkspaceTask = defineListener("create-workspace", {
  on: userSignedUpEvent,
  handler: async () => undefined,
});

function requestsOf(tasks: readonly AnyTaskDefinition[], events: readonly EventDefinition[]): EnqueueRequests {
  const { tasksByName, listenerNamesByEventName } = workerDefinitionsOf(tasks);
  return createEnqueueRequests({
    tasksByName,
    eventsByName: new Map(events.map((event) => [event.name, event])),
    listenerNamesByEventName,
  });
}

const SIGN_UP_REQUESTS = requestsOf([sendWelcomeEmailTask, createWorkspaceTask], [userSignedUpEvent]);

async function lockedParentRunOf(engine: InMemoryEngine): Promise<LockedRun> {
  await engine.enqueue({ runs: [{ runId: "onboard-ada", task: "onboard-user", payload: "{}", retries: 0 }] });
  const reply = await engine.fetch({
    lockTokens: freshLockTokensOf(1),
    concurrencyLimitsByTask: {},
    lockDurationInMilliseconds: DEFAULT_WORKER_TIMING.lockDurationInMilliseconds,
  });
  if (reply.kind !== "fetched" || reply.lockedRuns[0] === undefined) {
    throw new Error(`expected one locked run, got ${JSON.stringify(reply)}`);
  }
  return reply.lockedRuns[0];
}

function fenceOf({ task, runId, lockToken }: LockedRun, positionInScope: number): SendFence {
  return {
    parentTask: task,
    parentRunId: runId,
    lockToken,
    stepId: deriveHandlerSendId(task, runId, undefined, positionInScope),
  };
}

describe("the handler's sender", () => {
  test("writes run dispatched on the instance's logger with the child run's task and run id", async () => {
    const engine = createInMemoryEngine();
    const { logger, lines } = createRecordingLogger();
    const sender = handlerSenderOn(engine, SIGN_UP_REQUESTS, logger);
    const parentRun = await lockedParentRunOf(engine);

    const { runId } = await sender.dispatch(
      fenceOf(parentRun, 0),
      "send-welcome-email",
      { email: "ada@example.com" },
      { runId: "welcome-ada" },
    );

    expect(runId).toBe("welcome-ada");
    expect(engine.storedRunOf("send-welcome-email", "welcome-ada")?.state).toBe("pending");
    expect(lines).toEqual([
      { level: "debug", fields: { task: "send-welcome-email", runId: "welcome-ada" }, message: "run dispatched" },
    ]);
  });

  test("writes duplicate dispatch dropped for a run id taken already, from another send", async () => {
    const engine = createInMemoryEngine();
    const { logger, lines } = createRecordingLogger();
    const sender = handlerSenderOn(engine, SIGN_UP_REQUESTS, logger);
    const parentRun = await lockedParentRunOf(engine);
    const payload = { email: "ada@example.com" };
    await sender.dispatch(fenceOf(parentRun, 0), "send-welcome-email", payload, { runId: "welcome-ada" });

    await sender.dispatch(fenceOf(parentRun, 1), "send-welcome-email", payload, { runId: "welcome-ada" });

    expect(lines.map(({ message }) => message)).toEqual(["run dispatched", "duplicate dispatch dropped"]);
    expect(lines[1]?.fields).toEqual({ task: "send-welcome-email", runId: "welcome-ada" });
  });

  test("writes nothing for a send its step already saved, and answers the saved id", async () => {
    const engine = createInMemoryEngine();
    const { logger, lines } = createRecordingLogger();
    const sender = handlerSenderOn(engine, SIGN_UP_REQUESTS, logger);
    const parentRun = await lockedParentRunOf(engine);
    const fence = fenceOf(parentRun, 0);
    await sender.dispatch(fence, "send-welcome-email", { email: "ada@example.com" }, { runId: "welcome-ada" });
    await sender.emit(fenceOf(parentRun, 1), "user.signed-up", { userId: "ada" }, { eventId: "signed-up-ada" });
    const linesBefore = [...lines];

    const repeatedDispatch = await sender.dispatch(
      fence,
      "send-welcome-email",
      { email: "ada@example.com" },
      { runId: "welcome-again" },
    );
    const repeatedEmit = await sender.emit(
      fenceOf(parentRun, 1),
      "user.signed-up",
      { userId: "ada" },
      { eventId: "signed-up-again" },
    );

    expect([repeatedDispatch, repeatedEmit]).toEqual([{ runId: "welcome-ada" }, { eventId: "signed-up-ada" }]);
    expect(lines).toEqual(linesBefore);
    expect(
      engine.storedRunOf("create-workspace", deriveListenerRunId("signed-up-ada", "create-workspace")),
    ).toBeDefined();
  });
});

describe("the app's senders", () => {
  function replayingSendersOf(replayReply: ReplayReply) {
    const sentReplays: unknown[] = [];
    const sendTarget: SendTarget = {
      enqueue: () => Promise.reject(new Error("this test enqueues nothing")),
      replay: (request) => {
        sentReplays.push(request);
        return Promise.resolve(replayReply);
      },
    };
    const { tasksByName } = workerDefinitionsOf([sendWelcomeEmailTask]);
    const { logger, lines } = createRecordingLogger();
    const senders = appSendersOn({ requests: SIGN_UP_REQUESTS, tasksByName, sendTarget: () => sendTarget, logger });
    return { senders, sentReplays, lines };
  }

  test.each([
    [{ kind: "notFound" }, "no such run is kept, its id is unknown or the run was removed"],
    [{ kind: "wrongState", state: "completed" }, "it is not failed"],
    [
      { kind: "unknownVersion", recordVersion: 2 },
      "a newer release wrote the run's record, which this release cannot read: replay it from the newer release",
    ],
  ] as const)("refuse a replay answered %o with RunNotReplayableError", async (replayReply, shownRefusal) => {
    const { senders, lines } = replayingSendersOf(replayReply);

    const replaying = senders.replay("send-welcome-email", "welcome-ada");

    await expect(replaying).rejects.toThrow(RunNotReplayableError);
    await expect(replaying).rejects.toThrow(
      `Run "welcome-ada" of task "send-welcome-email" can't be replayed: ${shownRefusal}.`,
    );
    expect(lines).toEqual([]);
  });

  test("refuse the replay of an empty run id as one not found, without sending it", async () => {
    const { senders, sentReplays } = replayingSendersOf({ kind: "replayed", task: "send-welcome-email" });

    await expect(senders.replay("send-welcome-email", "")).rejects.toThrow(
      `Run "" of task "send-welcome-email" can't be replayed: no such run is kept`,
    );
    expect(sentReplays).toEqual([]);
  });

  test("throw an emit whose listener's concurrency key throws, naming the listener, and send nothing", async () => {
    const brokenListenerTask = defineListener("bill-workspace", {
      on: userSignedUpEvent,
      concurrency: {
        key: () => {
          throw new Error("no tenant");
        },
        limit: 1,
      },
      handler: async () => undefined,
    });
    const tasks = [createWorkspaceTask, brokenListenerTask] as AnyTaskDefinition[];
    const sentRequests: unknown[] = [];
    const sendTarget: SendTarget = {
      enqueue: (request) => {
        sentRequests.push(request);
        return Promise.resolve({ kind: "enqueued", createdRuns: [], skippedRuns: [] });
      },
      replay: () => Promise.reject(new Error("this test replays nothing")),
    };
    const senders = appSendersOn({
      requests: requestsOf(tasks, [userSignedUpEvent]),
      tasksByName: workerDefinitionsOf(tasks).tasksByName,
      sendTarget: () => sendTarget,
      logger: createRecordingLogger().logger,
    });

    await expect(senders.emit("user.signed-up", { userId: "ada" })).rejects.toThrow(
      'Task "bill-workspace" has an invalid concurrency.key: it threw "no tenant"',
    );
    expect(sentRequests).toEqual([]);
  });
});
