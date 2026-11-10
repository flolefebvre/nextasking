import { setImmediate as macrotaskYield } from "node:timers/promises";
import { inspect } from "node:util";
import type { TestContext } from "vitest";

import { runsLeftExecutingIn, storedKeysSnapshotOf, storedKeyViolationsIn } from "./stored-key-violations.js";
import type { StoredKeysSnapshot } from "./stored-key-violations.js";
import { testClientNeverReconnectingOf, withRedis } from "./with-redis.js";
import type { LockToken } from "../src/engine/engine-keys.js";
import { isSettledWithin } from "../src/settled-within.js";
import { workerOptionsFrom } from "../src/worker-options.js";

export type Invariant =
  "stored keys" | "worker error" | "process warning" | "unhandled rejection" | "slow close" | "run left executing";

export type InvariantViolation = { invariant: Invariant; message: string; detail?: string };

export type ExpectInvariantViolation = (invariant: Invariant, message: string) => void;

type TestInvariants = {
  report: (violation: InvariantViolation) => void;
  reportLockTokensMadeByTheTest: (lockTokens: readonly LockToken[]) => void;
  stopRecordingProcessEvents: () => void;
  waitBeforeJudging: (settled: Promise<unknown>) => void;
  checkStoredKeysOn: (redisUrl: string) => Promise<void>;
};

export const CLOSE_BOUND_PAST_THE_SHUTDOWN_TIMEOUT_IN_MILLISECONDS = 2000;

export const STORED_KEYS_READ_BOUND_IN_MILLISECONDS = 5_000;

const DEFAULT_CLOSE_BOUND_IN_MILLISECONDS =
  workerOptionsFrom({}).shutdownTimeoutInMilliseconds + CLOSE_BOUND_PAST_THE_SHUTDOWN_TIMEOUT_IN_MILLISECONDS;

let invariantsOfCurrentTest: TestInvariants | undefined;

export function invariantsOfCurrentTestIfAny(): TestInvariants | undefined {
  return invariantsOfCurrentTest;
}

export function invariantsOfCurrentTestOrThrow(sourceName: string): TestInvariants {
  if (invariantsOfCurrentTest === undefined) {
    throw new Error(
      `${sourceName} reports to the invariants of the current test: start it inside an integrationTest, an e2eTest or a benchmarkTest body`,
    );
  }
  return invariantsOfCurrentTest;
}

function messageOfReason(reason: unknown): string {
  return reason instanceof Error ? reason.message : String(reason);
}

function detailOfReason(reason: unknown): string {
  return reason instanceof Error ? (reason.stack ?? reason.message) : inspect(reason);
}

function isSameViolation(left: InvariantViolation, right: InvariantViolation): boolean {
  return left.invariant === right.invariant && left.message === right.message;
}

function shownViolation({ invariant, message }: InvariantViolation): string {
  return `  ${invariant}: ${message}`;
}

function shownDetailOf({ invariant, message, detail }: InvariantViolation): string {
  return `${invariant}: ${message}\n${String(detail).replace(/^/gm, "  ")}`;
}

function invariantViolationsErrorOf(
  undeclaredViolations: readonly InvariantViolation[],
  unseenExpectations: readonly InvariantViolation[],
): Error {
  const sections: string[] = [];
  if (undeclaredViolations.length > 0) {
    sections.push(
      `The test ended with invariant violations it did not declare:\n${undeclaredViolations.map(shownViolation).join("\n")}`,
    );
  }
  if (unseenExpectations.length > 0) {
    sections.push(
      `The test declared invariant violations that never happened:\n${unseenExpectations.map(shownViolation).join("\n")}`,
    );
  }
  const details = undeclaredViolations.filter((violation) => violation.detail !== undefined).map(shownDetailOf);
  return new Error(
    sections.join("\n"),
    details.length === 0 ? undefined : { cause: new Error(`Details of the violations:\n${details.join("\n")}`) },
  );
}

async function storedKeysSnapshotReadWithinTheBoundOf(
  redisUrl: string,
  keyPrefix: string,
): Promise<StoredKeysSnapshot> {
  const redis = testClientNeverReconnectingOf(redisUrl);
  let boundTimer: NodeJS.Timeout | undefined;
  const unansweredWithinTheBound = new Promise<never>((_resolve, reject) => {
    boundTimer = setTimeout(
      () => reject(new Error(`no answer within ${String(STORED_KEYS_READ_BOUND_IN_MILLISECONDS)} ms`)),
      STORED_KEYS_READ_BOUND_IN_MILLISECONDS,
    );
  });
  const snapshotRead = (async () => {
    await redis.connect();
    return storedKeysSnapshotOf(redis, keyPrefix);
  })();
  snapshotRead.catch(() => undefined);
  try {
    return await Promise.race([snapshotRead, unansweredWithinTheBound]);
  } finally {
    clearTimeout(boundTimer);
    redis.destroy();
  }
}

function storedKeyInvariantViolationsIn(
  snapshot: StoredKeysSnapshot,
  keyPrefix: string,
  lockTokensMadeByTheTest: ReadonlySet<LockToken>,
): InvariantViolation[] {
  const runsLeftExecuting = runsLeftExecutingIn(snapshot, keyPrefix, lockTokensMadeByTheTest);
  return [
    ...storedKeyViolationsIn(snapshot, keyPrefix).map((message) => ({ invariant: "stored keys" as const, message })),
    ...runsLeftExecuting.map((message) => ({ invariant: "run left executing" as const, message })),
  ];
}

export function checkInvariantsOfThisTest({
  redisUrl,
  keyPrefix,
  onTestFinished,
}: {
  redisUrl: string;
  keyPrefix: string;
  onTestFinished: TestContext["onTestFinished"];
}): ExpectInvariantViolation {
  const observedViolations: InvariantViolation[] = [];
  const expectedViolations: InvariantViolation[] = [];
  const settledSources: Promise<unknown>[] = [];
  const storedKeyChecks: Promise<void>[] = [];
  const recordUnhandledRejection = (reason: unknown) => {
    observedViolations.push({
      invariant: "unhandled rejection",
      message: messageOfReason(reason),
      detail: detailOfReason(reason),
    });
  };
  const recordProcessWarning = (warning: Error) => {
    observedViolations.push({
      invariant: "process warning",
      message: `${warning.name}: ${warning.message}`,
      detail: warning.stack,
    });
  };
  const lockTokensMadeByTheTest = new Set<LockToken>();
  const stopRecordingProcessEvents = () => {
    process.off("unhandledRejection", recordUnhandledRejection);
    process.off("warning", recordProcessWarning);
  };
  const invariants: TestInvariants = {
    report: (violation) => {
      observedViolations.push(violation);
    },
    reportLockTokensMadeByTheTest: (lockTokens) => {
      for (const lockToken of lockTokens) lockTokensMadeByTheTest.add(lockToken);
    },
    stopRecordingProcessEvents,
    waitBeforeJudging: (settled) => {
      settledSources.push(settled.catch(() => undefined));
    },
    checkStoredKeysOn: (otherRedisUrl) => {
      const storedKeyCheck = storedKeysSnapshotReadWithinTheBoundOf(otherRedisUrl, keyPrefix).then(
        (snapshot) => {
          observedViolations.push(...storedKeyInvariantViolationsIn(snapshot, keyPrefix, lockTokensMadeByTheTest));
        },
        (error: unknown) => {
          observedViolations.push({
            invariant: "stored keys",
            message: `the stored keys on ${otherRedisUrl} could not be read`,
            detail: detailOfReason(error),
          });
        },
      );
      storedKeyChecks.push(storedKeyCheck);
      return storedKeyCheck;
    },
  };
  invariantsOfCurrentTest?.stopRecordingProcessEvents();
  process.on("unhandledRejection", recordUnhandledRejection);
  process.on("warning", recordProcessWarning);
  invariantsOfCurrentTest = invariants;
  let areSourcesSettled = true;

  onTestFinished(async () => {
    try {
      if (areSourcesSettled) {
        await Promise.all(settledSources);
      }
      await Promise.all(storedKeyChecks);
      await macrotaskYield();
    } finally {
      stopRecordingProcessEvents();
      invariantsOfCurrentTest = undefined;
    }
    const recordedViolations = [...observedViolations];
    const violationsSeen = [
      ...recordedViolations,
      ...storedKeyInvariantViolationsIn(
        await withRedis(redisUrl, (redis) => storedKeysSnapshotOf(redis, keyPrefix)),
        keyPrefix,
        lockTokensMadeByTheTest,
      ),
    ];
    const undeclaredViolations = violationsSeen.filter(
      (violation) => !expectedViolations.some((expected) => isSameViolation(expected, violation)),
    );
    const unseenExpectations = expectedViolations.filter(
      (expected) => !violationsSeen.some((violation) => isSameViolation(expected, violation)),
    );
    if (undeclaredViolations.length > 0 || unseenExpectations.length > 0) {
      throw invariantViolationsErrorOf(undeclaredViolations, unseenExpectations);
    }
  });

  onTestFinished(async ({ task }) => {
    const settleBoundInMilliseconds = task.timeout + DEFAULT_CLOSE_BOUND_IN_MILLISECONDS;
    areSourcesSettled = await isSettledWithin(Promise.all(settledSources), settleBoundInMilliseconds);
    if (!areSourcesSettled) {
      throw new Error(
        `A worker this test started, or a worker process it spawned, had not stopped ${String(settleBoundInMilliseconds)} ms after the test ended (its timeout plus the close bound of the default shutdown timeout): close every instance whose worker the test starts, in a finally or with closeWhenTestFinishes, and stop every worker process in a finally`,
      );
    }
  }, Number.POSITIVE_INFINITY);

  return (invariant, message) => {
    expectedViolations.push({ invariant, message });
  };
}
