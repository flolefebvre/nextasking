import type { DispatchOrEmitRequest, HeldLock, Engine, StaleReply, UnknownVersionReply } from "./engine/engine.js";
import type { EventId, RunId, StepId } from "./ids.js";
import { LostRunLockError } from "./job-lock.js";
import type { LockLoss, RefusedStepCall } from "./job-lock.js";
import { sentIdOf, serializeStepValue } from "./step-engine.js";
import type { SendFence, StepStore } from "./step-engine.js";

type FenceRefusal = StaleReply | UnknownVersionReply;

function lostRunLockErrorOf(
  refusedSubject: string,
  refusedCall: RefusedStepCall,
  refusal: FenceRefusal,
): LostRunLockError {
  const lockLoss: LockLoss =
    refusal.kind === "stale"
      ? { kind: "lockTakenOver" }
      : { kind: "recordRewritten", recordVersion: refusal.recordVersion };
  return new LostRunLockError(refusedSubject, { refusedCall, lockLoss });
}

export function createEngineStepStore(
  engine: Pick<Engine, "saveStep" | "readStep">,
  { runId, lockToken }: HeldLock,
): StepStore {
  const stepSubjectOf = (stepId: StepId) => `Step "${stepId}" of run "${runId}"`;
  return {
    readStep: async (stepId) => {
      const reply = await engine.readStep({ runId, lockToken, stepId });
      if (reply.kind !== "read") {
        throw lostRunLockErrorOf(stepSubjectOf(stepId), "read", reply);
      }
      return reply.savedValue;
    },
    saveStep: async (stepId, value) => {
      const reply = await engine.saveStep({ runId, lockToken, stepId, step: { kind: "value", value } });
      if (reply.kind !== "saved") {
        throw lostRunLockErrorOf(stepSubjectOf(stepId), "saved", reply);
      }
    },
    saveSleep: async (stepId, sleep) => {
      const reply = await engine.saveStep({ runId, lockToken, stepId, step: sleep });
      if (reply.kind !== "saved") {
        throw lostRunLockErrorOf(stepSubjectOf(stepId), "saved", reply);
      }
      if (reply.resumesAt === undefined) {
        throw new Error(`The saveStep script saved sleep "${stepId}" of run "${runId}" without a resume time`);
      }
      return { resumesAt: reply.resumesAt, isDue: reply.isDue };
    },
  };
}

export async function sendFenced(
  engine: Pick<Engine, "enqueue">,
  request: DispatchOrEmitRequest,
  { parentRunId, lockToken, stepId }: SendFence,
  sentId: RunId | EventId,
): Promise<RunId | EventId> {
  const reply = await engine.enqueue({
    ...request,
    parentFence: { parentRunId, lockToken, stepId, stepValue: serializeStepValue(sentId) },
  });
  switch (reply.kind) {
    case "enqueued":
    case "duplicate":
      return sentId;
    case "alreadySent":
      return sentIdOf(stepId, reply.savedValue);
    default:
      throw lostRunLockErrorOf(`Send "${stepId}" of run "${parentRunId}"`, "sent", reply);
  }
}
