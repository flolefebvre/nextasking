import type { RunId } from "./ids.js";

export type AttemptAbortCause = "lock lost" | "shutdown deadline";

const EXPLANATION_BY_ABORT_CAUSE: Record<AttemptAbortCause, string> = {
  "lock lost": "it lost its lock, so another attempt may own the run now",
  "shutdown deadline": "the worker reached its shutdown deadline while the attempt was running",
};

export class AttemptAbortedError extends Error {
  override readonly name = "AttemptAbortedError";
  readonly abortCause: AttemptAbortCause;

  constructor(abortCause: AttemptAbortCause, runId: RunId) {
    super(`The attempt of run "${runId}" was aborted: ${EXPLANATION_BY_ABORT_CAUSE[abortCause]}.`);
    this.abortCause = abortCause;
  }
}

export function abortCauseOf(abortSignal: AbortSignal): AttemptAbortCause | undefined {
  return abortSignal.aborted && abortSignal.reason instanceof AttemptAbortedError
    ? abortSignal.reason.abortCause
    : undefined;
}
