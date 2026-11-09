import type { Duration } from "../src/durations.js";
import { closeNextasking } from "../src/registry.js";
import { workerOptionsFrom } from "../src/worker-options.js";
import {
  CLOSE_BOUND_PAST_THE_SHUTDOWN_TIMEOUT_IN_MILLISECONDS,
  invariantsOfCurrentTestOrThrow,
} from "./invariant-violations.js";

export async function closeNextaskingAndCheckItsDuration(
  nextasking: object,
  { shutdownTimeout }: { shutdownTimeout?: Duration } = {},
): Promise<void> {
  const invariants = invariantsOfCurrentTestOrThrow("closeNextaskingAndCheckItsDuration");
  const closeBoundInMilliseconds =
    workerOptionsFrom({ shutdownTimeout }).shutdownTimeoutInMilliseconds +
    CLOSE_BOUND_PAST_THE_SHUTDOWN_TIMEOUT_IN_MILLISECONDS;
  const closeStartedAt = Date.now();
  await closeNextasking(nextasking);
  if (Date.now() - closeStartedAt > closeBoundInMilliseconds) {
    invariants.report({
      invariant: "slow close",
      message: `closing a nextasking instance took longer than ${String(closeBoundInMilliseconds)} ms`,
    });
  }
}
