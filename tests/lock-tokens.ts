import { randomBytes } from "node:crypto";

import type { LockToken } from "../src/engine/engine-keys.js";
import { invariantsOfCurrentTestIfAny } from "./invariant-violations.js";

export function freshLockTokensOf(count: number): LockToken[] {
  const lockTokens = Array.from({ length: count }, () => randomBytes(16).toString("hex"));
  invariantsOfCurrentTestIfAny()?.reportLockTokensMadeByTheTest(lockTokens);
  return lockTokens;
}
