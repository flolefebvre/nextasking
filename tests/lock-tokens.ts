import { randomBytes } from "node:crypto";

import type { LockToken } from "../src/engine/engine-keys.js";

export function freshLockTokensOf(count: number): LockToken[] {
  return Array.from({ length: count }, () => randomBytes(16).toString("hex"));
}
