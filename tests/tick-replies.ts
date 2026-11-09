import { expect } from "vitest";

import type { PromoteReply, TickCommandReply } from "../src/engine/engine.js";

export function repliedOf<Reply>(reply: Reply): TickCommandReply<Reply> {
  return { kind: "replied", reply };
}

export const REPLIED_EMPTY_PROMOTE: TickCommandReply<PromoteReply> = repliedOf({
  kind: "promoted",
  promotedRunCount: 0,
  stalledRuns: [],
  thirdStalls: [],
});

export const REPLIED_ONE_RENEWAL = repliedOf([{ kind: "renewed", lockedUntil: expect.any(Number) as number }]);
