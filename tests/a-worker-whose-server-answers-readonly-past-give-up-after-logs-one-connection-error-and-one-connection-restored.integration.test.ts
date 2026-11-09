import { expect, vi } from "vitest";

import { RedisUnavailableError } from "../src/index.js";
import { closeNextaskingAndCheckItsDuration } from "./close-nextasking-and-check-its-duration.js";
import { createGiveUpAfterOneSecondNextasking } from "./give-up-after-one-second-nextasking.js";
import { integrationTest } from "./integration-test.js";
import { SERVER_CONTAINER_START_BOUND_IN_MILLISECONDS, startOwnServer } from "./own-server.js";
import {
  createRecordingLogger,
  expectOneConnectionErrorAndOneConnectionRestored,
  waitForLogMessage,
} from "./recording-logger.js";
import { providedRedisUrl } from "./valkey-container.js";
import { withRedis } from "./with-redis.js";

const UNREACHABLE_PRIMARY_PORT = "1";

const READ_ONLY_REPLY_PATTERN = /^READONLY /;

function isBlockedInTheWakeUpWait(clientListLine: string): boolean {
  return /\bflags=\S*b/.test(clientListLine) && clientListLine.includes("cmd=bzpopmin");
}

integrationTest.skipIf(providedRedisUrl() !== undefined)(
  "a worker whose server answers READONLY past giveUpAfter logs one connection error naming the reply and one connection restored once it accepts writes, and a dispatch giving up meanwhile carries the reply",
  { timeout: SERVER_CONTAINER_START_BOUND_IN_MILLISECONDS + 10_000 + 15_000 + 15_000 + 10_000 + 5_000 },
  async ({ keyPrefix }) => {
    const ownServer = await startOwnServer();
    const { logger, lines } = createRecordingLogger();
    const completedEmails: string[] = [];
    const nextasking = createGiveUpAfterOneSecondNextasking(ownServer.url, keyPrefix, {
      logger,
      onWelcomeEmail: (email) => completedEmails.push(email),
    });
    try {
      await withRedis(ownServer.url, async (admin) => {
        await nextasking.startWorker();
        await vi.waitFor(
          async () => {
            const clientList = String(await admin.sendCommand(["CLIENT", "LIST"]));
            expect(clientList.split("\n").some(isBlockedInTheWakeUpWait)).toBe(true);
          },
          { timeout: 10_000 },
        );
        await admin.sendCommand(["REPLICAOF", "127.0.0.1", UNREACHABLE_PRIMARY_PORT]);

        const dispatchOutcome = nextasking
          .dispatch("send-welcome-email", { email: "ada@example.com" }, { runId: "welcome-ada" })
          .catch((error: unknown) => error);
        await waitForLogMessage(lines, "connection error", { timeout: 15_000 });
        const gaveUp = await dispatchOutcome;
        await admin.sendCommand(["REPLICAOF", "NO", "ONE"]);
        await waitForLogMessage(lines, "connection restored", { timeout: 15_000 });
        await nextasking.dispatch("send-welcome-email", { email: "grace@example.com" }, { runId: "welcome-grace" });
        await vi.waitFor(() => expect(completedEmails).toContain("grace@example.com"), { timeout: 10_000 });

        expect(gaveUp).toBeInstanceOf(RedisUnavailableError);
        expect(gaveUp).toMatchObject({
          message: "Redis didn't answer within 1s",
          runId: "welcome-ada",
          cause: expect.objectContaining({ message: expect.stringMatching(READ_ONLY_REPLY_PATTERN) }),
        });
        expectOneConnectionErrorAndOneConnectionRestored(lines);
        expect(lines.find(({ message }) => message === "connection error")?.fields).toEqual({
          err: expect.objectContaining({ message: expect.stringMatching(READ_ONLY_REPLY_PATTERN) }),
        });
      });
    } finally {
      await closeNextaskingAndCheckItsDuration(nextasking);
      await ownServer.stop();
    }
  },
);
