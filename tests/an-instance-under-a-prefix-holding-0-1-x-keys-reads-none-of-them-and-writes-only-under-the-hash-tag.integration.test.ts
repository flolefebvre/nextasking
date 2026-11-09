import { Redis } from "ioredis";
import { expect, vi } from "vitest";
import { z } from "zod";

import { hashTaggedPrefixOf } from "../src/engine/engine-keys.js";
import { createNextasking, defineScheduledTask, defineTask } from "../src/index.js";
import { closeNextaskingAndCheckItsDuration } from "./close-nextasking-and-check-its-duration.js";
import { integrationTest } from "./integration-test.js";
import { createRecordingLogger, waitForLogMessage } from "./recording-logger.js";
import { storedValuesByKeyOf } from "./engine-test.js";
import { withRedis } from "./with-redis.js";

type MonitoredCommand = { args: string[]; source: string };

async function seedReleaseZeroOneKeys(redis: Redis, runsKeyStart: string): Promise<Map<string, Buffer | null>> {
  await redis.hset(`${runsKeyStart}3f0c2d4e-run`, "name", "send-welcome-email", "data", '{"runId":"welcome-ada"}');
  await redis.rpush(`${runsKeyStart}wait`, "3f0c2d4e-run");
  await redis.zadd(`${runsKeyStart}delayed`, 1, "3f0c2d4e-delayed");
  await redis.hset(`${runsKeyStart}meta`, "opts.maxLenEvents", "10000");
  const seededKeys = (await redis.keys(`${runsKeyStart.replace(/[*?[\]\\]/g, "\\$&")}*`)).sort();
  return new Map(await Promise.all(seededKeys.map(async (key) => [key, await redis.dumpBuffer(key)] as const)));
}

integrationTest(
  "an instance under a prefix holding 0.1.x keys sends no command naming one of them and writes only under the hash tag",
  { timeout: 30_000 },
  async ({ redisUrl, keyPrefix }) => {
    const runsKeyStart = `${keyPrefix}:runs:`;
    const hashTaggedKeyStart = `${hashTaggedPrefixOf(keyPrefix)}:`;
    const { logger, lines } = createRecordingLogger();
    const nextasking = createNextasking({
      connection: { url: redisUrl },
      prefix: keyPrefix,
      logger,
      tasks: [
        defineTask("send-welcome-email", { schema: z.object({ email: z.string() }), handler: async () => undefined }),
        defineScheduledTask("purge-expired-sessions", {
          schedule: { cron: "0 3 * * *" },
          handler: async () => undefined,
        }),
      ],
    });
    const monitoredCommands: MonitoredCommand[] = [];
    const monitorClient = new Redis(redisUrl, { lazyConnect: true, monitor: true });
    monitorClient.on("error", () => undefined);

    try {
      await withRedis(redisUrl, async (redis) => {
        const seededValuesByKey = await seedReleaseZeroOneKeys(redis, runsKeyStart);
        monitorClient.on("monitor", (_time: string, args: string[], source: string) => {
          if (args.some((argument) => argument.includes(keyPrefix))) {
            monitoredCommands.push({ args, source });
          }
        });
        const monitoring = new Promise<void>((resolve) => {
          monitorClient.once("monitoring", () => {
            resolve();
          });
        });
        await monitorClient.connect();
        await monitoring;

        await nextasking.startWorker();
        const { runId } = await nextasking.dispatch("send-welcome-email", { email: "ada@example.com" });
        await waitForLogMessage(lines, "run completed");
        await nextasking.getRun("send-welcome-email", runId);
        await nextasking.listRuns({ state: "completed" });
        await nextasking.countRuns();
        await nextasking.listSchedules();
        await vi.waitFor(() => expect(monitoredCommands.some(({ source }) => source.includes("lua"))).toBe(true), {
          timeout: 5_000,
        });
        monitorClient.disconnect();
        const keysUnderThePrefix = (await redis.keys(`*${keyPrefix.replace(/[*?[\]\\]/g, "\\$&")}*`)).sort();
        const seededValuesAfter = new Map(
          [...seededValuesByKey.keys()].map((key) => [key, null as Buffer | null] as const),
        );
        for (const key of seededValuesAfter.keys()) {
          seededValuesAfter.set(key, await redis.dumpBuffer(key));
        }

        expect(
          monitoredCommands.filter(({ args }) => args.some((argument) => argument.startsWith(`${keyPrefix}:`))),
        ).toEqual([]);
        expect(keysUnderThePrefix.filter((key) => !key.startsWith(hashTaggedKeyStart))).toEqual([
          ...seededValuesByKey.keys(),
        ]);
        expect(keysUnderThePrefix.some((key) => key.startsWith(hashTaggedKeyStart))).toBe(true);
        expect(seededValuesAfter).toEqual(seededValuesByKey);
        expect((await storedValuesByKeyOf(redis, keyPrefix)).size).toBeGreaterThan(0);
      });
    } finally {
      monitorClient.disconnect();
      await closeNextaskingAndCheckItsDuration(nextasking);
    }
  },
);
