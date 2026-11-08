import { expect, vi } from "vitest";

import { integrationTest } from "../../tests/integration-test.js";
import { openEngine, startInProcessWorker } from "./prototype-harness.js";

integrationTest(
  "prototype: two engines ticking the same planned instant at once start one run",
  { timeout: 10_000 },
  async ({ redisUrl, keyPrefix }) => {
    const first = openEngine(redisUrl, keyPrefix);
    const second = openEngine(redisUrl, keyPrefix);
    try {
      const instant = Date.UTC(2026, 10, 8, 12, 0, 0);

      const fired = await Promise.all([
        first.cronTick("nightly", instant, "noop", {}),
        second.cronTick("nightly", instant, "noop", {}),
      ]);

      expect(fired.filter(Boolean)).toHaveLength(1);
      expect(await first.redis.zcard(first.key("pending"))).toBe(1);
      expect(await first.run(`cron:nightly:${String(instant)}`)).toMatchObject({ state: "pending" });
    } finally {
      first.redis.disconnect();
      second.redis.disconnect();
    }
  },
);

integrationTest(
  "prototype: two workers with the same every-second schedule start one run per firing",
  { timeout: 20_000 },
  async ({ redisUrl, keyPrefix }) => {
    const engine = openEngine(redisUrl, keyPrefix);
    const options = {
      redisUrl,
      keyPrefix,
      schedules: [{ name: "every-second", cron: "* * * * * *", task: "noop" }],
      handlers: { noop: async () => "done" },
    };
    const workers = [startInProcessWorker(options), startInProcessWorker(options)];
    try {
      await vi.waitUntil(async () => (await engine.redis.zcard(engine.key("completed"))) >= 4, { timeout: 10_000 });
      await Promise.all(workers.map(({ worker }) => worker.close()));
      const completed = [
        ...(await engine.redis.zrange(engine.key("completed"), 0, -1)),
        ...(await engine.redis.zrange(engine.key("pending"), 0, -1)),
      ];
      const startedRunIds = workers.flatMap(({ events }) =>
        events.filter((event) => event.type === "started").map((event) => event.runId),
      );
      const instants = completed.map((runId) => Number(runId.split(":")[2]));

      expect(new Set(startedRunIds).size).toBe(startedRunIds.length);
      expect(instants.every((instant) => instant % 1000 === 0)).toBe(true);
      expect(new Set(instants).size).toBe(instants.length);
      expect((Math.max(...instants) - Math.min(...instants)) / 1000 + 1).toBe(instants.length);
    } finally {
      await Promise.all(workers.map(({ worker }) => worker.close()));
      engine.redis.disconnect();
    }
  },
);
