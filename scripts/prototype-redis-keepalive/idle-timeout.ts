import { setTimeout as sleep } from "node:timers/promises";

import { createClient } from "@redis/client";
import { z } from "zod";

import { createNextasking, defineTask } from "../../src/index.js";

const url = process.env.REDIS_URL ?? "redis://127.0.0.1:6379";
const shape = process.argv[2] ?? "worker";
const serverTimeoutInSeconds = Number(process.argv[3] ?? 3);
const observeForSeconds = Number(process.argv[4] ?? 12);
const startedAt = Date.now();
const stamp = () => `${((Date.now() - startedAt) / 1000).toFixed(2).padStart(6)}s`;
const say = (text: string) => console.log(`${stamp()}  ${text}`);

const line = (level: string) => (fields: Record<string, unknown>, message: string) => {
  const { err, ...rest } = fields as { err?: Error };
  say(
    `LOG ${level.toUpperCase()} "${message}" ${err ? `err=${err.name}: ${err.message}` : ""} ${Object.keys(rest).length ? JSON.stringify(rest) : ""}`,
  );
};
const logger = { debug: () => undefined, info: line("info"), warn: line("warn"), error: line("error") };

const noopTask = defineTask("prototype-noop", {
  schema: z.object({ busyForMilliseconds: z.number().optional() }),
  handler: async ({ payload }) => {
    await sleep(payload.busyForMilliseconds ?? 0);
  },
});
const nextasking = createNextasking({
  connection: { url },
  logger,
  prefix: `proto-${process.pid}`,
  tasks: [noopTask],
} as never) as unknown as {
  startWorker: (options?: object) => Promise<void>;
  dispatch: (name: string, payload?: unknown) => Promise<unknown>;
};

const admin = createClient({ url, name: "ADMIN" });
admin.on("error", () => undefined);
await admin.connect();
await admin.sendCommand(["CONFIG", "SET", "timeout", "0"]);

say(
  `shape=${shape} server timeout=${serverTimeoutInSeconds}s driverPing=${process.env.PROTOTYPE_DRIVER_PING_MS ?? "-"} idleCheck=${process.env.PROTOTYPE_IDLE_CHECK_MS ?? "-"}`,
);
if (shape === "worker") {
  await nextasking.startWorker({});
} else if (shape === "saturated-worker") {
  await nextasking.startWorker({ concurrency: 1 });
  await nextasking.dispatch("prototype-noop", { busyForMilliseconds: (observeForSeconds + 1) * 1000 });
  say("one slot, one long attempt: the worker has no free slot to wait for a wake-up with");
} else {
  await nextasking.dispatch("prototype-noop", {});
  say("dispatched once; this process never starts a worker");
}
await sleep(1500);
await admin.sendCommand(["CONFIG", "SET", "timeout", String(serverTimeoutInSeconds)]);
say(`CONFIG SET timeout ${serverTimeoutInSeconds}`);

const clientRows = async () => {
  const list = (await admin.sendCommand(["CLIENT", "LIST"])) as string;
  return list
    .trim()
    .split("\n")
    .map((row) => Object.fromEntries(row.split(" ").map((pair) => pair.split("=") as [string, string])))
    .filter((row) => row.name !== "ADMIN")
    .map((row) => `id=${row.id} age=${row.age} idle=${row.idle} cmd=${row.cmd}`);
};

for (let second = 0; second < observeForSeconds; second += 1) {
  await sleep(1000);
  say(`clients: ${(await clientRows()).join("  |  ")}`);
}

const dispatchStartedAt = Date.now();
try {
  await nextasking.dispatch("prototype-noop", {});
  say(`dispatch after the idle stretch: ok in ${Date.now() - dispatchStartedAt} ms`);
} catch (error) {
  say(`dispatch after the idle stretch: THREW ${(error as Error).name} after ${Date.now() - dispatchStartedAt} ms`);
}
await admin.sendCommand(["CONFIG", "SET", "timeout", "0"]);
process.exit(0);
