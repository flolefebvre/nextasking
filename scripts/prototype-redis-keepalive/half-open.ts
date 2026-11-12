import { setTimeout as sleep } from "node:timers/promises";

import { createRedisConnections } from "../../src/connection.js";
import { builtInCommandOf } from "../../src/redis-client.js";

const url = process.env.REDIS_URL ?? "redis://vk:6379";
const observeForSeconds = Number(process.argv[2] ?? 75);
const clock = () => new Date().toISOString().slice(11, 23);
const say = (text: string) => console.log(`${clock()}  ${text}`);
const line = (level: string) => (fields: Record<string, unknown>, message: string) => {
  const { err } = fields as { err?: Error & { code?: string } };
  say(`LOG ${level.toUpperCase()} "${message}" ${err ? `err=${err.name}: ${err.message} ${err.code ?? ""}` : ""}`);
};
const logger = { debug: () => undefined, info: line("info"), warn: line("warn"), error: line("error") };

say(`driverPing=${process.env.PROTOTYPE_DRIVER_PING_MS ?? "-"} idleCheck=${process.env.PROTOTYPE_IDLE_CHECK_MS ?? "-"}`);
const connections = createRedisConnections({ url }, logger as never);
const connection = connections.createCommandConnection();
await connection.instanceCalls.send(builtInCommandOf("PING"));
say("OPEN: the command connection answered a PING and now sits idle");
await sleep(observeForSeconds * 1000);
say(`end of the ${observeForSeconds} s observation`);
process.exit(0);
