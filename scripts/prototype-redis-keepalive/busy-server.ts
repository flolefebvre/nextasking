import { setTimeout as sleep } from "node:timers/promises";

import { createClient } from "@redis/client";

import { createRedisConnections } from "../../src/connection.js";
import { builtInCommandOf } from "../../src/redis-client.js";

const url = process.env.REDIS_URL ?? "redis://127.0.0.1:6379";
const startedAt = Date.now();
const say = (text: string) => console.log(`${((Date.now() - startedAt) / 1000).toFixed(2).padStart(6)}s  ${text}`);
const line = (level: string) => (fields: Record<string, unknown>, message: string) => {
  const { err } = fields as { err?: Error };
  say(`LOG ${level.toUpperCase()} "${message}" ${err ? `err=${err.name}: ${err.message}` : ""}`);
};
const logger = { debug: () => undefined, info: line("info"), warn: line("warn"), error: line("error") };

say(`driverPing=${process.env.PROTOTYPE_DRIVER_PING_MS ?? "-"} idleCheck=${process.env.PROTOTYPE_IDLE_CHECK_MS ?? "-"}`);
const connections = createRedisConnections({ url }, logger as never);
const connection = connections.createCommandConnection();
await connection.instanceCalls.send(builtInCommandOf("PING"));
say("idle command connection is open");

const admin = createClient({ url, name: "ADMIN" });
admin.on("error", () => undefined);
await admin.connect();
await admin.sendCommand(["CONFIG", "SET", "busy-reply-threshold", "200"]);
say("another client runs a 3 s Lua script: the server answers -BUSY to everyone else meanwhile");
const busyScript =
  "local startedAt = redis.call('TIME') local function elapsed() local now = redis.call('TIME') return (now[1] - startedAt[1]) * 1000 + (now[2] - startedAt[2]) / 1000 end while elapsed() < 3000 do end return 'done'";
await admin.sendCommand(["EVAL", busyScript, "0"]);
say("the script ended");
await sleep(4000);
const callStartedAt = Date.now();
await connection.instanceCalls.send(builtInCommandOf("PING"));
say(`a call after the script: ok in ${Date.now() - callStartedAt} ms`);
say("end of observation: was a 'connection error' left without its 'connection restored'?");
await admin.sendCommand(["CONFIG", "SET", "busy-reply-threshold", "5000"]);
process.exit(0);
