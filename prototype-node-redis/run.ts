/*
 * PROTOTYPE — throwaway, never merged.
 *
 * Question: does node-redis (@redis/client 6.3.1) give nextasking's connection, outage and
 * shutdown paths what they get from ioredis today?
 *
 * Run: pnpm tsx prototype-node-redis/run.ts [image ...]
 * Default images: valkey/valkey:8 valkey/valkey:9 redis:7.4 redis:8
 */
import { connect, createServer } from "node:net";
import type { AddressInfo, Socket } from "node:net";
import { setTimeout as sleep } from "node:timers/promises";

import { createClient, defineScript } from "@redis/client";
import { GenericContainer, Wait } from "testcontainers";

type AnyClient = ReturnType<typeof createClient>;

const startedAt = () => performance.now();

function line(text: string): void {
  console.log(`  ${text}`);
}

function section(title: string): void {
  console.log(`\n── ${title}`);
}

function describeError(error: unknown): string {
  if (!(error instanceof Error)) {
    return `non-error ${JSON.stringify(error)}`;
  }
  const code = (error as { code?: string }).code;
  return `${error.constructor.name}${code ? `[${code}]` : ""}: ${error.message.slice(0, 110)}`;
}

async function observe(label: string, promise: Promise<unknown>, capInMilliseconds: number): Promise<string> {
  const start = startedAt();
  const pending = Symbol("pending");
  let outcome: string;
  try {
    const value = await Promise.race([promise, sleep(capInMilliseconds).then(() => pending)]);
    const elapsed = Math.round(performance.now() - start);
    outcome =
      value === pending
        ? `STILL PENDING after ${String(capInMilliseconds)} ms`
        : `resolved in ${String(elapsed)} ms → ${JSON.stringify(value)?.slice(0, 140)}`;
  } catch (error) {
    outcome = `rejected in ${String(Math.round(performance.now() - start))} ms → ${describeError(error)}`;
  }
  promise.catch(() => undefined);
  line(`${label}: ${outcome}`);
  return outcome;
}

function recordEvents(client: AnyClient): { events: string[]; since: (index: number) => string } {
  const events: string[] = [];
  const start = startedAt();
  const stamp = () => `${String(Math.round(performance.now() - start))}ms`;
  for (const name of ["connect", "ready", "end", "reconnecting", "terminated"]) {
    client.on(name, () => events.push(`${name}@${stamp()}`));
  }
  client.on("error", (error: unknown) => events.push(`error(${describeError(error).slice(0, 60)})@${stamp()}`));
  return { events, since: (index) => events.slice(index).join(", ") || "(none)" };
}

function onceEvent(client: AnyClient, name: string): Promise<void> {
  return new Promise((resolve) => client.once(name, () => resolve()));
}

function commandNamesOf(requestBytes: string): string[] {
  const names: string[] = [];
  const parts = requestBytes.split("\r\n");
  let position = 0;
  while (position < parts.length) {
    const header = parts[position]!;
    if (!header.startsWith("*")) {
      position += 1;
      continue;
    }
    const argumentCount = Number(header.slice(1));
    const first = parts[position + 2] ?? "";
    const second = argumentCount > 1 ? (parts[position + 4] ?? "") : "";
    const isLong = second.length > 60;
    names.push(
      ["CLIENT", "SCRIPT", "HELLO"].includes(first.toUpperCase())
        ? `${first} ${second}`
        : first.toUpperCase() === "EVAL"
          ? `EVAL <body ${String(second.length)} bytes>`
          : first.toUpperCase() === "EVALSHA"
            ? "EVALSHA <sha>"
            : isLong
              ? first
              : first,
    );
    position += 1 + argumentCount * 2;
  }
  return names;
}

type Proxy = {
  url: string;
  requests: string[];
  writes: string[][];
  connectionCount: () => number;
  freezeLiveConnections: () => void;
  cut: () => Promise<void>;
  restore: () => Promise<void>;
  stop: () => Promise<void>;
};

async function startProxy(targetHost: string, targetPort: number): Promise<Proxy> {
  type Pair = { clientSocket: Socket; targetSocket: Socket; isFrozen: boolean };
  const pairs = new Set<Pair>();
  const requests: string[] = [];
  const writes: string[][] = [];
  let connectionCount = 0;
  const server = createServer((clientSocket) => {
    connectionCount += 1;
    const targetSocket = connect(targetPort, targetHost);
    const pair: Pair = { clientSocket, targetSocket, isFrozen: false };
    pairs.add(pair);
    clientSocket.on("data", (chunk: Buffer) => {
      const names = commandNamesOf(chunk.toString("latin1"));
      requests.push(...names);
      writes.push(names);
      if (!pair.isFrozen) {
        targetSocket.write(chunk);
      }
    });
    targetSocket.on("data", (chunk: Buffer) => {
      if (!pair.isFrozen) {
        clientSocket.write(chunk);
      }
    });
    const end = () => {
      pairs.delete(pair);
      clientSocket.destroy();
      targetSocket.destroy();
    };
    for (const socket of [clientSocket, targetSocket]) {
      socket.on("error", end);
      socket.on("close", end);
    }
  });
  const listen = (port: number) =>
    new Promise<void>((resolve) => {
      server.listen(port, "127.0.0.1", () => resolve());
    });
  await listen(0);
  const port = (server.address() as AddressInfo).port;
  return {
    url: `redis://127.0.0.1:${String(port)}`,
    requests,
    writes,
    connectionCount: () => connectionCount,
    freezeLiveConnections: () => {
      for (const pair of pairs) {
        pair.isFrozen = true;
      }
    },
    cut: async () => {
      const closing = server.listening ? new Promise<void>((resolve) => server.close(() => resolve())) : undefined;
      for (const pair of pairs) {
        pair.clientSocket.resetAndDestroy();
        pair.targetSocket.destroy();
      }
      await closing;
    },
    restore: async () => {
      if (!server.listening) {
        await listen(port);
      }
    },
    stop: async () => {
      for (const pair of pairs) {
        pair.clientSocket.destroy();
        pair.targetSocket.destroy();
      }
      if (server.listening) {
        await new Promise<void>((resolve) => server.close(() => resolve()));
      }
    },
  };
}

type Options = Parameters<typeof createClient>[0];

function open(url: string, options: Options = {}): { client: AnyClient; log: ReturnType<typeof recordEvents> } {
  const client = createClient({ url, maintNotifications: "disabled", ...options });
  const log = recordEvents(client);
  return { client, log };
}

const SCRIPT = `return redis.call("INCRBY", KEYS[1], ARGV[1])`;

async function runOn(image: string): Promise<void> {
  console.log(`\n\n════════ ${image} ════════`);
  const container = await new GenericContainer(image)
    .withExposedPorts(6379)
    .withWaitStrategy(Wait.forLogMessage(/Ready to accept connections/))
    .start();
  const host = container.getHost();
  const port = container.getMappedPort(6379);
  const directUrl = `redis://${host}:${String(port)}`;
  const admin = createClient({ url: directUrl, maintNotifications: "disabled" });
  admin.on("error", () => undefined);
  await admin.connect();
  const proxy = await startProxy(host, port);

  try {
    section("A. Handshake and reply shapes");
    for (const RESP of [2, 3] as const) {
      const mark = proxy.requests.length;
      const { client } = open(proxy.url, { RESP });
      await client.connect();
      line(`RESP${String(RESP)} handshake sent: ${proxy.requests.slice(mark).join(" | ") || "(nothing)"}`);
      await client.sendCommand(["DEL", "h", "z", "s"]);
      await client.sendCommand(["HSET", "h", "a", "1", "b", "2"]);
      await client.sendCommand(["ZADD", "z", "1.5", "m1", "2", "m2"]);
      await client.sendCommand(["SADD", "s", "x"]);
      line(`RESP${String(RESP)} raw HGETALL: ${JSON.stringify(await client.sendCommand(["HGETALL", "h"]))}`);
      line(`RESP${String(RESP)} typed hGetAll: ${JSON.stringify(await client.hGetAll("h"))}`);
      line(
        `RESP${String(RESP)} raw HMGET (one missing): ${JSON.stringify(await client.sendCommand(["HMGET", "h", "a", "zz"]))}`,
      );
      line(
        `RESP${String(RESP)} raw ZRANGE REV WITHSCORES: ${JSON.stringify(await client.sendCommand(["ZRANGE", "z", "0", "-1", "REV", "WITHSCORES"]))}`,
      );
      line(
        `RESP${String(RESP)} script ZRANGE WITHSCORES: ${JSON.stringify(await client.sendCommand(["EVAL", `return redis.call("ZRANGE", KEYS[1], 0, -1, "REV", "WITHSCORES")`, "1", "z"]))}`,
      );
      line(`RESP${String(RESP)} raw TIME: ${JSON.stringify(await client.sendCommand(["TIME"]))}`);
      line(`RESP${String(RESP)} raw SMEMBERS: ${JSON.stringify(await client.sendCommand(["SMEMBERS", "s"]))}`);
      line(`RESP${String(RESP)} raw SCAN: ${JSON.stringify(await client.sendCommand(["SCAN", "0", "MATCH", "h*"]))}`);
      line(`RESP${String(RESP)} raw BZPOPMIN hit: ${JSON.stringify(await client.sendCommand(["BZPOPMIN", "z", "1"]))}`);
      line(
        `RESP${String(RESP)} raw BZPOPMIN miss: ${JSON.stringify(await client.sendCommand(["BZPOPMIN", "nope", "0.2"]))}`,
      );
      const info = String(await client.sendCommand(["INFO", "server"]));
      line(
        `RESP${String(RESP)} INFO version lines: ${info
          .split("\r\n")
          .filter((l) => /^(redis|valkey)_version/.test(l))
          .join(", ")}`,
      );
      client.destroy();
    }

    section("B. A connection reset with a command in flight, and commands sent while disconnected");
    {
      const { client, log } = open(proxy.url, { commandOptions: { timeout: undefined } });
      await client.connect();
      proxy.freezeLiveConnections();
      const inFlight = client.get("h-in-flight");
      await sleep(100);
      const mark = log.events.length;
      await proxy.cut();
      await observe("in-flight GET at the reset", inFlight, 1500);
      const whileDown = client.sendCommand(["ECHO", "sent-while-down"]);
      line(`isOpen=${String(client.isOpen)} isReady=${String(client.isReady)} while down`);
      await sleep(700);
      await proxy.restore();
      await observe("command sent while down, after restore", whileDown, 4000);
      line(`events: ${log.since(mark)}`);
      client.destroy();
    }

    {
      const { client, log } = open(proxy.url, { commandOptions: { timeout: undefined } });
      await client.connect();
      const blocked = client.sendCommand(["BZPOPMIN", "b2", "30"]);
      await sleep(150);
      const mark = log.events.length;
      await admin.sendCommand(["CLIENT", "KILL", "TYPE", "normal", "SKIPME", "yes"]);
      await observe("B2 in-flight command when the server closes the connection (no reset)", blocked, 1500);
      await sleep(400);
      line(`B2 events: ${log.since(mark)}`);
      client.destroy();
    }

    section("C. A black-holed connection (no reset): who notices an unanswered command?");
    {
      const { client, log } = open(proxy.url, { commandOptions: { timeout: 1000 } });
      await client.connect();
      proxy.freezeLiveConnections();
      const mark = log.events.length;
      const inFlight = client.get("k");
      await observe("C1 in-flight GET, commandOptions.timeout 1000, no socketTimeout", inFlight, 3000);
      const start = startedAt();
      client.destroy();
      line(
        `C1 destroy() returned in ${String(Math.round(performance.now() - start))} ms, isOpen=${String(client.isOpen)}`,
      );
      await observe("C1 the in-flight GET after destroy()", inFlight, 500);
      await observe(
        "C1 connect() again on the same client",
        client.connect().then(() => "connected"),
        3000,
      );
      await observe("C1 a command on the re-connected client", client.sendCommand(["ECHO", "back"]), 2000);
      line(`C1 events: ${log.since(mark)}`);
      client.destroy();
    }
    {
      const { client, log } = open(proxy.url, { socket: { socketTimeout: 1000 } });
      await client.connect();
      const mark = log.events.length;
      await sleep(2600);
      line(`C2 healthy idle 2.6 s, socketTimeout 1000, default strategy → events: ${log.since(mark)}`);
      line(`C2 isOpen=${String(client.isOpen)} isReady=${String(client.isReady)}`);
      await observe("C2 a command afterwards", client.sendCommand(["ECHO", "x"]), 1500);
      client.destroy();
    }
    {
      const { client, log } = open(proxy.url, { socket: { socketTimeout: 1000, reconnectStrategy: () => 50 } });
      await client.connect();
      const mark = log.events.length;
      await sleep(3600);
      line(`C3 healthy idle 3.6 s, socketTimeout 1000, strategy always reconnects → events: ${log.since(mark)}`);
      client.destroy();
    }
    {
      const { client, log } = open(proxy.url, {
        commandOptions: { timeout: undefined },
        socket: { socketTimeout: 1000, reconnectStrategy: () => 50 },
      });
      await client.connect();
      proxy.freezeLiveConnections();
      const mark = log.events.length;
      await observe("C4a frozen, one in-flight GET, nothing else written", client.get("k"), 3000);
      line(`C4a events: ${log.since(mark)}`);
      client.destroy();
    }
    {
      const { client, log } = open(proxy.url, {
        commandOptions: { timeout: undefined },
        socket: { socketTimeout: 1000, reconnectStrategy: () => 50 },
      });
      await client.connect();
      proxy.freezeLiveConnections();
      const mark = log.events.length;
      const inFlight = client.get("k");
      const writer = setInterval(() => {
        client.sendCommand(["ECHO", "tick"]).catch(() => undefined);
      }, 300);
      await observe("C4b frozen, in-flight GET, another command written every 300 ms", inFlight, 3500);
      clearInterval(writer);
      line(`C4b events: ${log.since(mark)}`);
      client.destroy();
    }

    section("D. READONLY from a demoted primary whose connection stays open");
    {
      const { client, log } = open(proxy.url);
      await client.connect();
      await admin.sendCommand(["REPLICAOF", "127.0.0.1", "1"]);
      const mark = log.events.length;
      await observe("SET on the demoted server", client.sendCommand(["SET", "d", "1"]), 1500);
      line(`isReady=${String(client.isReady)} after the READONLY reply; events: ${log.since(mark)}`);
      await admin.sendCommand(["REPLICAOF", "NO", "ONE"]);
      client.destroy();
      await observe(
        "destroy() then connect(), server promoted again",
        client.connect().then(() => "connected"),
        3000,
      );
      await observe("SET after the forced reconnect", client.sendCommand(["SET", "d", "1"]), 1500);
      client.destroy();
    }

    section("E. Scripts and pipelines");
    {
      const { client } = open(proxy.url, {
        scripts: {
          incrementBy: defineScript({
            SCRIPT,
            NUMBER_OF_KEYS: 1,
            parseCommand(
              parser: { pushKey: (key: string) => void; push: (argument: string) => void },
              key: string,
              by: string,
            ) {
              parser.pushKey(key);
              parser.push(by);
            },
            transformReply: undefined as unknown as () => number,
          }),
        },
      } as Options);
      await client.connect();
      const scripted = client as unknown as { incrementBy: (key: string, by: string) => Promise<number> };
      await admin.sendCommand(["SCRIPT", "FLUSH"]);
      let mark = proxy.requests.length;
      await observe("E1 defineScript, single call after SCRIPT FLUSH", scripted.incrementBy("counter", "1"), 1500);
      line(`E1 sent: ${proxy.requests.slice(mark).join(" | ")}`);
      mark = proxy.requests.length;
      await observe("E1 the same single call again", scripted.incrementBy("counter", "1"), 1500);
      line(`E1 sent: ${proxy.requests.slice(mark).join(" | ")}`);

      for (const round of [1, 2]) {
        mark = proxy.requests.length;
        const multi = client.multi() as unknown as {
          incrementBy: (key: string, by: string) => unknown;
          addCommand: (args: string[]) => unknown;
          execAsPipeline: () => Promise<unknown>;
        };
        multi.incrementBy("counter", "1");
        multi.incrementBy("counter", "1");
        multi.addCommand(["TIME"]);
        await observe(`E2 defineScript twice in a pipeline, round ${String(round)}`, multi.execAsPipeline(), 1500);
        line(`E2 sent: ${proxy.requests.slice(mark).join(" | ")}`);
      }

      const sha = String(await client.sendCommand(["SCRIPT", "LOAD", SCRIPT]));
      const rawPipeline = () =>
        client
          .multi()
          .addCommand(["EVALSHA", sha, "1", "counter", "1"])
          .addCommand(["TIME"])
          .addCommand(["EVALSHA", sha, "1", "counter", "1"])
          .execAsPipeline();
      mark = proxy.requests.length;
      await observe("E3 raw EVALSHA pipeline, script loaded", rawPipeline(), 1500);
      line(`E3 sent: ${proxy.requests.slice(mark).join(" | ")}`);
      await admin.sendCommand(["SCRIPT", "FLUSH"]);
      const before = await client.sendCommand(["GET", "counter"]);
      try {
        await rawPipeline();
        line("E4 raw EVALSHA pipeline after SCRIPT FLUSH: resolved (unexpected)");
      } catch (error) {
        const multiError = error as { replies?: unknown[]; errorIndexes?: number[] };
        line(`E4 raw EVALSHA pipeline after SCRIPT FLUSH: ${describeError(error)}`);
        line(`E4 errorIndexes=${JSON.stringify(multiError.errorIndexes)}`);
        line(
          `E4 replies=${JSON.stringify(multiError.replies?.map((reply) => (reply instanceof Error ? `<${describeError(reply)}>` : reply)))}`,
        );
      }
      line(
        `E4 counter before=${String(before)} after=${String(await client.sendCommand(["GET", "counter"]))} (nothing ran)`,
      );
      await client.sendCommand(["SCRIPT", "LOAD", SCRIPT]);
      await observe("E4 the same pipeline after SCRIPT LOAD", rawPipeline(), 1500);

      await client.sendCommand(["SET", "str", "v"]);
      try {
        await client
          .multi()
          .addCommand(["TIME"])
          .addCommand(["LRANGE", "str", "0", "-1"])
          .addCommand(["GET", "str"])
          .execAsPipeline();
      } catch (error) {
        const multiError = error as { replies?: unknown[]; errorIndexes?: number[] };
        line(
          `E5 one failing command in a pipeline: ${error?.constructor.name}, errorIndexes=${JSON.stringify(multiError.errorIndexes)}, replies=${JSON.stringify(multiError.replies?.map((reply) => (reply instanceof Error ? `<${describeError(reply)}>` : reply)))}`,
        );
      }
      const writesBefore = proxy.writes.length;
      const settled = await Promise.allSettled([
        client.sendCommand(["TIME"]),
        client.sendCommand(["LRANGE", "str", "0", "-1"]),
        client.sendCommand(["EVALSHA", sha, "1", "counter", "1"]),
        client.sendCommand(["GET", "str"]),
      ]);
      line(
        `E6 four commands issued in one tick, one failing → ${String(proxy.writes.length - writesBefore)} socket write(s): ${JSON.stringify(proxy.writes.slice(writesBefore))}`,
      );
      line(
        `E6 per-command results: ${JSON.stringify(settled.map((result) => (result.status === "fulfilled" ? result.value : `<${describeError(result.reason)}>`)))}`,
      );
      client.destroy();
    }

    section("F. BZPOPMIN on a dedicated client");
    {
      const { client, log } = open(proxy.url);
      await client.connect();
      await observe(
        "F1 BZPOPMIN 6.5 s under the default 5 s command timeout",
        client.sendCommand(["BZPOPMIN", "wake", "6.5"]),
        8000,
      );
      let mark = log.events.length;
      const blocked = client.sendCommand(["BZPOPMIN", "wake", "30"]);
      await sleep(200);
      await proxy.cut();
      await observe("F2 blocked BZPOPMIN at a reset", blocked, 1500);
      await proxy.restore();
      await onceEvent(client, "ready");
      line(`F2 events: ${log.since(mark)}`);
      const blockedAgain = client.sendCommand(["BZPOPMIN", "wake", "30"]);
      await sleep(200);
      await admin.sendCommand(["ZADD", "wake", "1", "marker"]);
      await observe("F3 BZPOPMIN after the reconnect, marker armed", blockedAgain, 1500);
      mark = log.events.length;
      const blockedAtDestroy = client.sendCommand(["BZPOPMIN", "wake", "30"]);
      await sleep(200);
      client.destroy();
      await observe("F4 blocked BZPOPMIN at destroy()", blockedAtDestroy, 500);
      line(`F4 events: ${log.since(mark)}`);
    }

    section("G. Closing during a reconnect");
    {
      const { client, log } = open(proxy.url);
      await client.connect();
      await proxy.cut();
      await onceEvent(client, "reconnecting");
      const mark = log.events.length;
      await observe(
        "G1 close() while reconnecting",
        client.close().then(() => "closed"),
        2500,
      );
      await sleep(1500);
      line(`G1 isOpen=${String(client.isOpen)}; events in the next 1.5 s: ${log.since(mark)}`);
      client.destroy();
      await proxy.restore();
    }
    {
      const { client, log } = open(proxy.url);
      await client.connect();
      await proxy.cut();
      await onceEvent(client, "reconnecting");
      const mark = log.events.length;
      const start = startedAt();
      client.destroy();
      line(`G2 destroy() while reconnecting returned in ${String(Math.round(performance.now() - start))} ms`);
      const connectionsBefore = proxy.connectionCount();
      await proxy.restore();
      await sleep(2500);
      line(
        `G2 events in the next 2.5 s: ${log.since(mark)}; new connections after restore: ${String(proxy.connectionCount() - connectionsBefore)}`,
      );
    }
    {
      const { client, log } = open(proxy.url, { commandOptions: { timeout: undefined } });
      await client.connect();
      proxy.freezeLiveConnections();
      const inFlight = client.get("k");
      const mark = log.events.length;
      const closing = client.close().then(() => "closed");
      await observe("G3 close() with an unanswered command in flight", closing, 1000);
      client.destroy();
      await observe("G3 close() after destroy()", closing, 500);
      await observe("G3 the in-flight GET after destroy()", inFlight, 500);
      line(`G3 events: ${log.since(mark)}`);
    }
    {
      const { client, log } = open(proxy.url);
      await client.connect();
      await proxy.cut();
      await onceEvent(client, "reconnecting");
      const mark = log.events.length;
      await proxy.restore();
      const closing = client.close().then(() => "closed");
      await observe("G4 restore() just before close() in the reconnect window", closing, 1000);
      client.destroy();
      const connectionsBefore = proxy.connectionCount();
      await sleep(1500);
      line(
        `G4 isOpen=${String(client.isOpen)}; events: ${log.since(mark)}; connections after destroy: ${String(proxy.connectionCount() - connectionsBefore)}`,
      );
    }

    section("H. The server unreachable at boot");
    {
      await proxy.cut();
      const { client, log } = open(proxy.url, { commandOptions: { timeout: 1000 } });
      await observe("H0 a command before connect() is called", client.sendCommand(["ECHO", "early"]), 500);
      const connecting = client.connect().then(() => "connected");
      await observe("H1 connect() while unreachable", connecting, 1200);
      await observe(
        "H2 a command while unreachable, commandOptions.timeout 1000",
        client.sendCommand(["ECHO", "queued"]),
        2500,
      );
      const unbounded = client.withCommandOptions({ timeout: undefined }).sendCommand(["ECHO", "queued-unbounded"]);
      await sleep(1500);
      await proxy.restore();
      await observe("H3 connect() after restore", connecting, 4000);
      await observe("H3 the command queued with no timeout", unbounded, 2000);
      line(
        `H events: ${log.events.slice(0, 4).join(", ")} … ${log.events.slice(-3).join(", ")} (${String(log.events.length)} total)`,
      );
      client.destroy();
    }
  } finally {
    admin.destroy();
    await proxy.stop();
    await container.stop();
  }
}

const images = process.argv.slice(2);
for (const image of images.length > 0 ? images : ["valkey/valkey:8", "valkey/valkey:9", "redis:7.4", "redis:8"]) {
  await runOn(image);
}
console.log("\n--prototype done--");
process.exit(0);
