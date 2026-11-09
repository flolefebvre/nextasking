import { EventEmitter } from "node:events";

import type { OwnedRedisClient, RedisClientRole } from "../src/connection.js";
import type { DurationInMilliseconds } from "../src/durations.js";
import type { Engine } from "../src/engine/engine.js";
import type { CommandResult, RedisClient, RedisCommand } from "../src/redis-client.js";
import { untilAborted } from "../src/redis-outage.js";
import type { WaitForWakeUp } from "../src/worker-loop.js";
import type { InMemoryEngine } from "./in-memory-engine.js";

export type FakeQuitReply = "answered" | "never answered";

export type FakeDisconnect = "rejects every command" | "rejects nothing";

export class FakeRedisClient extends EventEmitter implements OwnedRedisClient {
  status = "connecting";
  readonly sentCommands: string[] = [];
  disconnectCount = 0;
  quitReply: FakeQuitReply = "answered";
  disconnectEffect: FakeDisconnect = "rejects every command";
  commandTimeoutInMilliseconds: DurationInMilliseconds | undefined;
  infoReply: () => Promise<string> = () => Promise.reject(new Error("this fake client has no INFO reply"));
  configReply: () => Promise<unknown> = () => Promise.reject(new Error("this fake client has no CONFIG reply"));
  private readonly disconnectController = new AbortController();
  private readonly readyWaiters: (() => void)[] = [];

  get isDisconnected(): boolean {
    return this.disconnectCount > 0;
  }

  get disconnectSignal(): AbortSignal {
    return this.disconnectController.signal;
  }

  becomeReady(): void {
    this.status = "ready";
    this.emit("ready");
    for (const readyWaiter of this.readyWaiters.splice(0)) {
      readyWaiter();
    }
  }

  startReconnecting(): void {
    this.status = "reconnecting";
    this.emit("close");
    this.emit("reconnecting");
  }

  sendRepliedBy<Reply>(commandName: string, call: () => Promise<Reply>): Promise<Reply> {
    this.sentCommands.push(commandName);
    if (this.disconnectSignal.aborted) {
      return Promise.reject(new Error("Connection is closed."));
    }
    const answered =
      this.status === "ready"
        ? call()
        : new Promise<void>((resolve) => {
            this.readyWaiters.push(resolve);
          }).then(call);
    return untilAborted(this.timedOutUnlessAnswered(answered), this.disconnectSignal);
  }

  private timedOutUnlessAnswered<Reply>(answered: Promise<Reply>): Promise<Reply> {
    const { commandTimeoutInMilliseconds } = this;
    if (commandTimeoutInMilliseconds === undefined) {
      return answered;
    }
    return new Promise<Reply>((resolve, reject) => {
      const endTimeout = () => {
        clearTimeout(timeoutTimer);
        this.disconnectSignal.removeEventListener("abort", endTimeout);
      };
      const timeoutTimer = setTimeout(() => {
        endTimeout();
        reject(new Error("Command timed out"));
      }, commandTimeoutInMilliseconds);
      this.disconnectSignal.addEventListener("abort", endTimeout, { once: true });
      answered.then(
        (reply) => {
          endTimeout();
          resolve(reply);
        },
        (error: unknown) => {
          endTimeout();
          reject(error as Error);
        },
      );
    });
  }

  send(command: RedisCommand): Promise<unknown> {
    const sentCommand =
      command.kind === "builtIn" ? [command.name, ...command.commandArguments].join(" ") : command.script.name;
    const replyByCommandName: Record<string, () => Promise<unknown>> = {
      INFO: this.infoReply,
      CONFIG: this.configReply,
    };
    const reply = (command.kind === "builtIn" ? replyByCommandName[command.name] : undefined) ?? noReplyTo(sentCommand);
    return this.sendRepliedBy(sentCommand, reply);
  }

  sendBatch(commands: readonly RedisCommand[]): Promise<CommandResult[]> {
    return Promise.reject(new Error(`this fake client has no reply to a batch of ${String(commands.length)} commands`));
  }

  quit(): Promise<"OK"> {
    this.sentCommands.push("QUIT");
    if (this.quitReply === "never answered") {
      return new Promise<"OK">(() => undefined);
    }
    this.status = "end";
    this.emit("end");
    return Promise.resolve("OK");
  }

  disconnect(): void {
    this.disconnectCount += 1;
    if (this.disconnectEffect === "rejects nothing" || this.disconnectSignal.aborted) {
      return;
    }
    this.status = "end";
    this.disconnectController.abort(new Error("Connection is closed."));
    this.emit("close");
    this.emit("end");
  }
}

function noReplyTo(sentCommand: string): () => Promise<never> {
  return () => Promise.reject(new Error(`this fake client has no reply to ${sentCommand}`));
}

export type FakeRedisClients = {
  createClient: (role: RedisClientRole) => FakeRedisClient;
  clientsOfRole: (role: RedisClientRole) => FakeRedisClient[];
  onCreate: (prepare: (fakeRedisClient: FakeRedisClient, role: RedisClientRole) => void) => void;
};

export type FakeRedisClientsOptions = { instanceCommandTimeoutInMilliseconds?: DurationInMilliseconds };

export function createFakeRedisClients({
  instanceCommandTimeoutInMilliseconds,
}: FakeRedisClientsOptions = {}): FakeRedisClients {
  const createdClients: { role: RedisClientRole; fakeRedisClient: FakeRedisClient }[] = [];
  let prepareClient: (fakeRedisClient: FakeRedisClient, role: RedisClientRole) => void = () => undefined;
  return {
    createClient: (role) => {
      const fakeRedisClient = new FakeRedisClient();
      if (role === "instance") {
        fakeRedisClient.commandTimeoutInMilliseconds = instanceCommandTimeoutInMilliseconds;
      }
      prepareClient(fakeRedisClient, role);
      createdClients.push({ role, fakeRedisClient });
      return fakeRedisClient;
    },
    clientsOfRole: (role) =>
      createdClients
        .filter((createdClient) => createdClient.role === role)
        .map(({ fakeRedisClient }) => fakeRedisClient),
    onCreate: (prepare) => {
      prepareClient = prepare;
    },
  };
}

function fakeRedisClientOf(client: RedisClient): FakeRedisClient {
  if (!(client instanceof FakeRedisClient)) {
    throw new Error("this scenario runs on fake clients only");
  }
  return client;
}

function missingFromTheInMemoryEngine(method: string): () => Promise<never> {
  return () => Promise.reject(new Error(`the in-memory engine has no ${method}`));
}

export function engineOnFakeClient(engine: InMemoryEngine, client: RedisClient): Engine {
  const fakeRedisClient = fakeRedisClientOf(client);
  const sentOnTheClient =
    <Request, Reply>(method: string, call: (request: Request) => Promise<Reply>) =>
    (request: Request): Promise<Reply> =>
      fakeRedisClient.sendRepliedBy(method, () => call(request));
  return {
    enqueue: sentOnTheClient("enqueue", engine.enqueue),
    fetch: sentOnTheClient("fetch", engine.fetch),
    renew: sentOnTheClient("renew", engine.renew),
    settle: sentOnTheClient("settle", engine.settle),
    promote: () => fakeRedisClient.sendRepliedBy("promote", engine.promote),
    saveStep: sentOnTheClient("saveStep", engine.saveStep),
    readStep: sentOnTheClient("readStep", engine.readStep),
    prune: sentOnTheClient("prune", engine.prune),
    tick: sentOnTheClient("tick", engine.tick),
    fire: sentOnTheClient("fire", engine.fire),
    syncSchedules: sentOnTheClient("syncSchedules", engine.syncSchedules),
    serverInstant: () => fakeRedisClient.sendRepliedBy("serverInstant", engine.serverInstant),
    makeDue: (request) => fakeRedisClient.sendRepliedBy("makeDue", () => engine.makeDue(request)),
    fireNow: missingFromTheInMemoryEngine("fireNow"),
    replay: missingFromTheInMemoryEngine("replay"),
    busyRunCounts: missingFromTheInMemoryEngine("busyRunCounts"),
  };
}

export function wakeUpWaitOnFakeClient(engine: InMemoryEngine, client: RedisClient): WaitForWakeUp {
  const fakeRedisClient = fakeRedisClientOf(client);
  return (timeoutInMilliseconds) =>
    fakeRedisClient.sendRepliedBy("BZPOPMIN", () =>
      engine.waitForWakeUp(timeoutInMilliseconds, fakeRedisClient.disconnectSignal),
    );
}
