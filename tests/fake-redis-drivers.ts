import { randomUUID } from "node:crypto";

import { AbortError, ClientClosedError, DisconnectsClientError } from "@redis/client";

import type { Engine } from "../src/engine/engine.js";
import type { DurationInMilliseconds } from "../src/durations.js";
import type { CreateRedisDriver, RedisDriver, RedisDriverEvents } from "../src/node-redis-driver.js";
import { builtInCommandOf, commandResultOf } from "../src/redis-client.js";
import type { CommandResult, RedisClient, RedisCommand } from "../src/redis-client.js";
import type { WaitForWakeUp } from "../src/worker-loop.js";
import type { InMemoryEngine } from "./in-memory-engine.js";

export type FakeCloseReply = "answered" | "never answered";

type FakeReply = (connectionLostSignal: AbortSignal) => Promise<unknown>;

const repliesByToken = new Map<string, { commandName: string; reply: FakeReply }>();

const FAKE_REPLY_COMMAND_NAME = "FAKE_REPLY";

export function fakeReplyCommandOf(commandName: string, reply: FakeReply): RedisCommand {
  const token = randomUUID();
  repliesByToken.set(token, { commandName, reply });
  return builtInCommandOf(FAKE_REPLY_COMMAND_NAME, token);
}

type PendingCommand = { settle: (outcome: Promise<unknown>) => void; reject: (error: Error) => void };

export class FakeRedisDriver implements RedisDriver {
  isReady = false;
  readonly sentCommands: string[] = [];
  connectCount = 0;
  forceReconnectCount = 0;
  closeCount = 0;
  destroyCount = 0;
  closeReply: FakeCloseReply = "answered";
  isAnswering = true;
  infoReply: () => Promise<string> = () => Promise.reject(new Error("this fake driver has no INFO reply"));
  configReply: () => Promise<unknown> = () => Promise.reject(new Error("this fake driver has no CONFIG reply"));
  onForceReconnect: () => void = () => undefined;
  private readonly events: RedisDriverEvents;
  private readonly heldCommands = new Set<{ run: () => void; reject: (error: Error) => void }>();
  private readonly commandsInFlight = new Set<PendingCommand>();
  private connectionLostController = new AbortController();
  private isDestroyed = false;

  constructor(events: RedisDriverEvents) {
    this.events = events;
  }

  get isClosed(): boolean {
    return this.isDestroyed;
  }

  becomeReady(): void {
    if (this.isDestroyed) {
      return;
    }
    this.events.onConnect();
    this.isReady = true;
    this.events.onReady();
    for (const heldCommand of [...this.heldCommands]) {
      this.heldCommands.delete(heldCommand);
      heldCommand.run();
    }
  }

  startHandshake(): void {
    this.events.onConnect();
  }

  startReconnecting(): void {
    this.isReady = false;
  }

  loseConnection(error: Error): void {
    this.isReady = false;
    this.events.onError(error);
    this.rejectCommandsInFlight(error);
  }

  connect(): void {
    this.connectCount += 1;
  }

  forceReconnect(): void {
    this.forceReconnectCount += 1;
    this.isReady = false;
    this.rejectEveryCommand(new DisconnectsClientError());
    this.onForceReconnect();
  }

  destroy(): void {
    this.destroyCount += 1;
    this.isReady = false;
    this.isDestroyed = true;
    this.rejectEveryCommand(new DisconnectsClientError());
  }

  close(): Promise<void> {
    this.closeCount += 1;
    if (this.closeReply === "never answered") {
      return new Promise<void>(() => undefined);
    }
    this.isReady = false;
    this.isDestroyed = true;
    return Promise.resolve();
  }

  send(command: RedisCommand, abortSignal?: AbortSignal): Promise<unknown> {
    const { commandName, reply } = this.replyTo(command);
    this.sentCommands.push(commandName);
    if (this.isDestroyed) {
      return Promise.reject(new ClientClosedError());
    }
    if (abortSignal?.aborted === true) {
      return Promise.reject(new AbortError());
    }
    return new Promise<unknown>((resolve, reject) => {
      const write = () => {
        const commandInFlight: PendingCommand = {
          settle: (outcome) => {
            this.commandsInFlight.delete(commandInFlight);
            outcome.then(resolve, reject);
          },
          reject: (error) => {
            this.commandsInFlight.delete(commandInFlight);
            reject(error);
          },
        };
        this.commandsInFlight.add(commandInFlight);
        if (this.isAnswering) {
          void reply(this.connectionLostController.signal).then(
            (value) => {
              if (this.commandsInFlight.has(commandInFlight)) {
                commandInFlight.settle(Promise.resolve(value));
              }
            },
            (error: unknown) => {
              if (this.commandsInFlight.has(commandInFlight)) {
                commandInFlight.settle(Promise.reject(error as Error));
              }
            },
          );
        }
      };
      if (this.isReady) {
        write();
        return;
      }
      const heldCommand = {
        run: () => {
          abortSignal?.removeEventListener("abort", abortHeldCommand);
          write();
        },
        reject: (error: Error) => {
          abortSignal?.removeEventListener("abort", abortHeldCommand);
          reject(error);
        },
      };
      const abortHeldCommand = () => {
        this.heldCommands.delete(heldCommand);
        reject(new AbortError());
      };
      abortSignal?.addEventListener("abort", abortHeldCommand, { once: true });
      this.heldCommands.add(heldCommand);
    });
  }

  sendBatch(commands: readonly RedisCommand[], abortSignal?: AbortSignal): Promise<CommandResult[]> {
    return Promise.all(commands.map((command) => commandResultOf(this.send(command, abortSignal))));
  }

  private replyTo(command: RedisCommand): { commandName: string; reply: FakeReply } {
    if (command.kind === "script") {
      return { commandName: command.script.name, reply: noReplyTo(command.script.name) };
    }
    const [token] = command.commandArguments;
    const fakeReply = command.name === FAKE_REPLY_COMMAND_NAME ? repliesByToken.get(String(token)) : undefined;
    if (fakeReply !== undefined) {
      return fakeReply;
    }
    const sentCommand = [command.name, ...command.commandArguments].join(" ");
    const replyByCommandName: Record<string, () => Promise<unknown>> = {
      INFO: this.infoReply,
      CONFIG: this.configReply,
    };
    return { commandName: sentCommand, reply: replyByCommandName[command.name] ?? noReplyTo(sentCommand) };
  }

  private rejectCommandsInFlight(error: Error): void {
    this.connectionLostController.abort(error);
    this.connectionLostController = new AbortController();
    for (const commandInFlight of [...this.commandsInFlight]) {
      commandInFlight.reject(error);
    }
  }

  private rejectEveryCommand(error: Error): void {
    this.rejectCommandsInFlight(error);
    for (const heldCommand of [...this.heldCommands]) {
      this.heldCommands.delete(heldCommand);
      heldCommand.reject(error);
    }
  }
}

function noReplyTo(sentCommand: string): FakeReply {
  return () => Promise.reject(new Error(`this fake driver has no reply to ${sentCommand}`));
}

export type FakeRedisDrivers = {
  createDriver: CreateRedisDriver;
  createdDrivers: () => FakeRedisDriver[];
  onCreate: (prepare: (fakeRedisDriver: FakeRedisDriver) => void) => void;
};

export function createFakeRedisDrivers(): FakeRedisDrivers {
  const createdDrivers: FakeRedisDriver[] = [];
  let prepareDriver: (fakeRedisDriver: FakeRedisDriver) => void = () => undefined;
  return {
    createDriver: (_url, events) => {
      const fakeRedisDriver = new FakeRedisDriver(events);
      createdDrivers.push(fakeRedisDriver);
      prepareDriver(fakeRedisDriver);
      return fakeRedisDriver;
    },
    createdDrivers: () => [...createdDrivers],
    onCreate: (prepare) => {
      prepareDriver = prepare;
    },
  };
}

function missingFromTheInMemoryEngine(method: string): () => Promise<never> {
  return () => Promise.reject(new Error(`the in-memory engine has no ${method}`));
}

export function engineOnFakeDriver(engine: InMemoryEngine, client: RedisClient): Engine {
  const sentThroughTheClient =
    <Request, Reply>(method: string, call: (request: Request) => Promise<Reply>) =>
    (request: Request): Promise<Reply> =>
      client.send(fakeReplyCommandOf(method, () => call(request))) as Promise<Reply>;
  return {
    enqueue: sentThroughTheClient("enqueue", engine.enqueue),
    fetch: sentThroughTheClient("fetch", engine.fetch),
    renew: sentThroughTheClient("renew", engine.renew),
    settle: sentThroughTheClient("settle", engine.settle),
    promote: () => client.send(fakeReplyCommandOf("promote", () => engine.promote())) as ReturnType<Engine["promote"]>,
    saveStep: sentThroughTheClient("saveStep", engine.saveStep),
    readStep: sentThroughTheClient("readStep", engine.readStep),
    prune: sentThroughTheClient("prune", engine.prune),
    tick: sentThroughTheClient("tick", engine.tick),
    fire: sentThroughTheClient("fire", engine.fire),
    syncSchedules: sentThroughTheClient("syncSchedules", engine.syncSchedules),
    serverInstant: () =>
      client.send(fakeReplyCommandOf("serverInstant", () => engine.serverInstant())) as ReturnType<
        Engine["serverInstant"]
      >,
    makeDue: sentThroughTheClient("makeDue", engine.makeDue),
    fireNow: missingFromTheInMemoryEngine("fireNow"),
    replay: missingFromTheInMemoryEngine("replay"),
    busyRunCounts: missingFromTheInMemoryEngine("busyRunCounts"),
  };
}

export function wakeUpWaitOnFakeDriver(engine: InMemoryEngine, client: RedisClient): WaitForWakeUp {
  return (timeoutInMilliseconds: DurationInMilliseconds) =>
    client.send(
      fakeReplyCommandOf("BZPOPMIN", (connectionLostSignal) =>
        engine.waitForWakeUp(timeoutInMilliseconds, connectionLostSignal),
      ),
    ) as Promise<void>;
}
