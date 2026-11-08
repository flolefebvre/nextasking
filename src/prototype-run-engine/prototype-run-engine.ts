import { readFileSync } from "node:fs";

import type { Redis } from "ioredis";

const SCRIPT_NAMES = [
  "enqueue",
  "fetch",
  "renew",
  "settle",
  "promote-due",
  "save-step",
  "read-step",
  "cron-tick",
] as const;

type ScriptName = (typeof SCRIPT_NAMES)[number];

const LIB = readFileSync(new URL("./prototype-lua/lib.lua", import.meta.url), "utf8");

function commandNameOf(scriptName: ScriptName): string {
  return `prototype_${scriptName.replace("-", "_")}`;
}

export type Transition =
  | { kind: "complete"; result: string }
  | { kind: "fail"; reason: string }
  | { kind: "retry"; delayInMilliseconds: number }
  | { kind: "suspend"; untilEpochMilliseconds: number }
  | { kind: "handback" };

export type SettleOutcome = "ok" | "already" | "stale";

export type Lease = {
  runId: string;
  epoch: string;
  task: string;
  payload: string;
  attempt: number;
  maxAttempts: number;
};

export type EnqueueOptions = {
  runId: string;
  task: string;
  payload?: unknown;
  concurrencyScope?: string;
  concurrencyLimit?: number;
  delayInMilliseconds?: number;
  maxAttempts?: number;
};

export const STALL_LIMIT = 3;

function argumentOf(transition: Transition): string {
  switch (transition.kind) {
    case "complete":
      return transition.result;
    case "fail":
      return transition.reason;
    case "retry":
      return String(transition.delayInMilliseconds);
    case "suspend":
      return String(transition.untilEpochMilliseconds);
    case "handback":
      return "";
  }
}

export class RunEngine {
  readonly redis: Redis;
  readonly prefix: string;
  private readonly sources: string[] = [];

  constructor(redis: Redis, keyPrefix: string) {
    this.redis = redis;
    this.prefix = `{${keyPrefix}}:`;
    for (const scriptName of SCRIPT_NAMES) {
      const lua = readFileSync(new URL(`./prototype-lua/${scriptName}.lua`, import.meta.url), "utf8");
      this.sources.push(`${LIB}\n${lua}`);
      redis.defineCommand(commandNameOf(scriptName), { numberOfKeys: 0, lua: `${LIB}\n${lua}` });
    }
  }

  async loadScripts(): Promise<void> {
    await Promise.all(this.sources.map((source) => this.redis.script("LOAD", source)));
  }

  key(suffix: string): string {
    return `${this.prefix}${suffix}`;
  }

  private async call<Result>(scriptName: ScriptName, ...args: (string | number)[]): Promise<Result> {
    const command = (this.redis as unknown as Record<string, (...a: (string | number)[]) => Promise<Result>>)[
      commandNameOf(scriptName)
    ] as (...a: (string | number)[]) => Promise<Result>;
    return command.call(this.redis, this.prefix, ...args);
  }

  async enqueue(options: EnqueueOptions): Promise<boolean> {
    const added = await this.call<number>(
      "enqueue",
      options.runId,
      options.task,
      JSON.stringify(options.payload ?? null),
      options.concurrencyScope ?? "",
      options.concurrencyLimit ?? 0,
      options.delayInMilliseconds ?? 0,
      options.maxAttempts ?? 3,
    );
    return added === 1;
  }

  async fetch(token: string, leaseInMilliseconds: number, workerId: string): Promise<Lease | undefined> {
    const reply = await this.call<[string, string, string, string, string, string] | null>(
      "fetch",
      token,
      leaseInMilliseconds,
      workerId,
    );
    if (reply === null) {
      return undefined;
    }
    const [runId, epoch, task, payload, attempt, maxAttempts] = reply;
    return { runId, epoch, task, payload, attempt: Number(attempt), maxAttempts: Number(maxAttempts) };
  }

  async renew(leaseInMilliseconds: number, leases: readonly { runId: string; epoch: string }[]): Promise<boolean[]> {
    if (leases.length === 0) {
      return [];
    }
    const renewed = await this.call<number[]>(
      "renew",
      leaseInMilliseconds,
      ...leases.flatMap(({ runId, epoch }) => [runId, epoch]),
    );
    return renewed.map((flag) => flag === 1);
  }

  async settle(runId: string, epoch: string, transition: Transition): Promise<SettleOutcome> {
    return this.call<SettleOutcome>("settle", runId, epoch, transition.kind, argumentOf(transition));
  }

  async promoteDue(): Promise<{ promoted: number; reclaimed: number }> {
    const [promoted, reclaimed] = await this.call<[number, number]>("promote-due", STALL_LIMIT);
    return { promoted, reclaimed };
  }

  async saveStep(runId: string, epoch: string, stepName: string, value: string): Promise<boolean> {
    return (await this.call<number>("save-step", runId, epoch, stepName, value)) === 1;
  }

  async readStep(runId: string, epoch: string, stepName: string): Promise<{ held: boolean; value: string | null }> {
    const [held, value] = await this.call<[number, string | null | undefined]>("read-step", runId, epoch, stepName);
    return { held: held === 1, value: value ?? null };
  }

  async savedStep(runId: string, stepName: string): Promise<string | null> {
    return this.redis.hget(this.key(`steps:${runId}`), stepName);
  }

  async cronTick(scheduleName: string, instant: number, task: string, payload: unknown): Promise<boolean> {
    return (await this.call<number>("cron-tick", scheduleName, instant, task, JSON.stringify(payload), 3)) === 1;
  }

  async waitForMarker(blockingRedis: Redis, timeoutInSeconds: number): Promise<boolean> {
    return (await blockingRedis.bzpopmin(this.key("marker"), timeoutInSeconds)) !== null;
  }

  async run(runId: string): Promise<Record<string, string>> {
    return this.redis.hgetall(this.key(`run:${runId}`));
  }
}
