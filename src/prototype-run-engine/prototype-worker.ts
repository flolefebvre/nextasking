import { randomUUID } from "node:crypto";
import { setTimeout as sleep } from "node:timers/promises";

import { CronExpressionParser } from "cron-parser";
import { Redis } from "ioredis";

import { RunEngine } from "./prototype-run-engine.js";
import type { Lease, SettleOutcome, Transition } from "./prototype-run-engine.js";

export class LeaseLostError extends Error {
  override readonly name = "LeaseLostError";
}

class SuspendSignal {
  readonly until: number;
  constructor(until: number) {
    this.until = until;
  }
}

export type HandlerContext = {
  runId: string;
  epoch: string;
  attempt: number;
  payload: unknown;
  signal: AbortSignal;
  step: <Value>(name: string, execute: () => Promise<Value>) => Promise<Value>;
  sleepUntil: (name: string, until: number) => Promise<void>;
};

export type Handler = (context: HandlerContext) => Promise<unknown>;

export type WorkerEvent =
  | { type: "started"; runId: string; epoch: string; attempt: number; at: number }
  | { type: "settled"; runId: string; epoch: string; kind: Transition["kind"]; outcome: SettleOutcome; at: number }
  | { type: "lease-lost"; runId: string; epoch: string; at: number }
  | { type: "worker-error"; message: string; at: number };

export type PrototypeWorkerOptions = {
  redisUrl: string;
  keyPrefix: string;
  handlers: Record<string, Handler>;
  concurrency?: number;
  leaseInMilliseconds?: number;
  tickInMilliseconds?: number;
  backoffInMilliseconds?: (attempt: number) => number;
  schedules?: { name: string; cron: string; task: string }[];
  onEvent?: (event: WorkerEvent) => void;
};

type InFlight = { lease: Lease; abort: AbortController; done: Promise<void> };

export class PrototypeWorker {
  readonly workerId = randomUUID();
  readonly redis: Redis;
  readonly engine: RunEngine;
  private readonly blockingRedis: Redis;
  private readonly options: Required<Omit<PrototypeWorkerOptions, "onEvent">> & Pick<PrototypeWorkerOptions, "onEvent">;
  private readonly inFlight = new Map<string, InFlight>();
  private running = false;
  private loop: Promise<void> | undefined;
  private timers: NodeJS.Timeout[] = [];
  private capacityFreed: () => void = () => undefined;
  private lastFiredBySchedule = new Map<string, number>();

  constructor(options: PrototypeWorkerOptions) {
    this.options = {
      concurrency: 10,
      leaseInMilliseconds: 1000,
      tickInMilliseconds: 50,
      backoffInMilliseconds: (attempt) => 200 * 2 ** (attempt - 1),
      schedules: [],
      ...options,
    };
    this.redis = new Redis(options.redisUrl, { maxRetriesPerRequest: null });
    this.blockingRedis = new Redis(options.redisUrl, { maxRetriesPerRequest: null });
    this.redis.on("error", () => undefined);
    this.blockingRedis.on("error", () => undefined);
    this.engine = new RunEngine(this.redis, options.keyPrefix);
  }

  private emit(event: WorkerEvent): void {
    this.options.onEvent?.(event);
  }

  private reportError(error: unknown): void {
    this.emit({ type: "worker-error", message: (error as Error).message, at: Date.now() });
  }

  start(): void {
    this.running = true;
    this.loop = this.fetchLoop();
    const every = (milliseconds: number, task: () => Promise<void>) => {
      let busy = false;
      this.timers.push(
        setInterval(() => {
          if (busy) {
            return;
          }
          busy = true;
          task()
            .catch((error: unknown) => this.reportError(error))
            .finally(() => {
              busy = false;
            });
        }, milliseconds),
      );
    };
    every(Math.floor(this.options.leaseInMilliseconds / 3), () => this.renewAll());
    every(this.options.tickInMilliseconds, () => this.tick());
  }

  private async fetchLoop(): Promise<void> {
    while (this.running) {
      if (this.inFlight.size >= this.options.concurrency) {
        await new Promise<void>((resolve) => {
          this.capacityFreed = resolve;
        });
        continue;
      }
      let lease: Lease | undefined;
      try {
        lease = await this.engine.fetch(randomUUID(), this.options.leaseInMilliseconds, this.workerId);
      } catch (error) {
        if (this.running) {
          this.reportError(error);
          await sleep(100);
        }
        continue;
      }
      if (lease !== undefined) {
        this.execute(lease);
        continue;
      }
      try {
        await this.engine.waitForMarker(this.blockingRedis, 1);
      } catch {
        if (this.running) {
          await sleep(50);
        }
      }
    }
  }

  private execute(lease: Lease): void {
    const abort = new AbortController();
    const flightKey = `${lease.runId}|${lease.epoch}`;
    const done = this.attempt(lease, abort).finally(() => {
      this.inFlight.delete(flightKey);
      this.capacityFreed();
    });
    this.inFlight.set(flightKey, { lease, abort, done });
  }

  private async attempt(lease: Lease, abort: AbortController): Promise<void> {
    const { runId, epoch, attempt } = lease;
    this.emit({ type: "started", runId, epoch, attempt, at: Date.now() });
    const handler = this.options.handlers[lease.task];
    let transition: Transition;
    if (handler === undefined) {
      transition = { kind: "handback" };
    } else {
      try {
        const result = await handler(this.contextOf(lease, abort.signal));
        transition = { kind: "complete", result: JSON.stringify(result ?? null) };
      } catch (error) {
        if (error instanceof SuspendSignal) {
          transition = { kind: "suspend", untilEpochMilliseconds: error.until };
        } else if (error instanceof LeaseLostError || abort.signal.aborted) {
          this.emit({ type: "lease-lost", runId, epoch, at: Date.now() });
          return;
        } else if (attempt < lease.maxAttempts) {
          transition = { kind: "retry", delayInMilliseconds: this.options.backoffInMilliseconds(attempt) };
        } else {
          transition = { kind: "fail", reason: (error as Error).message };
        }
      }
    }
    try {
      const outcome = await this.engine.settle(runId, epoch, transition);
      if (outcome === "stale") {
        this.emit({ type: "lease-lost", runId, epoch, at: Date.now() });
        return;
      }
      this.emit({ type: "settled", runId, epoch, kind: transition.kind, outcome, at: Date.now() });
    } catch (error) {
      this.reportError(error);
    }
  }

  private contextOf(lease: Lease, signal: AbortSignal): HandlerContext {
    const { runId, epoch } = lease;
    const savedStep = async (name: string): Promise<string | null> => {
      const { held, value } = await this.engine.readStep(runId, epoch, name);
      if (!held) {
        throw new LeaseLostError(`lease lost on ${runId} before step ${name}`);
      }
      return value;
    };
    const step = async <Value>(name: string, execute: () => Promise<Value>): Promise<Value> => {
      const saved = await savedStep(name);
      if (saved !== null) {
        return JSON.parse(saved) as Value;
      }
      const value = await execute();
      if (!(await this.engine.saveStep(runId, epoch, name, JSON.stringify(value ?? null)))) {
        throw new LeaseLostError(`lease lost on ${runId} at step ${name}`);
      }
      return value;
    };
    return {
      runId,
      epoch,
      attempt: lease.attempt,
      payload: JSON.parse(lease.payload) as unknown,
      signal,
      step,
      sleepUntil: async (name, until) => {
        if ((await savedStep(`sleep:${name}`)) !== null) {
          return;
        }
        if (!(await this.engine.saveStep(runId, epoch, `sleep:${name}`, String(until)))) {
          throw new LeaseLostError(`lease lost on ${runId} at sleep ${name}`);
        }
        throw new SuspendSignal(until);
      },
    };
  }

  private async renewAll(): Promise<void> {
    const flights = [...this.inFlight.values()];
    const renewed = await this.engine.renew(
      this.options.leaseInMilliseconds,
      flights.map(({ lease }) => lease),
    );
    flights.forEach((flight, index) => {
      if (renewed[index] === false && !flight.abort.signal.aborted) {
        flight.abort.abort(new LeaseLostError(`lease lost on ${flight.lease.runId}`));
      }
    });
  }

  private async tick(): Promise<void> {
    await this.engine.promoteDue();
    const now = Date.now();
    for (const { name, cron, task } of this.options.schedules) {
      const instant = CronExpressionParser.parse(cron, { tz: "UTC", currentDate: new Date(now) }).prev().getTime();
      if (instant > (this.lastFiredBySchedule.get(name) ?? now - 5000)) {
        await this.engine.cronTick(name, instant, task, { scheduledAt: instant });
        this.lastFiredBySchedule.set(name, instant);
      }
    }
  }

  async close({ waitForInFlight = true }: { waitForInFlight?: boolean } = {}): Promise<void> {
    this.running = false;
    for (const timer of this.timers) {
      clearInterval(timer);
    }
    this.capacityFreed();
    this.blockingRedis.disconnect();
    await this.loop;
    if (waitForInFlight) {
      await Promise.all([...this.inFlight.values()].map(({ done }) => done));
    }
    this.redis.disconnect();
  }
}
