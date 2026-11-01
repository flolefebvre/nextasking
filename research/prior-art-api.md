# Prior-art API survey

Question (issue #6): how do the main background-work libraries shape their developer-facing API, and what can nextasking learn from each? This file records facts and contrasts. It does not recommend an API.

Read on 2026-11-01. Versions and the sources pinned for each library:

| Library          | Version read                                                 | Primary sources                                                                                                                                                                                                 |
| ---------------- | ------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Inngest (TS SDK) | `inngest@4.26.2`, `@inngest/test@1.0.0`                      | [inngest/inngest-js@204e05e](https://github.com/inngest/inngest-js/tree/204e05e1aed3340f9991e72b60680e8375678703); v4 docs in [inngest/website](https://github.com/inngest/website) via ctx7 `/inngest/website` |
| Laravel          | 13.x                                                         | [laravel/docs@960eedf](https://github.com/laravel/docs/tree/960eedf0911155ad16326ade49c0ea5d18eda0b8) (`events.md`, `queues.md`, `scheduling.md`)                                                               |
| Trigger.dev      | `@trigger.dev/sdk@4.8.3`                                     | [triggerdotdev/trigger.dev@41ad199](https://github.com/triggerdotdev/trigger.dev/tree/41ad1995b30573a9b55cfaab80c3293b7c35c1f8) (`docs/`, `packages/`)                                                          |
| Hatchet (TS SDK) | `@hatchet-dev/typescript-sdk@1.46.0` (engine tag `v0.116.5`) | [hatchet-dev/hatchet@964f6bc](https://github.com/hatchet-dev/hatchet/tree/964f6bca03f58578f81c06b11f572aad1dbe1785) (`sdks/typescript/`, `frontend/docs/content/docs/v1/`)                                      |
| graphile-worker  | `graphile-worker@0.18.0`                                     | [graphile/worker@7a2405b](https://github.com/graphile/worker/tree/7a2405bdd3b6322fb6b0d48dc0543bcc89166930) (`src/`, `website/docs/`)                                                                           |
| pg-boss          | `pg-boss@12.38.0`                                            | [timgit/pg-boss@71ea068](https://github.com/timgit/pg-boss/tree/71ea068b144c75b014a1eb90327aaa4a92da1cf2) (`src/`, `docs/api/`)                                                                                 |

Each was the latest version on npm on the reading date. Short link names below (`inngest-js:`, `trigger.dev:`, …) point into those pinned trees.

## Summary

- **Definition is a value you hold.** Inngest, Trigger.dev and Hatchet all return an object from a factory (`createFunction`, `task`, `hatchet.task`) and dispatch through it (`myTask.trigger(...)`, `simple.run(...)`), so the name and payload are checked by the compiler. Laravel does the same with a class (`ProcessPodcast::dispatch(...)`). graphile-worker and pg-boss define work by a string name plus a handler and dispatch by string.
- **Dispatch by string name exists everywhere, and is weakly typed everywhere.** An unknown name compiles in every TypeScript library surveyed, except Trigger.dev's `tasks.trigger<typeof t>("id", …)`, which ties the string to the task's literal `id`. graphile-worker types `addJob` through a global `GraphileWorker.Tasks` interface, but still accepts any other string with an `unknown` payload.
- **Runtime validation is opt-in and placed differently.** Inngest validates `eventType(...).create()` payloads at send time and every event again before the run. Trigger.dev's `schemaTask` validates at `myTask.trigger()` and before each run, but not on `tasks.trigger<typeof t>()`. Hatchet's TS `inputValidator` only produces a JSON Schema for its dashboard. graphile-worker tells users to write assertion functions. pg-boss has nothing.
- **Events with fan-out to listeners are first-class in four of six.** Inngest and Hatchet trigger every function or workflow whose trigger matches the event name. Laravel registers listeners per event class, and each queued listener becomes its own job. pg-boss has `publish`/`subscribe` from an event to queues. Trigger.dev v4 and graphile-worker have no event primitive: you fan out by triggering several tasks yourself.
- **Options sit on the definition in five of six.** That covers retries, backoff, concurrency and schedule. graphile-worker is the exception: options are per job at dispatch (`TaskSpec`), backoff is a fixed formula, and concurrency belongs to the worker. Most libraries also let some options be overridden per dispatch (delay, idempotency or debounce key, concurrency key, priority).
- **Testing splits two ways.** Some libraries fake the dispatcher and assert on what was dispatched; only Laravel does this (`Event::fake`, `Queue::fake`, `Bus::fake`). Others run the real engine in-process and watch it: pg-boss spies plus `TestClock`, graphile-worker `runOnce`, Hatchet embedded mode. Inngest and Trigger.dev provide harnesses that execute a function in isolation with mocked steps or context. Neither offers a fake for the app-side `send`/`trigger` call.
- **Vocabulary does not agree.** The unit of code is a _function_ (Inngest), _job_ plus _listener_ (Laravel), _task_ (Trigger.dev, Hatchet, graphile-worker), or a _queue_ with a _worker_ (pg-boss). One execution is a _run_ in Inngest, Trigger.dev and Hatchet, and a _job_ in Laravel, graphile-worker and pg-boss.

## Inngest (TypeScript SDK v4)

**Define.** An event is a value made with `eventType(name, { schema })`. The schema is any Standard Schema (Zod, Valibot, …) or `staticSchema<T>()` for type only. A function is `inngest.createFunction(options, handler)`, with its triggers in `options.triggers` ([inngest-js: `components/triggers/triggers.ts` L242–L285](https://github.com/inngest/inngest-js/blob/204e05e1aed3340f9991e72b60680e8375678703/packages/inngest/src/components/triggers/triggers.ts#L242-L285); [docs: v4 triggers](https://github.com/inngest/website/blob/main/pages/docs/reference/typescript/v4/functions/triggers.mdx)):

```ts
const orderPlaced = eventType("shop/order.placed", {
  schema: z.object({ orderId: z.string(), total: z.number() }),
});

inngest.createFunction({ id: "process-order", triggers: [orderPlaced] }, async ({ event, step }) => {
  // event.data is typed as { orderId: string; total: number }
});
```

v4 removed the v3 client-wide `EventSchemas` registry in favour of these decentralised `eventType()` values ([docs: v4 release blog](https://github.com/inngest/website/blob/main/content/blog/typescript-sdk-v4.0.mdx)). Schemas with transforms are rejected at the type level (`AssertNoTransform`, triggers.ts L215–L231).

**Dispatch.** `inngest.send(payload)` is typed as `SendEventPayload`, a plain `{ name: string; data?: any }` ([inngest-js: `helpers/types.ts` L68](https://github.com/inngest/inngest-js/blob/204e05e1aed3340f9991e72b60680e8375678703/packages/inngest/src/helpers/types.ts#L68); [`components/Inngest.ts` L1006](https://github.com/inngest/inngest-js/blob/204e05e1aed3340f9991e72b60680e8375678703/packages/inngest/src/components/Inngest.ts#L1006)). An unknown name or a wrong payload therefore compiles when you send a literal object. The typed path is `orderPlaced.create(data)`: its `data` parameter is the schema type, and `send` calls `.validate()` on the result before sending ([Inngest.ts L1119–L1124](https://github.com/inngest/inngest-js/blob/204e05e1aed3340f9991e72b60680e8375678703/packages/inngest/src/components/Inngest.ts#L1119-L1124)):

```ts
await inngest.send(orderPlaced.create({ orderId: "order_123", total: 99.99 }));
```

On the worker side, every incoming event is validated against the schema of its matching trigger before the handler runs. An event whose name matches no trigger throws `Event not found in triggers` ([`components/triggers/utils.ts` L130–L186](https://github.com/inngest/inngest-js/blob/204e05e1aed3340f9991e72b60680e8375678703/packages/inngest/src/components/triggers/utils.ts#L130-L186)). Function-to-function calls go through `step.invoke(id, { function, data })`. In v4, `function` must be an imported function or a `referenceFunction({ functionId, appId?, schemas? })`. Raw string IDs were removed "catching ID mistakes at compile time" ([docs: v3→v4 migration](https://github.com/inngest/website/blob/main/pages/docs/reference/typescript/v4/migrations/v3-to-v4.mdx)).

**Listen / fan-out.** Any number of functions may list the same event in `triggers`, and each runs, and retries, on its own ([docs: fan-out pattern](https://github.com/inngest/website/blob/main/shared/Patterns/_patterns/running-functions-in-parallel.mdx)). A trigger may add a CEL filter, `{ event: orderPlaced, if: "event.data.total > 100" }`, and wildcard event names are matched (utils.ts L170–L180).

**Options** sit on the `createFunction` options object ([`components/InngestFunction.ts` L412–L700](https://github.com/inngest/inngest-js/blob/204e05e1aed3340f9991e72b60680e8375678703/packages/inngest/src/components/InngestFunction.ts#L412-L700)): `retries` (a literal `0`–`20`, default `3`, "maximum number of retries for all steps across this function"), `concurrency` (a number or up to two `{ key, limit, scope }`), `throttle`, `rateLimit`, `debounce`, `priority`, `batchEvents`, `idempotency`, `singleton`, `timeouts`, `cancelOn`, `onFailure`, `middleware`. Schedules are triggers too: `triggers: [cron("0 0 * * *")]`. Backoff is not configurable: the engine's default policy applies, and a handler can throw `RetryAfterError(msg, "30s")` or `NonRetriableError` ([docs: Inngest errors](https://github.com/inngest/website/blob/main/pages/docs/features/inngest-functions/error-retries/inngest-errors.mdx)). Durable steps live on the handler context: `step.run`, `step.sleep`, `step.waitForEvent`, `step.invoke`, `step.sendEvent`. Locally, functions are served to the Inngest Dev Server, a separate process started with `npx inngest-cli@latest dev` ([docs: Next.js quick start](https://github.com/inngest/website/blob/main/pages/docs/getting-started/nextjs-quick-start.mdx)).

**Testing.** `@inngest/test` provides `InngestTestEngine({ function })`. `t.execute()` runs a function to completion and `t.executeStep(id)` stops at a step. You can mock `events` and `steps`, and `ctx.step.*` are Jest-compatible spies, so `step.sendEvent` calls made inside a function can be asserted ([inngest-js: `packages/test/README.md`](https://github.com/inngest/inngest-js/blob/204e05e1aed3340f9991e72b60680e8375678703/packages/test/README.md)):

```ts
const t = new InngestTestEngine({ function: helloWorld });
const { result, ctx } = await t.execute({ events: [{ name: "demo/event.sent", data: { message: "Hi!" } }] });
expect(ctx.step.run).toHaveBeenCalledWith("my-step", expect.any(Function));
```

The package ships no fake for an app-side `inngest.send()` (its `src/` has no send mock: `InngestTestEngine.ts`, `spy.ts`, `util.ts`).

**Vocabulary:** event, function, trigger, step, run, invoke, app.

## Laravel 13 (events, listeners, queued jobs)

**Define.** An event is a plain class whose constructor holds the payload. A listener is a class whose `handle` method type-hints the event. A job is a class that implements `ShouldQueue` and has a `handle` method ([laravel/docs `events.md` L251–L311](https://github.com/laravel/docs/blob/960eedf0911155ad16326ade49c0ea5d18eda0b8/events.md?plain=1#L251-L311), [`queues.md` L221–L259](https://github.com/laravel/docs/blob/960eedf0911155ad16326ade49c0ea5d18eda0b8/queues.md?plain=1#L221-L259)):

```php
class OrderShipped { use Dispatchable, SerializesModels;
    public function __construct(public Order $order) {} }

class SendShipmentNotification implements ShouldQueue {
    public function handle(OrderShipped $event): void { /* $event->order */ } }
```

**Dispatch** is by class: `OrderShipped::dispatch($order)`, `ProcessPodcast::dispatch($podcast)`, plus `dispatchIf`, `dispatchUnless`, `dispatchSync` and `->delay(...)`. The arguments go to the constructor (events.md L919–L959, queues.md L1053–L1170). PHP has no compile step. A misspelt class or a mistyped constructor argument fails when the code runs (a `TypeError` for declared parameter types, see [PHP manual: type declarations](https://www.php.net/manual/en/language.types.declarations.php)). Events can also be dispatched after commit (`ShouldDispatchAfterCommit`), deferred (`Event::defer(fn)`), or dispatched with a job chain (`Bus::chain([...])->dispatch()`) or a batch (events.md L963–L1024, queues.md L1280–L1300).

**Listen / fan-out.** Listeners are found automatically by scanning `app/Listeners` for `handle`/`__invoke` methods and reading the event type-hint, union types included. They can also be registered with `Event::listen(Event::class, Listener::class)`, as closures (`Event::listen(fn (OrderShipped $e) => ...)`), as queued closures (`queueable(fn ...)`), as wildcards (`Event::listen('event.*', fn (string $name, array $data) => ...)`), or as subscriber classes that return an `[Event::class => 'method']` map (events.md L61–L249, L1027–L1110). Each listener runs in turn. A listener that implements `ShouldQueue` is pushed as its own queued job, so fan-out to queued listeners yields one job per listener. Returning `false` from a synchronous listener stops propagation (events.md L313–L338).

**Options** are PHP attributes or methods on the job or listener class: `#[Tries(5)]`, `#[Backoff([1, 5, 10])]`, `#[Timeout(120)]`, `#[MaxExceptions(3)]`, `#[UniqueFor(3600)]` with `ShouldBeUnique` and `uniqueId()`, `#[DebounceFor(30, maxWait: 120)]` with `debounceId()`, `#[Connection]`/`#[Queue]`/`#[Delay]`. Dynamic variants are methods (`backoff()`, `tries()`, `viaQueue()`) (queues.md L349–L520, L1557–L1780, L2905–L2950). Rate limiting and overlap control are job middleware: `middleware(): [new RateLimited('backups'), new WithoutOverlapping($this->user->id)]` (queues.md L619–L740). Schedules live outside the job: `Schedule::job(new Heartbeat)->everyFiveMinutes()` ([`scheduling.md` L112](https://github.com/laravel/docs/blob/960eedf0911155ad16326ade49c0ea5d18eda0b8/scheduling.md?plain=1#L112)). The default is one attempt per job unless `Tries` or `--tries` raises it (queues.md L1580–L1590).

**Testing.** Facade fakes record dispatches and do not run them (events.md L1138–L1330, queues.md L3229–L3560):

```php
Event::fake();            // or Event::fake([OrderCreated::class]) / ->except([...]) / Event::fakeFor(fn)
// ... code under test ...
Event::assertDispatched(OrderShipped::class, fn (OrderShipped $e) => $e->order->id === $order->id);
Event::assertListening(OrderShipped::class, SendShipmentNotification::class);

Queue::fake();
Queue::assertPushedOn('queue-name', ShipOrder::class);
Bus::fake(); Bus::assertChained([...]); Bus::assertBatched(fn (PendingBatch $b) => ...);

$job = (new ProcessPodcast)->withFakeQueueInteractions(); $job->handle(); $job->assertReleased(delay: 30);
```

There is also a `sync` queue driver that runs jobs immediately in-process, and a `deferred` connection that runs them after the response (queues.md L73, L1172–L1188).

**Vocabulary:** event, listener, subscriber, job, queue, connection, worker, chain, batch, middleware, dispatch.

## Trigger.dev v4

**Define.** `task({ id, run, ...options })` declares the payload type on the `run` parameter. `schemaTask({ id, schema, run })` takes a schema (Zod or any parser function) and infers the payload from it ([trigger.dev: `docs/tasks/overview.mdx`](https://github.com/triggerdotdev/trigger.dev/blob/41ad1995b30573a9b55cfaab80c3293b7c35c1f8/docs/tasks/overview.mdx), [`docs/tasks/schemaTask.mdx`](https://github.com/triggerdotdev/trigger.dev/blob/41ad1995b30573a9b55cfaab80c3293b7c35c1f8/docs/tasks/schemaTask.mdx)):

```ts
export const helloWorld = task({
  id: "hello-world",
  run: async (payload: { message: string }) => {
    /* ... */
  },
});
```

**Dispatch.** By handle: `helloWorld.trigger(payload, options)`, plus `batchTrigger`, `triggerAndWait` and `batchTriggerAndWait`. By name: `tasks.trigger<typeof emailSequence>("email-sequence", payload)` with a type-only import, which the docs recommend in Next.js to keep task code out of the app bundle ([`docs/triggering.mdx` L6–L98](https://github.com/triggerdotdev/trigger.dev/blob/41ad1995b30573a9b55cfaab80c3293b7c35c1f8/docs/triggering.mdx#L6-L98)). The `id` parameter is `TaskIdentifier<TTask>`, which resolves to the task's literal `id`, so with the generic a wrong name does not compile ([`packages/trigger-sdk/src/v3/shared.ts` L573–L578](https://github.com/triggerdotdev/trigger.dev/blob/41ad1995b30573a9b55cfaab80c3293b7c35c1f8/packages/trigger-sdk/src/v3/shared.ts#L573-L578), [`packages/core/src/v3/types/tasks.ts` L762–L763](https://github.com/triggerdotdev/trigger.dev/blob/41ad1995b30573a9b55cfaab80c3293b7c35c1f8/packages/core/src/v3/types/tasks.ts#L762-L763)). Without the generic, `TTask` defaults to `AnyTask` and any string compiles. A `schemaTask` validates on `myTask.trigger()` but not on `tasks.trigger<typeof myTask>()`. It validates again before every run, and fails without retry (`TaskPayloadParsedError`) when the payload does not match (schemaTask.mdx):

```ts
await myTask.trigger({ name: "Alice", age: "oops" }); // throws
await tasks.trigger<typeof myTask>("my-task", { name: "Alice", age: "oops" }); // does not throw
```

**Listen / fan-out.** The v4 docs list only `trigger`/`batch*` functions and schedules as entry points, with no event or subscription primitive (triggering.mdx L6–L26). Fan-out to several tasks is explicit: `batch.trigger([{ id, payload }, …])` or `batch.triggerByTask([{ task, payload }, …])` (triggering.mdx L170–L192, L583–L618).

**Options.** On the task: `retry: { maxAttempts, factor, minTimeoutInMs, maxTimeoutInMs, randomize }` (3 attempts by default), `concurrency: { total, perKey }` or a shared `concurrencyLimit({ name, total })` value (the older `queue.concurrencyLimit` is deprecated), `machine`, `maxDuration`, `ttl`, and lifecycle hooks (`onStartAttempt`, `onSuccess`, `onFailure`, `onComplete`, `catchError`, `onCancel`, `middleware`) (overview.mdx, [`docs/concurrency.mdx`](https://github.com/triggerdotdev/trigger.dev/blob/41ad1995b30573a9b55cfaab80c3293b7c35c1f8/docs/concurrency.mdx)). Per trigger, in the third argument: `delay`, `ttl`, `idempotencyKey`, `idempotencyKeyTTL`, `debounce: { key, delay }`, `queue`, `concurrencyKey`, `maxAttempts`, `priority`, `tags`, `metadata`, `machine` (triggering.mdx L680–L1140). Schedules are a separate factory, `schedules.task({ id, cron, run })`, which receives a fixed payload (`timestamp`, `lastTimestamp`, `timezone`, `scheduleId`, `externalId`, `upcoming`). Schedules can also be attached imperatively ([`docs/tasks/scheduled.mdx`](https://github.com/triggerdotdev/trigger.dev/blob/41ad1995b30573a9b55cfaab80c3293b7c35c1f8/docs/tasks/scheduled.mdx)). Durable waits are `triggerAndWait`, `wait.for` and `wait.forToken`, with no `step.run` wrapper ([`docs/wait.mdx`](https://github.com/triggerdotdev/trigger.dev/blob/41ad1995b30573a9b55cfaab80c3293b7c35c1f8/docs/wait.mdx)). Locally, `trigger.dev dev` runs tasks against the platform ([`docs/cli-dev-commands.mdx`](https://github.com/triggerdotdev/trigger.dev/blob/41ad1995b30573a9b55cfaab80c3293b7c35c1f8/docs/cli-dev-commands.mdx)).

**Testing.** The documented general path is the dashboard's "Test" action on a task, against a dev or deployed environment ([`docs/run-tests.mdx`](https://github.com/triggerdotdev/trigger.dev/blob/41ad1995b30573a9b55cfaab80c3293b7c35c1f8/docs/run-tests.mdx)). For offline runs, `runInMockTaskContext` (re-exported from `@trigger.dev/sdk/ai/test`) drives `myTask.fns.run(payload, { ctx, signal })` with mocked streams and runtime. It is documented under the AI-chat testing page ([`docs/ai-chat/testing.mdx` L648–L676](https://github.com/triggerdotdev/trigger.dev/blob/41ad1995b30573a9b55cfaab80c3293b7c35c1f8/docs/ai-chat/testing.mdx#L648-L676)). There is no fake for asserting `trigger()` calls from app code.

**Vocabulary:** task, run, attempt, trigger, batch, queue, concurrency key, schedule, waitpoint, machine.

## Hatchet (TypeScript SDK v1)

**Define.** `hatchet.task({ name, fn, ...options })` declares a standalone task. `hatchet.workflow<I, O>({ name, ... })` declares a workflow, with tasks attached through `.task({ name, fn, parents })` (a DAG) or `.durableTask(...)`. The payload type comes from the `fn` parameter or explicit generics. An optional `inputValidator` (a Zod schema) may be given ([hatchet: `sdks/typescript/src/v1/examples/simple/workflow.ts`, `zod.ts`](https://github.com/hatchet-dev/hatchet/tree/964f6bca03f58578f81c06b11f572aad1dbe1785/sdks/typescript/src/v1/examples/simple)):

```ts
export const simple = hatchet.task({
  name: "simple",
  retries: 3,
  fn: async (input: { Message: string }) => ({ TransformedMessage: input.Message.toLowerCase() }),
});
```

The TS `inputValidator` is documented in source as "a JSON Schema is generated and sent to the Hatchet backend, which can be used on the dashboard for autocomplete" ([`v1/declaration.ts` L211–L215](https://github.com/hatchet-dev/hatchet/blob/964f6bca03f58578f81c06b11f572aad1dbe1785/sdks/typescript/src/v1/declaration.ts#L211-L215)). Its only use in the SDK is `z.toJSONSchema` at registration ([`v1/client/worker/workflow-proto.ts` L229–L232](https://github.com/hatchet-dev/hatchet/blob/964f6bca03f58578f81c06b11f572aad1dbe1785/sdks/typescript/src/v1/client/worker/workflow-proto.ts#L229-L232)). The general docs say the SDKs support "type-checked and runtime-validated input" ([`docs/v1/tasks.mdx`](https://github.com/hatchet-dev/hatchet/blob/964f6bca03f58578f81c06b11f572aad1dbe1785/frontend/docs/content/docs/v1/tasks.mdx)). No input parse call was found in the TS SDK at this version.

**Dispatch.** By handle: `simple.run(input)` waits for the result, and `simple.runNoWait(input)`, `simple.schedule(date, input)` and `simple.cron(name, expr, input)` do not. By name: `hatchet.run<I, O>(workflowOrName, input)`, where `I` defaults to `UnknownInputType` and is not tied to the string ([`v1/client/client.ts` L444–L485](https://github.com/hatchet-dev/hatchet/blob/964f6bca03f58578f81c06b11f572aad1dbe1785/sdks/typescript/src/v1/client/client.ts#L444-L485)). For services that cannot import the definition, the docs recommend a "stub": `hatchet.workflow<I, O>({ name: "simple" })` with no function, which keeps the run methods typed but repeats the types by hand ([`docs/v1/inter-service-triggering.mdx`](https://github.com/hatchet-dev/hatchet/blob/964f6bca03f58578f81c06b11f572aad1dbe1785/frontend/docs/content/docs/v1/inter-service-triggering.mdx), `examples/simple/stub-workflow.ts`).

**Events / fan-out.** `hatchet.events.push<T>("simple-event:create", payload)`. Here `T` is a caller-asserted generic, and the key is any string. A workflow subscribes with `onEvents: ["simple-event:create"]` (or `on: { event }`), and "multiple tasks can be triggered by the same event". Wildcards (`subscription:*`) and CEL filters scoped per tenant (`defaultFilters`, a `scope` on push) narrow which workflows run. An event that arrives before a task is registered does not run it ([`docs/v1/events.mdx`](https://github.com/hatchet-dev/hatchet/blob/964f6bca03f58578f81c06b11f572aad1dbe1785/frontend/docs/content/docs/v1/events.mdx), `examples/on_event/`):

```ts
export const lower = hatchet.workflow<Input, LowerOutput>({ name: "lower", onEvents: ["simple-event:create"] });
await hatchet.events.push<Input>("simple-event:create", { Message: "hello", ShouldSkip: false });
```

Durable tasks can also suspend on an event: `ctx.waitForEvent(key, expr)`, `ctx.sleepFor(...)`, `ctx.waitFor(conditions)` (`examples/durable/workflow.ts`).

**Options** sit on the task or workflow definition ([`v1/task.ts`](https://github.com/hatchet-dev/hatchet/blob/964f6bca03f58578f81c06b11f572aad1dbe1785/sdks/typescript/src/v1/task.ts)): `retries`, `backoff: { factor, maxSeconds }`, `timeout`/`executionTimeout`/`scheduleTimeout`, `concurrency: { expression: "input.GroupKey", maxRuns, limitStrategy }` (CEL key; strategies such as `GROUP_ROUND_ROBIN` and cancel-in-progress), `rateLimits`, `desiredWorkerLabels`, `batch`, and at workflow level `onCrons`/`on.cron`, `defaultPriority` and `idempotency`. A handler can throw `NonRetryableError` or `RetryAfterError(msg, { after })`.

**Testing.** The TS SDK has no mock-run helper. Python has `.mock_run()`, per [`docs/v1/from-celery-to-hatchet.mdx` L695](https://github.com/hatchet-dev/hatchet/blob/964f6bca03f58578f81c06b11f572aad1dbe1785/frontend/docs/content/docs/v1/from-celery-to-hatchet.mdx), and a grep of `sdks/typescript/src` finds no equivalent. The documented path is embedded mode: `HatchetEmbeddedClient.init()` from a separate entry point downloads and spawns an engine sidecar with embedded Postgres, "to test workers end to end in ephemeral CI environments" ([`docs/v1/embedded.mdx`](https://github.com/hatchet-dev/hatchet/blob/964f6bca03f58578f81c06b11f572aad1dbe1785/frontend/docs/content/docs/v1/embedded.mdx)).

**Vocabulary:** task (standalone or in a workflow), workflow, DAG, durable task, event, filter, scope, run, worker, slot, step (legacy SDK).

## graphile-worker

**Define.** A task is an async function `(payload, helpers) => Promise<void>`, registered by its _task identifier_. It comes either from a `taskList: { send_email: fn }` object or from files in `tasks/` (the file path becomes the identifier) ([graphile/worker: `website/docs/tasks.md`](https://github.com/graphile/worker/blob/7a2405bdd3b6322fb6b0d48dc0543bcc89166930/website/docs/tasks.md)). Payloads are `unknown` by default. The docs recommend assertion functions, because jobs can be inserted from SQL or by older code. They offer global declaration merging as a weaker alternative ([`website/docs/typescript.md`](https://github.com/graphile/worker/blob/7a2405bdd3b6322fb6b0d48dc0543bcc89166930/website/docs/typescript.md)):

```ts
declare global {
  namespace GraphileWorker {
    interface Tasks {
      send_email: { to: string; subject: string; body: string };
    }
  }
}
export const send_email: Task<"send_email"> = async (payload) => {
  /* payload typed */
};
```

**Dispatch** is by string: `addJob(identifier, payload, spec)` from JS, or `graphile_worker.add_job(...)` from SQL, so jobs can be enqueued inside the caller's transaction. The type is `TIdentifier extends keyof GraphileWorker.Tasks | (string & {})`, with the payload `unknown` when the name is not a key. Names are autocompleted, but an unknown name still compiles ([`src/interfaces.ts` L48–L67](https://github.com/graphile/worker/blob/7a2405bdd3b6322fb6b0d48dc0543bcc89166930/src/interfaces.ts#L48-L67)). No runtime payload validation is built in.

**Listen / fan-out.** No event or subscription primitive. One job runs one task. The "worker events" are an `EventEmitter` for lifecycle telemetry, not domain events ([`website/docs/worker-events.md`](https://github.com/graphile/worker/blob/7a2405bdd3b6322fb6b0d48dc0543bcc89166930/website/docs/worker-events.md)).

**Options** are per job at dispatch, through `TaskSpec`: `queueName` (jobs in one named queue run one at a time), `runAt`, `priority`, `maxAttempts` (default 25), `jobKey` + `jobKeyMode` (`replace` | `preserve_run_at` | `unsafe_dedupe`, used for dedupe, debounce and throttle), and `flags` ([`src/interfaces.ts` L596–L641](https://github.com/graphile/worker/blob/7a2405bdd3b6322fb6b0d48dc0543bcc89166930/src/interfaces.ts#L596-L641), [`website/docs/library/add-job.md`](https://github.com/graphile/worker/blob/7a2405bdd3b6322fb6b0d48dc0543bcc89166930/website/docs/library/add-job.md)). Backoff is fixed at `exp(least(10, attempt))` seconds ([`website/docs/exponential-backoff.md`](https://github.com/graphile/worker/blob/7a2405bdd3b6322fb6b0d48dc0543bcc89166930/website/docs/exponential-backoff.md)). Concurrency is a worker setting (`run({ concurrency })`). Schedules live in a `crontab` file or option (`* * * * * task ?opts {payload}`), with distributed de-duplication and optional backfill ([`website/docs/cron.md`](https://github.com/graphile/worker/blob/7a2405bdd3b6322fb6b0d48dc0543bcc89166930/website/docs/cron.md)).

**Testing.** No fake. `runOnce(options)` processes every runnable job and then resolves, which lets a test enqueue, drain, and assert against a real Postgres ([`website/docs/library/run.md`](https://github.com/graphile/worker/blob/7a2405bdd3b6322fb6b0d48dc0543bcc89166930/website/docs/library/run.md)).

**Vocabulary:** job (one unit to be done), task (the kind of work), task identifier, task executor (the function), queue, job key, crontab, worker.

## pg-boss

**Define.** A queue is created by name, with its options: `createQueue(name, { policy, retryLimit, retryDelay, retryBackoff, retryDelayMax, deadLetter, ... })`. A handler is attached with `work(name, options?, handler)`, which receives an array of jobs ([timgit/pg-boss: `docs/api/queues.md` L3–L160](https://github.com/timgit/pg-boss/blob/71ea068b144c75b014a1eb90327aaa4a92da1cf2/docs/api/queues.md), [`docs/api/workers.md`](https://github.com/timgit/pg-boss/blob/71ea068b144c75b014a1eb90327aaa4a92da1cf2/docs/api/workers.md)):

```js
await boss.createQueue("order-processing", { policy: "singleton", retryLimit: 5, retryBackoff: true });
await boss.work("order-processing", async ([job]) => {
  /* job.data */
});
await boss.send("order-processing", { orderId: 1 });
```

**Dispatch.** `send(name: string, data?: object | null, options?)`, plus `sendAfter`, `sendThrottled`, `sendDebounced`, `insert` (bulk), `upsert` and `flow` (dependencies). Nothing ties the name to the data's type. `work<ReqData>` is a caller-asserted generic ([`src/index.ts` L451–L452, L498–L499](https://github.com/timgit/pg-boss/blob/71ea068b144c75b014a1eb90327aaa4a92da1cf2/src/index.ts#L451-L499)). `send` takes a `db` option to enqueue inside a caller-owned transaction, with ORM adapters shipped ([`docs/api/jobs.md` L76–L86](https://github.com/timgit/pg-boss/blob/71ea068b144c75b014a1eb90327aaa4a92da1cf2/docs/api/jobs.md)).

**Pub-sub / fan-out.** `subscribe(event, queueName)` stores a mapping, and `publish(event, data, options)` sends one job to each subscribed queue. Outside a caller transaction, a partial failure leaves the deliveries that succeeded, "so retrying the publication can duplicate them" ([`docs/api/pubsub.md`](https://github.com/timgit/pg-boss/blob/71ea068b144c75b014a1eb90327aaa4a92da1cf2/docs/api/pubsub.md)):

```js
await boss.subscribe("user.signed-up", "email-welcome");
await boss.subscribe("user.signed-up", "crm-sync");
await boss.publish("user.signed-up", { userId: 123 }); // one job in each queue
```

**Options** live on the queue (policy: `standard` | `short` | `singleton` | `stately` | `exclusive` | `key_strict_fifo`, plus retry, dead letter and heartbeat). `send` options can override per job (`priority`, `startAfter`, `singletonKey`, `group: { id, tier }`, retry fields). Concurrency is a worker option (`localConcurrency`, `groupConcurrency`, `localGroupConcurrency`) (queues.md, jobs.md L12–L160, workers.md L132–L200). Schedules are rows: `schedule(name, cronOrRRULE, data, { tz, key, missed })` ([`docs/api/scheduling.md` L119–L158](https://github.com/timgit/pg-boss/blob/71ea068b144c75b014a1eb90327aaa4a92da1cf2/docs/api/scheduling.md)).

**Testing.** Opt-in spies run against the real engine. `new PgBoss({ __test__enableSpies: true })` and `boss.getSpy(queue).waitForJob(selector, "completed")` (or `waitForJobWithId`) resolve even when the job settled first. `TestClock` moves JS timers, the worker's poll and the Postgres-side `job_now()` together ([`docs/api/testing.md`](https://github.com/timgit/pg-boss/blob/71ea068b144c75b014a1eb90327aaa4a92da1cf2/docs/api/testing.md)):

```js
const clock = new TestClock("2026-01-01T00:00:00Z");
const boss = new PgBoss({ connectionString, clock, __test__enableSpies: true });
const id = await boss.send("q", {}, { startAfter: 60 });
await clock.tick(60_000);
await boss.getSpy("q").waitForJobWithId(id, "completed");
```

**Vocabulary:** queue, job, worker, policy, singleton key, group, schedule, event (pub-sub only), subscription, dead letter, flow.

## Comparison

| Aspect                               | Inngest v4                                            | Laravel 13                                                          | Trigger.dev v4                                       | Hatchet TS v1                                | graphile-worker 0.18                         | pg-boss 12                               |
| ------------------------------------ | ----------------------------------------------------- | ------------------------------------------------------------------- | ---------------------------------------------------- | -------------------------------------------- | -------------------------------------------- | ---------------------------------------- |
| Unit of code                         | function                                              | job class; listener class                                           | task                                                 | task / workflow                              | task (executor)                              | queue + `work` handler                   |
| Defined by                           | `createFunction(opts, fn)` value                      | class                                                               | `task({ id, run })` value                            | `hatchet.task({ name, fn })` value           | string key in a task list or file name       | `createQueue(name)` + `work(name, fn)`   |
| Payload type from                    | `eventType` schema or `staticSchema<T>`               | constructor signature                                               | `run` parameter or `schemaTask` schema               | `fn` parameter or generics                   | global `GraphileWorker.Tasks` merge (opt-in) | caller generic on `work<T>`              |
| Runtime validation                   | yes: on `.create()` send, and before each run         | none (PHP types at construction)                                    | `schemaTask` only: on handle trigger, and before run | TS: none (schema goes to the dashboard)      | none (assertion functions advised)           | none                                     |
| Dispatch by handle                   | `send(evt.create(data))`, `step.invoke({ function })` | `ProcessPodcast::dispatch(...)`, `OrderShipped::dispatch(...)`      | `myTask.trigger(payload)`                            | `myTask.run / runNoWait(input)`              | n/a                                          | n/a                                      |
| Dispatch by name                     | `send({ name, data })`, untyped                       | string event names reach wildcard listeners; jobs are by class only | `tasks.trigger<typeof t>("id", p)`                   | `hatchet.run<I>("name", input)`, untyped     | `addJob("id", p)`                            | `send("name", data)`                     |
| Unknown name fails to compile        | no (by name); n/a by handle                           | no compile step                                                     | yes with the generic; no without                     | no                                           | no (`string & {}` accepted)                  | no                                       |
| Events with fan-out                  | yes: every function whose trigger matches             | yes: every listener; queued ones = one job each                     | no: explicit `batch.trigger`                         | yes: every workflow with matching `onEvents` | no                                           | `publish`/`subscribe` to queues          |
| Where retry, backoff, concurrency go | function options                                      | class attributes / methods / middleware                             | task options; some per trigger                       | task / workflow options                      | per job (`TaskSpec`); worker concurrency     | queue options; per-job overrides; worker |
| Backoff configurable                 | no (engine default; `RetryAfterError`)                | yes (`Backoff` seconds or array)                                    | yes (factor, min, max, jitter)                       | yes (factor, maxSeconds)                     | no (fixed formula)                           | yes (delay, backoff flag, max)           |
| Schedule declared                    | trigger `cron(...)` on the function                   | `Schedule::job(...)` in a console routes file                       | `schedules.task({ cron })`                           | workflow `on.cron` / `onCrons`               | `crontab` file or option                     | `boss.schedule(name, cron)` at runtime   |
| Testing dispatchers                  | no fake for `send`                                    | `Event::fake`, `Queue::fake`, `Bus::fake`                           | no fake for `trigger`                                | none in TS                                   | `runOnce` on real Postgres                   | spies + `TestClock` on the real engine   |
| Testing handlers                     | `InngestTestEngine` (mocked steps, spies)             | call `handle()`; `withFakeQueueInteractions`                        | `runInMockTaskContext` + `task.fns.run`              | embedded engine sidecar                      | `runOnce`                                    | spies                                    |
| Sync / in-process mode               | Dev Server (separate process)                         | `sync` / `deferred` drivers, `dispatchSync`                         | `trigger dev` CLI                                    | embedded mode                                | the worker is a library in your process      | the worker is a library in your process  |

## Recurring patterns

1. **A definition object carries the name, the payload type and the options together**, and dispatch goes through that object so the compiler checks it: Inngest `eventType` + `createFunction`, Trigger.dev `task`, Hatchet `hatchet.task`, Laravel classes. The three hosted TypeScript platforms all use this, which is why their type safety needs no global registry.
2. **A string-name path is kept beside the handle path**, for code that cannot import the definition: other services, webhooks, bundle size in Next.js. Every library has one. In every library it either drops the types (Inngest, Hatchet, pg-boss, graphile-worker for unregistered names) or puts them back through a type-only import (Trigger.dev `tasks.trigger<typeof t>`) or a hand-written stub (Hatchet).
3. **Payload schemas are Standard Schema or Zod values, with a type-only escape hatch** (Inngest `staticSchema<T>()`; Trigger.dev and Hatchet accept a plain TS type on the handler).
4. **Options are declared once on the definition, and a few are overridable per dispatch.** The per-dispatch set recurs: delay or run-at, idempotency, dedupe or job key, debounce key, concurrency or group key, priority. Inngest, Trigger.dev and Hatchet name concurrency keys by an expression or a key per dispatch. Laravel uses `uniqueId()`/`debounceId()` methods. pg-boss uses `singletonKey`/`group`.
5. **Retry control from inside the handler goes through error types**: `NonRetriableError`/`RetryAfterError` (Inngest), `NonRetryableError`/`RetryAfterError` (Hatchet), `AbortTaskRunError` and `retry.onThrow` (Trigger.dev, [`docs/errors-retrying.mdx` L70, L309](https://github.com/triggerdotdev/trigger.dev/blob/41ad1995b30573a9b55cfaab80c3293b7c35c1f8/docs/errors-retrying.mdx)), `$this->release()`/`$this->fail()` (Laravel).
6. **Fan-out from an event creates one independent unit per subscriber, each with its own retries** (Inngest functions, Hatchet workflows, Laravel queued listeners, pg-boss subscribed queues).
7. **Enqueueing in the caller's DB transaction appears only where the queue lives in the same database**: graphile-worker SQL `add_job`, pg-boss `db` option. Laravel offers the after-commit variant instead (`ShouldDispatchAfterCommit`, `ShouldQueueAfterCommit`, `after_commit`).

## Disagreements

- **Event-first or task-first.** Inngest makes the event the trigger and functions subscribe, so even direct calls are `inngest/function.invoked` events. Trigger.dev v4 and graphile-worker have no events at all. Laravel, Hatchet and pg-boss offer both, as separate concepts.
- **Who owns the subscription.** In Inngest and Hatchet the handler declares what it listens to (`triggers`, `onEvents`). In Laravel either side can: discovery reads the listener's type-hint, while `Event::listen` and subscribers register centrally. In pg-boss a third party calls `subscribe(event, queue)` at runtime, and the mapping is stored in the database.
- **Where validation happens.** At send (Inngest `.create()`, Trigger.dev handle trigger), before the run (Inngest, Trigger.dev `schemaTask`), or not at all (Hatchet TS, graphile-worker, pg-boss). graphile-worker argues payload types are inherently untrustworthy because jobs can come from SQL or old deploys, and recommends assertions over trusted types.
- **Where options live.** On the definition (Inngest, Trigger.dev, Hatchet, Laravel), on the queue (pg-boss), or on each job (graphile-worker). Concurrency in particular is a per-function setting for the hosted platforms and a per-worker-process setting for graphile-worker and pg-boss.
- **Retries by default, and what the number counts.** Laravel makes one attempt unless told otherwise (queues.md L1581). Trigger.dev's `maxAttempts` counts every attempt and defaults to 3. graphile-worker's `maxAttempts` defaults to 25. pg-boss's `retryLimit` (default 2), Inngest's `retries` (default 3) and Hatchet's `retries` count retries after the first attempt ([hatchet `docs/v1/retry-policies.mdx`](https://github.com/hatchet-dev/hatchet/blob/964f6bca03f58578f81c06b11f572aad1dbe1785/frontend/docs/content/docs/v1/retry-policies.mdx)).
- **What testing means.** Laravel fakes the dispatcher and asserts intent without running anything. pg-boss and graphile-worker run the real engine and wait on outcomes. Inngest and Trigger.dev run one function in isolation with mocks. Hatchet runs a whole engine locally. No TypeScript library surveyed ships a Laravel-style fake for the app-side dispatch call.
- **Names.** _Function_ vs _task_ vs _job_ for the code. _Run_ vs _job_ for one execution. _Event_ means a domain signal everywhere except graphile-worker, where worker events are lifecycle telemetry. _Step_ is a memoised unit inside a durable handler in Inngest (`step.run`), and a legacy name for a workflow task in Hatchet. _Listener_ and _subscriber_ are Laravel-only terms. _Trigger_ is a noun in Inngest (`triggers: [...]`) and a verb in Trigger.dev (`myTask.trigger()`).
