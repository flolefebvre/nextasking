# Steps

A retry runs the handler again from the top. Wrap each side effect in `step.run(id, fn)`: the first attempt that finishes the step saves its result, and every later attempt gets that result back without running `fn` again.

## Wrap each side effect in step.run

```ts
// src/nextasking/tasks/charge-invoice.ts
import { defineTask } from "@flefebvre/nextasking";
import { z } from "zod";

export const chargeInvoice = defineTask("charge-invoice", {
  schema: z.object({ invoiceId: z.string() }),
  handler: async ({ payload, step }) => {
    const invoice = await step.run("load-invoice", async () => {
      const row = await db.invoice.find(payload.invoiceId);
      return { customerId: row.customerId, amountInCents: row.amountInCents };
    });
    const charge = await step.run("create-charge", async ({ idempotencyKey }) => {
      const paymentIntent = await stripe.paymentIntents.create(
        { customer: invoice.customerId, amount: invoice.amountInCents, currency: "eur", confirm: true },
        { idempotencyKey },
      );
      return { paymentIntentId: paymentIntent.id };
    });
    await step.run("send-receipt", async () => {
      await email.send({ template: "receipt", paymentIntentId: charge.paymentIntentId });
    });
  },
});
```

- If `send-receipt` throws, the retry gets `load-invoice` and `create-charge` back from their saved results and runs only `send-receipt`.
- `step.run` resolves with `fn`'s result. `fn` may be sync or async.
- A step whose `fn` throws is not saved: the attempt fails as usual, and the next attempt runs `fn` again.
- Code outside `step.run` runs again on every attempt. Keep it free of side effects.

## Step ids

- Give every `step.run` an explicit id, unique within the run. A repeated id fails the run; see [Misuse fails the run at once](#misuse-fails-the-run-at-once).
- In a loop, put the item in the id:

```ts
for (const followerId of payload.followerIds) {
  await step.run(`notify-${followerId}`, async () => {
    await notifications.send(followerId);
  });
}
```

- Keep an id stable once deployed: a saved result is found by its id. Renaming a step makes runs already in progress run it again. See [Deploys](deploys.md#rename-or-rework-a-step).
- A saved step whose id no longer appears in the code is ignored.

## Results are JSON

- `fn` must return JSON: strings, numbers, booleans, `null`, arrays and plain objects. A `Date`, a `Map` or an object with methods doesn't compile. Return a date as an ISO string.
- Return the fields you need, as an object literal. A value typed by an `interface`, such as an SDK's response type, may not compile even when it holds only JSON: TypeScript doesn't treat an interface as a plain object.
- `undefined`, or no return value, is fine: `step.run` resolves with `undefined`.
- The handler always gets the JSON round-tripped value, on the first attempt too: a property set to `undefined` disappears, `NaN` becomes `null`. The first attempt and a retry see the same thing.
- A value that slips past the types and can't be stored as JSON, such as a `BigInt`, a circular object or a function, fails the run at once.

## Run steps in parallel

```ts
const [plan, orderCount] = await Promise.all([
  step.run("load-plan", async () => (await crm.profile(payload.userId)).plan),
  step.run("count-orders", async () => (await shop.orders(payload.userId)).length),
]);
```

- Each step is saved as soon as it settles. If one fails, the others that finished are still saved.

## Sleep for minutes or days

`step.sleep(id, duration)` and `step.sleepUntil(id, time)` suspend the run. It holds no worker while it waits, and resumes at its resume time.

```ts
// src/nextasking/tasks/onboard-user.ts
export const onboardUser = defineTask("onboard-user", {
  schema: z.object({ userId: z.string() }),
  handler: async ({ payload, step }) => {
    await step.run("send-welcome-email", async () => {
      await email.send({ userId: payload.userId, template: "welcome" });
    });
    await step.sleep("wait-before-tips", "3d");
    await step.run("send-tips-email", async () => {
      await email.send({ userId: payload.userId, template: "tips" });
    });
  },
});
```

- `sleep` takes a [duration](tasks-and-events.md#durations). `sleepUntil` takes a `Date`, or an ISO date string with `Z` or an offset (`"2026-12-01T09:00:00Z"`, `"2026-12-01T10:00:00+01:00"`).
- The resume time is fixed and saved the first time the sleep is reached, under the sleep's id: a resume or a crash keeps it. On resume the handler re-enters from the top: saved steps return their results, the sleep returns at once, and the code after it runs.
- A resume keeps the same `attempt` and uses no retry.
- A `sleepUntil` time already past returns at once, without suspending. It is still saved.
- A sleep reached while other steps, `emit` or `dispatch` calls run in parallel suspends once they settle, so they are saved.
- Sleep ids follow the [step id](#step-ids) rules: unique within the run, shared with `step.run` ids, stable once deployed. Don't sleep inside a step's `fn`.
- To start a run later, pass `delay` to `dispatch` rather than sleeping first.
- The run suspends by throwing from the sleep. If your handler catches that error, the run still suspends, and every later step, `emit` or `dispatch` of the attempt throws without running.

Sleep until a time a step returned. A step result is JSON, so the date comes back as an ISO string, which `sleepUntil` takes as is:

```ts
const trial = await step.run("load-trial", async () => {
  const row = await db.trial.find(payload.accountId);
  return { endsAt: row.endsAt.toISOString() };
});
await step.sleepUntil("wait-for-trial-end", trial.endsAt);
```

### Skipping work that is no longer needed

The world changes while a run sleeps. After a sleep, re-check the business fact in a step, and return early if the work is no longer needed:

```ts
await step.sleep("wait-before-reminder", "7d");
const isStillUnpaid = await step.run("check-invoice-unpaid", async () => {
  const invoice = await db.invoice.find(payload.invoiceId);
  return invoice.paidAt === null;
});
if (!isStillUnpaid) return;
await step.run("send-reminder", async () => {
  await email.send({ invoiceId: payload.invoiceId, template: "payment-reminder" });
});
```

- Read the fact after the sleep, never before: a value loaded before the sleep is as old as the sleep.
- Returning completes the run.

## Emit and dispatch from a handler

Use the handler's `emit` and `dispatch`, not the instance's: a handler can't import the instance without a circular import, and these happen once per run.

```ts
// src/nextasking/tasks/sign-up-user.ts
export const signUpUser = defineTask("sign-up-user", {
  schema: z.object({ userId: z.string(), email: z.email() }),
  handler: async ({ payload, step, emit, dispatch }) => {
    await step.run("create-account", async () => {
      await db.account.create({ userId: payload.userId });
    });
    await emit("user.signed-up", { userId: payload.userId });
    await dispatch("send-welcome-email", { userId: payload.userId, email: payload.email }, { delay: "5m" });
  },
});
```

- They take the same names, payloads and options as the instance's, typed through the `Register` augmentation. Without it, no name compiles. See [Register the tasks and events](tasks-and-events.md#register-the-tasks-and-events).
- Each call that succeeds is saved like a step, with the `runId` or `eventId` it sent. A retry, a resume or a replay that reaches it again sends nothing, and resolves with the same `{ runId }` or `{ eventId }`.
- A call is recognised by its position: the run, the enclosing step (or the top level), and how many `emit` and `dispatch` calls came before it in that scope during the attempt. Keep their order stable, and put a call that depends on data in its own step.
- Without your own id, the run or event gets an id derived from that position, the same on every attempt. A `runId` or `eventId` you pass wins over it.
- A call that throws is not saved: the next attempt sends it again.
- Inside a step's `fn`, a call is counted within that step. Once the step is saved, its `fn`, and the calls in it, don't run again.

## Pass the idempotency key to the provider

`fn` receives `{ idempotencyKey }`, a UUID derived from the task name, the run id and the step id. It is the same on every attempt of the run, so a provider that supports idempotency keys drops a repeat when an attempt dies after the call but before the step is saved.

```ts
await step.run("create-charge", async ({ idempotencyKey }) => {
  const paymentIntent = await stripe.paymentIntents.create(
    { amount: 1000, currency: "eur", customer: customerId },
    { idempotencyKey },
  );
  return { paymentIntentId: paymentIntent.id };
});
```

- A provider remembers a key for a limited time, and some take none. See [Duplicate runs](duplicate-runs.md#provider-idempotency-windows) for each provider's window and what to do without keys.

## Limits

- A step result can be at most 1 MiB once serialized, and the saved steps of one run at most 10 MiB in all. Any number of steps is fine.
- Past either limit the run fails at once. Store large data in your database or object storage, and return its id.

## Misuse fails the run at once

These are bugs in the handler, so they fail the run at once with a `StepMisuseError`, skipping the retries left (`reason: "non-retriable"`):

| Misuse                                                   | Error message starts with                      |
| -------------------------------------------------------- | ---------------------------------------------- |
| Two steps or sleeps with the same id                     | `Step id "<id>" is used twice`                 |
| A `step.run` or a sleep called inside another step       | `Step "<inner>" was started inside the fn of`  |
| A result that can't be stored as JSON                    | `The result of step "<id>" is not JSON`        |
| A result over 1 MiB                                      | `The result of step "<id>" is <n> bytes`       |
| A result that brings the run's steps over 10 MiB         | `Saving step "<id>" would bring`               |
| A `sleep` duration that isn't a duration                 | `The duration of step "<id>" is invalid`       |
| A `sleepUntil` time that isn't a `Date` or an ISO string | `The resume time of step "<id>" is not a Date` |

- The step the error came from is not saved.
- The run fails with the `StepMisuseError` even if the handler catches it, and even if the handler goes on to throw another error or to return.
- Once one is thrown, every later step of the attempt throws it too, without running its `fn`.
- A misuse wins over a sleep of the same attempt: the run fails rather than suspends.
- Don't call `step.run` or a sleep inside another step's `fn`. Call every step from the handler itself.

```ts
import { StepMisuseError } from "@flefebvre/nextasking";
```

## Where steps are kept

- Saved steps, resume times and sent calls live in Redis next to the run, under the instance's `prefix`.
- They are removed when the run completes.
- A run that fails for good keeps its saved steps for as long as the run is kept: 14 days. See [Retries and failures](retries-and-failures.md#what-a-failed-run-keeps).
- Only the attempt that currently owns the run can save a step. If another attempt has taken the run over, the next `step.run` of the old attempt throws and saves nothing, and its `abortSignal` aborts. See [Worker](worker.md#stop-a-handler-cleanly-with-abortsignal).
- Each saved step resets the run's stall count. See [Worker](worker.md#stalls).
