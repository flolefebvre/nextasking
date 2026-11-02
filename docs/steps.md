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

- Keep an id stable once deployed: a saved result is found by its id. Renaming a step makes runs already in progress run it again.
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

## Limits

- A step result can be at most 1 MiB once serialized, and the saved steps of one run at most 10 MiB in all. Any number of steps is fine.
- Past either limit the run fails at once. Store large data in your database or object storage, and return its id.

## Misuse fails the run at once

These are bugs in the handler, so they fail the run at once with a `StepMisuseError`, skipping the retries left (`reason: "non-retriable"`):

| Misuse                                           | Error message starts with                     |
| ------------------------------------------------ | --------------------------------------------- |
| Two `step.run` calls with the same id            | `Step id "<id>" is used twice`                |
| A `step.run` called inside another step          | `Step "<inner>" was started inside the fn of` |
| A result that can't be stored as JSON            | `The result of step "<id>" is not JSON`       |
| A result over 1 MiB                              | `The result of step "<id>" is <n> bytes`      |
| A result that brings the run's steps over 10 MiB | `Saving step "<id>" would bring`              |

- The step the error came from is not saved.
- The run fails with the `StepMisuseError` even if the handler catches it, and even if the handler goes on to throw another error or to return.
- Once one is thrown, every later `step.run` of the attempt throws it too, without running its `fn`.
- Don't call `step.run` inside another step's `fn`. Call every step from the handler itself.

```ts
import { StepMisuseError } from "@flefebvre/nextasking";
```

## Where steps are kept

- Saved steps live in Redis next to the run, under the instance's `prefix`.
- They are removed when the run completes.
- A run that fails for good keeps its saved steps for as long as the run is kept: 14 days. See [Retries and failures](retries-and-failures.md#what-a-failed-run-keeps).
- Only the attempt that currently owns the run can save a step. If another attempt has taken the run over, the next `step.run` of the old attempt throws and saves nothing.
