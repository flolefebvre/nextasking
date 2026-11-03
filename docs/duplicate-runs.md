# Duplicate runs

nextasking closes every path to a duplicate side effect it can, and leaves six windows open. This page lists what it guarantees, each open window with how to close it, and how long each email or payment provider remembers an idempotency key. Read it before writing a task that sends an email, charges a card or calls any API that must not run twice.

## What nextasking guarantees

```ts
// src/nextasking/tasks/send-receipt.ts
export const sendReceipt = defineTask("send-receipt", {
  schema: z.object({ invoiceId: z.string(), email: z.email() }),
  handler: async ({ payload, step }) => {
    await step.run("send-receipt-email", async ({ idempotencyKey }) => {
      await email.send({ to: payload.email, template: "receipt", invoiceId: payload.invoiceId }, { idempotencyKey });
    });
  },
});

// the call site, built from the business fact
await nextasking.dispatch("send-receipt", { invoiceId, email }, { runId: `send-receipt-${invoiceId}` });
```

- A saved step never runs again in the same run: a retry, a resume or a replay gets its saved result. See [Steps](steps.md).
- A repeated `dispatch` with the same `runId`, or `emit` with the same `eventId`, within the [dedup window](tasks-and-events.md#the-dedup-window) starts nothing and returns the first id.
- Each listener gets one run per emit, even when the fan-out retries part-way or is replayed.
- A handler's `emit` and `dispatch` happen once per run, across retries and resumes. See [Steps](steps.md#emit-and-dispatch-from-a-handler).
- One firing of a schedule starts one run, however many workers run.
- An attempt that lost its lock saves nothing more: its `abortSignal` aborts, and its next step, `emit` or `dispatch` throws without running.
- `step.run` hands `fn` an `idempotencyKey`, the same on every attempt, resume and replay of the run, so a provider can drop a repeat.

## What stays open

| Window                                                                                    | Close it                                                                                        |
| ----------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------- |
| [A crash between a side effect and its save](#a-crash-between-a-side-effect-and-its-save) | Pass `idempotencyKey` to the provider, inside its window; or record the effect in your database |
| [A duplicate attempt finishing its step](#a-duplicate-attempt-finishing-its-step)         | Pass `idempotencyKey`; keep CPU-heavy work off the event loop                                   |
| [Code between steps](#code-between-steps)                                                 | Put every side effect in a step                                                                 |
| [A dispatch without a runId](#a-dispatch-without-a-runid)                                 | Pass a `runId` or `eventId` built from the business fact                                        |
| [The same runId after the dedup window](#the-same-runid-after-the-dedup-window)           | Check the business fact in the handler                                                          |
| [A Redis failover](#a-redis-failover)                                                     | Use a Valkey or Redis without failover, or accept the gap                                       |

## A crash between a side effect and its save

A step's result is saved after `fn` returns. If the process crashes or is killed after the side effect but before the save, the next attempt runs `fn` again. A step still running at `shutdownTimeout` is in this window too: the process exits before it saves.

```ts
await step.run("charge-invoice", async ({ idempotencyKey }) => {
  const paymentIntent = await stripe.paymentIntents.create(
    { amount: invoice.amountInCents, currency: "eur", customer: invoice.customerId, confirm: true },
    { idempotencyKey },
  );
  return { paymentIntentId: paymentIntent.id };
});
```

- Pass `idempotencyKey` to every provider that takes one, and keep the run's retries inside the provider's window. See [Provider idempotency windows](#provider-idempotency-windows).
- For a provider without keys, write a record in your database inside the step and check it first. See [A provider without idempotency keys](#a-provider-without-idempotency-keys).
- Pass `abortSignal` to the calls a step makes, so a step stops at the shutdown deadline instead of being cut. See [Worker](worker.md#stop-a-handler-cleanly-with-abortsignal).
- Put one side effect per step: a step that sends two emails repeats both.

## A duplicate attempt finishing its step

An attempt holds its run's lock for 2 minutes, renewed every minute. When its event loop blocks for about a minute or more, or its worker can't reach Redis for that long, the lock can expire, and another worker starts a second attempt of the same run. The first attempt can no longer save anything and stops at its next step, but it can finish the step it is inside: that step's side effect then runs in both attempts.

- Pass `idempotencyKey` to the provider: both attempts get the same key, so the provider drops the second call.
- Keep CPU-heavy work off the event loop: in a worker thread, another service, or a separate `nextasking worker` process where it can't starve other attempts. See [Worker](worker.md#run-the-worker-in-its-own-process).
- Check `abortSignal.aborted` in long loops inside a step, and pass the signal to `fetch` and other calls that take one.

## Code between steps

The handler re-enters from the top on every attempt and every resume. Code outside `step.run` runs again each time, and so do its `logger` lines.

```ts
handler: async ({ payload, step, logger }) => {
  logger.info({}, "starting");
  await step.run("notify-account-owner", async () => {
    await notifications.send(payload.accountId);
  });
};
```

- Put every side effect in a step. Code between steps reads and computes only.
- Expect each `logger` line outside a step once per attempt and per resume: don't count runs from it.
- A handler's `emit` and `dispatch` need no step: they are already once per run.

## A dispatch without a runId

Without your own id, each `dispatch` gets a random `runId` and each `emit` a random `eventId`, so a repeated call starts the work again. A webhook retried by its sender, a form submitted twice and a server action replayed by the client are such calls.

```ts
await nextasking.emit("invoice.paid", payload, { eventId: `stripe-${stripeEvent.id}` });
await nextasking.dispatch("ship-order", { orderId: order.id }, { runId: `ship-order-${order.id}` });
```

- Build the id from what makes the fact unique: the provider's event id, the order id, the user id and the day. Never from the time of the call or a random value.
- On `RedisUnavailableError`, retry with the id on the error. See [Tasks and events](tasks-and-events.md#when-redis-is-down).

## The same runId after the dedup window

A `runId` or `eventId` is dropped only while the first run or fan-out is kept: at least 24 hours after the dispatch, longer while it runs or once it failed. Under load, the queue's caps can remove a completed run sooner. A repeat after that starts a new run. See [The dedup window](tasks-and-events.md#the-dedup-window).

```ts
handler: async ({ payload, step }) => {
  const isAlreadyShipped = await step.run("check-order-not-shipped", async () => {
    const order = await db.order.find(payload.orderId);
    return order.shippedAt !== null;
  });
  if (isAlreadyShipped) return;
  await step.run("ship-order", async ({ idempotencyKey }) => {
    await carrier.createShipment({ orderId: payload.orderId }, { idempotencyKey });
  });
};
```

- When a repeat can come days later, check the business fact in a step first, and return when it is already done.

## A Redis failover

A failover to a replica that hadn't received the latest writes loses them: a saved step, a dedup record or a lock. A lost saved step runs again; a lost dedup record lets a repeat through; a lost lock lets a second attempt start.

- Use a Valkey or Redis without failover, with persistence, for the queues, or accept the gap.
- The fixes for the other windows narrow this one too: an idempotency key, a database record, a check of the business fact.

## Provider idempotency windows

A provider drops a repeated call with the same key only while it remembers the key. Keep every retry of a step inside that window, and replay a failed run before it closes.

| Provider | Keeps a key for                                                             | Key                              |
| -------- | --------------------------------------------------------------------------- | -------------------------------- |
| Resend   | 24 hours                                                                    | Up to 256 characters             |
| Brevo    | 30 minutes, documented for batch sends: confirm for single sends with Brevo | A UUID                           |
| Stripe   | At least 24 hours (API v1)                                                  | Up to 255 characters             |
| Postmark | No idempotency keys                                                         | Record the send in your database |
| SES      | No idempotency keys                                                         | Record the send in your database |

- `idempotencyKey` is a UUID, which every provider above takes.
- Check the provider's own documentation before relying on a figure: providers change them.
- The default retries wait about 12 to 16 minutes in all, inside every window above. Longer retries add up fast: `backoff: "1m"` with the default 5 retries waits up to 31 minutes, past Brevo's. In all, the waits add up to at most `backoff` × (2^`retries` − 1). See [Retries and failures](retries-and-failures.md#set-retries-and-backoff).
- A replay reuses the run's keys: a step re-run by a replay after the provider's window sends again.
- The key is derived from the task name, the run id and the step id. Renaming any of them changes it. See [Deploys](deploys.md#rename-or-rework-a-step).

### A provider without idempotency keys

Record the side effect in your database inside the step, and check the record first:

```ts
await step.run("send-welcome-email", async ({ idempotencyKey }) => {
  if (await db.sentEmail.exists({ idempotencyKey })) return;
  await smtp.send({ to: payload.email, template: "welcome" });
  await db.sentEmail.create({ idempotencyKey });
});
```

- This closes every window except a crash between the send and the record. Keep that gap short: write the record right after the call.
