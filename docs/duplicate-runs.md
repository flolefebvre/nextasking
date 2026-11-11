# Duplicate runs

nextasking closes every path to a duplicate side effect it can, and leaves six windows open. This page lists what it guarantees, each open window with how to close it, and how long each email or payment provider remembers an idempotency key. Read it before writing a task that sends an email, charges a card or calls any API that must not run twice.

## What nextasking guarantees

```ts
// src/nextasking/tasks/send-receipt.ts
export const sendReceiptTask = defineTask("send-receipt", {
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
- Each listener gets one run per emit.
- A handler's `emit` and `dispatch` happen once per run, across retries and resumes. See [Steps](steps.md#emit-and-dispatch-from-a-handler).
- One firing of a schedule starts one run, however many workers run.
- A command whose reply was lost in an outage applies once: a resent fetch gets the same lock, a resent save or settle changes nothing.
- An attempt that lost its lock saves nothing more, nor reads: its `abortSignal` aborts, and its next step, `emit` or `dispatch` throws without running.
- `step.run` hands `fn` an `idempotencyKey`, the same on every attempt, resume and replay of the run, so a provider can drop a repeat.

## What stays open

| Window                                                                                    | Close it                                                                                        |
| ----------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------- |
| [A crash between a side effect and its save](#a-crash-between-a-side-effect-and-its-save) | Pass `idempotencyKey` to the provider, inside its window; or record the effect in your database |
| [A duplicate attempt finishing its step](#a-duplicate-attempt-finishing-its-step)         | Pass `idempotencyKey`; keep CPU-heavy work off the event loop                                   |
| [Code between steps](#code-between-steps)                                                 | Put every side effect in a step                                                                 |
| [A dispatch without a runId](#a-dispatch-without-a-runid)                                 | Pass a `runId` or `eventId` built from the business fact                                        |
| [The same runId after the dedup window](#the-same-runid-after-the-dedup-window)           | Check the business fact in the handler                                                          |
| [A failover or a restart](#a-failover-or-a-restart)                                       | Keep AOF on; close it with the idempotency key, as for the other windows                        |

## A crash between a side effect and its save

A step's result is saved after `fn` returns. If the process crashes or is killed after the side effect but before the save, the next attempt runs `fn` again. A step still running at `shutdownTimeout` is in this window too: its `abortSignal` aborts, and a step that throws on the abort, or is still running when the process exits about a second later, saves nothing. See [Worker](worker.md#let-a-deploy-finish-the-running-attempts).

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
- Keep CPU-heavy work off the event loop: in a worker thread or another service, where it can't starve other attempts.
- Check `abortSignal.aborted` in long loops inside a step, and pass the signal to `fetch` and other calls that take one.
- After a stall, one log line can misreport an outcome. See [Worker](worker.md#stalls).

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

A `runId` is dropped while its run exists, and for 24 hours after the run ends, even when retention removed the run sooner. A completed run is removed shortly after those 24 hours; a failed run is kept up to 14 days, and its id stays dropped for as long. A repeat after that starts a new run. See [The dedup window](tasks-and-events.md#the-dedup-window).

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

## A failover or a restart

A failover promotes a replica that hadn't received the primary's last writes; a single server that restarts comes back with what it had written to disk. Both lose the last writes the server acknowledged, with nobody told. Both lose whole transitions: a save, a completion or a dispatch is kept whole or lost whole, never half-written.

The one guarantee: two attempts never both save. Each lock carries a random token, so an attempt whose lock the server forgot can't save over the attempt that holds the run now.

What a lost write does, in the order a run meets it:

- **A saved step**: the attempt goes on without it. If this attempt completes, the step never runs again; if the run comes back to an attempt, after a retry, a resume or a stall, the step runs again.
- **A completion**: the run is executing again, under a lock nobody renews. Once that lock expires, a stall is counted within about a second (see [Worker](worker.md#stalls)) and a new attempt runs again whatever came after the last saved step. A repeat of the run's id is still dropped meanwhile.
- **A lock**: a second attempt can start while the first one runs, as in [A duplicate attempt finishing its step](#a-duplicate-attempt-finishing-its-step), and nothing more: the first attempt's next save is refused, and it stops.
- **A dispatch or an emit**: the call returned an id, and the run never starts. Build the id from the business fact, and send it again with that id when `getRun` finds nothing, or `getEvent` finds no listener run: if the run does exist, the repeat is dropped.

How much is lost:

| Setup                            | Loses                   | How much                      |
| -------------------------------- | ----------------------- | ----------------------------- |
| Primary with a standby, failover | the replication lag     | milliseconds to a second      |
| Single node, AOF `everysec`      | the last fsync interval | about one second              |
| Single node, RDB snapshots only  | since the last snapshot | up to an hour on the defaults |
| Single node, no persistence      | everything              | the whole queue               |

- A single node loses its tail when it crashes, is killed or loses its host. A clean shutdown first writes what the persistence configured asks for: AOF flushed, a snapshot saved when save points are set. A node with no persistence loses everything on any restart, clean or not.
- Pick the server for availability, and keep AOF on: `--appendonly yes` brings the tail down to about a second. `appendfsync always` loses nothing acknowledged, at the disk's fsync rate. See [Setup](setup.md#run-valkey-or-redis).
- nextasking sends no `WAIT` and recommends no topology: no setting closes the window, so close it in the task, with the `idempotencyKey`, a database record or a check of the business fact, as for the other windows.

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
