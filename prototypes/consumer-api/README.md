# PROTOTYPE: consumer-facing API (throwaway, never merged)

Answers [Prototype the consumer-facing API](https://github.com/flolefebvre/nextasking/issues/10): what the code a consumer writes looks like. The library is a type-level stub (`stub/`); only the consumer code is meant to be read.

Run: `pnpm prototype:consumer-api` typechecks both variants. Each `src/misuse.ts` holds one `@ts-expect-error` per misuse that must not compile, so a passing run proves every one of them fails.

## The app

A small SaaS: sign-up emits `user.signed-up`, a Stripe webhook emits `invoice.paid`, a server action dispatches `generate-invoice-pdf`, and a schedule purges sessions at 3 am.

```
shared/                         identical in both variants
  jobs/events.ts                the three events
  jobs/tasks/send-welcome-email.ts       listener of user.signed-up, with steps
  jobs/tasks/record-invoice-payment.ts   listener of invoice.paid
  jobs/tasks/purge-expired-sessions.ts   scheduled task
variant-a-handles/src/          dispatch and emit take the definition itself
variant-b-names/src/            dispatch and emit take the name as a literal key
  jobs/nextasking.ts            the registry: connection, events, tasks
  jobs/tasks/generate-invoice-pdf.ts     dispatched task: schema, concurrency key, emits from its handler
  app/sign-up/actions.ts        server action emitting an event
  app/invoices/[invoiceId]/actions.ts    server action dispatching tasks
  app/api/webhooks/stripe/route.ts       route handler emitting an event
  instrumentation.ts, worker.ts embedded and standalone worker (placeholder for #11)
  misuse.ts                     what must not compile
```

## Findings

| Misuse                                     | A (handles)                                                    | B (names)                                                     |
| ------------------------------------------ | -------------------------------------------------------------- | ------------------------------------------------------------- |
| Wrong payload (emit, dispatch, in handler) | short, points at the field                                     | short, points at the field                                    |
| Typo in a name                             | can't be written                                               | short: lists the valid names                                  |
| Event passed where a task is expected      | long: prints every registered task's full type                 | short: lists the valid task names                             |
| Definition missing from the registry       | caught at dispatch sites through the instance, long message    | caught: the name is not in the list                           |
| Same, from inside a handler                | not caught (the handler's `dispatch` can't see the registry)   | caught, through `Register` module augmentation                |
| Listener reading a field its event lacks   | short                                                          | short                                                         |
| Non-JSON payload (`z.date()`)              | short first line                                               | short first line                                              |
| Payload passed to a scheduled task         | misleading: "'force' does not exist in type 'DispatchOptions'" | same                                                          |
| Name from a webhook (`string`)             | n/a (needs a separate by-name path)                            | rejected at compile time; a runtime by-name path still needed |

- Variant B's augmentation types `emit` and `dispatch` inside handlers by name with no circularity error, although the registry imports the task files whose handlers read it (TS 6.0.3).
- `defineTask` with three overloads (`schema`, `on`, `schedule`) turns any typo in an option (`retrys: 5`) into a three-overload error on the whole call, not on the property. Separate functions, or one signature without overloads, would give a one-line error.
- In both variants, every module that dispatches imports the registry, and so every task module and the dependencies of their handlers.
