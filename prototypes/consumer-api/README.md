# PROTOTYPE: consumer-facing API (throwaway, never merged)

Answers [Prototype the consumer-facing API](https://github.com/flolefebvre/nextasking/issues/10): what the code a consumer writes looks like. The library is a type-level stub (`stub/nextasking.ts`); only the consumer code under `src/` is meant to be read.

Run: `pnpm prototype:consumer-api` typechecks it. `src/misuse.ts` holds one `@ts-expect-error` per misuse that must not compile, so a passing run proves every one of them fails.

Round 1 compared dispatching through handles with dispatching by name; that version is in the branch history (commit `21f36ff`). Names won.

## The app

A small SaaS: sign-up emits `user.signed-up`, a Stripe webhook emits `invoice.paid`, a server action dispatches `generate-invoice-pdf`, and a schedule purges sessions at 3 am.

```
src/nextasking/index.ts                  the nextasking instance: connection and registry, plus the Register augmentation
src/nextasking/events.ts                 the three events
src/nextasking/tasks/send-welcome-email.ts       listener of user.signed-up, with steps
src/nextasking/tasks/record-invoice-payment.ts   listener of invoice.paid
src/nextasking/tasks/generate-invoice-pdf.ts     dispatched task: schema, concurrency key, emits from its handler
src/nextasking/tasks/purge-expired-sessions.ts   scheduled task
src/app/sign-up/actions.ts               server action emitting an event
src/app/invoices/[invoiceId]/actions.ts  server action dispatching tasks
src/app/api/webhooks/stripe/route.ts     route handler emitting an event
src/instrumentation.ts, src/worker.ts    embedded and standalone worker (placeholder for #11)
src/misuse.ts                            what must not compile
```

## Decided in round 1

- Dispatch and emit by name, as literal keys, typed through a `Register` module augmentation.
- Three definers: `defineTask` (`schema`), `defineListener` (`on`), `defineScheduledTask` (`schedule`).
- A handler receives one context object.
- Any Standard Schema; the payload is a JSON object before and after the schema; validated at dispatch, stored validated, handed to the handler as stored.
- `dispatch(name, payload?, { runId?, delay? })` returns `{ runId }`; `Duration` is a whole number plus `s`, `m`, `h` or `d`, checked at compile time.
- The misleading error for a payload passed to a scheduled task is accepted.
- Consumer folder `src/nextasking/`; `createNextasking` returns `nextasking`; "registry" means the `events` and `tasks` lists.
- Name format is a documented convention, not enforced; duplicate names fail at startup.
- Dispatching imports every task's handler code.

## Error messages

| Misuse                                     | Error                                                          |
| ------------------------------------------ | -------------------------------------------------------------- |
| Wrong payload (emit, dispatch, in handler) | one line, points at the field                                  |
| Typo in a task or event name               | one line, lists the valid names                                |
| Event name passed to `dispatch`            | one line, lists the valid task names                           |
| Not a duration (`"5sfdsfsdfs"`)            | one line: not assignable to `Duration`                         |
| Misspelt option (`retrys`)                 | one line, on the property                                      |
| Listener reading a field its event lacks   | one line                                                       |
| Non-JSON payload (`z.date()`)              | short first line                                               |
| Payload passed to a scheduled task         | misleading: "'force' does not exist in type 'DispatchOptions'" |
| Name from a webhook (`string`)             | rejected; a runtime by-name path is still in the map's fog     |
