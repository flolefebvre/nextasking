# Dev

The worker runs under `next dev` the same way it runs under `next start`: `register()` in `instrumentation.ts` starts it. It keeps the code it booted with, so restart the dev server after editing a task.

## Run the worker under `next dev`

Turn the worker on in the dev script with the variable `instrumentation.ts` checks; see [Start the worker](tasks-and-events.md#start-the-worker).

```json
// package.json
{
  "scripts": {
    "dev": "NEXTASKING_WORKER=on next dev"
  }
}
```

- Turbopack, the default bundler, needs no `next.config` entry for the library.
- Point dev at its own Redis or Valkey, or give it its own `prefix`, so a dev worker never runs another environment's runs.

## Restart the dev server after editing a task

The worker starts once per dev server, from `register()`. Editing a task doesn't reach it: it goes on running the handlers it booted with.

```sh
# after editing anything under src/nextasking/
# Ctrl-C, then:
pnpm dev
```

- Restart after any edit to a task, a listener, an event or the instance: a handler, a schema, an option, a name.

## What Ctrl-C does to the attempt in flight

On Ctrl-C or SIGTERM, `next dev` passes the signal to its server process and kills it a moment later, without waiting for the worker. An attempt in flight is cut where it stands: no drain, no step boundary, even with `NEXT_MANUAL_SIG_HANDLE` set. That is why the worker doesn't warn about the variable under `next dev`.

- The cut attempt comes back as stalled once its lock expires, in the next dev server that starts on the same prefix. It counts as one stall: the third stall with no step saved in between fails the run for good with `reason: "stalled"`, whatever retries it has left. The timing is on [Worker](worker.md#stalls).
- To keep a long run safe across restarts, put its side effects in steps: the next attempt reuses what they saved. See [Steps](steps.md).

## Change a task's steps

The next attempt of a run reuses the steps an earlier attempt saved, by id, even when that attempt ran older code:

- a step whose id is unchanged returns the old code's result, even if the new code would compute it differently or return another shape;
- a step whose id is new runs;
- a step whose id was removed is ignored.

So after changing a handler's steps, dispatch a new run to try the change rather than letting an old run resume on the new code. In production, runs in progress do resume on the new code: see [Deploys](deploys.md#rename-or-rework-a-step).

```ts
// after restarting the dev server
await nextasking.dispatch("send-welcome-email", { userId: "user-1", email: "ada@example.com" });
```
