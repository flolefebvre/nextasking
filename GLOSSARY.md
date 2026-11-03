# nextasking

Events and background tasks for Next.js apps: the work an app hands off to run outside the request, and the events that start it.

## Language

### Definitions

**Task**:
A named piece of work a consumer defines, with a payload schema and a handler.
_Avoid_: Job, function, workflow

**Event**:
A named fact, defined by a consumer or by the library, with a payload schema, that tasks can subscribe to.
_Avoid_: Message, signal

**Built-in event**:
An event the library defines and emits itself when a run or a fan-out fails; a consumer can listen to it but not emit it.
_Avoid_: System event, hook, notification

**Listener**:
A task subscribed to exactly one event, declared by the task itself, whose payload is that event's payload.
_Avoid_: Subscriber, handler

**Schedule**:
A recurring time rule declared by a task, each firing of which starts a run of that task.
_Avoid_: Cron job, scheduler

**Firing**:
One time a schedule comes due, starting one run of its task.
_Avoid_: Occurrence, tick, trigger

**Step**:
A named part of a run whose result is saved, so a later attempt of the same run reuses it instead of executing it again.
_Avoid_: Activity, checkpoint

**Registry**:
The list of every task and event an app defines.
_Avoid_: Client, app, catalog

### Execution

**Dispatch**:
To start exactly one run of a task.
_Avoid_: Trigger, send, enqueue

**Emit**:
To announce an event, starting one run of each of its listeners, which may be none.
_Avoid_: Send, fire, publish, trigger

**Fan-out**:
The turning of one emitted event into one run per listener, done by a worker rather than by the emitter.
_Avoid_: Broadcast, dispatch

**Dedup window**:
How long a repeated dispatch or emit with the same id is dropped.
_Avoid_: TTL, retention

**Run**:
One execution of a task, from its dispatch to its final outcome, across all its attempts.
_Avoid_: Job, execution

**Attempt**:
One try at completing a run; a retry is a new attempt of the same run.
_Avoid_: Try, execution

**Duplicate attempt**:
An attempt of a run that starts while another attempt of the same run is still executing, after the first lost its lock.
_Avoid_: Zombie, double run

**Lock**:
The hold an attempt has on its run while it executes, renewed by its worker; an attempt that loses it can no longer save or complete anything, and another attempt of the run may start.
_Avoid_: Lease, claim, mutex

**Stall**:
An attempt that lost its lock because its process crashed, was killed, or blocked its event loop past the lock duration; the run goes back to pending, and the third stall with no step saved in between fails it.
_Avoid_: Crash, timeout, zombie

**Backoff**:
The wait between a failed attempt and the next attempt of the same run.
_Avoid_: Delay, cooldown

**Failed run**:
A run whose final outcome is failure: its last attempt failed with no retry left, or it threw an error that skips the remaining retries.
_Avoid_: Exhausted job, dead job, dead letter

**Replay**:
To start a failed run again under its own run id, with a fresh retry budget, reusing the steps it has already saved; or to start a failed fan-out again, skipping the listeners that already have a run.
_Avoid_: Retry, rerun, redrive

**Suspend**:
To pause a run's attempt at a step until a set time, so it holds no worker and no concurrency slot while it waits.
_Avoid_: Pause, park; not yield, which is the hand-back at shutdown (see **Yield**)

**Resume**:
To continue a suspended attempt once its time comes, reusing the steps it has already saved.
_Avoid_: Wake, restart, retry

**Yield**:
To send a running attempt's run back to pending at a step boundary because its worker is shutting down, once its steps in flight are saved, using no retry and counting no stall.
_Avoid_: Suspend, requeue, hand back

**Drain**:
The state of a worker between the shutdown signal and its exit, in which it takes no new attempt and lets the running ones yield or finish until its shutdown timeout.
_Avoid_: Stopping, winding down

**Pending**:
Said of a run that is due and waits for a worker to start or continue its next attempt.
_Avoid_: Queued, waiting, ready

**Idle**:
Said of the queue when no run is pending, executing or waiting for a concurrency slot, and no fan-out is left; runs that wait for a set time don't count.
_Avoid_: Settled, empty; not drained, which describes a worker shutting down (see **Drain**)

**Idempotency key**:
A value that stays the same for one step of one run, handed to that step so an external service can recognise a repeat.
_Avoid_: Dedup key, request id

**Concurrency limit**:
The most attempts of one task allowed to execute at the same time, counted across the whole task or per concurrency key.
_Avoid_: Throttle, rate limit

**Concurrency key**:
A value derived from a run's payload when it is dispatched, grouping the runs of one task that share a concurrency limit.
_Avoid_: Group, partition, lock key

**Worker**:
The role of a process that executes attempts, whether embedded in the Next.js server or standalone.
_Avoid_: Consumer, processor

**Lifecycle log**:
A line the library writes through the app's logger when a run or an attempt changes state.
_Avoid_: Event, hook, notification
