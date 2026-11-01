# nextasking

Events and background tasks for Next.js apps: the work an app hands off to run outside the request, and the events that start it.

## Language

### Definitions

**Task**:
A named piece of work a consumer defines, with a payload schema and a handler.
_Avoid_: Job, function, workflow

**Event**:
A named fact a consumer defines, with a payload schema, that tasks can subscribe to.
_Avoid_: Message, signal

**Listener**:
A task subscribed to exactly one event, declared by the task itself, whose payload is that event's payload.
_Avoid_: Subscriber, handler

**Schedule**:
A recurring time rule declared by a task, each firing of which starts a run of that task.
_Avoid_: Cron job, scheduler

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

**Run**:
One execution of a task, from its dispatch to its final outcome, across all its attempts.
_Avoid_: Job, execution

**Attempt**:
One try at completing a run; a retry is a new attempt of the same run.
_Avoid_: Try, execution

**Worker**:
The role of a process that executes attempts, whether embedded in the Next.js server or standalone.
_Avoid_: Consumer, processor
