# Prototype: an option a call doesn't know is a type error

Throwaway, never merged. Made for #259, #260 and #262 on 2026-11-11, on `main` at db3eecd.

## The question

Can the types alone refuse an unknown option, in a literal, through a variable and through a spread, on every call that takes options, without changing what a correct call infers? On TypeScript 6.0.3, the repo's, and 5.9.3, the floor of the peer range.

The calls: `listRuns`, `dispatch` (a conditional rest tuple by task name), `emit`, `startWorker`, the four definers (the handler's payload is inferred from `schema` in the same object), `createNextasking`'s config and its nested `connection`.

## The verdict

**Yes, on every call, on both versions.** No call has to be left out, so no runtime check is needed for an unknown option.

`strict-options.ts` declares a strict twin of each signature, built from the real types of `src/`, next to the real one:

- 63 wrong calls are each refused, for the reason expected (`messages.typescript-*.txt`).
- The correct calls compile, and infer exactly what the real signatures infer: `IsEqual` holds between the strict and the real definitions, instance members and `listRuns` pages, explicit type arguments included.
- An editor completes the same option names, state values and task names (`completions.mts`).

## The shape that works

```ts
type UnknownOption<Key> = Key extends string ? `"${Key}" is not an option of this call` : "not an option of this call";

type UnknownKeysRefused<Given, Known> = Given extends object
  ? { [Key in Exclude<keyof Given, keyof Known>]: UnknownOption<Key> }
  : never;

type KeysRead<Given> = { [Key in keyof Given]?: unknown };

type OnlyKnown<Given, Known> = Known & UnknownKeysRefused<Given, Known> & KeysRead<Given>;

startWorker: <Given = StartWorkerOptions>(options?: OnlyKnown<Given, StartWorkerOptions>) => Promise<void>;
```

- `Given` is one more type parameter, last, with a default. It is not constrained.
- `KeysRead<Given>` is where `Given` is inferred. It reads the keys of the argument and nothing of their types, so the known options are still checked against `Known` alone.
- `listRuns` keeps `State` and `Name` first, with today's defaults, and still infers them from `ListRunsOptionsOf<Name, State>`. The unknown keys are counted against `ListRunsOptionsOf<string>`.
- `dispatch` carries `Given` into both branches of its conditional rest tuple.
- `createNextasking` takes a second parameter for the nested `connection`.

## What was tried and dropped

- **`Given & Known & UnknownKeysRefused<Given, Known>`** (`variant-a.intersect-given.ts`). It refuses the same calls, but intersecting `Given` with `Known` turns the error for a wrong value of a known option into `Type 'number' is not assignable to type 'never'`. `delay: 300` lost its message (`variant-a.messages.txt`).
- **`never` for a refused key.** The error did not name the option in a literal. A string literal type does: `Type '"5m"' is not assignable to type '"\"delayy\" is not an option of this call"'`.
- **A required `KeysRead`** (no `?`). It refuses `condition ? { delay: "5m" } : {}`, a correct call.
- **A constraint `Given extends Known`.** A variable with no known option falls back to the constraint, and passes.

## What it does not do

- A wrapper with a type parameter of its own (`<Options extends DispatchOptions>(options: Options) => dispatch(name, payload, options)`) compiles, whatever `Options` holds: the keys are not known where the call is checked.
- A variable typed as an interface that extends the options and adds a key is refused. That is the point, and it is also the one correct-looking call that stops compiling.
- A literal typo no longer gets TypeScript's `Did you mean 'state'?`. The message names the unknown option instead.
- A string passed in place of the options is still refused, but the message lists the members of `String`. Worth a guard in the real helper.
- The emitted declarations were not built. The dist typecheck of the implementation PR covers that.

## Also checked, for #260

- A task or a listener is refused where an event goes (`events`, a listener's `on`), by giving the event type `handler?: never`.
- A definer refuses the names `""` and `nextasking/…`, by a conditional type on the name.

## Run it

```sh
pnpm exec tsc -p prototypes/strict-options/tsconfig.json
tests/dist-consumer/node_modules/.bin/tsc -p prototypes/strict-options/tsconfig.json
pnpm exec tsx prototypes/strict-options/completions.mts
```

The first two print nothing when every case holds, on TypeScript 6 and 5.9. To read the messages, compile a copy without its directives:

```sh
sed '/@ts-expect-error/d' prototypes/strict-options/strict-options.ts > prototypes/strict-options/strict-options.without-directives.ts
pnpm exec tsc -p prototypes/strict-options/tsconfig.messages.json --pretty false
```
