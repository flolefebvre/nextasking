# Testing

- The suite runs on Vitest 5 (`vitest.config.ts`) as three projects: `unit` (`pnpm test:unit`), which never needs Docker, `integration` (`pnpm test:integration`), which runs against a real server, and `e2e` (`pnpm test:e2e`), which runs the packed package in a Next app. `pnpm test` and `pnpm check` run `unit` and `integration`, so they need Docker unless `NEXTASKING_TEST_REDIS_URL` is set; `pnpm gate` runs all three, `e2e` last.
- Name a test file after its project: `foo.test.ts` is a unit test, `foo.integration.test.ts` an integration test, `foo.e2e.test.ts` an e2e test. The suffix alone decides the project.
- Collocate a test with the file it tests (`foo.test.ts` or `foo.integration.test.ts` next to `foo.ts`) under `src/`; `tsconfig.build.json` keeps them out of `dist`.
- Put a scenario that crosses modules or processes in `tests/` instead, named as a sentence saying what it proves (`tests/a-server-round-trips-a-value.integration.test.ts`). The shared harness lives there too, named after what it exports.
- Lay out each test body as Arrange / Act / Assert, one blank line between phases; two phases that fuse into one statement share a block, and a phase a test doesn't have is simply absent.

## Integration harness

- `tests/global-setup.ts` starts one container per run with testcontainers and discards it at teardown; every integration test talks to that server.
- The image is `valkey/valkey:9` unless `NEXTASKING_TEST_IMAGE` names another; CI runs the `test` job once per image in its matrix (`docs/codebase/ci.md`).
- Set `NEXTASKING_TEST_REDIS_URL` to a full URL (credentials, port, `rediss:` included) to run against a server already running instead; no container starts then. An empty variable counts as unset, for both.
- Write an integration test with `integrationTest` from `tests/integration-test.ts` rather than `test`: it hands the test `redisUrl` and a `keyPrefix` of its own. Put every key the test touches under that prefix, so tests sharing the server never see each other's keys; never build a prefix in the test.
- The test opens its own client on `redisUrl`, with an `error` listener attached before it connects, and closes it in a `finally`, so a failed assertion leaves no open handle.

## E2E harness

- The e2e project proves the library the way a consumer uses it: packed, installed by name, bundled by Next, started from `instrumentation.ts`. Its scenarios, its harness and its fixture app live in `tests/e2e/`; the fixture app, `tests/e2e/fixture-app/`, is the manual's example made real, with no `next.config` so that a missing `serverExternalPackages` entry would fail it.
- `tests/e2e/global-setup.ts` runs once per run: it fails unless `dist/` exists (run `pnpm build` first; the gate's `build` stage precedes `test:e2e`), copies the fixture app into a fresh directory under the OS temp dir, `pnpm pack`s the root into that copy as `nextasking.tgz`, installs it with no lockfile, runs `next build`, then starts the server exactly as the integration harness does (`NEXTASKING_TEST_IMAGE`, `NEXTASKING_TEST_REDIS_URL`). Teardown stops the server and deletes the copy.
- The harness builds a copy; never run `next` in `tests/e2e/fixture-app` itself. `next build` and `next dev` rewrite `tsconfig.json` and write `next-env.d.ts`, `.next/` and, on recent versions, an `AGENTS.md` into the directory they run in.
- The fixture pins `next` to the floor of the peer range; `NEXTASKING_E2E_NEXT_VERSION` replaces that pin in the copy (`16.4.3`, `latest`). `latest` resolves through pnpm's minimum release age, so it can be one release behind npm's `latest` tag.
- Write a scenario with `e2eTest` from `tests/e2e/e2e-test.ts`: it hands the test `redisUrl`, `keyPrefix`, `fixtureAppDir` and `fixtureAppEnv`, the environment that points the fixture at that server, under that prefix, with the embedded worker on.
- Start every process the scenario needs with `startNext` from `tests/e2e/next-process.ts` (`next start` or `next dev`, on a free port, waiting until it answers), and `stop()` it in a `finally`. Scenarios run one file at a time.
- Assert on what a consumer observes: HTTP responses, the process output, and the records the fixture's tasks write under the scenario's prefix (`fixtureRecordsOf`). Drive a server action as a browser without JavaScript does, with `submitServerActionForm`.
- Run the project alone with `pnpm build`, then `pnpm test:e2e`; it needs Docker, unless `NEXTASKING_TEST_REDIS_URL` is set, and the npm registry.

## Waiting

- Wait for an asynchronous outcome by polling it with `vi.waitFor` (retries a callback until it stops throwing) or `vi.waitUntil` (until it returns a truthy value), each with an explicit `timeout`. A fixed sleep is never a wait.
- `retry` is 0 in `vitest.config.ts`: a test that only passes on a second try is a failing test.
