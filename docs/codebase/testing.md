# Testing

- The suite runs on Vitest 5 (`vitest.config.ts`) as two projects: `unit` (`pnpm test:unit`), which never needs Docker, and `integration` (`pnpm test:integration`), which runs against a real server. `pnpm test`, `pnpm check` and `pnpm gate` run both, so they need Docker unless `NEXTASKING_TEST_REDIS_URL` is set.
- Name a test file after its project: `foo.test.ts` is a unit test, `foo.integration.test.ts` an integration test, and `foo.e2e.test.ts` is reserved for the e2e project when it lands. The suffix alone decides the project.
- Collocate a test with the file it tests (`foo.test.ts` or `foo.integration.test.ts` next to `foo.ts`) under `src/`; `tsconfig.build.json` keeps them out of `dist`.
- Put a scenario that crosses modules or processes in `tests/` instead, named as a sentence saying what it proves (`tests/a-server-round-trips-a-value.integration.test.ts`). The shared harness lives there too, named after what it exports.
- Lay out each test body as Arrange / Act / Assert, one blank line between phases; two phases that fuse into one statement share a block, and a phase a test doesn't have is simply absent.

## Integration harness

- `tests/global-setup.ts` starts one container per run with testcontainers and discards it at teardown; every integration test talks to that server.
- The image is `valkey/valkey:9` unless `NEXTASKING_TEST_IMAGE` names another; CI runs the `test` job once per image in its matrix (`docs/codebase/ci.md`).
- Set `NEXTASKING_TEST_REDIS_URL` to a full URL (credentials, port, `rediss:` included) to run against a server already running instead; no container starts then. An empty variable counts as unset, for both.
- Write an integration test with `integrationTest` from `tests/integration-test.ts` rather than `test`: it hands the test `redisUrl` and a `keyPrefix` of its own. Put every key the test touches under that prefix, so tests sharing the server never see each other's keys; never build a prefix in the test.
- The test opens its own client on `redisUrl`, with an `error` listener attached before it connects, and closes it in a `finally`, so a failed assertion leaves no open handle.

## Waiting

- Wait for an asynchronous outcome by polling it with `vi.waitFor` (retries a callback until it stops throwing) or `vi.waitUntil` (until it returns a truthy value), each with an explicit `timeout`. A fixed sleep is never a wait.
- `retry` is 0 in `vitest.config.ts`: a test that only passes on a second try is a failing test.
