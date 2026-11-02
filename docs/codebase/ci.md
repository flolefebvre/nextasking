# CI

`.github/workflows/ci.yml` runs on every pull request, whatever its base branch, and every push to `main`. A superseded run on the same ref is cancelled, except on `main`, where every push keeps its run. Its jobs run in parallel: `static`, `test` (one cell per server image: `valkey/valkey:8`, `valkey/valkey:9`, `redis:8`, passed as `NEXTASKING_TEST_IMAGE`), `build`, then `ci`, which waits for the other three.

`build` runs the gate's last two stages after `Build`, in the same job because both read the built `dist`:

- `pack:check` (`scripts/pack-check.ts`) reads the real packlist (`npm pack --dry-run`) and the manifest. It fails when the `exports` map holds anything but `.` and `./package.json`, when an `exports` or `bin` target, a top-level `docs/*.md` page or `README.md` is missing, when a file outside `dist/`, the top-level manual pages, `README.md`, `LICENSE` and `package.json` ships, when a test file ships, or when a script runs on a consumer's install (`preinstall`, `install`, `postinstall`, `prepare`). Declare a new subpath in its `EXPECTED_SUBPATHS` on purpose; a new `bin` entry is checked with no edit.
- `typecheck:dist` compiles `tests/dist-consumer`, a workspace package that imports `@flefebvre/nextasking` by name through the `exports` map, with `skipLibCheck: false` and a `Register` augmentation. It runs twice: on the root's TypeScript, the newest, and on the consumer's pinned `typescript`, the floor of the peer range; raise that pin with the peer range. Its assertions are `@ts-expect-error` lines and `Expect<IsEqual<…>>` types, so a shape that declaration emit loses fails it.

- Give each stage of the `gate` line in package.json its own job, and each script in it its own step, in the gate's order, so a failing check names itself in the run summary.
- Keep the summary job named `ci`: it is the status check the `main` ruleset (`.github/rulesets/main.json`) requires, so a renamed job blocks every merge. Add every new job to its `needs`, and keep its `if: ${{ !cancelled() }}` and its check that every needed job succeeded — a required check that is skipped counts as passing, so without them a failed job would leave a mergeable PR.
- Restore caches only for tools that reuse them across runs — `node_modules/.cache` (Prettier, ESLint) in `static` — keyed on the lockfile and the commit, restored from the latest key with the same lockfile, and placed after the install.
- Put a step every job needs in `.github/actions/setup`, the composite action each job calls right after checkout, never copied into the jobs.
- Pin `node-version` to the major the repo is developed on. `engines` admits several majors (`^22.22 || ^24.5 || >=26`), so a spec taken from it would let CI drift onto a major nobody has run.
- Keep `pnpm/action-setup` ahead of `actions/setup-node`, which needs pnpm on the PATH for `cache: pnpm`. It takes no `version` input: pnpm resolves from the `packageManager` pin in package.json.

Before pushing a change to the workflow, lint it with `docker run --rm -v "$PWD:/repo" -w /repo rhysd/actionlint:latest`.
