# CI

`.github/workflows/ci.yml` runs on every pull request, whatever its base branch, and every push to `main`. A superseded run on the same ref is cancelled, except on `main`, where every push keeps its run. Its jobs run in parallel: `static`, `test` (one cell per server image: `valkey/valkey:8`, `valkey/valkey:9`, `redis:8`, passed as `NEXTASKING_TEST_IMAGE`), `build`, then `ci`, which waits for the other three.

- Give each stage of the `gate` line in package.json its own job, and each script in it its own step, in the gate's order, so a failing check names itself in the run summary.
- Keep the summary job named `ci`: it is the status check the `main` ruleset (`.github/rulesets/main.json`) requires, so a renamed job blocks every merge. Add every new job to its `needs`, and keep its `if: ${{ !cancelled() }}` and its check that every needed job succeeded — a required check that is skipped counts as passing, so without them a failed job would leave a mergeable PR.
- Restore caches only for tools that reuse them across runs — `node_modules/.cache` (Prettier, ESLint) in `static` — keyed on the lockfile and the commit, restored from the latest key with the same lockfile, and placed after the install.
- Put a step every job needs in `.github/actions/setup`, the composite action each job calls right after checkout, never copied into the jobs.
- Pin `node-version` to the major the repo is developed on. `engines` admits several majors (`^22.22 || ^24.5 || >=26`), so a spec taken from it would let CI drift onto a major nobody has run.
- Keep `pnpm/action-setup` ahead of `actions/setup-node`, which needs pnpm on the PATH for `cache: pnpm`. It takes no `version` input: pnpm resolves from the `packageManager` pin in package.json.

Before pushing a change to the workflow, lint it with `docker run --rm -v "$PWD:/repo" -w /repo rhysd/actionlint:latest`.
