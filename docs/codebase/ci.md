# CI

`.github/workflows/ci.yml` runs on every pull request, whatever its base branch, every push to `main`, and when `release.yml` calls it. A superseded run on the same ref is cancelled, except on `main`, where every push keeps its run. Its jobs run in parallel: `static`, `test` (one cell per server image: `valkey/valkey:8`, `valkey/valkey:9`, `redis:7.4`, the floor of the supported servers, and `redis:8`, passed as `NEXTASKING_TEST_IMAGE`), `build`, `e2e` (one cell per Next version: `16.0.10`, the floor of the peer range, and `latest`, passed as `NEXTASKING_E2E_NEXT_VERSION`), then `ci`, which waits for the other four.

`build` runs the `pack:check+typecheck:dist` stage after `Build`, in the same job because both read the built `dist`:

- `pack:check` (`scripts/pack-check.ts`) reads the real packlist (`npm pack --dry-run`) and the manifest. It fails when the `exports` map holds anything but its `EXPECTED_SUBPATHS`, when the manifest declares a bin not in its `EXPECTED_BIN_NAMES`, which is empty because the package ships no bin, when an `exports` target, a top-level `docs/*.md` page or `README.md` is missing, when a file outside `dist/`, the top-level manual pages, `README.md`, `LICENSE` and `package.json` ships, when a test file ships, or when a script runs on a consumer's install (`preinstall`, `install`, `postinstall`, `prepare`). Declare a new subpath in its `EXPECTED_SUBPATHS`, and a bin, public surface too, in its `EXPECTED_BIN_NAMES`, on purpose.
- `typecheck:dist` compiles `tests/dist-consumer`, a workspace package that imports `@flefebvre/nextasking` by name through the `exports` map, with `skipLibCheck: false` and a `Register` augmentation. It runs twice: on the root's TypeScript, the newest, and on the consumer's pinned `typescript`, the floor of the peer range; raise that pin with the peer range. Its assertions are `@ts-expect-error` lines and `Expect<IsEqual<…>>` types, so a shape that declaration emit loses fails it.

`e2e` runs the gate's last stage, `test:e2e`, after its own `Build`: it reads the built `dist` too, but runs once per Next version, so it is a matrix job rather than a step of `build`. Its 20-minute timeout covers the pack, a cold install of the fixture app from the registry, two `next build`s (the second with standalone output) and the scenarios. Raise the `16.0.10` cell with the `next` peer range, and the fixture's `next` pin with it (`docs/codebase/testing.md`, E2E harness).

- Give each stage of the `gate` line in package.json its own job, and each script in it its own step, in the gate's order, so a failing check names itself in the run summary. The stages after `build` are the exception: they run as steps of the `build` job, which holds the `dist` they read, except a stage with a matrix of its own, such as `test:e2e`, which gets its own job that builds first.
- Keep the summary job named `ci`: it is the status check the `main` ruleset (`.github/rulesets/main.json`) requires, so a renamed job blocks every merge. Add every new job to its `needs`, and keep its `if: ${{ !cancelled() }}` and its check that every needed job succeeded — a required check that is skipped counts as passing, so without them a failed job would leave a mergeable PR.
- Restore caches only for tools that reuse them across runs — `node_modules/.cache` (Prettier, ESLint) in `static` — keyed on the lockfile and the commit, restored from the latest key with the same lockfile, and placed after the install.
- Put a step every job needs in `.github/actions/setup`, the composite action each job calls right after checkout, never copied into the jobs. A job that needs it set up differently passes an input (`registry-url`, `store-cache`) whose default leaves every other job as it was.
- Pin `node-version` to the major the repo is developed on. `engines` admits several majors (`^22.22 || ^24.5 || >=26`), so a spec taken from it would let CI drift onto a major nobody has run.
- Keep `pnpm/action-setup` ahead of `actions/setup-node`, which needs pnpm on the PATH for `cache: pnpm`. It takes no `version` input: pnpm resolves from the `packageManager` pin in package.json.

Before pushing a change to the workflow, lint it with `docker run --rm -v "$PWD:/repo" -w /repo rhysd/actionlint:latest`.

## Release

`.github/workflows/release.yml` publishes to npm when a `v*` tag is pushed, and on nothing else. To release:

1. Bump `version` in package.json in a PR, and merge it.
2. Tag the merge commit `vX.Y.Z`, the same version, and push the tag: `git tag v0.1.0`, then `git push origin v0.1.0`.

The run is `guard`, then `ci` (the whole `ci.yml`, called through `workflow_call`), then `publish`:

- `guard` fails when the `NPM_TOKEN` secret is missing, or when the tag is not `v` plus the package.json `version`.
- `publish` runs `npm publish --provenance --access public` on a cold install (`prepublishOnly` rebuilds `dist`), then `gh release create --generate-notes --verify-tag`.
- Keep `release.yml`'s concurrency group a constant: inside the call, `ci.yml`'s group is `release-<ref>` and cancels in progress, so a group of that shape in `release.yml` would cancel the release itself.

The first publish authenticates with `NPM_TOKEN`, an npm automation token, because a package that does not exist yet has no settings page for a trusted publisher. Once it is live, add this repository and `release.yml` as its trusted publisher on npmjs.com, then delete `publish`'s `env:`, `guard`'s credential step and the secret. Provenance comes from the OIDC token either way.

`.github/rulesets/release-tags.json` lets only repository admins create, move or delete a `v*` tag.
