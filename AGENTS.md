# nextasking

## Context

This is `@flefebvre/nextasking`, a library published on npm. Its users are the apps that install it and the agents that write those apps, so the public API, and later the docs that ship in the package, are the product as much as the code. Every change is judged as something every consumer will install.

Its purpose: a Next.js app on long-running Node hosting defines tasks and events in a few files, lists them in one `createNextasking` call, dispatches or emits them by literal name from server code, and a worker embedded in `next start` executes them on the library's own run engine, on Redis or Valkey. The public API is the root export (the definers such as `defineTask`, `createNextasking` and the instance it returns with `dispatch`, `emit`, `startWorker` and the read API a dashboard is built on (`getRun`, `listRuns`, `countRuns`, `getEvent`, `listSchedules`, `taskDefinitions`, `eventDefinitions`), the `Register` augmentation, the error classes and the public types), and a `./testing` subpath. The v1 spec is issue #40; its tickets add to this surface one by one.

## Stack

- TypeScript 6, `strict`, ESM only, `NodeNext` resolution, built with `tsc`
- Node `^22.22 || ^24.5 || >=26`, pnpm 11
- Vitest 5 as the only test runner
- ESLint 10 (typescript-eslint), Prettier 3, jscpd 5 (copy/paste detection)
- tsx, for the repo scripts

## Layout

```
src/            The published package source; builds into dist/
scripts/        Repo tooling, run with tsx
tests/          The test harness and the scenarios that cross modules or processes
skills/         The git-only skill stub pointing consumers' agents at the shipped manual; never shipped
docs/           Top-level pages: the shipped manual, for consumers' agents
  adr/          Decisions, numbered
  agents/       Agent-skill config: issue tracker, triage labels, domain docs
  codebase/     Conventions, one file per area, linked from the AGENTS.md section it governs
```

## Commands

```
pnpm check                Format check, lint, duplication, typecheck, then the tests affected since $CHECK_BASE (default origin/main) — the inner loop
pnpm gate                 Format check, lint, duplication, typecheck, then test, build, then pack check and dist typecheck, then test:e2e — the full check, the same as CI
pnpm build                Build the package into dist/
pnpm pack:check           Check the tarball after build: exports targets and manual pages ship, nothing else does, no bin, no install script
pnpm typecheck:dist       Typecheck tests/dist-consumer against the built dist, on the TypeScript floor and the newest
pnpm format               Prettier, rewriting files in place
pnpm format:check         Prettier in check mode (fails on unformatted files)
pnpm lint                 ESLint flat config (eslint.config.mjs), warnings are failures
pnpm duplication          jscpd copy/paste detection (.jscpd.json); any clone is a failure
pnpm typecheck            TypeScript with no emit
pnpm test                 Vitest, once, the unit and integration projects (integration needs Docker)
pnpm test:unit            Vitest, once, the unit project alone (no Docker)
pnpm test:integration     Vitest, once, the integration project alone, against a Valkey container
pnpm test:changed         Vitest, once, only the unit and integration tests affected since $CHECK_BASE (default origin/main)
pnpm test:e2e             Vitest, once, the e2e project: packs the package into a fixture Next app, builds it, runs the scenarios (needs pnpm build first, Docker and the npm registry)
```

## Verification

- check: `CHECK_BASE=origin/<base-branch> pnpm check`
- gate: `pnpm gate`
- final: ci

`<base-branch>` is the branch the change will merge into, `main` unless told otherwise. Run `check` after each step and each fix; it passed only if the output ends with `--check OK--`. A change is done when the pushed PR's `ci` check is green; a change that will not be pushed passes `pnpm gate` (ends with `--gate OK--`) once before it is handed over; later fixes re-run `check`, and CI re-validates the change when it is pushed. Any other ending is a failure, whatever the rest of the output says.

The `check` and `gate` lines in package.json are the sequences: `,` separates stages run in order, `+` joins scripts run in parallel. Never join two scripts that write the same files. Add a check to the `gate` line and to the CI workflow, which runs one job per stage, except the stages that read the built `dist`, which are steps of the `build` job unless they run over a matrix of their own (`test:e2e`). See `docs/codebase/ci.md`.

## Worktrees

- Create and leave worktrees with the `EnterWorktree` and `ExitWorktree` tools, never with `git worktree add`.
- Run `git fetch` before creating one: a worktree starts from `origin/main`, not local `main`.
- In a new worktree, run `pnpm install` before anything else.

## Testing

This project uses TDD. Tests are collocated with the file they test; scenarios crossing modules or processes live in `tests/`. See `docs/codebase/testing.md` before writing tests.

## Naming

Every name passes the test in `docs/codebase/naming.md`; read it before naming or renaming anything.

## Documentation

Always reach for documentation rather than working from memory: the installed package's own docs in `node_modules` first, then ctx7.

## Comments and docs

Don't write comments in the code — `pnpm lint` fails on any that isn't a directive.

- The top-level pages of `docs/` are the shipped manual, written for consumers' agents first: short, imperative, example-dense. A change to the public interface updates the manual in the same PR; `docs/index.md` routes each job to its page.
- Edit the `docs/` subfolders only when the user asks, with one standing exception: a change to the test harness (`tests/`, the Vitest config) updates `docs/codebase/testing.md` in the same PR.

## Language

Prose in this repo — docs, commit messages, issues — is written in English, regardless of the language used in conversation.

## Agent skills

Adding, updating, or removing a dependency changes its agent skill too: read `docs/codebase/skills.md` first.

### Issue tracker

Issues and specs live as GitHub issues on `flolefebvre/nextasking`, managed via the `gh` CLI. See `docs/agents/issue-tracker.md`.

### Triage labels

The five canonical triage roles, each label named after its role. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context: one `GLOSSARY.md` and one `docs/adr/` at the repo root, both created on first use. See `docs/agents/domain.md`.

## Keeping this file current

If your changes mandate an update to a section of this `AGENTS.md` file, make the change. Do not add any sections on your own. How this file and `docs/codebase/` are written: `docs/codebase/agent-docs.md`.
