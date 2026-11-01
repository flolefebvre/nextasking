# Agent skills

How third-party agent skills enter and track this repo. They live in `.agents/skills/`, which `.claude/skills` links to; `skills-lock.json` records the source of each one pulled from GitHub. An official skill is one its library's maintainers publish, from a repo they own or inside their package.

- When adding a dependency, look for its official skill: `npx skills find <name>` ranks by installs and shows only the top hits, so also list the maintainers' repo with `npx skills add <owner/repo> -l`, and confirm the repo's owner is the library's.
- Install an official skill published on GitHub at project scope, `npx skills add <owner/repo> -s <skill> -y`, which writes `.agents/skills/<skill>/` and its `skills-lock.json` entry.
- Install an official skill that ships inside its package with that package's own installer, so the skill always matches the installed version, and add the command to the update rule below.
- Install a community skill only once the user approves it.
- After `pnpm update`, run `npx skills update -p -y`, then read each skill's diff as you would a changelog: a skill that drops the version this repo runs, or links to a skill missing from `.agents/skills/`, goes to the user before the update lands.
- When removing a dependency, remove its skill: `npx skills remove -s <skill> -y`, or delete its folder when its package installed it.
