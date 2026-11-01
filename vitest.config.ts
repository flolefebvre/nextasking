import { configDefaults, defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["src/**/*.test.ts"],
    exclude: [...configDefaults.exclude, ".claude/worktrees/**", ".tmp/**"],
    passWithNoTests: true,
  },
});
