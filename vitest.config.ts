import { configDefaults, defineConfig } from "vitest/config";

const INTEGRATION_TESTS = ["src/**/*.integration.test.ts", "tests/**/*.integration.test.ts"];

export default defineConfig({
  test: {
    retry: 0,
    passWithNoTests: true,
    exclude: [...configDefaults.exclude, ".claude/worktrees/**", ".tmp/**"],
    projects: [
      {
        extends: true,
        test: {
          name: "unit",
          include: ["src/**/*.test.ts", "tests/**/*.test.ts"],
          exclude: INTEGRATION_TESTS,
        },
      },
      {
        extends: true,
        test: {
          name: "integration",
          include: INTEGRATION_TESTS,
          globalSetup: ["tests/global-setup.ts"],
          hookTimeout: 120_000,
        },
      },
    ],
  },
});
