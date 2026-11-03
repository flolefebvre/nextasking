import { configDefaults, defineConfig } from "vitest/config";

const INTEGRATION_TESTS = ["src/**/*.integration.test.ts", "tests/**/*.integration.test.ts"];
const E2E_TESTS = ["tests/e2e/**/*.e2e.test.ts"];

export default defineConfig({
  test: {
    retry: 0,
    passWithNoTests: true,
    exclude: [...configDefaults.exclude, ".claude/worktrees/**", ".tmp/**", "**/.next/**"],
    projects: [
      {
        extends: true,
        test: {
          name: "unit",
          include: ["src/**/*.test.ts", "tests/**/*.test.ts"],
          exclude: [...INTEGRATION_TESTS, ...E2E_TESTS],
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
      {
        extends: true,
        test: {
          name: "e2e",
          include: E2E_TESTS,
          globalSetup: ["tests/e2e/global-setup.ts"],
          fileParallelism: false,
          testTimeout: 120_000,
          hookTimeout: 120_000,
        },
      },
    ],
  },
});
