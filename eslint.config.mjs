import { includeIgnoreFile } from "@eslint/compat";
import noComments from "eslint-plugin-no-comments";
import { defineConfig, globalIgnores } from "eslint/config";
import { fileURLToPath } from "node:url";
import tseslint from "typescript-eslint";

const gitignorePath = fileURLToPath(new URL(".gitignore", import.meta.url));

const CLOSE_NEXTASKING_RESTRICTION = {
  group: ["**/registry.js"],
  importNames: ["closeNextasking"],
  message:
    "Close an instance in a test with closeNextaskingAndCheckItsDuration (tests/close-nextasking-and-check-its-duration.ts): it checks the close against its bound.",
};

const NODE_REDIS_RESTRICTION = {
  group: ["@redis/client", "@redis/client/*"],
  message:
    "Only the driver (src/node-redis-driver.ts) imports @redis/client: the rest of the library reaches Redis through it.",
};

const TASK_CONST_RESTRICTION = {
  selector:
    'VariableDeclaration[kind="const"] > VariableDeclarator[id.type="Identifier"][id.name!=/Task$/]:matches([init.callee.name=/^define(Task|Listener|ScheduledTask)$/], [init.expression.callee.name=/^define(Task|Listener|ScheduledTask)$/])',
  message:
    "End a const holding a task, a listener or a scheduled task with Task (sendWelcomeEmailTask): without it, the const reads as a function to call.",
};

const EVENT_CONST_RESTRICTION = {
  selector:
    'VariableDeclaration[kind="const"] > VariableDeclarator[id.type="Identifier"][id.name!=/Event$/]:matches([init.callee.name="defineEvent"], [init.expression.callee.name="defineEvent"])',
  message: "End a const holding an event with Event (invoicePaidEvent): without it, the const reads as a boolean.",
};

export default defineConfig([
  includeIgnoreFile(gitignorePath, "Ignore what git ignores"),
  globalIgnores([".agents/skills/**"], "Ignore the agent skills"),
  {
    files: ["**/*.{ts,tsx,mts,mjs}"],
    extends: [tseslint.configs.recommended],
    plugins: { "no-comments": noComments },
    rules: {
      "no-comments/disallowComments": [
        "error",
        { allow: ["eslint", "global", "@ts-", "prettier-ignore", "/ <reference "] },
      ],
      "no-restricted-syntax": ["error", TASK_CONST_RESTRICTION, EVENT_CONST_RESTRICTION],
    },
  },
  {
    files: ["src/**", "tests/**"],
    ignores: ["tests/e2e/fixture-app/**", "tests/dist-consumer/**"],
    rules: {
      "no-restricted-syntax": "off",
    },
  },
  {
    files: ["src/**/*.ts"],
    ignores: ["src/**/*.test.ts", "src/node-redis-driver.ts"],
    rules: {
      "no-restricted-imports": ["error", { patterns: [NODE_REDIS_RESTRICTION] }],
    },
  },
  {
    files: ["tests/**/*.ts"],
    ignores: ["tests/close-nextasking-and-check-its-duration.ts", "tests/close-while-booting-process-entry.ts"],
    rules: {
      "no-restricted-imports": ["error", { patterns: [CLOSE_NEXTASKING_RESTRICTION] }],
    },
  },
  {
    files: ["**/*.integration.test.ts", "**/*.e2e.test.ts", "**/*.benchmark.test.ts"],
    rules: {
      "no-restricted-imports": [
        "error",
        {
          paths: [
            {
              name: "vitest",
              importNames: ["test", "it"],
              message:
                "Write an integration test with integrationTest (tests/integration-test.ts), an e2e test with e2eTest (tests/e2e/e2e-test.ts) and a benchmark with benchmarkTest (tests/benchmark/benchmark-test.ts): they check the invariants after every test.",
            },
          ],
          patterns: [CLOSE_NEXTASKING_RESTRICTION],
        },
      ],
    },
  },
]);
