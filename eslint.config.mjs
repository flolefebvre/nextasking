import { includeIgnoreFile } from "@eslint/compat";
import noComments from "eslint-plugin-no-comments";
import { defineConfig, globalIgnores } from "eslint/config";
import { fileURLToPath } from "node:url";
import tseslint from "typescript-eslint";

const gitignorePath = fileURLToPath(new URL(".gitignore", import.meta.url));

export default defineConfig([
  includeIgnoreFile(gitignorePath, "Ignore what git ignores"),
  globalIgnores([".agents/skills/**"], "Ignore the agent skills"),
  {
    files: ["**/*.{ts,tsx,mts,mjs}"],
    extends: [tseslint.configs.recommended],
    plugins: { "no-comments": noComments },
    rules: {
      "no-comments/disallowComments": ["error", { allow: ["eslint", "global", "@ts-", "prettier-ignore"] }],
    },
  },
  {
    files: ["**/*.integration.test.ts", "**/*.e2e.test.ts"],
    rules: {
      "no-restricted-imports": [
        "error",
        {
          paths: [
            {
              name: "vitest",
              importNames: ["test", "it"],
              message:
                "Write an integration test with integrationTest (tests/integration-test.ts) and an e2e test with e2eTest (tests/e2e/e2e-test.ts): they check the invariants after every test.",
            },
          ],
        },
      ],
    },
  },
]);
