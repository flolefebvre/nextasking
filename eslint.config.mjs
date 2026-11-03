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
    files: ["src/bin.ts"],
    rules: {
      "no-comments/disallowComments": [
        "error",
        { allow: ["eslint", "global", "@ts-", "prettier-ignore", "/usr/bin/env node$"] },
      ],
    },
  },
]);
