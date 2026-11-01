# Testing

- The suite runs on Vitest 5 (`vitest.config.ts`).
- Tests are collocated with the file they test, as `.test.ts` files (`foo.test.ts` next to `foo.ts`) under `src/`; `tsconfig.build.json` keeps them out of `dist`.
- Lay out each test body as Arrange / Act / Assert, one blank line between phases; two phases that fuse into one statement share a block, and a phase a test doesn't have is simply absent.
