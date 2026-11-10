import { expect, test } from "vitest";

import { manualExportProblemsOf, publicExportsOf } from "./manual-exports.js";
import type { PublicExport } from "./manual-exports.js";

const declarationsReaderOf =
  (declarationTextsByPath: Record<string, string>) =>
  (path: string): string | undefined =>
    declarationTextsByPath[path];

const indexPageOf = (importMap: string, tableRows: string[]) =>
  [
    "# nextasking",
    "",
    "## Import map",
    "",
    "```ts",
    importMap,
    "```",
    "",
    "| Export | What it is |",
    "| ------ | ---------- |",
    ...tableRows,
    "",
  ].join("\n");

const ROOT_IMPORT_MAP = [
  'import { createNextasking } from "@flefebvre/nextasking";',
  'import type { RunId } from "@flefebvre/nextasking";',
].join("\n");

const ROOT_EXPORTS: PublicExport[] = [
  { entry: ".", name: "createNextasking", kind: "value" },
  { entry: ".", name: "RunId", kind: "type" },
];

test("an entry's exports are named with their kind, through re-exports, a class and a type-only re-export of a class included", () => {
  const readDeclaration = declarationsReaderOf({
    "/package/dist/index.d.ts": [
      'export { createNextasking, NonRetriableError } from "./registry.js";',
      'export type { RunId, Nextasking } from "./registry.js";',
      'export type { InternalError } from "./errors.js";',
      'export * from "./steps.js";',
    ].join("\n"),
    "/package/dist/registry.d.ts": [
      "export declare function createNextasking(): Nextasking;",
      "export declare class NonRetriableError extends Error {}",
      "export type RunId = string;",
      "export interface Nextasking { runId: RunId }",
    ].join("\n"),
    "/package/dist/errors.d.ts": "export declare class InternalError extends Error {}",
    "/package/dist/steps.d.ts":
      "export declare const runFailed: { name: string };\nexport declare namespace StepTools { type Id = string }",
  });

  const publicExports = publicExportsOf([{ entry: ".", declarationPath: "/package/dist/index.d.ts" }], readDeclaration);

  expect(publicExports).toEqual([
    { entry: ".", name: "createNextasking", kind: "value" },
    { entry: ".", name: "InternalError", kind: "type" },
    { entry: ".", name: "Nextasking", kind: "type" },
    { entry: ".", name: "NonRetriableError", kind: "value" },
    { entry: ".", name: "runFailed", kind: "value" },
    { entry: ".", name: "RunId", kind: "type" },
    { entry: ".", name: "StepTools", kind: "type" },
  ]);
});

test("a manual whose import map and export table list exactly the exports has no problem", () => {
  const indexPage = indexPageOf(ROOT_IMPORT_MAP, [
    "| `createNextasking` | Builds the instance |",
    "| `RunId` | The type of a run id |",
  ]);

  expect(manualExportProblemsOf(ROOT_EXPORTS, indexPage)).toEqual([]);
});

test("an export with no row, or with two, and a row naming no export are each named with what to do", () => {
  const indexPage = indexPageOf(ROOT_IMPORT_MAP, [
    "| `RunId` | The type of a run id |",
    "| `RunId` | Again |",
    "| `replayEvent` | Gone |",
  ]);

  expect(manualExportProblemsOf(ROOT_EXPORTS, indexPage)).toEqual([
    "`createNextasking`, a value export of the root entry, has no row in the export table of docs/index.md: add one.",
    "`RunId` has 2 rows in the export table of docs/index.md: keep one.",
    "The row `replayEvent` of the export table of docs/index.md names no export of the built types: remove the row, or fix its name.",
  ]);
});

test("a row of a ./testing export starts with From `./testing`, and a row of a root export does not", () => {
  const publicExports: PublicExport[] = [
    ...ROOT_EXPORTS,
    { entry: "./testing", name: "fakeNextasking", kind: "value" },
  ];
  const indexPage = indexPageOf(`${ROOT_IMPORT_MAP}\nimport { fakeNextasking } from "@flefebvre/nextasking/testing";`, [
    "| `createNextasking` | From `./testing`: builds the instance |",
    "| `RunId` | The type of a run id |",
    "| `fakeNextasking` | Switches an instance to recording mode |",
  ]);

  expect(manualExportProblemsOf(publicExports, indexPage)).toEqual([
    "The row `createNextasking` of the export table of docs/index.md starts with “From `./testing`: ”, but `createNextasking` is exported by the root entry: remove that prefix.",
    "The row `fakeNextasking` of the export table of docs/index.md does not start with “From `./testing`: ”, but `fakeNextasking` is exported by `./testing`: add that prefix.",
  ]);
});

test("an import map missing an export, importing a name no entry exports, or importing a value as a type names each with what to do", () => {
  const publicExports: PublicExport[] = [
    ...ROOT_EXPORTS,
    { entry: ".", name: "runFailed", kind: "value" },
    { entry: "./testing", name: "RecordedSleep", kind: "type" },
  ];
  const indexPage = indexPageOf(
    [
      'import { createNextasking, replayEvent } from "@flefebvre/nextasking";',
      'import type { RunId, runFailed } from "@flefebvre/nextasking";',
      'import type { RecordedSleep, RunId } from "@flefebvre/nextasking/testing";',
    ].join("\n"),
    [
      "| `createNextasking` | Builds the instance |",
      "| `RunId` | The type of a run id |",
      "| `runFailed` | The built-in event |",
      "| `RecordedSleep` | From `./testing`: a sleep |",
    ],
  );

  expect(manualExportProblemsOf(publicExports, indexPage)).toEqual([
    "`replayEvent`, imported as a value from the root entry in the import map of docs/index.md, is no export of it: remove it from the import map.",
    "`runFailed` is a value export of the root entry, imported as a type in the import map of docs/index.md: move it to the value import.",
    "`RunId`, imported as a type from `./testing` in the import map of docs/index.md, is no export of it: remove it from the import map.",
  ]);
});

test("an export missing from the import map is named with the import to add it to", () => {
  const indexPage = indexPageOf('import { createNextasking } from "@flefebvre/nextasking";', [
    "| `createNextasking` | Builds the instance |",
    "| `RunId` | The type of a run id |",
  ]);

  expect(manualExportProblemsOf(ROOT_EXPORTS, indexPage)).toEqual([
    "`RunId`, a type export of the root entry, is missing from the import map of docs/index.md: add it to the type import.",
  ]);
});
