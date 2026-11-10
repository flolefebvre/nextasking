import { execFileSync } from "node:child_process";
import { existsSync, readdirSync, readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { disallowedImportsOf, requiredPeerNamesOf } from "./declaration-imports.js";
import type { PeerManifest } from "./declaration-imports.js";
import { manualExportProblemsOf, publicExportsOf } from "./manual-exports.js";

type ExportTarget = string | null | ExportTarget[] | { [condition: string]: ExportTarget };

type Manifest = PeerManifest & {
  name?: string;
  exports?: Record<string, ExportTarget>;
  bin?: string | Record<string, string>;
  scripts?: Record<string, string>;
};

const PACKAGE_DIR = fileURLToPath(new URL("../", import.meta.url));

const EXPECTED_SUBPATHS = [".", "./testing", "./package.json"];

const EXPECTED_BIN_NAMES: string[] = [];

const ALWAYS_SHIPPED_PATHS = ["README.md", "LICENSE"];

const ALLOWED_PATH = /^(dist\/[\w./-]+|docs\/[\w-]+\.md|README\.md|LICENSE|package\.json)$/;

const TEST_FILE_PATH = /\.test\.[cm]?[jt]sx?(\.map)?$|\.test\.d\.[cm]?ts(\.map)?$/;

const INSTALL_LIFECYCLE_SCRIPTS = ["preinstall", "install", "postinstall", "prepare"];

const fail = (message: string): never => {
  console.error(message);
  process.exit(1);
};

const withoutLeadingDotSlash = (path: string) => path.replace(/^\.\//, "");

const targetPathsOf = (target: ExportTarget): string[] => {
  if (target === null) return [];
  if (typeof target === "string") return [withoutLeadingDotSlash(target)];
  if (Array.isArray(target)) return target.flatMap(targetPathsOf);
  return Object.values(target).flatMap(targetPathsOf);
};

const binListOf = (binNames: string[]) => (binNames.length === 0 ? "no bin" : `the bins ${binNames.join(", ")}`);

const binNamesOf = ({ name = "", bin }: Manifest): string[] => {
  if (bin === undefined) return [];
  if (typeof bin === "string") return [name.replace(/^@[^/]+\//, "")];
  return Object.keys(bin);
};

if (!existsSync(new URL("../dist", import.meta.url))) {
  fail("dist/ is missing: run `pnpm build` first.");
}

const manifest = JSON.parse(readFileSync(new URL("../package.json", import.meta.url), "utf-8")) as Manifest;

const declaredSubpaths = Object.keys(manifest.exports ?? {});
if (declaredSubpaths.join() !== EXPECTED_SUBPATHS.join()) {
  fail(
    `The exports map declares ${declaredSubpaths.join(", ")}, expected ${EXPECTED_SUBPATHS.join(", ")}. ` +
      "A new subpath is declared here on purpose.",
  );
}

const declaredBinNames = binNamesOf(manifest);
if (declaredBinNames.join() !== EXPECTED_BIN_NAMES.join()) {
  fail(
    `The manifest declares ${binListOf(declaredBinNames)}, expected ${binListOf(EXPECTED_BIN_NAMES)}. ` +
      "A bin is public surface: a new, renamed or dropped bin is declared here on purpose.",
  );
}

const installScripts = INSTALL_LIFECYCLE_SCRIPTS.filter((name) => name in (manifest.scripts ?? {}));
if (installScripts.length > 0) {
  fail(`The manifest runs scripts on a consumer's install: ${installScripts.join(", ")}`);
}

const [packResult] = JSON.parse(
  execFileSync("npm", ["pack", "--dry-run", "--json", "--ignore-scripts"], { cwd: PACKAGE_DIR, encoding: "utf-8" }),
) as [{ files: { path: string }[] }];
const packedPaths = packResult.files.map((file) => file.path).sort();

const strayPaths = packedPaths.filter((path) => !ALLOWED_PATH.test(path));
if (strayPaths.length > 0) {
  fail(`Unexpected files in the tarball: ${strayPaths.join(", ")}`);
}

const testFilePaths = packedPaths.filter((path) => TEST_FILE_PATH.test(path));
if (testFilePaths.length > 0) {
  fail(`Test files in the tarball: ${testFilePaths.join(", ")}`);
}

const manualPagePaths = readdirSync(new URL("../docs", import.meta.url), { withFileTypes: true })
  .filter((entry) => entry.isFile() && entry.name.endsWith(".md"))
  .map((entry) => `docs/${entry.name}`);

const requiredPaths = [
  ...Object.values(manifest.exports ?? {}).flatMap(targetPathsOf),
  ...manualPagePaths,
  ...ALWAYS_SHIPPED_PATHS,
];
const missingPaths = [...new Set(requiredPaths)].filter((path) => !packedPaths.includes(path));
if (missingPaths.length > 0) {
  fail(`Missing from the tarball: ${missingPaths.join(", ")}`);
}

const requiredPeerNames = requiredPeerNamesOf(manifest);
const declarationPaths = packedPaths.filter((path) => path.endsWith(".d.ts"));
const disallowedDeclarationImports = declarationPaths.flatMap((path) =>
  disallowedImportsOf(readFileSync(new URL(`../${path}`, import.meta.url), "utf-8"), requiredPeerNames).map(
    (disallowedImport) => `${path} imports ${disallowedImport}`,
  ),
);
if (disallowedDeclarationImports.length > 0) {
  fail(
    "A public declaration imports a package that is neither a required peer " +
      `(${requiredPeerNames.join(", ")}) nor a Node built-in: ${disallowedDeclarationImports.join(", ")}`,
  );
}

const publicExports = publicExportsOf(
  [
    { entry: ".", declarationPath: `${PACKAGE_DIR}dist/index.d.ts` },
    { entry: "./testing", declarationPath: `${PACKAGE_DIR}dist/testing.d.ts` },
  ],
  (path) => (existsSync(path) ? readFileSync(path, "utf-8") : undefined),
);
const manualExportProblems = manualExportProblemsOf(
  publicExports,
  readFileSync(new URL("../docs/index.md", import.meta.url), "utf-8"),
);
if (manualExportProblems.length > 0) {
  fail(
    "The manual's import map and export table disagree with the built types; a change to the public interface " +
      `updates them in the same PR:\n${manualExportProblems.map((problem) => `- ${problem}`).join("\n")}`,
  );
}

console.log(
  `Tarball OK: ${String(packedPaths.length)} files, the exports ${EXPECTED_SUBPATHS.join(", ")} ` +
    `and ${String(manualPagePaths.length)} manual pages ship, the manifest declares ${binListOf(EXPECTED_BIN_NAMES)}, ` +
    "nothing outside dist, the manual, README.md, LICENSE and package.json does, and no script runs on install; " +
    `the ${String(declarationPaths.length)} declarations import nothing but the required peers ` +
    `(${requiredPeerNames.join(", ")}) and Node's built-ins and types; ` +
    `the ${String(publicExports.length)} exports of the two entries each have one row in the export table ` +
    "of docs/index.md, and its import map lists exactly them, values as values and types as types.",
);
