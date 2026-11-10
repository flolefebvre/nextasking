import ts from "typescript";

export type PackageEntry = "." | "./testing";

export type ExportKind = "value" | "type";

export type PublicExport = { entry: PackageEntry; name: string; kind: ExportKind };

export type EntryDeclaration = { entry: PackageEntry; declarationPath: string };

export type ReadDeclaration = (path: string) => string | undefined;

type ImportedName = PublicExport;

type ExportRow = { name: string; description: string };

const MODULE_SPECIFIER_BY_ENTRY: Record<PackageEntry, string> = {
  ".": "@flefebvre/nextasking",
  "./testing": "@flefebvre/nextasking/testing",
};

const SHOWN_ENTRY_BY_ENTRY: Record<PackageEntry, string> = { ".": "the root entry", "./testing": "`./testing`" };

const TESTING_ROW_PREFIX = "From `./testing`: ";

const SHOWN_TESTING_ROW_PREFIX = `“${TESTING_ROW_PREFIX}”`;

const INDEX_PAGE_PATH = "docs/index.md";

const EXPORT_TABLE = `the export table of ${INDEX_PAGE_PATH}`;

const IMPORT_MAP = `the import map of ${INDEX_PAGE_PATH}`;

const TYPESCRIPT_FENCE = /^```ts\n([\s\S]*?)^```$/gm;

const EXPORT_ROW_NAME = /^`([^`]+)`$/;

const COMPILER_OPTIONS: ts.CompilerOptions = {
  module: ts.ModuleKind.ESNext,
  moduleResolution: ts.ModuleResolutionKind.Bundler,
  noLib: true,
  types: [],
  noEmit: true,
};

const compilerHostOf = (readDeclaration: ReadDeclaration): ts.CompilerHost => ({
  getSourceFile: (path, languageVersion) => {
    const declarationText = readDeclaration(path);
    return declarationText === undefined ? undefined : ts.createSourceFile(path, declarationText, languageVersion);
  },
  fileExists: (path) => readDeclaration(path) !== undefined,
  readFile: readDeclaration,
  getDefaultLibFileName: () => "/lib.d.ts",
  writeFile: () => undefined,
  getCurrentDirectory: () => "/",
  getCanonicalFileName: (path) => path,
  useCaseSensitiveFileNames: () => true,
  getNewLine: () => "\n",
});

const isTypeOnlyExportDeclaration = (declaration: ts.Declaration) =>
  ts.isExportSpecifier(declaration) && (declaration.isTypeOnly || declaration.parent.parent.isTypeOnly);

const exportKindOf = (checker: ts.TypeChecker, exportSymbol: ts.Symbol): ExportKind => {
  let symbol = exportSymbol;
  while ((symbol.flags & ts.SymbolFlags.Alias) !== 0) {
    if ((symbol.declarations ?? []).some(isTypeOnlyExportDeclaration)) return "type";
    symbol = checker.getImmediateAliasedSymbol(symbol) ?? checker.getAliasedSymbol(symbol);
  }
  return (symbol.flags & ts.SymbolFlags.Value) !== 0 ? "value" : "type";
};

const byName = (left: { name: string }, right: { name: string }) => left.name.localeCompare(right.name, "en");

export const publicExportsOf = (
  entryDeclarations: readonly EntryDeclaration[],
  readDeclaration: ReadDeclaration,
): PublicExport[] => {
  const program = ts.createProgram(
    entryDeclarations.map(({ declarationPath }) => declarationPath),
    COMPILER_OPTIONS,
    compilerHostOf(readDeclaration),
  );
  const checker = program.getTypeChecker();
  return entryDeclarations.flatMap(({ entry, declarationPath }) => {
    const sourceFile = program.getSourceFile(declarationPath);
    const moduleSymbol = sourceFile === undefined ? undefined : checker.getSymbolAtLocation(sourceFile);
    if (moduleSymbol === undefined) {
      throw new Error(`The declaration of the entry ${entry}, ${declarationPath}, can't be read`);
    }
    return checker
      .getExportsOfModule(moduleSymbol)
      .map((exportSymbol): PublicExport => ({
        entry,
        name: exportSymbol.name,
        kind: exportKindOf(checker, exportSymbol),
      }))
      .sort(byName);
  });
};

const entryOfModuleSpecifier = (moduleSpecifier: string): PackageEntry | undefined =>
  (Object.keys(MODULE_SPECIFIER_BY_ENTRY) as PackageEntry[]).find(
    (entry) => MODULE_SPECIFIER_BY_ENTRY[entry] === moduleSpecifier,
  );

const importedNamesOfStatement = (statement: ts.Statement): ImportedName[] => {
  if (!ts.isImportDeclaration(statement) || !ts.isStringLiteral(statement.moduleSpecifier)) return [];
  const entry = entryOfModuleSpecifier(statement.moduleSpecifier.text);
  const namedBindings = statement.importClause?.namedBindings;
  if (entry === undefined || namedBindings === undefined || !ts.isNamedImports(namedBindings)) return [];
  const isClauseTypeOnly = statement.importClause?.isTypeOnly === true;
  return namedBindings.elements.map((element) => ({
    entry,
    name: (element.propertyName ?? element.name).text,
    kind: isClauseTypeOnly || element.isTypeOnly ? "type" : "value",
  }));
};

const importedNamesOf = (indexPageText: string): ImportedName[] =>
  [...indexPageText.matchAll(TYPESCRIPT_FENCE)].flatMap(([, codeText = ""]) =>
    ts.createSourceFile("import-map.ts", codeText, ts.ScriptTarget.Latest).statements.flatMap(importedNamesOfStatement),
  );

const cellsOf = (tableLine: string) =>
  tableLine
    .trim()
    .replace(/^\||\|$/g, "")
    .split("|")
    .map((cell) => cell.trim());

const exportRowsOf = (indexPageText: string): ExportRow[] => {
  const lines = indexPageText.split("\n");
  const headerIndex = lines.findIndex((line) => line.startsWith("|") && cellsOf(line)[0] === "Export");
  if (headerIndex === -1) return [];
  const rowLines = [];
  for (const line of lines.slice(headerIndex + 2)) {
    if (!line.startsWith("|")) break;
    rowLines.push(line);
  }
  return rowLines.map((line) => {
    const [nameCell = "", description = ""] = cellsOf(line);
    return { name: EXPORT_ROW_NAME.exec(nameCell)?.[1] ?? nameCell, description };
  });
};

const tableProblemsOf = (publicExports: readonly PublicExport[], exportRows: readonly ExportRow[]): string[] => {
  const exportsByName = new Map(publicExports.map((publicExport) => [publicExport.name, publicExport]));
  const countProblems = publicExports.flatMap(({ entry, name, kind }) => {
    const rowCount = exportRows.filter((row) => row.name === name).length;
    if (rowCount === 0)
      return [
        `\`${name}\`, a ${kind} export of ${SHOWN_ENTRY_BY_ENTRY[entry]}, has no row in ${EXPORT_TABLE}: add one.`,
      ];
    if (rowCount > 1) return [`\`${name}\` has ${String(rowCount)} rows in ${EXPORT_TABLE}: keep one.`];
    return [];
  });
  const unknownRowProblems = exportRows
    .filter((row) => !exportsByName.has(row.name))
    .map(
      (row) =>
        `The row \`${row.name}\` of ${EXPORT_TABLE} names no export of the built types: remove the row, or fix its name.`,
    );
  const prefixProblems = exportRows.flatMap((row) => {
    const publicExport = exportsByName.get(row.name);
    if (publicExport === undefined) return [];
    const hasTestingPrefix = row.description.startsWith(TESTING_ROW_PREFIX);
    if (publicExport.entry === "./testing" && !hasTestingPrefix) {
      return [
        `The row \`${row.name}\` of ${EXPORT_TABLE} does not start with ${SHOWN_TESTING_ROW_PREFIX}, but \`${row.name}\` is exported by \`./testing\`: add that prefix.`,
      ];
    }
    if (publicExport.entry === "." && hasTestingPrefix) {
      return [
        `The row \`${row.name}\` of ${EXPORT_TABLE} starts with ${SHOWN_TESTING_ROW_PREFIX}, but \`${row.name}\` is exported by the root entry: remove that prefix.`,
      ];
    }
    return [];
  });
  return [...countProblems, ...unknownRowProblems, ...prefixProblems];
};

const isSameEntryAndName = (
  left: { entry: PackageEntry; name: string },
  right: { entry: PackageEntry; name: string },
) => left.entry === right.entry && left.name === right.name;

const importMapProblemsOf = (
  publicExports: readonly PublicExport[],
  importedNames: readonly ImportedName[],
): string[] => {
  const importProblems = importedNames.flatMap((importedName) => {
    const publicExport = publicExports.find((candidate) => isSameEntryAndName(candidate, importedName));
    const shownEntry = SHOWN_ENTRY_BY_ENTRY[importedName.entry];
    if (publicExport === undefined) {
      return [
        `\`${importedName.name}\`, imported as a ${importedName.kind} from ${shownEntry} in ${IMPORT_MAP}, is no export of it: remove it from the import map.`,
      ];
    }
    if (publicExport.kind !== importedName.kind) {
      return [
        `\`${importedName.name}\` is a ${publicExport.kind} export of ${shownEntry}, imported as a ${importedName.kind} in ${IMPORT_MAP}: move it to the ${publicExport.kind} import.`,
      ];
    }
    return [];
  });
  const missingProblems = publicExports
    .filter((publicExport) => !importedNames.some((importedName) => isSameEntryAndName(importedName, publicExport)))
    .map(
      ({ entry, name, kind }) =>
        `\`${name}\`, a ${kind} export of ${SHOWN_ENTRY_BY_ENTRY[entry]}, is missing from ${IMPORT_MAP}: add it to the ${kind} import.`,
    );
  return [...importProblems, ...missingProblems];
};

export const manualExportProblemsOf = (publicExports: readonly PublicExport[], indexPageText: string): string[] => [
  ...tableProblemsOf(publicExports, exportRowsOf(indexPageText)),
  ...importMapProblemsOf(publicExports, importedNamesOf(indexPageText)),
];
