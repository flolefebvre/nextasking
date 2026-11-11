import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import ts from "typescript";

const folder = dirname(fileURLToPath(import.meta.url));
const sourcePath = resolve(folder, "strict-options.ts");
const probePath = resolve(folder, "completions-probe.ts");

const probes: Record<string, string> = {
  "real listRuns": `void real.listRuns({ /*here*/ });`,
  "strict listRuns": `void strict.listRuns({ /*here*/ });`,
  "strict listRuns, one option given": `void strict.listRuns({ limit: 5, /*here*/ });`,
  "real dispatch options": `void real.dispatch("send-email", payload, { /*here*/ });`,
  "strict dispatch options": `void strict.dispatch("send-email", payload, { /*here*/ });`,
  "strict emit options": `void strict.emit("user-signed-up", payload, { /*here*/ });`,
  "strict startWorker": `void strict.startWorker({ /*here*/ });`,
  "real defineTask": `defineTask("x", { schema, /*here*/ });`,
  "strict defineTask": `strictDefineTask("x", { schema, /*here*/ });`,
  "real createNextasking": `createNextasking({ /*here*/ });`,
  "strict createNextasking": `strictCreateNextasking({ /*here*/ });`,
  "strict createNextasking connection": `strictCreateNextasking({ tasks: [sendEmail], connection: { /*here*/ } });`,
  "strict listRuns state value": `void strict.listRuns({ state: "/*here*/" });`,
  "strict dispatch name": `void strict.dispatch("/*here*/");`,
};

const source = readFileSync(sourcePath, "utf8");
let probeText = source;
const positions: Record<string, number> = {};
for (const [label, line] of Object.entries(probes)) {
  positions[label] = probeText.length + 1 + line.indexOf("/*here*/");
  probeText += `\n${line}`;
}

const configPath = resolve(folder, "tsconfig.json");
const config = ts.parseJsonConfigFileContent(ts.readConfigFile(configPath, ts.sys.readFile).config, ts.sys, folder);
const host: ts.LanguageServiceHost = {
  getScriptFileNames: () => [probePath],
  getScriptVersion: () => "1",
  getScriptSnapshot: (fileName) => {
    if (fileName === probePath) {
      return ts.ScriptSnapshot.fromString(probeText);
    }
    const text = ts.sys.readFile(fileName);
    return text === undefined ? undefined : ts.ScriptSnapshot.fromString(text);
  },
  getCurrentDirectory: () => folder,
  getCompilationSettings: () => config.options,
  getDefaultLibFileName: (options) => ts.getDefaultLibFilePath(options),
  fileExists: (fileName) => fileName === probePath || ts.sys.fileExists(fileName),
  readFile: (fileName) => (fileName === probePath ? probeText : ts.sys.readFile(fileName)),
  readDirectory: ts.sys.readDirectory,
  directoryExists: ts.sys.directoryExists,
  getDirectories: ts.sys.getDirectories,
};
const service = ts.createLanguageService(host);

console.log(`TypeScript ${ts.version}`);
for (const [label, position] of Object.entries(positions)) {
  const completions = service.getCompletionsAtPosition(probePath, position, {});
  const names = (completions?.entries ?? [])
    .filter(
      (entry) =>
        entry.kind === ts.ScriptElementKind.memberVariableElement || entry.kind === ts.ScriptElementKind.string,
    )
    .map((entry) => entry.name);
  console.log(`${label}: ${names.length === 0 ? "(none)" : names.join(", ")}`);
}
