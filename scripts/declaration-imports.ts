import { isBuiltin } from "node:module";

import ts from "typescript";

export type PeerManifest = {
  peerDependencies?: Record<string, string>;
  peerDependenciesMeta?: Record<string, { optional?: boolean }>;
};

const ALLOWED_TYPES_REFERENCES = ["node"];

export const requiredPeerNamesOf = ({ peerDependencies = {}, peerDependenciesMeta = {} }: PeerManifest): string[] =>
  Object.keys(peerDependencies).filter((peerName) => peerDependenciesMeta[peerName]?.optional !== true);

const isRelativeSpecifier = (specifier: string) => specifier.startsWith(".");

const packageNameOf = (specifier: string) => {
  const [scopeOrName = "", name] = specifier.split("/");
  return scopeOrName.startsWith("@") ? `${scopeOrName}/${name ?? ""}` : scopeOrName;
};

export const disallowedImportsOf = (declarationText: string, allowedPackageNames: readonly string[]): string[] => {
  const { importedFiles, typeReferenceDirectives } = ts.preProcessFile(declarationText, true, true);
  const disallowedPackageNames = importedFiles
    .map(({ fileName }) => fileName)
    .filter((specifier) => !isRelativeSpecifier(specifier) && !isBuiltin(specifier))
    .map(packageNameOf)
    .filter((packageName) => !allowedPackageNames.includes(packageName));
  const disallowedTypesReferences = typeReferenceDirectives
    .map(({ fileName }) => fileName)
    .filter((typesName) => !ALLOWED_TYPES_REFERENCES.includes(typesName))
    .map((typesName) => `/// <reference types="${typesName}" />`);
  return [...disallowedTypesReferences, ...disallowedPackageNames];
};
