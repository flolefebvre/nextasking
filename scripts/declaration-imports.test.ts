import { expect, test } from "vitest";

import { disallowedImportsOf, requiredPeerNamesOf } from "./declaration-imports.js";

test("the required peers are the declared peers not marked optional, and no dependency", () => {
  const manifest = {
    dependencies: { "@redis/client": "6.3.1" },
    peerDependencies: { next: ">=16.0.10", typescript: ">=5.9" },
    peerDependenciesMeta: { typescript: { optional: true } },
  };

  const requiredPeerNames = requiredPeerNamesOf(manifest);

  expect(requiredPeerNames).toEqual(["next"]);
});

test("a type imported inline from a dependency, as declaration emit writes an inferred type, is disallowed", () => {
  const declarationText = 'export declare const client: import("@redis/client").RedisClientType;\n';

  const disallowedImports = disallowedImportsOf(declarationText, ["next"]);

  expect(disallowedImports).toEqual(["@redis/client"]);
});

test("import type, export from, export * from and import = require each name the package they import", () => {
  const declarationText = [
    'import type { RedisClientType } from "@redis/client";',
    'export { Redis } from "ioredis";',
    'export * from "cron-parser";',
    'import zod = require("zod");',
  ].join("\n");

  const disallowedImports = disallowedImportsOf(declarationText, ["next"]);

  expect(disallowedImports).toEqual(["@redis/client", "ioredis", "cron-parser", "zod"]);
});

test("a subpath import names its package, scoped or not", () => {
  const declarationText = [
    'import type { RedisClientType } from "@redis/client/dist/lib/client/index.js";',
    'import type { NextRequest } from "next/server";',
    'import type { CronExpression } from "cron-parser/types";',
  ].join("\n");

  const disallowedImports = disallowedImportsOf(declarationText, ["next"]);

  expect(disallowedImports).toEqual(["@redis/client", "cron-parser"]);
});

test("a Node built-in, with or without the node: prefix, and a relative import are allowed", () => {
  const declarationText = [
    'import type { EventEmitter } from "node:events";',
    'import type { Readable } from "stream";',
    'import type { RunId } from "./ids.js";',
    'export * from "../engine/index.js";',
  ].join("\n");

  const disallowedImports = disallowedImportsOf(declarationText, []);

  expect(disallowedImports).toEqual([]);
});

test("a types reference is allowed for node only", () => {
  const declarationText = ['/// <reference types="node" preserve="true" />', '/// <reference types="react" />'].join(
    "\n",
  );

  const disallowedImports = disallowedImportsOf(declarationText, ["next"]);

  expect(disallowedImports).toEqual(['/// <reference types="react" />']);
});
