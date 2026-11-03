import { execFile } from "node:child_process";
import { cp, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

const runFile = promisify(execFile);

const REPO_ROOT_DIR = fileURLToPath(new URL("../../", import.meta.url));

export const FIXTURE_APP_SOURCE_DIR = fileURLToPath(new URL("./fixture-app/", import.meta.url));

const GENERATED_ENTRY_NAMES = new Set(["node_modules", ".next", "next-env.d.ts"]);

const PACKED_TARBALL_NAME = "nextasking.tgz";

const COMMAND_OUTPUT_LIMIT_IN_BYTES = 64 * 1024 * 1024;

export const NEXT_TELEMETRY_ENV = { NEXT_TELEMETRY_DISABLED: "1" };

export type PreparedFixtureApp = { fixtureAppDir: string; remove: () => Promise<void> };

export function fixtureNextVersion(env: NodeJS.ProcessEnv = process.env): string | undefined {
  return env.NEXTASKING_E2E_NEXT_VERSION?.trim() || undefined;
}

export function nextBinOf(fixtureAppDir: string): string {
  return join(fixtureAppDir, "node_modules", ".bin", "next");
}

async function runInDir(dir: string, command: string, args: string[], failure: string): Promise<void> {
  try {
    await runFile(command, args, {
      cwd: dir,
      env: { ...process.env, ...NEXT_TELEMETRY_ENV },
      maxBuffer: COMMAND_OUTPUT_LIMIT_IN_BYTES,
    });
  } catch (error) {
    const { stdout = "", stderr = "" } = error as { stdout?: string; stderr?: string };
    throw new Error(`${failure}:\n${stdout}${stderr}`, { cause: error });
  }
}

async function pinNextVersion(fixtureAppDir: string, nextVersion: string): Promise<void> {
  const manifestPath = join(fixtureAppDir, "package.json");
  const manifest = JSON.parse(await readFile(manifestPath, "utf-8")) as { dependencies: Record<string, string> };
  manifest.dependencies.next = nextVersion;
  await writeFile(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`);
}

export async function prepareFixtureApp(): Promise<PreparedFixtureApp> {
  if (!existsSync(join(REPO_ROOT_DIR, "dist"))) {
    throw new Error("dist/ is missing: the e2e project installs the packed package, run `pnpm build` first.");
  }
  const fixtureAppDir = await mkdtemp(join(tmpdir(), "nextasking-e2e-"));
  const remove = () => rm(fixtureAppDir, { recursive: true, force: true });
  try {
    await cp(FIXTURE_APP_SOURCE_DIR, fixtureAppDir, {
      recursive: true,
      filter: (source) => !GENERATED_ENTRY_NAMES.has(basename(source)),
    });
    await runInDir(
      REPO_ROOT_DIR,
      "pnpm",
      ["pack", "--out", join(fixtureAppDir, PACKED_TARBALL_NAME)],
      "pnpm pack failed",
    );
    const nextVersion = fixtureNextVersion();
    if (nextVersion !== undefined) {
      await pinNextVersion(fixtureAppDir, nextVersion);
    }
    await runInDir(fixtureAppDir, "pnpm", ["install"], "the fixture app failed to install");
    await runInDir(fixtureAppDir, nextBinOf(fixtureAppDir), ["build"], "the fixture app failed to build");
  } catch (error) {
    await remove();
    throw error;
  }
  return { fixtureAppDir, remove };
}
