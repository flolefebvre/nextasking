import { readdirSync, readFileSync } from "node:fs";

import { checkManualLinks } from "./manual-links.js";
import type { BrokenLink, ManualPage } from "./manual-links.js";

const SHIPPED_FILES_BESIDE_THE_MANUAL = ["README.md", "LICENSE"];

const fail = (message: string): never => {
  console.error(message);
  process.exit(1);
};

const missingPartDescriptionOf = ({ target, resolvedTargetPath, missing }: BrokenLink) =>
  missing === "page"
    ? `the page is missing: no file ${resolvedTargetPath} ships`
    : `the anchor is missing: no heading of ${resolvedTargetPath} has the slug ${target.slice(target.indexOf("#") + 1)}`;

const manualPagePaths = readdirSync(new URL("../docs", import.meta.url), { withFileTypes: true })
  .filter((entry) => entry.isFile() && entry.name.endsWith(".md"))
  .map((entry) => `docs/${entry.name}`);

const pages: ManualPage[] = ["README.md", ...manualPagePaths].map((path) => ({
  path,
  text: readFileSync(new URL(`../${path}`, import.meta.url), "utf-8"),
}));

const { checkedLinkCount, brokenLinks } = checkManualLinks(pages, [
  ...manualPagePaths,
  ...SHIPPED_FILES_BESIDE_THE_MANUAL,
]);

if (brokenLinks.length > 0) {
  fail(
    [
      ...brokenLinks.map(
        (brokenLink) =>
          `${brokenLink.pagePath}:${String(brokenLink.line)}: ${brokenLink.target}: ${missingPartDescriptionOf(brokenLink)}`,
      ),
      `${String(brokenLinks.length)} broken links among ${String(checkedLinkCount)} relative links of the manual.`,
    ].join("\n"),
  );
}

console.log(
  `Manual links OK: ${String(checkedLinkCount)} relative links between README.md and ${String(manualPagePaths.length)} manual pages resolve.`,
);
