import { expect, test } from "vitest";

import { checkManualLinks, headingSlugsOf } from "./manual-links.js";

test("a heading's slug is lower case, with punctuation other than - and _ removed and each space turned into -", () => {
  const pageText = "## Exit when the worker can't start\n\n### Retries, backoff_ms & limits: a -- b\n";

  const slugs = headingSlugsOf(pageText);

  expect(slugs).toEqual(["exit-when-the-worker-cant-start", "retries-backoff_ms--limits-a----b"]);
});

test("a heading keeps the text of its inline code and links, without their markup", () => {
  const pageText = "## Stop a handler cleanly with `abortSignal`\n\n## Run it under [`next dev`](dev.md#top)\n";

  const slugs = headingSlugsOf(pageText);

  expect(slugs).toEqual(["stop-a-handler-cleanly-with-abortsignal", "run-it-under-next-dev"]);
});

test("a repeated heading's slug is suffixed -1, -2 in order", () => {
  const pageText = "# Steps\n\n## Example\n\n## Example\n\n## Example\n\n## Example 1\n";

  const slugs = headingSlugsOf(pageText);

  expect(slugs).toEqual(["steps", "example", "example-1", "example-2", "example-1-1"]);
});

test("a line starting with # inside a fenced code block is not a heading", () => {
  const pageText = "# Setup\n\n```sh\n# Install\npnpm add x\n```\n\n~~~~md\n```\n## Not either\n~~~~\n\n## Configure\n";

  const slugs = headingSlugsOf(pageText);

  expect(slugs).toEqual(["setup", "configure"]);
});

test("a fenced code block nested in a list item holds no heading and no link", () => {
  const pageText =
    "# Setup\n\n1. Add the route:\n\n        ```md\n        # Not a heading\n        [x](x.md)\n        ```\n";

  const { checkedLinkCount } = checkManualLinks([{ path: "docs/setup.md", text: pageText }], ["docs/setup.md"]);

  expect(headingSlugsOf(pageText)).toEqual(["setup"]);
  expect(checkedLinkCount).toBe(0);
});

test("a link to a page that does not ship is broken, named by its page, line and target", () => {
  const pages = [{ path: "docs/setup.md", text: "# Setup\n\nRead [the worker](worker.md) first.\n" }];

  const { checkedLinkCount, brokenLinks } = checkManualLinks(pages, ["docs/setup.md"]);

  expect(checkedLinkCount).toBe(1);
  expect(brokenLinks).toEqual([
    { pagePath: "docs/setup.md", line: 3, target: "worker.md", resolvedTargetPath: "docs/worker.md", missing: "page" },
  ]);
});

test("a link with only an anchor targets a heading of its own page", () => {
  const pages = [
    { path: "docs/worker.md", text: "# Worker\n\nSee [stalls](#stalls), not [drains](#drains).\n\n## Stalls\n" },
  ];

  const { checkedLinkCount, brokenLinks } = checkManualLinks(pages, ["docs/worker.md"]);

  expect(checkedLinkCount).toBe(2);
  expect(brokenLinks).toEqual([
    { pagePath: "docs/worker.md", line: 3, target: "#drains", resolvedTargetPath: "docs/worker.md", missing: "anchor" },
  ]);
});

test("a link in a table cell is checked", () => {
  const pages = [
    { path: "docs/index.md", text: "| Job | Page |\n| --- | --- |\n| Set up | [Setup](setup.md#install) |\n" },
  ];

  const { checkedLinkCount, brokenLinks } = checkManualLinks(pages, ["docs/index.md"]);

  expect(checkedLinkCount).toBe(1);
  expect(brokenLinks).toEqual([
    {
      pagePath: "docs/index.md",
      line: 3,
      target: "setup.md#install",
      resolvedTargetPath: "docs/setup.md",
      missing: "page",
    },
  ]);
});

test("a link-shaped text in a fenced code block or an inline code span is not a link", () => {
  const pageText =
    "# Steps\n\nCall `handlers[name](payload)` or ``[a](b.md)``.\n\n```md\n[the worker](worker.md)\n```\n";

  const { checkedLinkCount } = checkManualLinks([{ path: "docs/steps.md", text: pageText }], ["docs/steps.md"]);

  expect(checkedLinkCount).toBe(0);
});

test("a link with a scheme is not checked", () => {
  const pageText =
    "# Links\n\n[npm](https://www.npmjs.com) [spec](http://example.com/a.md) [mail](mailto:a@example.com)\n";

  const { checkedLinkCount, brokenLinks } = checkManualLinks([{ path: "README.md", text: pageText }], ["README.md"]);

  expect(checkedLinkCount).toBe(0);
  expect(brokenLinks).toEqual([]);
});

test("a link from README.md resolves from the repo root, and a link wrapping an image is checked", () => {
  const pages = [
    {
      path: "README.md",
      text: '# nextasking\n\n[![license](https://img.shields.io/x)](./LICENSE) [Setup](docs/setup.md#install "Install it")\n',
    },
    { path: "docs/setup.md", text: "# Setup\n\n## Install\n\nBack to [the README](../README.md#nextasking).\n" },
  ];

  const { checkedLinkCount, brokenLinks } = checkManualLinks(pages, ["README.md", "LICENSE", "docs/setup.md"]);

  expect(checkedLinkCount).toBe(3);
  expect(brokenLinks).toEqual([]);
});

test("a link to a file that does not ship, or to a directory, is broken", () => {
  const pageText = "# Setup\n\nSee [the ADRs](adr/0001-x.md), [the agents](../AGENTS.md) and [the docs](./).\n";

  const { brokenLinks } = checkManualLinks([{ path: "docs/setup.md", text: pageText }], ["docs/setup.md"]);

  expect(brokenLinks.map(({ resolvedTargetPath, missing }) => `${resolvedTargetPath} ${missing}`)).toEqual([
    "docs/adr/0001-x.md page",
    "AGENTS.md page",
    "docs/ page",
  ]);
});

test("a link to a heading the target page does not have is broken, and one to a heading it has resolves", () => {
  const pages = [
    { path: "docs/setup.md", text: "# Setup\n\nSee [stalls](worker.md#stalls).\nSee [drains](worker.md#drains).\n" },
    { path: "docs/worker.md", text: "# Worker\n\n## Stalls\n" },
  ];

  const { checkedLinkCount, brokenLinks } = checkManualLinks(pages, ["docs/setup.md", "docs/worker.md"]);

  expect(checkedLinkCount).toBe(2);
  expect(brokenLinks).toEqual([
    {
      pagePath: "docs/setup.md",
      line: 4,
      target: "worker.md#drains",
      resolvedTargetPath: "docs/worker.md",
      missing: "anchor",
    },
  ]);
});
