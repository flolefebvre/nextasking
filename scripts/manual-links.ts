import { posix } from "node:path";

export type ManualPage = { path: string; text: string };

export type BrokenLink = { pagePath: string; line: number; target: string; missing: "page" | "anchor" };

export type ManualLinkCheck = { checkedLinkCount: number; brokenLinks: BrokenLink[] };

type ManualLink = { pagePath: string; line: number; target: string };

const LINK = /\[((?:[^[\]]|\[[^\]]*\])*)\]\(\s*([^\s)]+)[^)]*\)/g;

const HEADING = /^ {0,3}#{1,6}\s+(.*?)(?:\s+#+)?\s*$/;

const FENCE = /^ {0,3}(`{3,}|~{3,})/;

const CODE_SPAN_DELIMITERS = /`+/g;

const CODE_SPAN = /(?<!`)(`+)(?!`)[\s\S]*?(?<!`)\1(?!`)/g;

const URL_SCHEME = /^[a-z][a-z\d+.-]*:/i;

const PUNCTUATION_DROPPED_FROM_SLUGS = /[^\p{L}\p{M}\p{N}\p{Pc} -]/gu;

type ProseLine = { line: number; text: string };

const proseLinesOf = (pageText: string): ProseLine[] => {
  const proseLines: ProseLine[] = [];
  let openFence: string | undefined;
  for (const [index, lineText] of pageText.split("\n").entries()) {
    const fence = FENCE.exec(lineText)?.[1];
    if (openFence === undefined && fence !== undefined) {
      openFence = fence;
    } else if (openFence !== undefined) {
      const isClosingFence =
        fence !== undefined &&
        fence[0] === openFence[0] &&
        fence.length >= openFence.length &&
        lineText.trim() === fence;
      if (isClosingFence) openFence = undefined;
    } else {
      proseLines.push({ line: index + 1, text: lineText });
    }
  }
  return proseLines;
};

const targetsOf = (markdownText: string): string[] =>
  [...markdownText.matchAll(LINK)].flatMap(([, linkText = "", target = ""]) => [...targetsOf(linkText), target]);

const isRelative = (target: string) => !URL_SCHEME.test(target) && !target.startsWith("//");

const linksOfLine = (pagePath: string, { line, text }: ProseLine): ManualLink[] =>
  targetsOf(text.replace(CODE_SPAN, ""))
    .filter(isRelative)
    .map((target) => ({ pagePath, line, target }));

const linksOf = ({ path, text }: ManualPage): ManualLink[] =>
  proseLinesOf(text).flatMap((proseLine) => linksOfLine(path, proseLine));

const renderedTextOf = (headingText: string) =>
  headingText.replace(LINK, (_link, linkText: string) => linkText).replace(CODE_SPAN_DELIMITERS, "");

const slugOf = (headingText: string) =>
  renderedTextOf(headingText).toLowerCase().replace(PUNCTUATION_DROPPED_FROM_SLUGS, "").replace(/ /g, "-");

export const headingSlugsOf = (pageText: string): string[] => {
  const occurrencesBySlug = new Map<string, number>();
  const uniqueSlugOf = (slug: string) => {
    let uniqueSlug = slug;
    while (occurrencesBySlug.has(uniqueSlug)) {
      const occurrences = (occurrencesBySlug.get(slug) ?? 0) + 1;
      occurrencesBySlug.set(slug, occurrences);
      uniqueSlug = `${slug}-${String(occurrences)}`;
    }
    occurrencesBySlug.set(uniqueSlug, 0);
    return uniqueSlug;
  };
  return proseLinesOf(pageText).flatMap(({ text }) => {
    const headingText = HEADING.exec(text)?.[1];
    return headingText === undefined ? [] : [uniqueSlugOf(slugOf(headingText))];
  });
};

const resolvedPathOf = (pagePath: string, targetPath: string) =>
  targetPath === "" ? pagePath : posix.normalize(posix.join(posix.dirname(pagePath), targetPath));

const isMarkdownPath = (path: string) => path.endsWith(".md");

export const checkManualLinks = (pages: readonly ManualPage[], shippedPaths: readonly string[]): ManualLinkCheck => {
  const slugsByPagePath = new Map(pages.map((page) => [page.path, headingSlugsOf(page.text)]));
  const links = pages.flatMap(linksOf);
  const missingPartOf = (link: ManualLink): BrokenLink["missing"] | undefined => {
    const [targetPathAndQuery = "", anchor] = link.target.split("#");
    const [targetPath = ""] = targetPathAndQuery.split("?");
    const resolvedPath = resolvedPathOf(link.pagePath, decodeURI(targetPath));
    if (!shippedPaths.includes(resolvedPath)) return "page";
    if (anchor === undefined || !isMarkdownPath(resolvedPath)) return undefined;
    return slugsByPagePath.get(resolvedPath)?.includes(decodeURIComponent(anchor)) ? undefined : "anchor";
  };
  const brokenLinks = links.flatMap((link): BrokenLink[] => {
    const missing = missingPartOf(link);
    return missing === undefined ? [] : [{ ...link, missing }];
  });
  return { checkedLinkCount: links.length, brokenLinks };
};
