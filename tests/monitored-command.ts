export type MonitoredCommand = { args: string[]; source: string };

const MONITOR_LINE_PATTERN = /^\d+\.\d+ \[\d+ ([^\]]+)\]((?: "(?:[^"\\]|\\.)*")+)$/;

const QUOTED_ARGUMENT_PATTERN = / "((?:[^"\\]|\\.)*)"/g;

const ESCAPE_PATTERN = /\\(x[0-9a-f]{2}|.)/g;

const CHARACTER_BY_ESCAPE: Readonly<Record<string, string>> = {
  n: "\n",
  r: "\r",
  t: "\t",
  a: "\u0007",
  b: "\b",
  '"': '"',
  "\\": "\\",
};

function unescapedArgumentOf(quotedArgument: string): string {
  return quotedArgument.replace(ESCAPE_PATTERN, (_escape, escaped: string) => {
    if (escaped.startsWith("x") && escaped.length === 3) {
      return String.fromCharCode(Number.parseInt(escaped.slice(1), 16));
    }
    const character = CHARACTER_BY_ESCAPE[escaped];
    if (character === undefined) {
      throw new Error(`unknown escape \\${escaped} in a MONITOR argument: ${quotedArgument}`);
    }
    return character;
  });
}

export function monitoredCommandOf(monitorLine: string): MonitoredCommand {
  const match = MONITOR_LINE_PATTERN.exec(monitorLine);
  if (match === null) {
    throw new Error(`not a MONITOR line: ${monitorLine}`);
  }
  const [, source, quotedArguments] = match;
  return {
    source: source!,
    args: [...quotedArguments!.matchAll(QUOTED_ARGUMENT_PATTERN)].map(([, quotedArgument]) =>
      unescapedArgumentOf(quotedArgument!),
    ),
  };
}
