import { connectedTestClientOf } from "./with-redis.js";

export type MonitoredCommand = { args: string[]; source: string };

export type CommandMonitor = {
  monitoredCommands: () => MonitoredCommand[];
  stop: () => void;
};

const MONITOR_LINE_PATTERN = /^\d+\.\d+ \[\d+ (\S+)\]((?: "(?:[^"\\]|\\.)*")+)$/;

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

export async function startCommandMonitor(redisUrl: string): Promise<CommandMonitor> {
  const monitorClient = await connectedTestClientOf(redisUrl);
  const monitorLines: string[] = [];
  try {
    await monitorClient.monitor((monitorLine) => {
      monitorLines.push(monitorLine);
    });
  } catch (error) {
    monitorClient.destroy();
    throw error;
  }
  return {
    monitoredCommands: () => monitorLines.map(monitoredCommandOf),
    stop: () => {
      monitorClient.destroy();
    },
  };
}
