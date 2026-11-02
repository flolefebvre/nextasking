import type { Logger } from "../src/index.js";

export type LogLevel = keyof Logger;

export type RecordedLogLine = {
  level: LogLevel;
  fields: Record<string, unknown>;
  message: string;
};

export function createRecordingLogger(): { logger: Logger; lines: RecordedLogLine[] } {
  const lines: RecordedLogLine[] = [];
  const lineRecorderAt =
    (level: LogLevel): Logger[LogLevel] =>
    (fields, message) => {
      lines.push({ level, fields, message });
    };
  return {
    logger: {
      debug: lineRecorderAt("debug"),
      info: lineRecorderAt("info"),
      warn: lineRecorderAt("warn"),
      error: lineRecorderAt("error"),
    },
    lines,
  };
}
