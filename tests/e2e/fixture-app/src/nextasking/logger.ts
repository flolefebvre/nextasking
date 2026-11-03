import type { Logger } from "@flefebvre/nextasking";

function lineWriter(level: keyof Logger): Logger[keyof Logger] {
  return (fields, message) => {
    console.log(JSON.stringify({ level, message, ...fields }));
  };
}

export const logger: Logger = {
  debug: lineWriter("debug"),
  info: lineWriter("info"),
  warn: lineWriter("warn"),
  error: lineWriter("error"),
};
