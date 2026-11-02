export type Logger = {
  debug: (fields: Record<string, unknown>, message: string) => void;
  info: (fields: Record<string, unknown>, message: string) => void;
  warn: (fields: Record<string, unknown>, message: string) => void;
  error: (fields: Record<string, unknown>, message: string) => void;
};

const writeNothing = () => undefined;

export const silentLogger: Logger = {
  debug: writeNothing,
  info: writeNothing,
  warn: writeNothing,
  error: writeNothing,
};

export function loggerWithFields(logger: Logger, boundFields: Record<string, unknown>): Logger {
  return {
    debug: (fields, message) => {
      logger.debug({ ...fields, ...boundFields }, message);
    },
    info: (fields, message) => {
      logger.info({ ...fields, ...boundFields }, message);
    },
    warn: (fields, message) => {
      logger.warn({ ...fields, ...boundFields }, message);
    },
    error: (fields, message) => {
      logger.error({ ...fields, ...boundFields }, message);
    },
  };
}
