export type Duration = `${bigint}${"s" | "m" | "h" | "d"}`;

export type DurationInMilliseconds = number;

export type EpochMilliseconds = number;

type DurationUnit = "s" | "m" | "h" | "d";

const MILLISECONDS_BY_UNIT: Record<DurationUnit, DurationInMilliseconds> = {
  s: 1000,
  m: 60 * 1000,
  h: 60 * 60 * 1000,
  d: 24 * 60 * 60 * 1000,
};

const DURATION_PATTERN = /^(\d+)([smhd])$/;

export function parseDuration(duration: Duration): DurationInMilliseconds {
  const match = typeof duration === "string" ? DURATION_PATTERN.exec(duration) : null;
  if (match === null) {
    throw new Error(
      `${JSON.stringify(duration) ?? String(duration)} is not a duration: a whole number followed by s, m, h or d, such as "30s"`,
    );
  }
  return Number(match[1]) * MILLISECONDS_BY_UNIT[match[2] as DurationUnit];
}
