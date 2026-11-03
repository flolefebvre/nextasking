const SECONDS_IN_A_DAY = 24 * 60 * 60;

export const COMPLETED_JOB_RETENTION = { age: SECONDS_IN_A_DAY, count: 1000 };

export const FAILED_JOB_RETENTION = { age: 14 * SECONDS_IN_A_DAY, count: 10_000 };
