import { describe, expect, test } from "vitest";

import { previousFiringInstantBefore, scheduleChangesBetween } from "./schedules.js";

describe("scheduleChangesBetween", () => {
  test("adds a declared schedule nothing stores, and leaves one stored with the same cron and timezone", () => {
    const changes = scheduleChangesBetween(
      [{ taskName: "purge-expired-sessions", cron: "0 3 * * *", timezone: "UTC" }],
      [
        { taskName: "purge-expired-sessions", cron: "0 3 * * *", timezone: "UTC" },
        { taskName: "send-digest", cron: "0 8 * * 1", timezone: "Europe/Paris" },
      ],
    );

    expect(changes).toEqual([{ kind: "added", taskName: "send-digest", cron: "0 8 * * 1", timezone: "Europe/Paris" }]);
  });

  test("changes a stored schedule whose cron or timezone differs, or whose timezone is missing", () => {
    const changes = scheduleChangesBetween(
      [
        { taskName: "send-digest", cron: "0 8 * * 1", timezone: "UTC" },
        { taskName: "send-weekly-report", cron: "0 9 * * 1", timezone: "UTC" },
        { taskName: "compact-logs", cron: "0 4 * * *", timezone: undefined },
      ],
      [
        { taskName: "send-digest", cron: "0 7 * * 1", timezone: "UTC" },
        { taskName: "send-weekly-report", cron: "0 9 * * 1", timezone: "Europe/Paris" },
        { taskName: "compact-logs", cron: "0 4 * * *", timezone: "UTC" },
      ],
    );

    expect(changes).toEqual([
      { kind: "changed", taskName: "send-digest", cron: "0 7 * * 1", timezone: "UTC" },
      { kind: "changed", taskName: "send-weekly-report", cron: "0 9 * * 1", timezone: "Europe/Paris" },
      { kind: "changed", taskName: "compact-logs", cron: "0 4 * * *", timezone: "UTC" },
    ]);
  });

  test("removes a stored schedule no task declares, with its stored cron and timezone", () => {
    const changes = scheduleChangesBetween(
      [
        { taskName: "archive-invoices", cron: "0 1 * * *", timezone: "Europe/Paris" },
        { taskName: "ping", cron: undefined, timezone: undefined },
      ],
      [],
    );

    expect(changes).toEqual([
      { kind: "removed", taskName: "archive-invoices", cron: "0 1 * * *", timezone: "Europe/Paris" },
      { kind: "removed", taskName: "ping", cron: undefined, timezone: undefined },
    ]);
  });
});

describe("previousFiringInstantBefore", () => {
  test("is the cron's last instant before the given one, in the schedule's timezone", () => {
    const schedule = { taskName: "purge-expired-sessions", cron: "0 3 * * *", timezone: "Europe/Paris" };

    expect(previousFiringInstantBefore(schedule, Date.parse("2026-11-04T02:00:00.000Z"))).toBe(
      Date.parse("2026-11-03T02:00:00.000Z"),
    );
  });

  test("skips back over a daylight saving change", () => {
    const schedule = { taskName: "purge-expired-sessions", cron: "0 3 * * *", timezone: "Europe/Paris" };

    expect(previousFiringInstantBefore(schedule, Date.parse("2026-10-26T02:00:00.000Z"))).toBe(
      Date.parse("2026-10-25T02:00:00.000Z"),
    );
  });
});
