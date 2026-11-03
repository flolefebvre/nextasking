import { describe, expect, test } from "vitest";

import { scheduleChangesBetween } from "./schedules.js";

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
