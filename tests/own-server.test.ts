import { describe, expect, test, vi } from "vitest";

import { startRetryingOnceAPortBindingTimeout } from "./own-server.js";

const PORT_BINDING_TIMEOUT = new Error(
  "Timed out after 30000ms while waiting for container ports to be bound to the host",
);

describe("startRetryingOnceAPortBindingTimeout", () => {
  test("starts again once when testcontainers times out waiting for the ports to be bound, and returns the second start", async () => {
    const start = vi.fn().mockRejectedValueOnce(PORT_BINDING_TIMEOUT).mockResolvedValueOnce("second start");

    const started = await startRetryingOnceAPortBindingTimeout(start);

    expect(started).toBe("second start");
    expect(start).toHaveBeenCalledTimes(2);
  });

  test("throws a second port binding timeout without a third start", async () => {
    const start = vi.fn().mockRejectedValue(PORT_BINDING_TIMEOUT);

    await expect(startRetryingOnceAPortBindingTimeout(start)).rejects.toBe(PORT_BINDING_TIMEOUT);
    expect(start).toHaveBeenCalledTimes(2);
  });

  test("throws any other start failure as it is, without starting again", async () => {
    const logWaitTimeout = new Error('Log message "/Ready to accept connections/" not received after 60000ms');
    const start = vi.fn().mockRejectedValue(logWaitTimeout);

    await expect(startRetryingOnceAPortBindingTimeout(start)).rejects.toBe(logWaitTimeout);
    expect(start).toHaveBeenCalledTimes(1);
  });
});
