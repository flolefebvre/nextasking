import { vi } from "vitest";

export async function unhandledRejectionsDuring(body: () => Promise<void>): Promise<unknown[]> {
  const unhandledRejections: unknown[] = [];
  const recordUnhandledRejection = (reason: unknown) => {
    unhandledRejections.push(reason);
  };
  process.on("unhandledRejection", recordUnhandledRejection);
  try {
    await body();
    await vi.advanceTimersByTimeAsync(0);
  } finally {
    process.off("unhandledRejection", recordUnhandledRejection);
  }
  return unhandledRejections;
}
