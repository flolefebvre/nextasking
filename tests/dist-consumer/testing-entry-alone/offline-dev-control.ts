import { createNextaskingControl } from "@flefebvre/nextasking/testing";

export async function firePurgeAndWaitForIdle(): Promise<string> {
  await using control = createNextaskingControl({ connection: { url: "redis://localhost:6379" } });
  const { runId } = await control.fireSchedule("purge-expired-sessions");
  await control.waitForIdle({ timeout: "30s" });
  return runId;
}
