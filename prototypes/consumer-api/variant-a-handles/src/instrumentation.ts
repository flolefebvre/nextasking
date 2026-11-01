export async function register() {
  if (process.env.NEXT_RUNTIME !== "nodejs" || process.env.NEXTASKING_WORKER !== "embedded") return;
  const { startWorker } = await import("@flefebvre/nextasking");
  const { nextasking } = await import("@/jobs/nextasking");
  await startWorker(nextasking);
}
