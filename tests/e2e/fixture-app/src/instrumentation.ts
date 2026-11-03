export async function register() {
  if (process.env.NEXT_RUNTIME !== "nodejs" || process.env.NEXTASKING_WORKER !== "embedded") return;
  const { nextasking } = await import("@/nextasking");
  await nextasking.startWorker();
}
