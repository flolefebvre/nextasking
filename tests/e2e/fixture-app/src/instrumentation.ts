export async function register() {
  if (process.env.NEXT_RUNTIME !== "nodejs" || process.env.NEXTASKING_WORKER !== "on") return;
  const { nextasking } = await import("@/nextasking");
  await nextasking.startWorker();
}
