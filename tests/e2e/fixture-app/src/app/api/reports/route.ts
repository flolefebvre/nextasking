import { nextasking } from "@/nextasking";

export async function POST(request: Request) {
  const { reportId } = (await request.json()) as { reportId: string };
  const { runId } = await nextasking.dispatch("build-report", { reportId });
  return Response.json({ runId }, { status: 202 });
}
