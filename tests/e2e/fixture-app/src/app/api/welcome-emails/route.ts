import { nextasking } from "@/nextasking";

export async function POST(request: Request) {
  const { email } = (await request.json()) as { email: string };
  const { runId } = await nextasking.dispatch("send-welcome-email", { email });
  return Response.json({ runId }, { status: 202 });
}
