import { RedisUnavailableError } from "@flefebvre/nextasking";
import { nextasking } from "@/nextasking";

export async function POST(request: Request) {
  const { email } = (await request.json()) as { email: string };
  try {
    const { runId } = await nextasking.dispatch("send-welcome-email", { email });
    return Response.json({ runId }, { status: 202 });
  } catch (error) {
    if (error instanceof RedisUnavailableError) {
      return Response.json({ runId: error.runId }, { status: 503, headers: { "retry-after": "5" } });
    }
    throw error;
  }
}
