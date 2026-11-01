"use server";

import { db } from "@/lib/fakes";
import { nextasking } from "@/nextasking";

export async function signUp(formData: FormData) {
  const user = await db.user.create({ email: String(formData.get("email")), name: String(formData.get("name")) });
  await nextasking.emit("user.signed-up", { userId: user.id, email: user.email, plan: user.plan });
}
