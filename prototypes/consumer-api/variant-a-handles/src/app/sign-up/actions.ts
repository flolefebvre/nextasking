"use server";

import { db } from "@/lib/fakes";
import { userSignedUp } from "@/jobs/events";
import { emit } from "@/jobs/nextasking";

export async function signUp(formData: FormData) {
  const user = await db.user.create({ email: String(formData.get("email")), name: String(formData.get("name")) });
  await emit(userSignedUp, { userId: user.id, email: user.email, plan: user.plan });
}
