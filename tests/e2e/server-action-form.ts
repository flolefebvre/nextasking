const ACTION_ID_FIELD = /name="(\$ACTION_ID_[0-9a-f]+)"/;

export async function submitServerActionForm(pageUrl: string, fields: Record<string, string>): Promise<Response> {
  const page = await fetch(pageUrl);
  const html = await page.text();
  const actionIdField = ACTION_ID_FIELD.exec(html)?.[1];
  if (actionIdField === undefined) {
    throw new Error(`${pageUrl} answered ${String(page.status)} with no server action form:\n${html}`);
  }
  const formData = new FormData();
  formData.set(actionIdField, "");
  for (const [name, value] of Object.entries(fields)) formData.set(name, value);
  return fetch(pageUrl, { method: "POST", headers: { origin: new URL(pageUrl).origin }, body: formData });
}
