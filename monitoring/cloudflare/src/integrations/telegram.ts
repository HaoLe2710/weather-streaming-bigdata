export interface TelegramSendResult {
  ok: boolean;
  errorCode?: string;
}

export async function sendTelegramAlert(
  token: string,
  chatId: string,
  text: string,
): Promise<TelegramSendResult> {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), 5_000);
  try {
    const response = await fetch(`https://api.telegram.org/bot${token}/sendMessage`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ chat_id: chatId, text, disable_web_page_preview: true }),
      signal: controller.signal,
    });
    if (!response.ok) return { ok: false, errorCode: `TELEGRAM_HTTP_${response.status}` };
    const payload = await response.json() as { ok?: boolean };
    return payload.ok === true ? { ok: true } : { ok: false, errorCode: "TELEGRAM_API_REJECTED" };
  } catch (error) {
    return { ok: false, errorCode: error instanceof Error && error.name === "AbortError" ? "TELEGRAM_TIMEOUT" : "TELEGRAM_NETWORK_ERROR" };
  } finally {
    clearTimeout(timeoutId);
  }
}
