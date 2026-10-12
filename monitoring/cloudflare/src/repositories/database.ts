export async function databaseNow(db: D1Database): Promise<string> {
  const row = await db.prepare("SELECT strftime('%Y-%m-%dT%H:%M:%fZ', 'now') AS now").first<{ now: string }>();
  if (!row?.now) throw new Error("DATABASE_CLOCK_UNAVAILABLE");
  return row.now;
}

export function boundedInteger(value: string | undefined, fallback: number, min: number, max: number): number {
  const parsed = value === undefined || value === "" ? fallback : Number(value);
  return Number.isSafeInteger(parsed) && parsed >= min && parsed <= max ? parsed : fallback;
}

export function compactJson(value: unknown): string {
  return JSON.stringify(value);
}
