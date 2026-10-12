export async function deterministicStreamId(environment: string, cohortId: string, sourceIdentity: string): Promise<string> {
  const canonical = `${environment}\n${cohortId}\n${sourceIdentity}`;
  const digest = new Uint8Array(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(canonical)));
  return Array.from(digest, (byte) => byte.toString(16).padStart(2, "0")).join("");
}
