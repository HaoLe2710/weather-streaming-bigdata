import type { MiddlewareHandler } from "hono";
import { ApiError } from "../errors";
import type { AppEnv } from "../types";

async function tokenMatches(provided: string, expected: string): Promise<boolean> {
  const encoder = new TextEncoder();
  const [left, right] = await Promise.all([
    crypto.subtle.digest("SHA-256", encoder.encode(provided)),
    crypto.subtle.digest("SHA-256", encoder.encode(expected)),
  ]);
  const a = new Uint8Array(left);
  const b = new Uint8Array(right);
  let difference = 0;
  for (let index = 0; index < a.length; index += 1) difference |= a[index] ^ b[index];
  return difference === 0;
}

export const readAuth: MiddlewareHandler<AppEnv> = async (ctx, next) => {
  const expected = ctx.env.MONITOR_READ_API_TOKEN;
  if (!expected) throw new ApiError(503, "READ_AUTH_NOT_CONFIGURED", "Read API authentication is not configured");
  const authorization = ctx.req.header("Authorization") || "";
  const match = /^Bearer ([A-Za-z0-9._~-]{24,512})$/.exec(authorization);
  if (!match || !(await tokenMatches(match[1], expected))) {
    throw new ApiError(401, "READ_AUTH_REQUIRED", "A valid bearer token is required");
  }
  await next();
};
