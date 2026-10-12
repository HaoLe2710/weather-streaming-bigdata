import type { Context, MiddlewareHandler } from "hono";
import { ApiError } from "../errors";
import type { AppEnv } from "../types";

const encoder = new TextEncoder();
const UTC_TIMESTAMP = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?Z$/;
const BASE64URL = /^[A-Za-z0-9_-]{43}$/;

function toHex(bytes: Uint8Array): string {
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

function asArrayBuffer(bytes: Uint8Array): ArrayBuffer {
  const copy = new ArrayBuffer(bytes.byteLength);
  new Uint8Array(copy).set(bytes);
  return copy;
}

function decodeBase64Url(value: string): Uint8Array | null {
  if (!BASE64URL.test(value)) return null;
  const padded = value.replaceAll("-", "+").replaceAll("_", "/") + "=";
  try {
    const binary = atob(padded);
    return Uint8Array.from(binary, (character) => character.charCodeAt(0));
  } catch {
    return null;
  }
}

export function canonicalHmacMessage(keyId: string, timestamp: string, bodySha256Hex: string): string {
  return `v1\n${keyId}\n${timestamp}\n${bodySha256Hex}`;
}

export async function sha256Hex(bytes: Uint8Array): Promise<string> {
  return toHex(new Uint8Array(await crypto.subtle.digest("SHA-256", asArrayBuffer(bytes))));
}

export async function verifyRequestHmac(
  ctx: Context<AppEnv>,
  rawBody: Uint8Array,
): Promise<{ keyId: string; timestamp: string; bodySha256Hex: string }> {
  const env = ctx.env;
  const primaryId = env.MONITOR_HMAC_KEY_ID_PRIMARY || "agent-key-v1";
  const previousId = env.MONITOR_HMAC_KEY_ID_PREVIOUS || "";
  const keyId = ctx.req.header("X-Monitor-Key-Id") || "";
  const timestamp = ctx.req.header("X-Monitor-Timestamp") || "";
  const signatureHeader = ctx.req.header("X-Monitor-Signature") || "";
  const prefix = "v1=";
  const signatureText = signatureHeader.startsWith(prefix) ? signatureHeader.slice(prefix.length) : "";

  if (!env.MONITOR_HMAC_SECRET_PRIMARY) {
    throw new ApiError(503, "AUTHENTICATION_NOT_CONFIGURED", "Ingestion authentication is not configured");
  }
  if (!keyId || !timestamp || !signatureText) {
    throw new ApiError(401, "AUTHENTICATION_REQUIRED", "Signed request headers are required");
  }
  if (!UTC_TIMESTAMP.test(timestamp) || !Number.isFinite(Date.parse(timestamp))) {
    throw new ApiError(401, "AUTHENTICATION_FAILED", "Request authentication failed");
  }

  const toleranceSeconds = Number(env.HMAC_TIMESTAMP_TOLERANCE_SECONDS || "300");
  const tolerance = Number.isSafeInteger(toleranceSeconds) && toleranceSeconds >= 30 && toleranceSeconds <= 900
    ? toleranceSeconds
    : 300;
  const deltaSeconds = Math.abs(Date.now() - Date.parse(timestamp)) / 1000;
  if (deltaSeconds > tolerance) {
    throw new ApiError(401, "AUTHENTICATION_FAILED", "Request authentication failed");
  }

  let secret: string | undefined;
  if (keyId === primaryId) secret = env.MONITOR_HMAC_SECRET_PRIMARY;
  else if (previousId && keyId === previousId) secret = env.MONITOR_HMAC_SECRET_PREVIOUS;
  if (!secret) throw new ApiError(401, "AUTHENTICATION_FAILED", "Request authentication failed");

  const signature = decodeBase64Url(signatureText);
  if (!signature || signature.byteLength !== 32) {
    throw new ApiError(401, "AUTHENTICATION_FAILED", "Request authentication failed");
  }
  const bodySha256Hex = await sha256Hex(rawBody);
  const canonical = encoder.encode(canonicalHmacMessage(keyId, timestamp, bodySha256Hex));
  const cryptoKey = await crypto.subtle.importKey(
    "raw",
    asArrayBuffer(encoder.encode(secret)),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["verify"],
  );
  const valid = await crypto.subtle.verify("HMAC", cryptoKey, asArrayBuffer(signature), asArrayBuffer(canonical));
  if (!valid) throw new ApiError(401, "AUTHENTICATION_FAILED", "Request authentication failed");
  return { keyId, timestamp, bodySha256Hex };
}

export const hmacAuth: MiddlewareHandler<AppEnv> = async (ctx, next) => {
  const mediaType = (ctx.req.header("Content-Type") || "").split(";", 1)[0].trim().toLowerCase();
  if (mediaType !== "application/json") {
    throw new ApiError(415, "UNSUPPORTED_MEDIA_TYPE", "Content-Type must be application/json");
  }
  const declaredLength = Number(ctx.req.header("Content-Length") || "0");
  if (Number.isFinite(declaredLength) && declaredLength > 32 * 1024) {
    throw new ApiError(413, "PAYLOAD_TOO_LARGE", "Snapshot exceeds the 32 KiB limit");
  }

  const reader = ctx.req.raw.body?.getReader();
  if (!reader) throw new ApiError(400, "INVALID_JSON", "Request body is required");
  const chunks: Uint8Array[] = [];
  let total = 0;
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    total += value.byteLength;
    if (total > 32 * 1024) {
      await reader.cancel("snapshot too large");
      throw new ApiError(413, "PAYLOAD_TOO_LARGE", "Snapshot exceeds the 32 KiB limit");
    }
    chunks.push(value);
  }
  const rawBody = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    rawBody.set(chunk, offset);
    offset += chunk.byteLength;
  }

  await verifyRequestHmac(ctx, rawBody);
  ctx.set("rawBody", rawBody);
  await next();
};
