import { Hono } from "hono";
import { errorBody } from "../errors";
import { hmacAuth, sha256Hex } from "../security/hmac";
import { ingestSnapshot } from "../services/ingestion-service";
import type { AppEnv } from "../types";

const ingestion = new Hono<AppEnv>();

ingestion.post("/snapshots", hmacAuth, async (ctx) => {
  const rawBody = ctx.get("rawBody");
  try {
    const result = await ingestSnapshot(ctx.env.DB, ctx.env, rawBody, await sha256Hex(rawBody));
    const status = result.status === "ACCEPTED" ? 201 : result.status === "DUPLICATE" ? 200 : 202;
    return ctx.json(result, status);
  } catch (error) {
    if (error instanceof Error && "status" in error && "code" in error) throw error;
    console.error(JSON.stringify({ requestId: ctx.get("requestId"), code: "INGESTION_FAILED" }));
    return ctx.json(errorBody("INTERNAL_ERROR", "Snapshot could not be stored", ctx.get("requestId")), 500);
  }
});

export default ingestion;
