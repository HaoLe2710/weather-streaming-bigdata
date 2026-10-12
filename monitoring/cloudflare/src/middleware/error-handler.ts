import type { ErrorHandler } from "hono";
import { ApiError, errorBody } from "../errors";
import type { AppEnv } from "../types";

export const errorHandler: ErrorHandler<AppEnv> = (error, ctx) => {
  const requestId = ctx.get("requestId") || crypto.randomUUID();
  if (error instanceof ApiError) {
    return ctx.json(errorBody(error.code, error.message, requestId), error.status);
  }
  console.error(JSON.stringify({ requestId, code: "UNHANDLED_WORKER_ERROR" }));
  return ctx.json(errorBody("INTERNAL_ERROR", "Request could not be completed", requestId), 500);
};
