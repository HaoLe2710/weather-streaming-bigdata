import { Hono } from "hono";
import health from "./routes/health";
import ingestion from "./routes/ingestion";
import monitoring from "./routes/monitoring";
import { errorBody } from "./errors";
import { errorHandler } from "./middleware/error-handler";
import { runScheduledTask } from "./services/scheduled-service";
import type { AppEnv, Env } from "./types";

const app = new Hono<AppEnv>();

app.use("*", async (ctx, next) => {
  ctx.set("requestId", crypto.randomUUID());
  await next();
  ctx.header("X-Request-Id", ctx.get("requestId"));
});

app.route("/", health);
app.route("/api/v1/ingest", ingestion);
app.route("/api/v1/monitoring", monitoring);

app.onError(errorHandler);
app.notFound((ctx) => ctx.json(errorBody("NOT_FOUND", "Route was not found", ctx.get("requestId")), 404));

const worker = {
  async fetch(request: Request, env: Env, executionContext: ExecutionContext): Promise<Response> {
    return await app.fetch(request, env, executionContext);
  },
  async scheduled(event: ScheduledController, env: Env): Promise<void> {
    await runScheduledTask(event.cron, env);
  },
} satisfies ExportedHandler<Env>;

export default worker;
