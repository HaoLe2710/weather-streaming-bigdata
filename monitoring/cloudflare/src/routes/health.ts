import { Hono } from "hono";
import type { AppEnv } from "../types";

const health = new Hono<AppEnv>();

health.get("/healthz", (ctx) => ctx.json({ status: "ok", apiVersion: 1 }));

export default health;
