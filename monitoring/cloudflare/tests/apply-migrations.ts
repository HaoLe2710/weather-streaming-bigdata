import { applyD1Migrations, type D1Migration } from "cloudflare:test";
import { env as runtimeEnv } from "cloudflare:workers";
import type { Env } from "../src/types";

const testEnv = runtimeEnv as Env & { TEST_MIGRATIONS: D1Migration[] };

await applyD1Migrations(testEnv.DB, testEnv.TEST_MIGRATIONS);
