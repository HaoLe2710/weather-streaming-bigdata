import path from "node:path";
import { cloudflareTest, readD1Migrations } from "@cloudflare/vitest-plugin";
import { defineConfig } from "vitest/config";

export default defineConfig(async () => {
  const migrations = await readD1Migrations(path.join(import.meta.dirname, "migrations"));

  return {
    plugins: [
      cloudflareTest({
        wrangler: { configPath: "./wrangler.jsonc" },
        miniflare: {
          bindings: {
            TEST_MIGRATIONS: migrations,
            MONITOR_HMAC_SECRET_PRIMARY: "local-test-primary-secret-not-for-deployment",
            MONITOR_HMAC_SECRET_PREVIOUS: "local-test-previous-secret-not-for-deployment",
            MONITOR_READ_API_TOKEN: "local-test-read-token-not-for-deployment",
            MONITOR_HMAC_KEY_ID_PRIMARY: "agent-key-v1",
            MONITOR_HMAC_KEY_ID_PREVIOUS: "agent-key-v0",
            ALLOWED_ENVIRONMENTS: "local,azure-production",
            ALLOWED_SOURCE_HOSTNAMES: "phase2a-synthetic",
            TELEGRAM_ALERTS_ENABLED: "false",
          },
        },
      }),
    ],
    test: {
      include: ["tests/**/*.test.ts"],
      setupFiles: ["tests/apply-migrations.ts"],
      testTimeout: 20_000,
      hookTimeout: 20_000,
      maxWorkers: 1,
    },
  };
});
