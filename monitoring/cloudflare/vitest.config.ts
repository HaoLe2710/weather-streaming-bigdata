import { cloudflareTest } from "@cloudflare/vitest-plugin";
import { defineConfig } from "vitest/config";

export default defineConfig({
  plugins: [
    cloudflareTest({
      wrangler: { configPath: "./wrangler.jsonc" },
      miniflare: {
        vars: {
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
    testTimeout: 20_000,
    hookTimeout: 20_000,
    maxWorkers: 1,
  },
});
