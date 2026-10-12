import { readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import Ajv2020 from "ajv/dist/2020.js";
import addFormats from "ajv-formats";
import standaloneCode from "ajv/dist/standalone/index.js";

const directory = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(directory, "..");
const schemaPath = path.resolve(root, "../agent/schema/monitoring-snapshot-v1.schema.json");
const outputPath = path.resolve(root, "src/validation/generated-monitoring-snapshot-v1.ts");
const schema = JSON.parse(await readFile(schemaPath, "utf8"));

const ajv = new Ajv2020({
  allErrors: false,
  strict: true,
  code: { source: true, esm: true },
});
addFormats(ajv, ["date-time"]);
const validate = ajv.compile(schema);
const generated = `// Generated from ../agent/schema/monitoring-snapshot-v1.schema.json. Do not edit by hand.\n// @ts-nocheck\n${standaloneCode(ajv, validate)}\n`;

if (process.argv.includes("--check")) {
  let current = "";
  try {
    current = await readFile(outputPath, "utf8");
  } catch {}
  if (current !== generated) {
    console.error("Generated Worker validator is out of date. Run npm run generate-validator.");
    process.exitCode = 1;
  } else {
    console.log("Worker validator matches the Phase 1 JSON Schema.");
  }
} else {
  await writeFile(outputPath, generated, "utf8");
  console.log("Generated Worker validator from the Phase 1 JSON Schema.");
}
