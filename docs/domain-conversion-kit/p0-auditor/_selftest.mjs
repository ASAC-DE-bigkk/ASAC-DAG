import { auditPatternSql } from "./p0_port_sketch.js";
import { readFileSync } from "fs";
import { fileURLToPath } from "url";
import { dirname, join } from "path";
const here = dirname(fileURLToPath(import.meta.url));
const data = JSON.parse(readFileSync(join(here, "redteam_payloads.json"), "utf-8"));
const allow = new Set(data.allowlist_example.map((s) => s.toLowerCase()));
const fails = [];
for (const c of data.cases) {
  const f = auditPatternSql(c.sql, allow);
  const got = f.length ? "block" : "pass";
  if (got !== c.expect) fails.push(`${c.name}(got ${got})`);
}
console.log(fails.length ? "JS 스케치 불일치: " + JSON.stringify(fails) : `JS 스케치: ${data.cases.length}/${data.cases.length} 일치`);
process.exit(fails.length ? 1 : 0);
