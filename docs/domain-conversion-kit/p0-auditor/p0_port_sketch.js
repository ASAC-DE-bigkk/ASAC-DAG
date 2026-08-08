// P0 테이블 스코프 감사기 — 게이트웨이(JS) 이식 스케치.
//
// 정본 참고 구현(Python, 레드팀 검증됨):
//   ASAC-DAG/domains/commerce/include/gold/pattern_audit.py           (커머스 게시 감사)
//   ASAC-DAG/docs/domain-conversion-kit/pattern_audit.py              (도메인 무관 일반화판)
//   회귀 데이터: ./redteam_payloads.json (18 케이스)
//
// 이 스케치는 그 코어(토크나이저 + FROM/JOIN 절 모든 테이블 열거 + allowlist 검사)를 JS 로
// 옮긴 것이다. 게이트웨이 handleRunPattern 에서 env.DB.prepare(converted).bind() **직전**,
// 그 제품이 선언한 테이블(± 공용 축)을 allowlist 로 넘겨 호출한다. 위반이면 실행 거부(400).
//
// ★ 정규식("FROM 뒤 첫 식별자")로 만들지 말 것 — 콤마 조인(FROM a, _keys)의 2번째 테이블을
//   놓쳐 _keys 유출이 감사를 통과한다(레드팀 확증). 반드시 토크나이저로 FROM 절의 **모든**
//   테이블을 열거한다.

const ALLOWED_TVF = new Set(["json_each"]); // 배열 IN 관용구만 허용(pragma_* 는 차단)
const FORBIDDEN = new Set(["attach","detach","insert","update","delete","drop","alter",
  "create","replace","vacuum","reindex","analyze","load_extension"]);
const BOUNDARY = new Set(["where","group","order","having","limit","window","union",
  "except","intersect","on","using","returning","values"]);
const JOINMOD = new Set(["cross","inner","left","right","full","outer","natural"]);
const LIMIT_NAMES = new Set(["n","limit","top_n"]);

// 문자열/주석/인용식별자를 원자로 — 그 안의 콤마/세미콜론/키워드에 안 속게.
const TOKEN_RE = new RegExp([
  "\\s+",                       // ws
  "--[^\\n]*",                  // line comment
  "/\\*[\\s\\S]*?\\*/",         // block comment
  "'(?:[^']|'')*'",             // string
  "\"(?:[^\"]|\"\")*\"",        // dquote ident
  "`(?:[^`]|``)*`",             // bquote ident
  "\\[[^\\]]*\\]",              // bracket ident
  ":[a-zA-Z_][a-zA-Z0-9_]*",    // param
  "\\d+\\.?\\d*",               // number
  "[a-zA-Z_][a-zA-Z0-9_]*",     // ident
  "[(),.;]",                    // punct
  "[^\\s]",                     // other
].map((p) => `(${p})`).join("|"), "g");

function tokenize(sql) {
  const toks = [];
  for (const m of (sql || "").matchAll(TOKEN_RE)) {
    const t = m[0];
    if (/^\s/.test(t) || t.startsWith("--") || t.startsWith("/*")) continue; // skip ws/comments
    if (/^['"`\[]/.test(t)) toks.push(["ident_q", t]);       // quoted ident/string — 구분은 아래
    else if (/^[a-zA-Z_]/.test(t)) toks.push(["ident", t]);
    else if (/^\d/.test(t)) toks.push(["number", t]);
    else if (/^:/.test(t)) toks.push(["param", t]);
    else toks.push(["punct", t]);
  }
  // 문자열('...')과 인용식별자("..."/`...`/[...]) 재구분
  return toks.map(([k, v]) => {
    if (k === "ident_q") return v[0] === "'" ? ["string", v] : ["ident", stripIdent(v)];
    return [k, v];
  });
}
function stripIdent(v) { return v.replace(/^["`\[]|["`\]]$/g, ""); }

// FROM/JOIN 절의 모든 테이블 참조 열거(콤마 조인·파생 뒤 콤마·스키마 한정·TVF 포함).
function tableRefs(toks) {
  const refs = [];
  let depth = 0, inList = false, listDepth = 0, expect = false, i = 0;
  while (i < toks.length) {
    const [kind, val] = toks[i];
    const low = kind === "ident" ? val.toLowerCase() : val;
    if (kind === "punct" && val === "(") { if (expect) expect = false; depth++; i++; continue; }
    if (kind === "punct" && val === ")") { depth--; if (inList && depth < listDepth) { inList = false; expect = false; } i++; continue; }
    if (kind === "ident" && (low === "from" || low === "join")) { inList = true; listDepth = depth; expect = true; i++; continue; }
    if (inList && kind === "punct" && val === "," && depth === listDepth) { expect = true; i++; continue; }
    if (inList && kind === "ident" && BOUNDARY.has(low)) { inList = false; expect = false; i++; continue; }
    if (inList && kind === "ident" && JOINMOD.has(low)) { i++; continue; }
    if (inList && kind === "ident" && low === "as") { i++; continue; }
    if (expect && kind === "ident") {
      let name = val, j = i + 1;
      while (j + 1 < toks.length && toks[j][0] === "punct" && toks[j][1] === "." && toks[j + 1][0] === "ident") {
        name += "." + toks[j + 1][1]; j += 2;
      }
      refs.push(name); expect = false; i = j; continue;
    }
    i++;
  }
  return refs;
}

// allowedTables: Set<string> (lowercase) — 그 제품이 선언한 테이블 ∪ 공용 축.
// 반환: 위반 사유 배열(빈 배열 = 통과).
export function auditPatternSql(sql, allowedTables) {
  const findings = [];
  const toks = tokenize(sql);
  if (!toks.length || !(toks[0][0] === "ident" && ["select","with"].includes(toks[0][1].toLowerCase())))
    findings.push("SELECT/WITH 로 시작하지 않음");
  const semis = toks.map((t, k) => (t[0] === "punct" && t[1] === ";" ? k : -1)).filter((k) => k >= 0);
  if (semis.length && !(semis.length === 1 && semis[0] === toks.length - 1))
    findings.push("세미콜론 내부 등장 — 스택 쿼리 의심");
  for (const [kind, val] of toks) {
    if (kind === "ident") {
      const lw = val.toLowerCase();
      if (FORBIDDEN.has(lw) || lw.startsWith("pragma")) { findings.push(`금지 토큰 '${lw}'`); break; }
    }
  }
  const ctes = cteNames(toks);
  const refs = tableRefs(toks).map((r) => r.toLowerCase());
  const external = [...new Set(refs)].filter((r) => !allowedTables.has(r) && !ctes.has(r) && !ALLOWED_TVF.has(r));
  if (external.length) findings.push(`allowlist 밖 테이블 참조: ${JSON.stringify(external.sort())}`);
  for (let k = 0; k < toks.length - 1; k++) {
    if (toks[k][0] === "ident" && toks[k][1].toLowerCase() === "limit" && toks[k + 1][0] === "param") {
      const nm = toks[k + 1][1].slice(1);
      if (!LIMIT_NAMES.has(nm)) findings.push(`LIMIT :${nm} — n/limit/top_n 만 상한 적용`);
    }
  }
  return findings;
}

function cteNames(toks) {
  const names = new Set(); let depth = 0;
  for (let i = 0; i < toks.length; i++) {
    const [kind, val] = toks[i];
    if (kind === "punct" && val === "(") depth++;
    else if (kind === "punct" && val === ")") depth = Math.max(0, depth - 1);
    if (kind === "ident" && val.toLowerCase() === "as" && i >= 2 && i + 1 < toks.length) {
      const prev = toks[i - 1], nxt = toks[i + 1], pp = toks[i - 2];
      const ppWith = pp[0] === "ident" && pp[1].toLowerCase() === "with";
      if (prev[0] === "ident" && nxt[0] === "punct" && nxt[1] === "(" &&
          (ppWith || (pp[0] === "punct" && pp[1] === ",")) && depth === 0)
        names.add(prev[1].toLowerCase());
    }
  }
  return names;
}

// 게이트웨이 사용 예 (handleRunPattern 안, env.DB.prepare 직전):
//   const allowed = new Set(productDeclaredTables.map(s => s.toLowerCase())); // 그 제품 d1_table ∪ 공용 축
//   const violations = auditPatternSql(pattern.sql, allowed);
//   if (violations.length) return problem(400, "pattern out of scope", violations[0]);
//
// 회귀: redteam_payloads.json 을 로드해 각 케이스 auditPatternSql(sql, new Set(allowlist_example))
//   가 expect==="pass" 면 빈 배열, "block" 이면 비어있지 않음을 단언.
