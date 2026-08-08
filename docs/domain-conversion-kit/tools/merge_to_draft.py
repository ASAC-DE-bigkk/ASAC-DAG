"""accepted.json(검증 통과 신규 패턴)을 도메인 yml 삽입용 draft 블록으로 변환 — 미적용.

각 도메인 accepted.json 을 읽어 제품(모델)별로 usage_patterns 항목 yml 블록을 생성한다.
타 도메인은 d1_table 미선언(모델명=테이블명)이므로 블록에 d1_table 을 넣지 않는다. verified_rows
는 0(스탬핑 대기) 자리만. 실 D1 실측 행수는 주석으로 병기(스탬핑 시 참고).

출력: <authoring>/<domain>/draft_patterns.yml (제품별로 "삽입 대상 모델" 헤더 + 블록).
나중에 이 블록을 각 모델의 usage_patterns 리스트 끝에 붙이면 적용된다(구성만).
"""
import io, json, sys
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
BASE = Path(__file__).resolve().parent


def qy(s):
    return json.dumps(s, ensure_ascii=False)


def block(p, indent="            "):
    L = [f'{indent}- pattern_id: {qy(p["pattern_id"])}']
    L.append(f'{indent}  question_ko: {qy(p.get("question_ko",""))}')
    if p.get("axes"):
        L.append(f'{indent}  axes: {qy(p["axes"])}')
    rc = p.get("real_rows_by_combo") or []
    L.append(f'{indent}  verified_rows: 0                # 실 D1 실측: {rc} (스탬핑 대기)')
    if p.get("insight_sample_ko"):
        L.append(f'{indent}  insight_sample_ko: {qy(p["insight_sample_ko"])}')
    L.append(f'{indent}  sql: |')
    for ln in (p.get("sql") or "").rstrip().splitlines():
        L.append((f'{indent}    ' + ln).rstrip())
    req = ", ".join(p.get("requires") or [])
    L.append(f'{indent}  requires: [{req}]')
    return "\n".join(L)


def main():
    total = 0
    for dom_dir in sorted(BASE.glob("*/accepted.json")):
        dom = dom_dir.parent.name
        doc = json.loads(dom_dir.read_text(encoding="utf-8"))
        acc = doc.get("accepted", [])
        by_model = {}
        for p in acc:
            by_model.setdefault(p["table"], []).append(p)
        out = [f"# {dom} — 검증 통과 신규 패턴 draft (미적용, 구성만 하면 삽입)",
               f"# 총 {len(acc)}건 · 실 D1 read-검증 통과 · 정적 감사 통과 · verified_rows 스탬핑 대기", ""]
        for model in sorted(by_model):
            out.append(f"### 삽입 대상 모델: {model}  (usage_patterns 리스트 끝에 추가)")
            for p in by_model[model]:
                out.append(block(p))
            out.append("")
        (BASE / dom / "draft_patterns.yml").write_text("\n".join(out) + "\n", encoding="utf-8")
        total += len(acc)
        print(f"{dom}: {len(acc)}건 -> draft_patterns.yml ({len(by_model)}개 모델)")
    print("총 draft 패턴:", total)


if __name__ == "__main__":
    main()
