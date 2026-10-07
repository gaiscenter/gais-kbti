# -*- coding: utf-8 -*-
"""
논문 부록 B가 보고하는 진단 창을, 아카이브된 모집단 위에서 다시 감사한다.

왜
--
부록 B.1의 원래 수치(N=1,847 / 한일 152건 / 64.8%)는 감사 계층이 도입되기 전
개발 단계에서 손으로 수행한 진단이라 재현되지 않는다. 여기서는 archive_paper_window.py가
저장한 모집단 위에서 감사를 다시 돌려, 모집단·감사로그·백분율이 모두 저장소 안에 있는
하나의 사슬을 만든다.

두 가지 결정 규칙을 같은 기사 집합에 모두 적용한다.
  - two  : 논문 본문과 부록 C.1이 서술하고 부록 D가 κ로 검증한 '두 조건' 규칙.
           대표 수치는 반드시 이쪽이어야 한다. 그래야 검증된 규칙과 적용된 규칙이 같다.
  - three: 2026-10-07에 추가된 '논조의 주인' 조건을 포함한 세 조건 규칙.
           분석창 이후의 개선이므로 비교 수치로만 쓴다(부록 D의 검증 범위 밖).

원 주장은 상위 몇 건 표본에 기댔지만, 여기서는 해당 관계의 기사를 전수 감사한다.

실행
----
  ARCHIVE_CSV=paper_archive/matched_events_2026-09-07_to_2026-09-13.csv \
  AUDIT_PARTNER=JPN python audit_paper_window.py
"""
import csv, json, os, sys, time
import concurrent.futures
from collections import defaultdict
from datetime import datetime

# 원문 추출은 파이프라인과 '같은 코드'를 쓴다 — 절차가 동일함을 코드로 보장하기 위함.
from pipeline import _extract_article, NAMES

WINDOW_START = os.environ.get("WINDOW_START", "2026-09-07")
WINDOW_END = os.environ.get("WINDOW_END", "2026-09-13")
ARCHIVE_DIR = os.environ.get("ARCHIVE_DIR", "paper_archive")
ARCHIVE_CSV = os.environ.get(
    "ARCHIVE_CSV",
    os.path.join(ARCHIVE_DIR, f"matched_events_{WINDOW_START}_to_{WINDOW_END}.csv"))
AUDIT_PARTNER = os.environ.get("AUDIT_PARTNER", "JPN")
AUDIT_MODEL = os.environ.get("AUDIT_MODEL", "claude-haiku-4-5-20251001")
FETCH_WORKERS = int(os.environ.get("FETCH_WORKERS", "12"))
MAX_ARTICLES = int(os.environ.get("MAX_ARTICLES", "400"))
# 상대국 호칭은 파이프라인의 NAMES를 그대로 쓴다. 이 문자열이 감사 프롬프트에 들어가므로,
# 여기서 따로 정의하면 부록 D가 검증한 결정 규칙과 미세하게 달라질 수 있다.
# (NAMES는 "한국-일본"처럼 관계명이라 문장이 다소 어색하지만, 운영 파이프라인이 실제로
#  그렇게 보내고 있으므로 재감사도 동일해야 한다. 개선은 제출 이후에.)


# ── 결정 규칙 ────────────────────────────────────────────────────────────
# two: 논문이 서술·검증한 규칙. 문구를 바꾸지 말 것 — 바꾸면 부록 D의 κ가 이 수치를
#      더는 검증하지 않게 된다.
def _prompt_two(partner_ko, ev, article):
    g = ev["goldstein"]
    tone = "갈등적/부정적" if g < 0 else "협력적/긍정적"
    title_line = f"기사 제목: {article['title']}\n" if article.get("title") else ""
    return (
        f"다음은 GDELT가 \"{ev['a1']} -> {ev['a2']}\" 간 이벤트로 자동 분류한 기사입니다.\n"
        f"분류된 CAMEO 코드: {ev['code']}, GoldsteinScale: {g:+.1f} "
        f"(이 점수는 이 상호작용이 \"{tone}\"이라는 뜻입니다. "
        f"음수=갈등/충돌/위협, 양수=협력/지원/우호).\n\n"
        f"{title_line}"
        f"기사 본문 일부: {article['full_text']}\n\n"
        f"아래 두 조건을 모두 엄격하게 확인하세요:\n"
        f"1) 이 기사가 실제로 한국과 {partner_ko} 두 국가(정부/국가급 행위자)의 "
        f"직접적인 상호작용을 다루고 있는가 (제3국 사건에 곁가지로 언급된 게 아니라)\n"
        f"2) 기사의 실제 논조가 \"{tone}\" 방향과 일치하는가. "
        f"예를 들어 의료지원·인도적 지원·경제협력·정상회담처럼 실제로는 우호적인 내용인데 "
        f"부정적 코드가 붙었거나, 반대로 위협·제재·충돌 내용인데 긍정적 코드가 붙었다면 "
        f"이건 불일치이므로 반드시 \"아니오\"로 답하세요.\n\n"
        f"두 조건이 모두 참일 때만 \"예\", 하나라도 거짓이거나 애매하면 \"아니오\"로 답하세요. "
        f"다른 설명 없이 \"예\" 또는 \"아니오\"로만 답하세요."
    )


def _prompt_three(partner_ko, ev, article):
    base = _prompt_two(partner_ko, ev, article)
    insert = (
        f"3) 그 논조가 한국과 {partner_ko}이(가) 서로에게 한 행위에서 나온 것인가, "
        f"아니면 그 행위에 대한 제3자(다른 나라·국제기구·국제여론)의 반응·비판에서 "
        f"나온 것인가. 두 나라 사이의 행위 자체는 거래·협력·접촉인데 기사의 부정적 논조가 "
        f"제3자의 비난에서 비롯된 것이라면, 그 논조는 이 두 나라 관계의 논조가 아니므로 "
        f"\"아니오\"로 답하세요. 다만 두 나라가 서로를 향해 위협·제재·비난·충돌한 것을 "
        f"제3국 언론이 단순히 '보도'한 경우는 두 나라 사이의 행위가 맞으므로 여기에 "
        f"해당하지 않습니다 — 보도한 매체의 국적이 아니라, 논조를 만들어낸 행위의 "
        f"주체가 누구인지로 판단하세요.\n\n"
    )
    base = base.replace("아래 두 조건을", "아래 세 조건을")
    base = base.replace("\n\n두 조건이 모두 참일 때만", "\n" + insert + "세 조건이 모두 참일 때만")
    return base


def _ask(client, prompt):
    """'예'면 True(유지), '아니오'면 False(제외). 호출 실패 시 보수적으로 유지."""
    for attempt in range(3):
        try:
            r = client.messages.create(model=AUDIT_MODEL, max_tokens=10,
                                       messages=[{"role": "user", "content": prompt}])
            return r.content[0].text.strip().startswith("예")
        except Exception as e:
            if attempt == 2:
                print(f"    [warn] LLM 호출 실패, 보수적으로 유지: {e}")
                return True
            time.sleep(2 * (attempt + 1))


def load_records():
    if not os.path.exists(ARCHIVE_CSV):
        raise SystemExit(f"중단: 아카이브 CSV를 찾을 수 없습니다 — {ARCHIVE_CSV}\n"
                         f"먼저 archive_paper_window.py를 실행하십시오.")
    rows = []
    with open(ARCHIVE_CSV, encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            if r["in_window_by_event_date"] != "True":
                continue          # 창 밖(파티션 여유분)은 제외 — 창의 정의는 사건일
            if r["partner"] != AUDIT_PARTNER:
                continue
            r["goldstein"] = float(r["goldstein"])
            r["mentions"] = int(r["mentions"])
            r["impact"] = abs(r["goldstein"] * r["mentions"])
            rows.append(r)
    return rows


def main():
    key = os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        raise SystemExit("중단: ANTHROPIC_API_KEY가 없습니다.")
    import anthropic
    client = anthropic.Anthropic(api_key=key)

    rows = load_records()
    partner_ko = NAMES.get(AUDIT_PARTNER, AUDIT_PARTNER)
    print(f"[{datetime.now():%Y-%m-%d %H:%M}] 창 재감사 시작 "
          f"({WINDOW_START}~{WINDOW_END}, {NAMES.get(AUDIT_PARTNER, AUDIT_PARTNER)})")
    print(f"  대상 레코드 {len(rows)}건")

    # 기사 단위로 감사한다(한 기사가 여러 레코드로 중복 집계되는 것이 GDELT의 성질).
    by_url = defaultdict(list)
    for r in rows:
        by_url[r["url"]].append(r)
    urls = sorted(by_url, key=lambda u: -max(x["impact"] for x in by_url[u]))[:MAX_ARTICLES]
    print(f"  고유 기사 {len(by_url)}건 (감사 대상 {len(urls)}건)")

    print("  원문 수집 중…")
    articles = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=FETCH_WORKERS) as pool:
        fut = {pool.submit(_extract_article, u): u for u in urls}
        for f in concurrent.futures.as_completed(fut):
            u = fut[f]
            try:
                articles[u] = f.result()
            except Exception:
                articles[u] = None
    got = sum(1 for a in articles.values() if a)
    print(f"  원문 확보 {got}/{len(urls)}건")

    print("  감사 중…")
    log = []
    for i, u in enumerate(urls, 1):
        art = articles.get(u)
        rep = max(by_url[u], key=lambda x: x["impact"])
        ev = {"a1": rep["actor1_name"], "a2": rep["actor2_name"],
              "code": rep["event_code"], "goldstein": rep["goldstein"]}
        if art and art.get("full_text"):
            keep2 = _ask(client, _prompt_two(partner_ko, ev, art))
            keep3 = _ask(client, _prompt_three(partner_ko, ev, art))
        else:
            # 원문을 못 가져온 기사는 두 규칙 모두 보수적으로 유지(파이프라인과 동일한 기본값).
            keep2 = keep3 = True
        log.append({
            "url": u, "fetched": bool(art), "title": (art or {}).get("title"),
            "actor1": rep["actor1_name"], "actor2": rep["actor2_name"],
            "event_code": rep["event_code"], "goldstein": rep["goldstein"],
            "records": len(by_url[u]),
            "mentions_total": sum(x["mentions"] for x in by_url[u]),
            "verdict_two_condition": "pass" if keep2 else "flagged",
            "verdict_three_condition": "pass" if keep3 else "flagged",
        })
        if i % 20 == 0:
            print(f"    {i}/{len(urls)}")

    # ── 집계: '부정 보도가중 신호' 중 제외된 몫 ─────────────────────────────
    # 신호 = Goldstein<0 인 레코드의 Σ|G × 보도수|. 부록 B가 쓰는 분모와 같다.
    flagged = {rule: {e["url"] for e in log if e[f"verdict_{rule}"] == "flagged"}
               for rule in ("two_condition", "three_condition")}
    neg = [r for r in rows if r["goldstein"] < 0]
    total = sum(r["impact"] for r in neg)
    result = {
        "window": {"start": WINDOW_START, "end": WINDOW_END},
        "partner": AUDIT_PARTNER,
        "model": AUDIT_MODEL,
        "generated_at": datetime.now().isoformat(),
        "records_in_window": len(rows),
        "distinct_articles": len(by_url),
        "articles_audited": len(urls),
        "articles_text_retrieved": got,
        "negative_mention_weighted_signal_total": round(total, 2),
        "by_rule": {},
        "entries": log,
    }
    for rule in ("two_condition", "three_condition"):
        excl = sum(r["impact"] for r in neg if r["url"] in flagged[rule])
        result["by_rule"][rule] = {
            "articles_flagged": len(flagged[rule]),
            "articles_flagged_share": round(100 * len(flagged[rule]) / max(len(urls), 1), 1),
            "negative_signal_excluded": round(excl, 2),
            "negative_signal_excluded_share": round(100 * excl / total, 1) if total else None,
        }

    os.makedirs(ARCHIVE_DIR, exist_ok=True)
    out = os.path.join(ARCHIVE_DIR,
                       f"audit_{AUDIT_PARTNER}_{WINDOW_START}_to_{WINDOW_END}.json")
    with open(out, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print("\n  ── 결과 ──")
    print(f"  레코드 {len(rows)}건 / 고유 기사 {len(by_url)}건 / 원문 확보 {got}건")
    print(f"  부정 보도가중 신호 총량: {total:,.1f}")
    for rule, label in (("two_condition", "두 조건 (논문 규칙 · 부록 D가 검증)"),
                        ("three_condition", "세 조건 (분석창 이후 개선 · 비교용)")):
        b = result["by_rule"][rule]
        print(f"  [{label}]")
        print(f"      제외 기사 {b['articles_flagged']}건 ({b['articles_flagged_share']}%)")
        print(f"      제외된 부정 신호 {b['negative_signal_excluded']:,.1f} "
              f"= 총량의 {b['negative_signal_excluded_share']}%   ← 부록 B에 쓸 수치")
    print(f"\n  저장: {out}")


if __name__ == "__main__":
    main()
