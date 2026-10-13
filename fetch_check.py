#!/usr/bin/env python3
"""
추출 품질 점검 — 감사가 '본문'이라고 믿고 모델에게 보여준 것이 실제로 기사였는지 센다.

배경 (2026-10-13, 꼬리 전수조사에서 발견):
  꼬리 17건을 전수 검증했더니 16건이 제외됐다. 그런데 제외 사유를 읽어보니 셋은
  판정이 아니었다 — "브라우저 확인 페이지", "403 오류", "저작권 표시와 필자 소개만".
  즉 원문을 못 가져온 것을 모델이 "애매하면 아니오" 규칙에 따라 제외로 바꾼 것이다.

  파이프라인의 안전장치는 이 경우를 못 잡는다:
    _extract_article_direct()는 HTTP 200이면 성공으로 본다(514행). 클라우드플레어
    차단 페이지나 저작권 고지만 있는 페이지도 200으로 온다. <p> 태그가 없으면
    페이지 전체 텍스트로 폴백하므로(534행) full_text가 비어 있지 않다.
    그래서 _audit_with_llm()의 `if not article.get("full_text"): return True`
    보수적 기본값(1096행 부근)이 발동하지 않고, 모델이 쓰레기를 보고 제외 판정을 낸다.

  원고는 "원문을 못 가져오면 보수적으로 유지한다"고 적고 있다. 실제로는 그 반대가
  일어나고 있을 수 있다. 꼬리에서 19%(3/16)였다면 상위 K에서도 비슷할 수 있고,
  그렇다면 일본 창 "100개 기사 중 7개만 유지" 같은 핵심 수치가 영향을 받는다.

이 스크립트가 하는 일:
  audit_log.json에 기록된 URL들을 운영 추출기(_extract_article)로 다시 가져와,
  돌아온 것이 기사인지 차단/동의/고지 페이지인지 분류하고 판정과 교차표를 낸다.

  LLM을 전혀 호출하지 않는다. BigQuery도 쓰지 않는다. 비용 0.

사용법:
  python fetch_check.py                # audit_log.json 전체
  python fetch_check.py --limit 60
  python fetch_check.py --log audit_log.json --out fetch_check_report.json
"""
import argparse
import datetime
import json
import re
import sys
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor

import pipeline  # 추출 경로가 운영과 100% 동일해야 의미가 있다

# 본문이 아니라 '차단/동의/고지 페이지'임을 시사하는 표지.
# 느슨하게 잡으면 평범한 기사의 저작권 푸터까지 걸리므로, 페이지 전체가 그것뿐일 때만
# 의미가 있도록 길이 조건과 함께 쓴다.
BLOCK_PAT = re.compile(
    r"(just a moment|attention required|checking your browser|cloudflare|"
    r"enable javascript|자바스크립트를? (?:활성화|사용)|브라우저를? 확인|"
    r"access denied|forbidden|are you a robot|보안문자|captcha|"
    r"이 기사는 .{0,20}전용|로그인 후 이용|구독자만|페이지를 찾을 수 없)", re.I)

# 본문 없이 고지·크레딧만 남은 경우
ONLY_NOTICE_PAT = re.compile(
    r"(무단\s?전재|재배포\s?금지|저작권자|all rights reserved|ⓒ|©)", re.I)

SHORT = 300   # 이보다 짧으면 기사 본문으로 보기 어렵다
THIN = 600    # 짧지는 않지만 얇은 구간 — 경계로 따로 보고


def classify(art):
    """추출 결과를 네 등급으로."""
    if art is None:
        return "추출실패"
    ft = (art.get("full_text") or "").strip()
    title = (art.get("title") or "")
    if not ft:
        return "빈본문"
    head = (title + " " + ft[:400])
    if BLOCK_PAT.search(head):
        return "차단페이지"
    if len(ft) < SHORT:
        # 짧은데 고지문만 있으면 본문 없음으로 본다
        return "고지만" if ONLY_NOTICE_PAT.search(ft) else "너무짧음"
    if len(ft) < THIN:
        return "얇음"
    return "정상"


BAD = {"추출실패", "빈본문", "차단페이지", "고지만", "너무짧음"}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--log", default="audit_log.json")
    ap.add_argument("--limit", type=int, default=0, help="0이면 전체")
    ap.add_argument("--workers", type=int, default=10)
    ap.add_argument("--out", default="fetch_check_report.json")
    args = ap.parse_args()

    try:
        with open(args.log, "r", encoding="utf-8") as f:
            log = json.load(f)
    except FileNotFoundError:
        print(f"{args.log}이 없습니다. 저장소 루트에서 실행하십시오.", file=sys.stderr)
        sys.exit(1)

    entries = log.get("entries", [])
    seen, targets = set(), []
    for e in entries:
        if e["url"] in seen:
            continue
        seen.add(e["url"])
        targets.append(e)
    if args.limit:
        targets = targets[: args.limit]

    print(f"감사 로그 {log.get('date_audited')} — 고유 URL {len(targets)}건 재추출 "
          f"(LLM 호출 없음, 비용 0)\n" + "=" * 78)

    def work(e):
        art = pipeline._extract_article(e["url"])
        ft = (art or {}).get("full_text") or ""
        return {
            "url": e["url"], "verdict": e.get("verdict"),
            "partner": e.get("partner"), "direction": e.get("direction"),
            "goldstein": e.get("goldstein"), "mentions": e.get("mentions"),
            "grade": classify(art), "len": len(ft),
            "title": (art or {}).get("title"),
            "head": ft[:140].replace("\n", " "),
        }

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        rows = list(ex.map(work, targets))

    # ── 교차표 ──────────────────────────────────────────────────────────────
    grades = ["정상", "얇음", "너무짧음", "고지만", "차단페이지", "빈본문", "추출실패"]
    cross = defaultdict(Counter)
    for r in rows:
        cross[r["verdict"]][r["grade"]] += 1

    print(f"\n{'판정':<10}" + "".join(f"{g:>9}" for g in grades) + f"{'합계':>8}")
    for v in ("flagged", "kept", None):
        if v not in cross:
            continue
        c = cross[v]
        tot = sum(c.values())
        label = {"flagged": "제외", "kept": "유지"}.get(v, str(v))
        print(f"{label:<10}" + "".join(f"{c.get(g, 0):>9}" for g in grades) + f"{tot:>8}")

    n = len(rows)
    bad = [r for r in rows if r["grade"] in BAD]
    bad_flagged = [r for r in bad if r["verdict"] == "flagged"]
    flagged = [r for r in rows if r["verdict"] == "flagged"]

    print("\n" + "-" * 78)
    print(f"본문 확보 실패(차단·고지·너무짧음·실패): {len(bad)}/{n} ({len(bad)/n:.1%})")
    if flagged:
        print(f"  그중 '제외' 판정을 받은 것: {len(bad_flagged)}건")
        print(f"  => 전체 제외 {len(flagged)}건 중 {len(bad_flagged)/len(flagged):.1%}가 "
              f"판정이 아니라 추출 실패일 수 있습니다.")
    kept = [r for r in rows if r["verdict"] == "kept"]
    if kept:
        kb = sum(1 for r in kept if r["grade"] in BAD)
        print(f"  유지 {len(kept)}건 중 본문 실패: {kb}건 "
              f"({kb/len(kept):.1%}) ← 보수적 기본값이 제대로 작동한 경우")

    print("\n본문 확보 실패 사례 (최대 12건)")
    for r in sorted(bad, key=lambda x: x["len"])[:12]:
        print(f"  [{r['grade']}] {r['verdict']} {r['partner']}/{r['direction']} "
              f"{r['len']:>5}자  {(r['title'] or '(제목없음)')[:48]}")
        if r["head"]:
            print(f"      {r['head'][:110]}")

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump({
            "generated_at": datetime.datetime.now().isoformat(),
            "log_date": log.get("date_audited"),
            "n": n,
            "grade_counts": dict(Counter(r["grade"] for r in rows)),
            "cross": {str(k): dict(v) for k, v in cross.items()},
            "bad_n": len(bad), "bad_flagged_n": len(bad_flagged),
            "flagged_n": len(flagged),
            "share_of_exclusions_that_are_fetch_failures":
                (len(bad_flagged) / len(flagged)) if flagged else None,
            "rows": rows,
        }, f, ensure_ascii=False, indent=2)
    print(f"\n보고서 저장: {args.out}")


if __name__ == "__main__":
    main()
