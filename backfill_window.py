#!/usr/bin/env python3
"""
논문 분석 창을 현재 파이프라인으로 다시 계산한다 — 감사 전 / 감사 후 두 계열.

왜
--
Figure 1은 2026-09-22 운영 실행의 산출물이고, 그 시점의 kbti_output.json은 어디에도
보관돼 있지 않다(저장소의 kbti_output.json은 매일 덮어쓰기). 그래서 그림을 다시 그릴
데이터가 없었다. 또 오늘 측정한 타당도 통계(커버리지·추출품질·제외 분해)는 전부
2026-10-13 값이라 논문이 분석하는 창(8/18~9/22) 밖이다.

이 스크립트가 만드는 것
-----------------------
  1) 감사 전 계열 : GDELT가 코딩한 그대로. 보정 없음.
  2) 감사 후 계열 : 현재 감사(세 조건 + 2026-10-13 추출 품질 게이트)를 창 전체에 적용.
  3) 제외된 URL 목록과 조건별 분해 — 창 안에서 측정된 통계.

둘의 차이가 곧 "운영계열 대 사후보정계열"이며, 원고가 Appendix E에서 구분해야 하는
바로 그것이다. 주의: (2)는 2026-09-22에 돌던 감사의 재현이 아니다. 그 사이 프롬프트와
추출기가 바뀌었다. 재현이 아니라 '현재 기준으로 다시 본 창'이다.

재현성
------
쿼리는 pipeline의 상수를 문자열 치환으로 날짜만 바꿔 쓴다. 필드 목록·행위자 필터·
도메인 태그 정의를 건드리지 않기 위함이다. 지수 계산은 pipeline.build_events_domains()를
그대로 호출한다 — 재구현하면 그 차이가 결과가 되어버린다.

실행
----
  WINDOW_START=2026-08-23 WINDOW_END=2026-09-22 BACKFILL_PARTNERS=PRK \
  python backfill_window.py
"""
import json, os, re, sys, time
import concurrent.futures
from collections import defaultdict
from datetime import datetime, date, timedelta

import pipeline
from pipeline import (build_events_domains, _extract_article, _audit_with_llm,
                      _parse_conditions, NAMES, AUDIT_TOP_K, AUDIT_TOP_K_SPARSE)

WINDOW_START = os.environ.get("WINDOW_START", "2026-08-23")
WINDOW_END   = os.environ.get("WINDOW_END",   "2026-09-22")
# 희소 도메인이 7일 이동창을 쓰므로, 창 시작일의 값을 맞추려면 그 앞 데이터가 필요하다.
LEAD_DAYS    = int(os.environ.get("BACKFILL_LEAD_DAYS", "10"))
PARTNERS     = [p.strip() for p in
                os.environ.get("BACKFILL_PARTNERS", "PRK").split(",") if p.strip()]
MAX_CALLS    = int(os.environ.get("BACKFILL_MAX_CALLS", "2500"))
GUARD_GB     = float(os.environ.get("BACKFILL_GUARD_GB", "20"))
OUT_DIR      = os.environ.get("BACKFILL_DIR", "paper_archive")
FETCH_WORKERS = 12

_ABS = ("_PARTITIONTIME >= TIMESTAMP('{s}') "
        "AND _PARTITIONTIME < TIMESTAMP(DATE_ADD(DATE '{e}', INTERVAL 5 DAY))")
_REL = re.compile(r"_PARTITIONTIME >= TIMESTAMP\(DATE_SUB\(CURRENT_DATE\(\), INTERVAL \d+ DAY\)\)")


def windowed(query, start, end):
    """쿼리의 상대 날짜 조건 한 줄만 절대 구간으로 바꾼다. 나머지는 손대지 않는다."""
    new = _ABS.format(s=start, e=end)
    out, n = _REL.subn(new, query)
    if n != 1:
        raise RuntimeError(f"날짜 조건을 정확히 1개 찾지 못했습니다(발견 {n}개). "
                           "pipeline의 쿼리가 바뀌었는지 확인하십시오.")
    return out


def run(client, query, label):
    from google.cloud import bigquery
    dry = client.query(query, job_config=bigquery.QueryJobConfig(dry_run=True,
                                                                use_query_cache=False))
    gb = dry.total_bytes_processed / 1e9
    print(f"  [dry-run] {label}: {gb:.3f} GB")
    if gb > GUARD_GB:
        raise RuntimeError(f"{label} 예상 스캔 {gb:.1f} GB > 상한 {GUARD_GB} GB — 중단")
    return list(client.query(query, job_config=pipeline._job_config()).result()), gb


def buckets_for(raw_rows, latest_date):
    """
    pipeline.fetch_raw_daily_events()의 688-711행과 동일한 규칙으로 버킷을 만든다.
    차이는 '오늘'이 고정 날짜라는 것뿐. (그 함수는 쿼리까지 같이 하므로 재사용 불가)
    """
    ls = latest_date.replace("-", "")
    lo = (date.fromisoformat(latest_date) - timedelta(days=6)).strftime("%Y%m%d")
    b = defaultdict(list)
    for r in raw_rows:
        ds = str(r["date_str"])
        if ds > ls or ds < lo:
            continue
        a1, a2 = r["Actor1CountryCode"], r["Actor2CountryCode"]
        partner = a2 if a1 == "KOR" else a1
        if partner not in PARTNERS:
            continue
        direction = "response" if a1 == "KOR" else "threat"
        g = float(r["GoldsteinScale"] or 0); m = int(r["NumMentions"] or 0)
        ev = {"a1": r["Actor1Name"], "a2": r["Actor2Name"], "code": r["EventCode"],
              "goldstein": g, "mentions": m, "impact": g * m, "url": r["SOURCEURL"],
              "partner": partner, "direction": direction, "date": ds}
        if ds == ls:
            b[("all", partner, direction)].append(ev)
        if r["is_military"]:   b[("mil", partner, direction)].append(ev)
        if r["is_supply"]:     b[("sup", partner, direction)].append(ev)
        if r["is_diplomatic"]: b[("dip", partner, direction)].append(ev)
    return b


def main():
    key = os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        print("ANTHROPIC_API_KEY가 없습니다.", file=sys.stderr); sys.exit(1)
    import anthropic
    from google.cloud import bigquery
    llm = anthropic.Anthropic(api_key=key)
    bq = bigquery.Client(project=pipeline.PROJECT_ID)
    os.makedirs(OUT_DIR, exist_ok=True)

    qs = (date.fromisoformat(WINDOW_START) - timedelta(days=LEAD_DAYS)).isoformat()
    print(f"[{datetime.now():%Y-%m-%d %H:%M}] 백필 시작  창 {WINDOW_START} ~ {WINDOW_END}"
          f"  (선행 {LEAD_DAYS}일 포함 조회 {qs} ~)  파트너 {','.join(PARTNERS)}")

    ev_rows, gb1 = run(bq, windowed(pipeline.EVENTS_QUERY, qs, WINDOW_END), "events(집계)")
    raw_rows, gb2 = run(bq, windowed(pipeline.RAW_AUDIT_QUERY, qs, WINDOW_END), "raw(레코드)")
    print(f"  events {len(ev_rows)}행 / raw {len(raw_rows)}행\n")

    days = []
    d, dend = date.fromisoformat(WINDOW_START), date.fromisoformat(WINDOW_END)
    while d <= dend:
        days.append(d.isoformat()); d += timedelta(days=1)

    # ── 날짜별 감사 ────────────────────────────────────────────────────────
    verdicts = {}     # url -> {"keep":bool, "cond":{...}}  (창 전체에서 한 번만 판정)
    per_day  = {}     # 날짜 -> 그날 검증 대상 URL 목록
    calls = 0
    for i, day in enumerate(days, 1):
        b = buckets_for(raw_rows, day)
        to_check = {}
        for (dom, p, dr), evs in b.items():
            k = AUDIT_TOP_K if dom == "all" else AUDIT_TOP_K_SPARSE
            for ev in sorted(evs, key=lambda e: abs(e["impact"]), reverse=True)[:k]:
                to_check.setdefault(ev["url"], ev)
        per_day[day] = list(to_check)
        fresh = [u for u in to_check if u not in verdicts]
        if calls + len(fresh) > MAX_CALLS:
            fresh = fresh[:max(0, MAX_CALLS - calls)]
            print(f"  ::warning::호출 상한 {MAX_CALLS} 도달 — 이후 날짜는 미검증으로 남습니다")
        if not fresh:
            print(f"  [{i:>2}/{len(days)}] {day}  대상 {len(to_check):>3}  신규 0 (전부 캐시)")
            continue

        arts = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=FETCH_WORKERS) as ex:
            for u, a in zip(fresh, ex.map(_extract_article, fresh)):
                arts[u] = a
        kept = 0
        for u in fresh:
            ev = to_check[u]; detail = {}
            art = arts.get(u)
            if art is None:
                ok = ev["code"] not in {"150", "182", "190", "193", "194", "195"}
            else:
                ok = _audit_with_llm(llm, NAMES.get(ev["partner"], ev["partner"]),
                                     ev, art, detail)
                if art.get("full_text"):
                    calls += 1
            verdicts[u] = {"keep": bool(ok),
                           "cond": {k2: detail.get(k2) for k2 in ("c1", "c2", "c3")},
                           "fetched": bool(art) and not (art or {}).get("unparseable"),
                           "text_len": len((art or {}).get("full_text") or "")}
            kept += bool(ok)
        print(f"  [{i:>2}/{len(days)}] {day}  대상 {len(to_check):>3}  신규 {len(fresh):>3}"
              f"  유지 {kept:>3}  누적호출 {calls}")
        with open(os.path.join(OUT_DIR, "_backfill_progress.json"), "w", encoding="utf-8") as f:
            json.dump({"done_through": day, "verdicts": verdicts}, f, ensure_ascii=False)

    bad = sorted(u for u, v in verdicts.items() if not v["keep"])
    print(f"\n검증 {len(verdicts)}건 / 제외 {len(bad)}건  (LLM 호출 {calls}회)")

    # ── 감사 후 계열: 같은 쿼리에 제외 URL만 빼고 다시 집계 ────────────────
    post_q = windowed(pipeline.EVENTS_QUERY, qs, WINDOW_END)
    post_q = post_q.replace("FROM `gdelt-bq.gdeltv2.events_partitioned`\n  WHERE",
                            "FROM `gdelt-bq.gdeltv2.events_partitioned`\n  WHERE "
                            "SOURCEURL NOT IN UNNEST(@bad) AND", 1)
    from google.cloud import bigquery as _bq
    cfg = pipeline._job_config()
    cfg.query_parameters = [_bq.ArrayQueryParameter("bad", "STRING", bad)]
    dry = bq.query(post_q, job_config=_bq.QueryJobConfig(
        dry_run=True, use_query_cache=False,
        query_parameters=[_bq.ArrayQueryParameter("bad", "STRING", bad)]))
    print(f"  [dry-run] events(감사 후): {dry.total_bytes_processed/1e9:.3f} GB")
    ev_post = list(bq.query(post_q, job_config=cfg).result())

    dates_pre,  dom_pre  = build_events_domains(ev_rows)
    dates_post, dom_post = build_events_domains(ev_post)

    def slice_(dates, dom):
        idx = [i for i, d2 in enumerate(dates) if WINDOW_START <= d2 <= WINDOW_END]
        return ([dates[i] for i in idx],
                {dn: {f: [v["series"][f][i] for i in idx] for f in v["series"]}
                 for dn, v in dom.items()})

    ds, s_pre  = slice_(dates_pre,  dom_pre)
    _,  s_post = slice_(dates_post, dom_post)

    cond = defaultdict(int)
    for u in bad:
        c = verdicts[u]["cond"]
        cond["c1_fail" if c.get("c1") is False else "c1_pass_tone_fail"] += 1

    out = {
        "generated_at": datetime.now().isoformat(),
        "window": {"start": WINDOW_START, "end": WINDOW_END, "lead_days": LEAD_DAYS},
        "partners": PARTNERS,
        "note": ("감사 후 계열은 2026-09-22 당시 감사의 재현이 아니라 현재 감사"
                 "(세 조건 + 2026-10-13 추출 게이트)를 창에 적용한 결과다."),
        "query_gb": {"events_pre": gb1, "raw": gb2},
        "llm_calls": calls,
        "audited_urls": len(verdicts), "excluded_urls": len(bad),
        "exclusion_breakdown": dict(cond),
        "fetch_ok": sum(1 for v in verdicts.values() if v["fetched"]),
        "dates": ds,
        "series_pre_audit": s_pre,
        "series_post_audit": s_post,
        "excluded": bad,
        "verdicts": verdicts,
    }
    path = os.path.join(OUT_DIR, f"backfill_{WINDOW_START}_to_{WINDOW_END}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"\n저장: {path}")

    # ── 원고 Table 2의 다섯 날짜로 대조 ────────────────────────────────────
    TBL = {"2026-09-11": (0.71, 1.66, 0.27), "2026-09-12": (5.06, 6.45, 0.71),
           "2026-09-13": (1.41, 5.03, 0.55), "2026-09-16": (0.67, 0.32, -2.74),
           "2026-09-20": (5.91, 6.39, 4.18)}
    print("\nTable 2 대조 (threat 방향, PRK) — 공표값 대 재계산값")
    print(f"{'날짜':<12}{'채널':<11}{'공표':>8}{'감사전':>9}{'감사후':>9}")
    for d2, (ov, mi, di) in TBL.items():
        if d2 not in ds: continue
        i = ds.index(d2)
        for nm, dom, pub in (("overall", "overall", ov), ("military", "military", mi),
                             ("diplomatic", "diplomatic", di)):
            a = s_pre[dom].get("PRK_threat", [None]*len(ds))[i]
            b2 = s_post[dom].get("PRK_threat", [None]*len(ds))[i]
            fa = "—" if a is None else f"{a:+.2f}"
            fb = "—" if b2 is None else f"{b2:+.2f}"
            print(f"{d2:<12}{nm:<11}{pub:>8.2f}{fa:>9}{fb:>9}")
    print("\n공표값과 '감사전'이 가까우면 재계산이 충실한 것이고, 그림을 다시 그려도 된다.")
    print("크게 다르면 그 차이 자체가 운영계열/사후보정계열 구분의 근거다(Appendix E).")


if __name__ == "__main__":
    main()
