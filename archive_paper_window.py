# -*- coding: utf-8 -*-
"""
논문 부록 B.1이 보고하는 진단 창(기본 2026-09-07 ~ 09-13)의 '매칭 이벤트 모집단'을
GDELT에서 다시 뽑아 paper_archive/ 에 영구 보존한다.

왜 필요한가
-----------
파이프라인의 감사 계층은 2026-09-15에 도입됐고(부록 C.4 Table C1, Run 1),
일별 원시 CSV 보존은 09-16부터 시작됐다. 그런데 논문이 인용하는 64.8%는
09-07~09-13 창에서 나온 개발 단계 진단이다. 따라서 그 창의 기록은 저장소에
존재하지 않으며, Data availability 진술이 실제보다 넓게 주장하고 있다.

이 스크립트는 '판정'이 아니라 '모집단'을 복원한다. 어떤 레코드가 제외됐는지는
당시 LLM 판단이라 재현되지 않지만, N(전체 매칭 건수), 한일 관계 건수, 그리고
백분율의 분모가 되는 '부정 보도가중 신호' 총량은 결정적으로 재현된다.
심사자가 분모를 직접 셀 수 있게 되는 것이 요점이다.

출력
----
  paper_archive/matched_events_<start>_to_<end>.csv   전체 모집단(국가코드 포함)
  paper_archive/window_summary_<start>_to_<end>.json  집계 + 한일 상위 20건
  paper_archive/README.md                             이 아카이브가 무엇인지
"""
import csv, json, os, sys
from collections import defaultdict
from datetime import datetime, date, timedelta
from google.cloud import bigquery

PROJECT_ID = os.environ.get("GCP_PROJECT", "ancient-voltage-508302-v3")
WINDOW_START = os.environ.get("WINDOW_START", "2026-09-07")
WINDOW_END = os.environ.get("WINDOW_END", "2026-09-13")   # 포함
ARCHIVE_DIR = os.environ.get("ARCHIVE_DIR", "paper_archive")
GUARD_GB = float(os.environ.get("ARCHIVE_GUARD_GB", "12"))
# 늦게 수집되는 행을 놓치지 않기 위해 파티션은 뒤로 며칠 더 읽고, 날짜 판정은 SQLDATE로 따로 한다.
PARTITION_PAD_DAYS = int(os.environ.get("PARTITION_PAD_DAYS", "4"))

PARTNERS = ["PRK", "JPN", "CHN", "USA", "RUS"]
NAMES = {"PRK": "North Korea", "JPN": "Japan", "CHN": "China",
         "USA": "United States", "RUS": "Russia"}

QUERY = """
SELECT
  DATE(_PARTITIONTIME) AS partition_date,
  CAST(SQLDATE AS STRING) AS sqldate,
  Actor1Name, Actor1CountryCode,
  Actor2Name, Actor2CountryCode,
  EventCode, EventRootCode, QuadClass,
  GoldsteinScale, NumMentions, SOURCEURL,
  (COALESCE(Actor1Type1Code,'')='MIL' OR COALESCE(Actor1Type2Code,'')='MIL' OR COALESCE(Actor1Type3Code,'')='MIL'
   OR COALESCE(Actor2Type1Code,'')='MIL' OR COALESCE(Actor2Type2Code,'')='MIL' OR COALESCE(Actor2Type3Code,'')='MIL') AS is_military,
  (EventCode IN ('163','1621','061','071')) AS is_supply,
  (COALESCE(Actor1Type1Code,'')='GOV' OR COALESCE(Actor1Type2Code,'')='GOV' OR COALESCE(Actor1Type3Code,'')='GOV'
   OR COALESCE(Actor2Type1Code,'')='GOV' OR COALESCE(Actor2Type2Code,'')='GOV' OR COALESCE(Actor2Type3Code,'')='GOV') AS is_diplomatic
FROM `gdelt-bq.gdeltv2.events_partitioned`
WHERE
  _PARTITIONTIME >= TIMESTAMP(@pstart)
  AND _PARTITIONTIME < TIMESTAMP(@pend)
  AND (
    (Actor1CountryCode = 'KOR' AND Actor2CountryCode IN UNNEST(@partners))
    OR (Actor2CountryCode = 'KOR' AND Actor1CountryCode IN UNNEST(@partners))
  )
ORDER BY sqldate ASC, Actor1CountryCode ASC
"""


def _job_config():
    pstart = date.fromisoformat(WINDOW_START)
    pend = date.fromisoformat(WINDOW_END) + timedelta(days=PARTITION_PAD_DAYS)
    return bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("pstart", "DATE", pstart),
        bigquery.ScalarQueryParameter("pend", "DATE", pend),
        bigquery.ArrayQueryParameter("partners", "STRING", PARTNERS),
    ])


def _dry_run(client):
    cfg = _job_config()
    cfg.dry_run = True
    cfg.use_query_cache = False
    job = client.query(QUERY, job_config=cfg)
    gb = job.total_bytes_processed / 1e9
    print(f"  [dry-run] 예상 처리량 {gb:.3f} GB (한도 {GUARD_GB} GB)")
    if gb > GUARD_GB:
        raise SystemExit(f"중단: 예상 처리량 {gb:.2f}GB가 한도 {GUARD_GB}GB를 초과했습니다.")
    return gb


def _partner_of(r):
    a1, a2 = r["Actor1CountryCode"], r["Actor2CountryCode"]
    return a2 if a1 == "KOR" else a1


def _in_window_by_sqldate(r):
    s = str(r["sqldate"])
    iso = f"{s[:4]}-{s[4:6]}-{s[6:]}"
    return WINDOW_START <= iso <= WINDOW_END, iso


def main():
    print(f"[{datetime.now():%Y-%m-%d %H:%M}] 논문 창 아카이브 시작 "
          f"({WINDOW_START} ~ {WINDOW_END})")
    os.makedirs(ARCHIVE_DIR, exist_ok=True)
    client = bigquery.Client(project=PROJECT_ID)

    gb = _dry_run(client)
    rows = list(client.query(QUERY, job_config=_job_config()).result())
    print(f"  조회 결과 {len(rows)}행 (파티션 창 기준, 뒤로 {PARTITION_PAD_DAYS}일 여유 포함)")

    stem = f"{WINDOW_START}_to_{WINDOW_END}"
    csv_path = os.path.join(ARCHIVE_DIR, f"matched_events_{stem}.csv")
    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["partition_date", "event_date", "in_window_by_event_date",
                    "partner", "direction",
                    "actor1_name", "actor1_country", "actor2_name", "actor2_country",
                    "event_code", "event_root_code", "quad_class",
                    "goldstein", "mentions", "tension_contribution",
                    "is_military", "is_supply", "is_diplomatic", "url"])
        for r in rows:
            in_win, iso = _in_window_by_sqldate(r)
            g = float(r["GoldsteinScale"] or 0)
            m = int(r["NumMentions"] or 0)
            w.writerow([
                r["partition_date"], iso, in_win,
                _partner_of(r),
                "response" if r["Actor1CountryCode"] == "KOR" else "threat",
                r["Actor1Name"], r["Actor1CountryCode"],
                r["Actor2Name"], r["Actor2CountryCode"],
                r["EventCode"], r["EventRootCode"], r["QuadClass"],
                g, m, round(-g * m, 2),
                bool(r["is_military"]), bool(r["is_supply"]), bool(r["is_diplomatic"]),
                r["SOURCEURL"],
            ])
    print(f"  [csv] {len(rows)}건 저장: {csv_path}")

    # ── 집계 ─────────────────────────────────────────────────────────────
    # '부정 보도가중 신호' = Goldstein<0 인 레코드의 Σ|G × 보도수|.
    # 부록 B.1의 64.8%는 이 값을 분모로 한다.
    by_def = {"partition_window": [r for r in rows],
              "event_date_window": [r for r in rows if _in_window_by_sqldate(r)[0]]}
    summary = {
        "window": {"start": WINDOW_START, "end": WINDOW_END},
        "query_bytes_gb": round(gb, 3),
        "generated_at": datetime.now().isoformat(),
        "note": ("Two counts are reported because GDELT ingests late: rows are selected by "
                 "partition time (padded) and then also classified by event date (SQLDATE). "
                 "Appendix B.1 reports N = 1,847; compare against both definitions."),
        "counts": {},
    }
    for defname, subset in by_def.items():
        per_partner = defaultdict(lambda: {"events": 0, "mentions": 0,
                                           "negative_events": 0,
                                           "negative_mention_weighted_signal": 0.0})
        for r in subset:
            p = _partner_of(r)
            g = float(r["GoldsteinScale"] or 0)
            m = int(r["NumMentions"] or 0)
            d = per_partner[p]
            d["events"] += 1
            d["mentions"] += m
            if g < 0:
                d["negative_events"] += 1
                d["negative_mention_weighted_signal"] += abs(g * m)
        summary["counts"][defname] = {
            "total_events": len(subset),
            "by_partner": {p: {**per_partner[p],
                               "negative_mention_weighted_signal":
                                   round(per_partner[p]["negative_mention_weighted_signal"], 2)}
                           for p in PARTNERS},
        }

    # 한일 상위 20건 — 부록 B.1의 "상위 10건 중 9건" 주장을 직접 대조할 수 있도록
    jp = [r for r in by_def["event_date_window"] if _partner_of(r) == "JPN"]
    jp.sort(key=lambda r: -abs(float(r["GoldsteinScale"] or 0) * int(r["NumMentions"] or 0)))
    summary["japan_top20_by_absolute_impact"] = [{
        "rank": i + 1,
        "event_date": _in_window_by_sqldate(r)[1],
        "actor1": r["Actor1Name"], "actor2": r["Actor2Name"],
        "event_code": r["EventCode"],
        "goldstein": float(r["GoldsteinScale"] or 0),
        "mentions": int(r["NumMentions"] or 0),
        "absolute_impact": round(abs(float(r["GoldsteinScale"] or 0) * int(r["NumMentions"] or 0)), 2),
        "url": r["SOURCEURL"],
    } for i, r in enumerate(jp[:20])]

    json_path = os.path.join(ARCHIVE_DIR, f"window_summary_{stem}.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    print(f"  [json] 요약 저장: {json_path}")

    readme = os.path.join(ARCHIVE_DIR, "README.md")
    if not os.path.exists(readme):
        with open(readme, "w", encoding="utf-8") as f:
            f.write(
"""# paper_archive

Record populations supporting figures reported in the article that predate the
repository's continuous archiving.

The instrument's LLM verification layer entered service on 15 September 2026, and
daily archiving of matched record populations began on 16 September 2026. The
diagnostic window reported in Appendix B (7–13 September 2026) therefore has no
daily audit log or record file in the repository's ordinary output.

`matched_events_<start>_to_<end>.csv` restores the matched event population for
that window directly from `gdelt-bq.gdeltv2.events_partitioned`, using the same
actor-pair matching rule as the live pipeline, with country codes retained so the
bilateral assignment of every record can be checked independently.

`window_summary_<start>_to_<end>.json` reports event and mention counts by
partner under two date definitions (partition time and event date), the
negative mention-weighted signal that forms the denominator of the percentages
reported in Appendix B, and the twenty highest-impact South Korea–Japan records
with their source URLs.

Verification verdicts from the original diagnostic are not reproduced here: those
were model judgements made at the time, and re-running them under the current
prompt would not recover the same decisions. What is restored is the population
from which the reported percentages were computed.

Regenerate with `archive_paper_window.py` (see the repository workflow of the
same name).
""")
        print(f"  [readme] 생성: {readme}")

    # 콘솔 요약
    print("\n  ── 요약 ──")
    for defname in ("partition_window", "event_date_window"):
        c = summary["counts"][defname]
        print(f"  [{defname}] 전체 {c['total_events']}건")
        for p in PARTNERS:
            d = c["by_partner"][p]
            print(f"      {NAMES[p]:<14} {d['events']:>5}건 / 보도 {d['mentions']:>6} / "
                  f"부정신호 {d['negative_mention_weighted_signal']:>10.1f}")
    print("\n  부록 B.1 대조: N = 1,847 / 한일 152건 / 그 중 30건이 제3자 오염")


if __name__ == "__main__":
    main()
