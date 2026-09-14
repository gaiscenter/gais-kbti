"""
GAIS KBTI 10-Year Historical Backfill — 1회성 실행용
GDELT 2015-01-01 ~ 현재까지, 5개 쌍의 주간(week) 단위 KBTI 계산 → kbti_history.json 생성

실행 전 자동으로 dry-run을 먼저 돌려 예상 처리량(GB)을 확인하고,
COST_GUARD_GB(기본 800GB)를 초과하면 실제 쿼리를 실행하지 않고 중단합니다.
BigQuery 무료 한도는 매월 1TB(=1024GB)이므로 800GB는 안전 마진입니다.

partitioned 테이블(gdelt-bq.gdeltv2.events_partitioned)을 사용합니다.
10년 전체를 조회하므로 파티션 프루닝 자체의 비용 절감 효과는 없지만
(어차피 전체 기간을 원하므로), 향후 특정 구간만 재실행할 때 비용을 아낄 수 있어
이 테이블을 기본으로 사용합니다.

사용법 (GitHub Actions workflow_dispatch 또는 로컬 모두 가능):
  BACKFILL_START=2015-01-01 COST_GUARD_GB=800 python backfill_history.py
"""
import json, os
from datetime import datetime
from collections import defaultdict
from google.cloud import bigquery

PROJECT_ID = os.environ.get("GCP_PROJECT", "ancient-voltage-508302-v3")
START_DATE = os.environ.get("BACKFILL_START", "2015-01-01")
COST_GUARD_GB = float(os.environ.get("COST_GUARD_GB", "800"))

QUERY = """
SELECT
  Actor1CountryCode AS a1,
  Actor2CountryCode AS a2,
  DATE_TRUNC(DATE(SQLDATE_TS), WEEK(MONDAY)) AS week_start,
  SUM(GoldsteinScale * NumMentions) AS weighted_sum,
  SUM(NumMentions) AS mentions,
  COUNT(*) AS event_count
FROM (
  SELECT
    Actor1CountryCode,
    Actor2CountryCode,
    GoldsteinScale,
    NumMentions,
    PARSE_DATE('%Y%m%d', CAST(SQLDATE AS STRING)) AS SQLDATE_TS
  FROM `gdelt-bq.gdeltv2.events_partitioned`
  WHERE
    _PARTITIONTIME >= TIMESTAMP(@start_date)
    AND (
      (Actor1CountryCode = 'KOR' AND Actor2CountryCode IN ('PRK','JPN','CHN','USA','RUS'))
      OR (Actor2CountryCode = 'KOR' AND Actor1CountryCode IN ('PRK','JPN','CHN','USA','RUS'))
    )
)
GROUP BY a1, a2, week_start
HAVING event_count >= 5
ORDER BY week_start ASC
"""

NAMES = {"PRK": "한국-북한", "JPN": "한국-일본", "CHN": "한국-중국", "USA": "한국-미국", "RUS": "한국-러시아"}
PARTNERS = ["PRK", "JPN", "CHN", "USA", "RUS"]


def main():
    client = bigquery.Client(project=PROJECT_ID)
    params = [bigquery.ScalarQueryParameter("start_date", "STRING", START_DATE)]

    # 1) dry-run으로 예상 처리량 먼저 확인 (실제 과금 없음)
    dry_config = bigquery.QueryJobConfig(
        query_parameters=params, dry_run=True, use_query_cache=False
    )
    dry_job = client.query(QUERY, job_config=dry_config)
    gb = dry_job.total_bytes_processed / 1e9
    print(f"[dry-run] 예상 처리량: {gb:.2f} GB (참고: 무료 한도 1024GB/월)")

    if gb > COST_GUARD_GB:
        raise SystemExit(
            f"중단: 예상 처리량 {gb:.2f}GB가 안전 한도 COST_GUARD_GB={COST_GUARD_GB}GB를 초과했습니다.\n"
            f"연도별로 나눠서 (예: BACKFILL_START=2015-01-01, 다음엔 2018-01-01 ...) 여러 번 실행하거나,\n"
            f"COST_GUARD_GB 환경변수를 조정해 재실행하세요."
        )

    # 2) 실제 실행
    run_config = bigquery.QueryJobConfig(query_parameters=params)
    rows = list(client.query(QUERY, job_config=run_config).result())
    print(f"쿼리 완료: {len(rows)}행 (주간 집계 기준)")

    weekly = defaultdict(lambda: defaultdict(lambda: {"m": 0, "ws": 0, "e": 0}))
    for r in rows:
        week = r["week_start"].isoformat()
        a1, a2 = r["a1"], r["a2"]
        if a2 not in PARTNERS and a1 not in PARTNERS:
            continue
        partner = a2 if a1 == "KOR" else a1
        direction = "response" if a1 == "KOR" else "threat"
        key = f"{partner}_{direction}"
        m = int(r["mentions"] or 0)
        ws = float(r["weighted_sum"] or 0)
        weekly[week][key]["m"] += m
        weekly[week][key]["ws"] += ws
        weekly[week][key]["e"] += int(r["event_count"] or 0)

    weeks = sorted(weekly.keys())

    history = {}
    for p in PARTNERS:
        th_vals, re_vals = [], []
        for w in weeks:
            th_dd = weekly[w].get(f"{p}_threat", {})
            re_dd = weekly[w].get(f"{p}_response", {})
            th = round(th_dd["ws"] / th_dd["m"] * -1.0, 4) if th_dd.get("e", 0) >= 5 and th_dd.get("m", 0) > 0 else None
            re = round(re_dd["ws"] / re_dd["m"] * -1.0, 4) if re_dd.get("e", 0) >= 5 and re_dd.get("m", 0) > 0 else None
            th_vals.append(th)
            re_vals.append(re)
        history[f"{p}_threat"] = th_vals
        history[f"{p}_response"] = re_vals

    output = {
        "generated_at": datetime.now().isoformat(),
        "start_date": START_DATE,
        "granularity": "week",
        "weeks": weeks,
        "total_rows": len(rows),
        "estimated_gb_processed": round(gb, 2),
        "history": history,
        "partner_names": NAMES,
        "note": "10-year weekly KBTI backfill = Goldstein(1992,JCR) x NumMentions weighted avg x (-1)"
    }

    with open("kbti_history.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"저장 완료: kbti_history.json ({len(weeks)}주, {START_DATE} ~ 현재)")


if __name__ == "__main__":
    main()
