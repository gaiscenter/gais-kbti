# -*- coding: utf-8 -*-
"""11/20 부터 기사 유입이 급락한 것이 GDELT 전체인지 한국만인지 가린다. 읽기 전용.

무슨 일이 있었나 (2026-11-23 실측)
----------------------------------
일별 고유 URL 이 11/20 부터 무너졌다.
    11/20 이전 31일 평균 95 · 중앙값 87 · 최저 32
    11/20~23        50, 17, 40, 8   (평균 29)
기록 전체에서 가장 낮은 4일이고, 이전 최저 4일 구간(57)의 절반이다.
30일 집계도 같다 — 실행 사이 새 사건 증가가 +150~330 에서 +20~65 로 떨어졌다.

본문 추출·길이 문턱과는 무관하다. 원시 URL 단계에서 이미 적다(생존율 100%).

무엇을 가리나
-------------
세 가지를 **같은 날짜 축**에 놓고 본다:
  1. 전체          — GDELT 가 그날 적재한 모든 사건
  2. 한국 관련     — 한쪽이 KOR 인 사건 (상대국 무관)
  3. 우리 조건     — KOR + 5개 상대국

  1 도 떨어졌다        -> GDELT 적재 쪽 문제. 우리가 할 수 있는 게 없다
  1 은 멀쩡한데 2 가 ↓ -> 한국 기사의 행위자 코딩이 달라졌다
  2 는 멀쩡한데 3 이 ↓ -> 상대국 코딩 문제. 우리 조건을 손봐야 한다

쓰는 법
-------
    python check_volume.py          # dry-run
    RUN=1 python check_volume.py    # 실제 조회
"""
import os
import sys

from google.cloud import bigquery

PROJECT_ID = os.environ.get("GCP_PROJECT")
DAYS = int(os.environ.get("VOLUME_DAYS") or "21")
GUARD_GB = float(os.environ.get("QUERY_COST_GUARD_GB") or "5")
RUN = os.environ.get("RUN") == "1"

PARTNERS = "('PRK','JPN','CHN','USA','RUS')"

QUERY = f"""
SELECT
  CAST(SQLDATE AS STRING) AS d,
  COUNT(*) AS all_events,
  COUNTIF(Actor1CountryCode='KOR' OR Actor2CountryCode='KOR') AS kor_events,
  COUNTIF(
    (Actor1CountryCode='KOR' AND Actor2CountryCode IN {PARTNERS})
    OR (Actor2CountryCode='KOR' AND Actor1CountryCode IN {PARTNERS})
  ) AS ours
FROM `gdelt-bq.gdeltv2.events_partitioned`
WHERE _PARTITIONTIME >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL {DAYS} DAY))
GROUP BY d
ORDER BY d
"""


def main():
    if not PROJECT_ID:
        sys.exit("GCP_PROJECT 가 없습니다.")
    client = bigquery.Client(project=PROJECT_ID)

    dry = client.query(QUERY, job_config=bigquery.QueryJobConfig(
        dry_run=True, use_query_cache=False))
    gb = dry.total_bytes_processed / 1e9
    print(f"[dry-run] 최근 {DAYS}일 · 예상 처리량 {gb:.3f} GB (상한 {GUARD_GB} GB)",
          flush=True)
    if gb > GUARD_GB:
        sys.exit("중단: 상한 초과. VOLUME_DAYS 를 줄이십시오.")
    if not RUN:
        print("RUN=1 이 아니어서 멈춥니다.")
        return

    rows = [r for r in client.query(QUERY, job_config=bigquery.QueryJobConfig(
        maximum_bytes_billed=int(GUARD_GB * 1e9))).result()]
    rows = [r for r in rows if (r["d"] or "").startswith("202")]
    rows.sort(key=lambda r: r["d"])

    print(f"\n{'='*76}")
    print(f"  {'날짜':12s}{'전체 사건':>12s}{'한국 관련':>11s}{'우리 조건':>10s}"
          f"{'한국/전체':>10s}{'우리/한국':>10s}")
    print('='*76)
    for r in rows[-21:]:
        d = r["d"]
        dd = f"{d[:4]}-{d[4:6]}-{d[6:]}"
        a, k, o = r["all_events"], r["kor_events"], r["ours"]
        print(f"  {dd:12s}{a:>12,}{k:>11,}{o:>10,}"
              f"{(k/a if a else 0):>9.2%}{(o/k if k else 0):>10.1%}")

    # 11/20 전후로 나눠 본다
    cut = "20261120"
    pre = [r for r in rows if r["d"] < cut]
    post = [r for r in rows if r["d"] >= cut]
    if pre and post:
        def avg(rs, k):
            return sum(r[k] for r in rs) / len(rs)
        print(f"\n{'='*76}\n  11/20 전후 평균\n{'='*76}")
        print(f"  {'':12s}{'전체':>12s}{'한국 관련':>11s}{'우리 조건':>10s}")
        for name, rs in (("이전", pre), ("이후", post)):
            print(f"  {name:12s}{avg(rs,'all_events'):>12,.0f}"
                  f"{avg(rs,'kor_events'):>11,.0f}{avg(rs,'ours'):>10,.0f}")
        print(f"  {'변화':12s}"
              f"{avg(post,'all_events')/max(1,avg(pre,'all_events'))-1:>11.0%}"
              f"{avg(post,'kor_events')/max(1,avg(pre,'kor_events'))-1:>11.0%}"
              f"{avg(post,'ours')/max(1,avg(pre,'ours'))-1:>10.0%}")

    print(f"\n{'='*76}\n  읽는 법\n{'='*76}")
    print("  '전체' 도 같이 떨어졌다       -> GDELT 적재 문제. 우리 쪽 손댈 것이 없다")
    print("  '전체' 는 멀쩡한데 '한국' 이 ↓ -> 한국 기사의 행위자 코딩이 달라졌다")
    print("  '한국' 은 멀쩡한데 '우리' 가 ↓ -> 상대국 조건을 손봐야 한다")
    print("  마지막 날은 적재가 진행 중이라 원래 적다 — 추세에서 빼고 볼 것")


if __name__ == "__main__":
    main()
