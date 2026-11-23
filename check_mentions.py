# -*- coding: utf-8 -*-
"""통신사 기사가 GDELT 에 있는데 우리가 안 보고 있는 것인지 확인한다. **읽기 전용.**

왜 이걸 보나
------------
우리 쿼리는 `events_partitioned` 만 읽는다. 이 테이블은 사건 하나에 **대표 URL
한 개**(SOURCEURL)만 들고 있다. 그런데 같은 사건을 보도한 매체 전부는
`eventmentions_partitioned` 에 따로 쌓인다 — events 의 NumMentions 가 바로 그
행 수다. 즉 연합·로이터·블룸버그가 그 사건을 보도했어도, 대표 URL 로 뽑히지
않으면 우리 눈에는 없는 것이 된다.

실측(2026-11-23): 누적 6,448건·1,453개 매체에서 연합·로이터·블룸버그·AP·AFP·
매일경제·뉴시스·중앙·동아가 **전부 0건**. 1,453개나 되는데 주요 통신사만
하나도 없다는 것은 우연이 아니라 구조적이다.

이 스크립트가 답하는 것은 하나뿐이다:
    **그 매체들이 eventmentions 에는 있는가?**
있으면 쿼리를 바꾸면 되는 일이고, 없으면 GDELT 바깥에서 따로 받아와야 하는
일이다. 처방이 완전히 다르므로 추측하지 말고 먼저 확인한다.

쓰는 법
-------
    python check_mentions.py          # dry-run 으로 처리량만 보고 멈춤
    RUN=1 python check_mentions.py    # 실제 조회

비용
----
dry-run 이 먼저 돌고, 기존 파이프라인과 같은 상한(QUERY_COST_GUARD_GB)을 건다.
한도를 넘으면 BigQuery 가 쿼리를 거부하므로 과금이 나지 않는다.
"""
import os
import re
import sys
from collections import Counter

from google.cloud import bigquery

PROJECT_ID = os.environ.get("GCP_PROJECT")
DAYS = int(os.environ.get("MENTION_DAYS") or "7")
GUARD_GB = float(os.environ.get("QUERY_COST_GUARD_GB") or "5")
RUN = os.environ.get("RUN") == "1"

PARTNERS = ("PRK", "JPN", "CHN", "USA", "RUS")

# 반드시 들어가야 한다고 본 매체들. 확인 결과를 읽기 쉽게 묶어 보여주기 위한 것일 뿐,
# 쿼리는 아래 목록으로 거르지 않는다 — 전체 분포를 봐야 무엇이 더 있는지 알 수 있다.
WATCH = {
    "연합뉴스": ("yna.co.kr", "yonhap"),
    "Reuters": ("reuters.com",),
    "Bloomberg": ("bloomberg.com",),
    "AP": ("apnews.com",),
    "AFP": ("afp.com",),
    "매일경제": ("mk.co.kr",),
    "뉴시스": ("newsis.com",),
    "중앙일보": ("joongang.co.kr", "joins.com"),
    "동아일보": ("donga.com",),
    "한국경제": ("hankyung.com",),
    "경향신문": ("khan.co.kr",),
    "뉴스1": ("news1.kr",),
}

# events 와 eventmentions 를 GLOBALEVENTID 로 잇는다. 두 테이블 모두 파티션을
# 걸어야 한다 — 한쪽만 걸면 전체 스캔이 된다.
QUERY = f"""
WITH ev AS (
  SELECT GLOBALEVENTID
  FROM `gdelt-bq.gdeltv2.events_partitioned`
  WHERE _PARTITIONTIME >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL {DAYS} DAY))
    AND (
      (Actor1CountryCode='KOR' AND Actor2CountryCode IN {PARTNERS})
      OR (Actor2CountryCode='KOR' AND Actor1CountryCode IN {PARTNERS})
    )
)
SELECT
  m.MentionSourceName AS source,
  COUNT(*) AS mentions,
  COUNT(DISTINCT m.GLOBALEVENTID) AS events
FROM `gdelt-bq.gdeltv2.eventmentions_partitioned` AS m
JOIN ev USING (GLOBALEVENTID)
WHERE m._PARTITIONTIME >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL {DAYS} DAY))
GROUP BY source
ORDER BY mentions DESC
"""


def main():
    if not PROJECT_ID:
        sys.exit("GCP_PROJECT 가 없습니다.")
    client = bigquery.Client(project=PROJECT_ID)

    dry = client.query(QUERY, job_config=bigquery.QueryJobConfig(
        dry_run=True, use_query_cache=False))
    gb = dry.total_bytes_processed / 1e9
    print(f"[dry-run] 최근 {DAYS}일 · 예상 처리량 {gb:.3f} GB (상한 {GUARD_GB} GB)")
    if gb > GUARD_GB:
        sys.exit(f"중단: 예상 처리량이 상한을 넘었습니다. MENTION_DAYS 를 줄여 보십시오.")
    if not RUN:
        print("RUN=1 이 아니어서 여기서 멈춥니다. 실제 조회를 하려면 RUN=1 로 다시 돌리십시오.")
        return

    rows = list(client.query(QUERY, job_config=bigquery.QueryJobConfig(
        maximum_bytes_billed=int(GUARD_GB * 1e9))).result())
    total = sum(r["mentions"] for r in rows)
    print(f"\n최근 {DAYS}일, 한국-상대국 사건을 보도한 매체 {len(rows):,}곳 · "
          f"보도 {total:,}건\n")

    by_source = {str(r["source"] or "").lower(): (r["mentions"], r["events"]) for r in rows}

    print("═══ 반드시 들어가야 한다고 한 매체 ═══")
    found = missing = 0
    for name, keys in WATCH.items():
        hits = {s: v for s, v in by_source.items() if any(k in s for k in keys)}
        if hits:
            found += 1
            m = sum(v[0] for v in hits.values())
            e = max(v[1] for v in hits.values())
            top = sorted(hits.items(), key=lambda kv: -kv[1][0])[:3]
            print(f"  ✔ {name:10s} 보도 {m:>6,}건 · 사건 {e:>5,}개   {[k for k, _ in top]}")
        else:
            missing += 1
            print(f"  ✘ {name:10s} eventmentions 에도 없음")
    print(f"\n  {len(WATCH)}곳 중 {found}곳 확인 · {missing}곳 없음")

    print("\n═══ 보도 많은 매체 상위 30 ═══")
    for r in rows[:30]:
        print(f"  {str(r['source'])[:40]:42s}{r['mentions']:>7,}건 · 사건 {r['events']:>5,}개")

    # 지금 우리가 보는 것과 비교할 수 있도록, 수집된 대표 URL 매체도 같이 센다.
    try:
        import csv
        import glob
        seen = Counter()
        for p in sorted(glob.glob("raw_data/*.csv"))[-DAYS:]:
            for row in csv.DictReader(open(p, encoding="utf-8", errors="replace")):
                mt = re.match(r"https?://([^/]+)", row.get("url") or "")
                if mt:
                    seen[mt.group(1).replace("www.", "").split(":")[0]] += 1
        print(f"\n═══ 대조: 같은 기간 우리가 실제로 받은 매체 {len(seen)}곳 ═══")
        print("  " + ", ".join(k for k, _ in seen.most_common(12)))
        gained = len(rows) - len(seen)
        print(f"\n  eventmentions 를 쓰면 매체가 {len(seen)}곳 -> {len(rows):,}곳 "
              f"({gained:+,}곳) 으로 넓어진다")
    except Exception as e:                                   # noqa: BLE001
        print(f"\n(대조 생략: {e})")


if __name__ == "__main__":
    main()
