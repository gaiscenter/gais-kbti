# -*- coding: utf-8 -*-
"""GDELT 가 통신사를 **아예 안 긁는가**, 아니면 **긁는데 우리 필터에 안 걸리는가**.
**읽기 전용.**

왜 또 보나
----------
check_mentions.py 로 eventmentions 를 봤더니 통신사가 0곳이었다(2026-11-23).
그런데 그 조회는 `Actor1/2CountryCode 가 KOR+상대국` 인 사건만 본 것이다.
즉 아직 두 가지가 구분되지 않는다:

  (a) GDELT 가 연합·로이터를 아예 수집하지 않는다
      -> GDELT 바깥에서 따로 받아오는 수밖에 없다 (수집·본문·코딩을 새로 만든다)
  (b) 수집은 하는데 그 기사들이 KOR+상대국으로 행위자 코딩이 안 됐다
      -> 행위자 필터가 병목이다. GKG 쪽 경로나 필터 완화로 풀릴 수 있다

처방이 완전히 다르므로 확인하고 간다.

보는 곳 두 군데
---------------
1. eventmentions : 행위자 필터 **없이** 그 매체 이름이 있는지
2. gkg           : GDELT 가 처리한 문서 전체. 사건이 안 뽑힌 기사도 여기엔 남는다.
                   여기에도 없으면 (a) 로 확정된다.

쓰는 법
-------
    python check_sources.py          # dry-run 으로 처리량만 보고 멈춤
    RUN=1 python check_sources.py    # 실제 조회
    SOURCE_DAYS=3 RUN=1 python ...   # 처리량이 크면 날짜를 줄인다
"""
import os
import sys

from google.cloud import bigquery

PROJECT_ID = os.environ.get("GCP_PROJECT")
DAYS = int(os.environ.get("SOURCE_DAYS") or "3")
GUARD_GB = float(os.environ.get("QUERY_COST_GUARD_GB") or "5")
RUN = os.environ.get("RUN") == "1"

# 찾는 매체. LIKE 로 거르므로 도메인 조각만 적는다.
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
    # 대조군 — 우리가 실제로 받고 있는 매체. 이게 잡히면 쿼리 자체는 멀쩡하다는 뜻이다.
    "[대조] 코리아헤럴드": ("koreaherald.com",),
    "[대조] 한겨레": ("hani.co.kr",),
}

_ALL = sorted({k for ks in WATCH.values() for k in ks})


def _like(col):
    return " OR ".join(f"{col} LIKE '%{k}%'" for k in _ALL)


# 1) eventmentions — 행위자 필터 없이, 그 매체가 '사건을 보도한 곳'으로 등장하는가
Q_MENTIONS = f"""
SELECT MentionSourceName AS source, COUNT(*) AS n
FROM `gdelt-bq.gdeltv2.eventmentions_partitioned`
WHERE _PARTITIONTIME >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL {DAYS} DAY))
  AND ({_like('MentionSourceName')})
GROUP BY source ORDER BY n DESC
"""

# 2) GKG — GDELT 가 처리한 문서 전체. 사건이 안 뽑힌 기사도 남는다.
Q_GKG = f"""
SELECT SourceCommonName AS source, COUNT(*) AS n
FROM `gdelt-bq.gdeltv2.gkg_partitioned`
WHERE _PARTITIONTIME >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL {DAYS} DAY))
  AND ({_like('SourceCommonName')})
GROUP BY source ORDER BY n DESC
"""


def _dry(client, q, label):
    j = client.query(q, job_config=bigquery.QueryJobConfig(dry_run=True,
                                                           use_query_cache=False))
    gb = j.total_bytes_processed / 1e9
    print(f"  [dry-run] {label}: {gb:.3f} GB")
    return gb


def _run(client, q):
    return list(client.query(q, job_config=bigquery.QueryJobConfig(
        maximum_bytes_billed=int(GUARD_GB * 1e9))).result())


def _report(label, rows):
    by = {str(r["source"] or "").lower(): r["n"] for r in rows}
    print(f"\n═══ {label} ═══")
    for name, keys in WATCH.items():
        hits = {s: n for s, n in by.items() if any(k in s for k in keys)}
        if hits:
            top = sorted(hits.items(), key=lambda kv: -kv[1])[:3]
            print(f"  ✔ {name:16s}{sum(hits.values()):>7,}건   {[k for k, _ in top]}")
        else:
            print(f"  ✘ {name:16s}      없음")


def main():
    if not PROJECT_ID:
        sys.exit("GCP_PROJECT 가 없습니다.")
    client = bigquery.Client(project=PROJECT_ID)

    print(f"최근 {DAYS}일 · 상한 {GUARD_GB} GB")
    g1 = _dry(client, Q_MENTIONS, "eventmentions (행위자 필터 없음)")
    g2 = _dry(client, Q_GKG, "gkg (처리 문서 전체)")
    if max(g1, g2) > GUARD_GB:
        sys.exit("중단: 상한 초과. SOURCE_DAYS 를 줄이십시오.")
    if not RUN:
        print("\nRUN=1 이 아니어서 멈춥니다.")
        return

    _report("1. eventmentions — 사건을 보도한 곳으로 등장하는가", _run(client, Q_MENTIONS))
    _report("2. GKG — GDELT 가 그 매체 기사를 처리하기는 하는가", _run(client, Q_GKG))

    print("\n── 읽는 법 ──")
    print("  GKG 에 있는데 eventmentions 에 없다 -> GDELT 는 긁지만 사건으로 안 뽑힌다(b)")
    print("  GKG 에도 없다                      -> GDELT 가 아예 안 긁는다(a)")
    print("  [대조] 두 곳이 모두 ✔ 가 아니면 쿼리나 컬럼명을 먼저 의심할 것")


if __name__ == "__main__":
    main()
