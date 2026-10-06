# -*- coding: utf-8 -*-
"""raw_data/ CSV 백필 — 감사 쿼리(7일 창) 안의 날짜를 완전한 내용으로 다시 저장한다.

파이프라인이 중단된 동안 그날의 마지막 성공 실행 시점까지만 기록된 부분 파일을
하루치 전체로 교체하는 것이 목적이다. 일회성 스크립트이며 정기 실행 대상이 아니다.

사용법:
    python backfill_raw.py 2026-10-05   # 특정 날짜만
    python backfill_raw.py              # 7일 창 전체

주의: 감사 쿼리는 _PARTITIONTIME 기준 최근 7일만 조회하므로, 그보다 오래된 날짜는
이 방법으로 복구할 수 없다.
"""

import sys
from collections import defaultdict

from google.cloud import bigquery

from pipeline import (
    RAW_AUDIT_QUERY,
    save_raw_daily_csv,
    _dry_run_check,
    PROJECT_ID,
)

ONLY = sys.argv[1] if len(sys.argv) > 1 else None


def main():
    client = bigquery.Client(project=PROJECT_ID)

    # 본 파이프라인과 같은 안전장치를 통과시킨다 — 쿼리가 비정상적으로 커졌으면 여기서 중단.
    _dry_run_check(client, RAW_AUDIT_QUERY, "원시 이벤트 쿼리(백필, 7일)")
    rows = list(client.query(RAW_AUDIT_QUERY).result())
    print(f"  조회 결과: {len(rows)}행")

    by_date = defaultdict(list)
    for r in rows:
        s = str(r["date_str"])
        by_date[f"{s[:4]}-{s[4:6]}-{s[6:]}"].append(r)

    available = sorted(by_date)
    print(f"  조회 범위에 포함된 날짜: {', '.join(available)}")

    if ONLY and ONLY not in by_date:
        print(f"  [중단] {ONLY}은(는) 7일 창 안에 없다. 복구 불가.")
        sys.exit(1)

    targets = [ONLY] if ONLY else available
    for d in targets:
        save_raw_daily_csv(d, by_date[d])

    print("백필 완료.")


if __name__ == "__main__":
    main()
