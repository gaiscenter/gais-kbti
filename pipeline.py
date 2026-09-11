"""
GAIS KBTI Pipeline
매일 BigQuery에서 GDELT 데이터를 쿼리하여 kbti_output.json 생성
GitHub Actions에서 자동 실행
"""

import json
import os
import math
from datetime import datetime
from google.cloud import bigquery

PROJECT_ID = os.environ.get("GCP_PROJECT", "ancient-voltage-508302-v3")

QUERY = """
SELECT
  CAST(SQLDATE AS STRING)      AS date_str,
  Actor1CountryCode            AS from_country,
  Actor2CountryCode            AS to_country,
  COUNT(*)                     AS event_count,
  SUM(NumMentions)             AS mentions,
  SUM(GoldsteinScale * NumMentions)
    / NULLIF(SUM(NumMentions), 0) * -1.0  AS kbti
FROM `gdelt-bq.gdeltv2.events`
WHERE
  SQLDATE >= CAST(FORMAT_DATE('%Y%m%d',
    DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)) AS INT64)
  AND (
    (Actor1CountryCode = 'KOR'
     AND Actor2CountryCode IN ('PRK','JPN','CHN','USA','RUS'))
    OR
    (Actor2CountryCode = 'KOR'
     AND Actor1CountryCode IN ('PRK','JPN','CHN','USA','RUS'))
  )
GROUP BY date_str, from_country, to_country
HAVING event_count >= 5
ORDER BY date_str ASC, from_country ASC
"""

PARTNER_NAMES = {
    "PRK": "한국-북한", "JPN": "한국-일본",
    "CHN": "한국-중국", "USA": "한국-미국", "RUS": "한국-러시아"
}

def zscore(series):
    valid = [v for v in series if v is not None]
    if len(valid) < 5:
        return series
    mean = sum(valid) / len(valid)
    std = math.sqrt(sum((x - mean) ** 2 for x in valid) / len(valid))
    if std < 0.001:
        return [0.0 if v is not None else None for v in series]
    return [round((v - mean) / std, 4) if v is not None else None for v in series]

def main():
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M')}] KBTI Pipeline 시작")

    client = bigquery.Client(project=PROJECT_ID)
    rows = list(client.query(QUERY).result())
    print(f"  쿼리 결과: {len(rows)}행")

    # 날짜 변환
    from collections import defaultdict
    daily = defaultdict(lambda: defaultdict(lambda: {
        "mentions": 0, "weighted_sum": 0, "events": 0
    }))

    for r in rows:
        date_str = str(r["date_str"])
        date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
        a1, a2 = r["from_country"], r["to_country"]

        # 상대국 파악
        partner = a2 if a1 == "KOR" else a1
        direction = "response" if a1 == "KOR" else "threat"

        key = f"{partner}_{direction}"
        m = int(r["mentions"])
        k = float(r["kbti"]) if r["kbti"] is not None else 0

        daily[date][key]["mentions"] += m
        daily[date][key]["weighted_sum"] += k * m
        daily[date][key]["events"] += int(r["event_count"])

    dates = sorted(daily.keys())
    partners = ["PRK", "JPN", "CHN", "USA", "RUS"]

    # 시계열 구성
    series = {}
    for p in partners:
        for direction in ["threat", "response"]:
            key = f"{p}_{direction}"
            vals = []
            for d in dates:
                dd = daily[d].get(key, {})
                if dd.get("events", 0) >= 5 and dd.get("mentions", 0) > 0:
                    v = round(dd["weighted_sum"] / dd["mentions"], 4)
                else:
                    v = None
                vals.append(v)
            series[key] = vals

    # 현재값 (최신)
    current = {}
    for p in partners:
        for direction in ["threat", "response"]:
            key = f"{p}_{direction}"
            valid = [v for v in series[key] if v is not None]
            current[key] = round(valid[-1], 4) if valid else 0.0

    # 결과 저장
    output = {
        "generated_at": datetime.now().isoformat(),
        "query_date": datetime.now().strftime("%Y-%m-%d"),
        "total_rows": len(rows),
        "dates": [d[5:] for d in dates],  # MM-DD
        "full_dates": dates,
        "series": series,
        "current": current,
        "partner_names": PARTNER_NAMES,
        "note": "KBTI = Goldstein Scale × NumMentions 가중평균 × (-1) | Goldstein(1992,JCR)"
    }

    with open("kbti_output.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"  저장 완료: kbti_output.json")
    print(f"  날짜 범위: {dates[0]} ~ {dates[-1]} ({len(dates)}일)")
    print(f"  현재 위협 지수 (상대→한국):")
    for p in partners:
        v = current.get(f"{p}_threat", 0)
        level = "위급" if v>2 else "높음" if v>1 else "고조" if v>0.5 else "보통" if v>-0.5 else "낮음"
        print(f"    {PARTNER_NAMES[p]}: {v:+.3f} [{level}]")

if __name__ == "__main__":
    main()
