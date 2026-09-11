"""
GAIS KBTI Pipeline — GitHub Actions 자동 실행용
매일 BigQuery GDELT 쿼리 → kbti_output.json 생성
"""
import json, os, math
from datetime import datetime
from collections import defaultdict
from google.cloud import bigquery

PROJECT_ID = os.environ.get("GCP_PROJECT", "ancient-voltage-508302-v3")

QUERY = """
SELECT
  CAST(SQLDATE AS STRING) AS date_str,
  Actor1CountryCode AS from_country,
  Actor2CountryCode AS to_country,
  COUNT(*) AS event_count,
  SUM(NumMentions) AS mentions,
  SUM(GoldsteinScale * NumMentions)
    / NULLIF(SUM(NumMentions), 0) * -1.0 AS kbti
FROM `gdelt-bq.gdeltv2.events`
WHERE
  SQLDATE >= CAST(FORMAT_DATE('%Y%m%d',
    DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)) AS INT64)
  AND (
    (Actor1CountryCode = 'KOR' AND Actor2CountryCode IN ('PRK','JPN','CHN','USA','RUS'))
    OR (Actor2CountryCode = 'KOR' AND Actor1CountryCode IN ('PRK','JPN','CHN','USA','RUS'))
  )
GROUP BY date_str, from_country, to_country
HAVING event_count >= 5
ORDER BY date_str ASC, from_country ASC
"""

NAMES = {"PRK":"한국-북한","JPN":"한국-일본","CHN":"한국-중국","USA":"한국-미국","RUS":"한국-러시아"}
PARTNERS = ["PRK","JPN","CHN","USA","RUS"]

def main():
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M')}] KBTI Pipeline 시작")
    client = bigquery.Client(project=PROJECT_ID)
    rows = list(client.query(QUERY).result())
    print(f"  쿼리 결과: {len(rows)}행")

    daily = defaultdict(lambda: defaultdict(lambda: {"m":0,"ws":0,"e":0}))
    for r in rows:
        ds = str(r["date_str"])
        date = f"{ds[:4]}-{ds[4:6]}-{ds[6:]}"
        a1, a2 = r["from_country"], r["to_country"]
        partner = a2 if a1=="KOR" else a1
        direction = "response" if a1=="KOR" else "threat"
        key = f"{partner}_{direction}"
        m = int(r["mentions"])
        k = float(r["kbti"]) if r["kbti"] is not None else 0
        daily[date][key]["m"] += m
        daily[date][key]["ws"] += k*m
        daily[date][key]["e"] += int(r["event_count"])

    dates = sorted(daily.keys())
    date_labels = [d[5:] for d in dates]  # MM-DD

    # 방향별 시계열
    dir_series = {}
    combined_series = {}
    current_dir = {}

    for p in PARTNERS:
        th_vals, re_vals, cb_vals = [], [], []
        for d in dates:
            th_dd = daily[d].get(f"{p}_threat", {})
            re_dd = daily[d].get(f"{p}_response", {})
            th = round(th_dd["ws"]/th_dd["m"],4) if th_dd.get("e",0)>=5 and th_dd.get("m",0)>0 else None
            re = round(re_dd["ws"]/re_dd["m"],4) if re_dd.get("e",0)>=5 and re_dd.get("m",0)>0 else None
            # combined for sparkline
            if th is not None and re is not None:
                cb = round((th+re)/2, 4)
            elif th is not None:
                cb = th
            elif re is not None:
                cb = re
            else:
                cb = None
            th_vals.append(th)
            re_vals.append(re)
            cb_vals.append(cb)

        dir_series[f"{p}_threat"]   = th_vals
        dir_series[f"{p}_response"] = re_vals
        combined_series[p] = cb_vals

        # 현재값 (최신 non-null)
        th_valid = [v for v in th_vals if v is not None]
        re_valid = [v for v in re_vals if v is not None]
        current_dir[p] = {
            "threat":   round(th_valid[-1], 4) if th_valid else 0.0,
            "response": round(re_valid[-1], 4) if re_valid else 0.0,
        }

    output = {
        "generated_at": datetime.now().isoformat(),
        "query_date":   datetime.now().strftime("%Y-%m-%d"),
        "total_rows":   len(rows),
        "dates":        date_labels,
        "full_dates":   dates,
        "series":       combined_series,   # 스파크라인용 (양방향 평균)
        "dir_series":   dir_series,         # 방향별 시계열
        "dir":          current_dir,        # 현재 방향별 값
        "partner_names": NAMES,
        "note": "KBTI = Goldstein(1992,JCR) x NumMentions weighted avg x (-1)"
    }

    with open("kbti_output.json","w",encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"  저장 완료: kbti_output.json ({len(dates)}일)")
    print(f"  현재 위협 지수 (상대→한국):")
    for p in PARTNERS:
        v = current_dir[p]["threat"]
        level = "위급" if v>2 else "높음" if v>1 else "고조" if v>0.5 else "보통" if v>-0.5 else "낮음"
        print(f"    {NAMES[p]}: {v:+.3f} [{level}]")

if __name__ == "__main__":
    main()
