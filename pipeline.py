"""
GAIS KBTI Pipeline — GitHub Actions 자동 실행용
매일 BigQuery GDELT 쿼리 → kbti_output.json 생성

[수정 내역 2026-09-14]
- index.html이 기대하는 필드 형식과 맞지 않던 버그 수정
  - 이전: output["dir"]["PRK"]["threat"]   (중첩 구조)
  - 이후: output["current"]["PRK_threat"]  (평면 구조, index.html이 실제로 읽는 형식)
  - 이전: output["series"]["PRK"]          (방향 미구분, 평균값만)
  - 이후: output["series"]["PRK_threat"] / ["PRK_response"] (방향별 스파크라인 데이터)
- 기존의 dir / dir_series 필드는 하위 호환을 위해 그대로 남겨둠 (다른 곳에서 참조할 경우 대비)
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

    daily = defaultdict(lambda: defaultdict(lambda: {"m": 0, "ws": 0, "e": 0}))
    for r in rows:
        ds = str(r["date_str"])
        date = f"{ds[:4]}-{ds[4:6]}-{ds[6:]}"
        a1, a2 = r["from_country"], r["to_country"]
        partner = a2 if a1 == "KOR" else a1
        direction = "response" if a1 == "KOR" else "threat"
        key = f"{partner}_{direction}"
        m = int(r["mentions"])
        k = float(r["kbti"]) if r["kbti"] is not None else 0
        daily[date][key]["m"] += m
        daily[date][key]["ws"] += k * m
        daily[date][key]["e"] += int(r["event_count"])

    dates = sorted(daily.keys())
    date_labels = [d[5:] for d in dates]  # MM-DD

    series = {}        # index.html이 읽는 평면 시계열: "PRK_threat", "PRK_response", ...
    dir_series = {}     # 하위 호환용 (기존과 동일한 값)
    combined_series = {}  # 하위 호환용 (양방향 평균)
    current = {}        # index.html이 읽는 평면 현재값: "PRK_threat", "PRK_response", ...
    current_dir = {}    # 하위 호환용 (중첩 구조)

    for p in PARTNERS:
        th_vals, re_vals, cb_vals = [], [], []
        for d in dates:
            th_dd = daily[d].get(f"{p}_threat", {})
            re_dd = daily[d].get(f"{p}_response", {})
            th = round(th_dd["ws"] / th_dd["m"], 4) if th_dd.get("e", 0) >= 5 and th_dd.get("m", 0) > 0 else None
            re = round(re_dd["ws"] / re_dd["m"], 4) if re_dd.get("e", 0) >= 5 and re_dd.get("m", 0) > 0 else None
            if th is not None and re is not None:
                cb = round((th + re) / 2, 4)
            elif th is not None:
                cb = th
            elif re is not None:
                cb = re
            else:
                cb = None
            th_vals.append(th)
            re_vals.append(re)
            cb_vals.append(cb)

        series[f"{p}_threat"] = th_vals
        series[f"{p}_response"] = re_vals
        dir_series[f"{p}_threat"] = th_vals
        dir_series[f"{p}_response"] = re_vals
        combined_series[p] = cb_vals

        th_valid = [v for v in th_vals if v is not None]
        re_valid = [v for v in re_vals if v is not None]
        cur_threat = round(th_valid[-1], 4) if th_valid else 0.0
        cur_response = round(re_valid[-1], 4) if re_valid else 0.0

        current[f"{p}_threat"] = cur_threat
        current[f"{p}_response"] = cur_response
        current_dir[p] = {"threat": cur_threat, "response": cur_response}

    output = {
        "generated_at": datetime.now().isoformat(),
        "query_date": datetime.now().strftime("%Y-%m-%d"),
        "total_rows": len(rows),
        "dates": date_labels,
        "full_dates": dates,
        "series": series,              # index.html이 실제로 읽는 필드 (방향별 접미사 포함)
        "current": current,            # index.html이 실제로 읽는 필드 (방향별 접미사 포함)
        "dir_series": dir_series,      # 하위 호환용, series와 동일
        "dir": current_dir,            # 하위 호환용, 중첩 구조
        "combined_series": combined_series,  # 하위 호환용, 양방향 평균 (구버전 스파크라인용)
        "partner_names": NAMES,
        "note": "KBTI = Goldstein(1992,JCR) x NumMentions weighted avg x (-1)"
    }

    with open("kbti_output.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"  저장 완료: kbti_output.json ({len(dates)}일)")
    print(f"  현재 위협 지수 (상대→한국):")
    for p in PARTNERS:
        v = current_dir[p]["threat"]
        level = "위급" if v > 2 else "높음" if v > 1 else "고조" if v > 0.5 else "보통" if v > -0.5 else "낮음"
        print(f"    {NAMES[p]}: {v:+.3f} [{level}]")


if __name__ == "__main__":
    main()
