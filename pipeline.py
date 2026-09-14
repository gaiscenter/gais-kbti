"""
GAIS KBTI Pipeline — GitHub Actions 자동 실행용
매일 BigQuery GDELT 쿼리 → kbti_output.json 생성

[2026-09-14 업데이트]
- 도메인 3종 추가: 전체(overall, 기존과 동일) / 군사(military) / 공급망·경제(supply)
  → events 테이블 한 번의 쿼리에서 CASE 분기로 동시 계산 (비용 거의 증가 없음)
- 사이버(cyber) 도메인 추가: GKG 테이블 별도 쿼리, CYBER_ATTACK 테마 기반
  → GKG는 Actor1/Actor2 구조가 없어 Threat/Response 방향 구분 불가, 단일 긴장도 지수만 제공
- 정체성(identity) 도메인은 보류 (GDELT에 정확히 대응하는 테마가 없어 신호 품질 낮음)
"""
import json, os
from datetime import datetime
from collections import defaultdict
from google.cloud import bigquery

PROJECT_ID = os.environ.get("GCP_PROJECT", "ancient-voltage-508302-v3")

# ── 1) events 테이블: 전체 / 군사 / 공급망 동시 계산 ──────────────────────
EVENTS_QUERY = """
WITH tagged AS (
  SELECT
    CAST(SQLDATE AS STRING) AS date_str,
    Actor1CountryCode AS from_country,
    Actor2CountryCode AS to_country,
    GoldsteinScale,
    NumMentions,
    -- 군사: '충돌 이벤트 코드'가 아니라 '군(MIL) 소속 행위자가 등장하는 모든 이벤트'로 정의.
    -- 이벤트 코드 기준(QuadClass=4 등)으로 걸렀을 때는 애초에 그 코드들 자체가
    -- Goldstein 값이 대부분 매우 부정적으로 설계되어 있어, 협력 코드를 조금 섞어도
    -- 거의 항상 극단값(모든 상대국 +10 근처)으로 쏠리는 구조적 편향이 있었음.
    -- 행위자 유형(Actor Type) 기준으로 바꾸면 군 관련 발언·협력·외교까지 전체 스펙트럼이
    -- 반영되어 Overall처럼 자연스럽게 오르내리는 지수가 됨.
    (Actor1Type1Code = 'MIL' OR Actor1Type2Code = 'MIL' OR Actor1Type3Code = 'MIL'
     OR Actor2Type1Code = 'MIL' OR Actor2Type2Code = 'MIL' OR Actor2Type3Code = 'MIL') AS is_military,
    (EventCode IN ('163','1621','061','071')) AS is_supply
  -- events_partitioned + _PARTITIONTIME 필터 사용 (기존 events 테이블은 파티션이 없어
  -- SQLDATE로 필터링해도 전체 이력을 다 스캔함 -> 무료 쿼터를 급속히 소진시킨 원인이었음)
  FROM `gdelt-bq.gdeltv2.events_partitioned`
  WHERE
    _PARTITIONTIME >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY))
    AND (
      (Actor1CountryCode = 'KOR' AND Actor2CountryCode IN ('PRK','JPN','CHN','USA','RUS'))
      OR (Actor2CountryCode = 'KOR' AND Actor1CountryCode IN ('PRK','JPN','CHN','USA','RUS'))
    )
)
SELECT
  date_str, from_country, to_country,
  COUNT(*) AS e_all,
  SUM(NumMentions) AS m_all,
  SUM(GoldsteinScale * NumMentions) AS ws_all,
  COUNTIF(is_military) AS e_mil,
  SUM(IF(is_military, NumMentions, 0)) AS m_mil,
  SUM(IF(is_military, GoldsteinScale * NumMentions, 0)) AS ws_mil,
  COUNTIF(is_supply) AS e_sup,
  SUM(IF(is_supply, NumMentions, 0)) AS m_sup,
  SUM(IF(is_supply, GoldsteinScale * NumMentions, 0)) AS ws_sup
FROM tagged
GROUP BY date_str, from_country, to_country
HAVING e_all >= 1
ORDER BY date_str ASC, from_country ASC
"""

# ── 2) GKG: 사이버 (CYBER_ATTACK 테마, 방향 구분 없음) ────────────────────
GKG_QUERY = """
SELECT
  DATE,
  V2Locations,
  V2Tone
FROM `gdelt-bq.gdeltv2.gkg_partitioned`
WHERE
  _PARTITIONTIME >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY))
  AND V2Themes LIKE '%CYBER_ATTACK%'
  AND (V2Locations LIKE '%South Korea%' OR V2Locations LIKE '%Korea, South%')
"""

PARTNER_NAME_MATCH = {
    "PRK": ["North Korea", "Korea, North"],
    "JPN": ["Japan"],
    "CHN": ["China"],
    "USA": ["United States"],
    "RUS": ["Russia"],
}

NAMES = {"PRK": "한국-북한", "JPN": "한국-일본", "CHN": "한국-중국", "USA": "한국-미국", "RUS": "한국-러시아"}
PARTNERS = ["PRK", "JPN", "CHN", "USA", "RUS"]
# 하드 컷오프(기준 미달이면 null) 대신 신뢰도 가중치(shrinkage) 방식을 전 도메인에 공통 적용.
# 신뢰도 = e / (e + CONFIDENCE_K) ,  표시값 = 원래 KBTI × 신뢰도
# 이벤트가 1건이라도 있으면 값을 보여주되, 표본이 적을수록 0에 가깝게 완화된다.
# (예: CONFIDENCE_K=5일 때 1건짜리 극단 기사는 신뢰도 1/6로 크게 완화되고,
#  20건이 쌓이면 신뢰도 0.8로 원래 값에 근접한다.)
CONFIDENCE_K = 5


def _shrink(raw_value, event_count, k=CONFIDENCE_K):
    """이벤트 수가 적을수록 0에 가깝게 완화된 값을 반환. 이벤트가 0건이면 None."""
    if event_count <= 0:
        return None
    confidence = event_count / (event_count + k)
    return round(raw_value * confidence, 4)


def build_events_domains(rows):
    """events 쿼리 결과 -> 전체/군사/공급망 3개 도메인의 current/series 구조로 변환"""
    daily = defaultdict(lambda: defaultdict(lambda: {
        "all": {"m": 0, "ws": 0, "e": 0},
        "mil": {"m": 0, "ws": 0, "e": 0},
        "sup": {"m": 0, "ws": 0, "e": 0},
    }))
    for r in rows:
        ds = str(r["date_str"])
        date = f"{ds[:4]}-{ds[4:6]}-{ds[6:]}"
        a1, a2 = r["from_country"], r["to_country"]
        partner = a2 if a1 == "KOR" else a1
        direction = "response" if a1 == "KOR" else "threat"
        key = f"{partner}_{direction}"

        d = daily[date][key]
        d["all"]["m"] += int(r["m_all"] or 0)
        d["all"]["ws"] += float(r["ws_all"] or 0)
        d["all"]["e"] += int(r["e_all"] or 0)
        d["mil"]["m"] += int(r["m_mil"] or 0)
        d["mil"]["ws"] += float(r["ws_mil"] or 0)
        d["mil"]["e"] += int(r["e_mil"] or 0)
        d["sup"]["m"] += int(r["m_sup"] or 0)
        d["sup"]["ws"] += float(r["ws_sup"] or 0)
        d["sup"]["e"] += int(r["e_sup"] or 0)

    dates = sorted(daily.keys())

    def domain_block(dom_key, window_days=1):
        """window_days>1이면 그날 포함 최근 N일을 누적해서 집계 (희소 도메인의 결측 완화용)"""
        series, current = {}, {}
        for p in PARTNERS:
            th_vals, re_vals = [], []
            for i, d in enumerate(dates):
                lo = max(0, i - window_days + 1)
                window_dates = dates[lo:i + 1]
                th_m = th_ws = th_e = 0
                re_m = re_ws = re_e = 0
                for wd in window_dates:
                    th = daily[wd].get(f"{p}_threat", {}).get(dom_key, {"m": 0, "ws": 0, "e": 0})
                    re = daily[wd].get(f"{p}_response", {}).get(dom_key, {"m": 0, "ws": 0, "e": 0})
                    th_m += th["m"]; th_ws += th["ws"]; th_e += th["e"]
                    re_m += re["m"]; re_ws += re["ws"]; re_e += re["e"]
                th_raw = (th_ws / th_m * -1.0) if th_m > 0 else 0.0
                re_raw = (re_ws / re_m * -1.0) if re_m > 0 else 0.0
                th_vals.append(_shrink(th_raw, th_e))
                re_vals.append(_shrink(re_raw, re_e))
            series[f"{p}_threat"] = th_vals
            series[f"{p}_response"] = re_vals
            th_valid = [v for v in th_vals if v is not None]
            re_valid = [v for v in re_vals if v is not None]
            current[f"{p}_threat"] = round(th_valid[-1], 4) if th_valid else 0.0
            current[f"{p}_response"] = round(re_valid[-1], 4) if re_valid else 0.0
        return series, current

    out = {}
    # 전체는 표본이 충분해 일단위(1일) 유지, 군사/공급망은 희소하므로 7일 이동창 적용
    windows = {"all": 1, "mil": 7, "sup": 7}
    for dom_key, dom_name in [("all", "overall"), ("mil", "military"), ("sup", "supply")]:
        series, current = domain_block(dom_key, windows[dom_key])
        out[dom_name] = {"series": series, "current": current}
    return dates, out


def build_cyber_domain(rows, dates):
    """GKG 쿼리 결과 -> 사이버 도메인 (방향 없음, 국가쌍당 단일 지수)"""
    daily = defaultdict(lambda: defaultdict(lambda: {"tones": [], "n": 0}))
    for r in rows:
        ds = str(r["DATE"])[:8]
        date = f"{ds[:4]}-{ds[4:6]}-{ds[6:]}"
        locs = r["V2Locations"] or ""
        tone_field = r["V2Tone"] or ""
        try:
            tone = float(tone_field.split(",")[0])
        except (ValueError, IndexError):
            continue
        for p, names in PARTNER_NAME_MATCH.items():
            if any(n in locs for n in names):
                daily[date][p]["tones"].append(tone)
                daily[date][p]["n"] += 1

    series, current = {}, {}
    CYBER_WINDOW_DAYS = 7
    for p in PARTNERS:
        vals = []
        for i, d in enumerate(dates):
            lo = max(0, i - CYBER_WINDOW_DAYS + 1)
            window_dates = dates[lo:i + 1]
            tones, n = [], 0
            for wd in window_dates:
                bucket = daily.get(wd, {}).get(p, {"tones": [], "n": 0})
                tones.extend(bucket["tones"])
                n += bucket["n"]
            if n > 0:
                avg_tone = sum(tones) / len(tones)
                raw = avg_tone * -1.0 / 10.0  # 이벤트 지수와 스케일 맞추기 위해 /10
                vals.append(_shrink(raw, n))
            else:
                vals.append(None)
        series[p] = vals
        valid = [v for v in vals if v is not None]
        current[p] = round(valid[-1], 4) if valid else 0.0

    return {"series": series, "current": current}


def compute_events_stats(rows):
    """events 쿼리 원시 결과에서 방법론 기술용 통계치 계산 (논문 인용 가능한 수치)"""
    total_e_all = total_m_all = 0
    total_e_mil = total_m_mil = 0
    total_e_sup = total_m_sup = 0
    by_partner = {p: {"events_all": 0, "events_military": 0, "events_supply": 0} for p in PARTNERS}

    for r in rows:
        a1, a2 = r["from_country"], r["to_country"]
        partner = a2 if a1 == "KOR" else a1
        e_all = int(r["e_all"] or 0)
        e_mil = int(r["e_mil"] or 0)
        e_sup = int(r["e_sup"] or 0)

        total_e_all += e_all
        total_m_all += int(r["m_all"] or 0)
        total_e_mil += e_mil
        total_m_mil += int(r["m_mil"] or 0)
        total_e_sup += e_sup
        total_m_sup += int(r["m_sup"] or 0)

        if partner in by_partner:
            by_partner[partner]["events_all"] += e_all
            by_partner[partner]["events_military"] += e_mil
            by_partner[partner]["events_supply"] += e_sup

    return {
        "total_events_matched": total_e_all,
        "total_mentions_matched": total_m_all,
        "military_events": total_e_mil,
        "military_mentions": total_m_mil,
        "supply_events": total_e_sup,
        "supply_mentions": total_m_sup,
        "by_partner": by_partner,
    }


def compute_cyber_stats(rows):
    """GKG 쿼리 원시 결과에서 사이버 도메인 통계치 계산"""
    by_partner = {p: 0 for p in PARTNERS}
    for r in rows:
        locs = r["V2Locations"] or ""
        for p, names in PARTNER_NAME_MATCH.items():
            if any(n in locs for n in names):
                by_partner[p] += 1
    return {
        "total_documents_matched": len(rows),
        "by_partner": by_partner,
    }


# 안전장치: 매일 자동 실행되는 쿼리이므로, 실행 전 dry-run으로 예상 처리량을 먼저 확인하고
# 비정상적으로 커지면(=코드 실수 등) 자동 중단한다. 사람이 매번 비용을 신경 쓰지 않아도
# 시스템이 스스로 지키도록 하기 위함. (2026-09-14: 파티션 안 된 테이블을 잘못 참조해
# 무료 쿼터를 소진시킨 사고 이후 추가)
QUERY_COST_GUARD_GB = float(os.environ.get("QUERY_COST_GUARD_GB", "20"))


def _dry_run_check(client, query, label):
    """쿼리를 실제 실행하기 전, 예상 처리량이 기준치를 넘으면 예외를 발생시켜 중단한다."""
    dry_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    dry_job = client.query(query, job_config=dry_config)
    gb = dry_job.total_bytes_processed / 1e9
    print(f"  [dry-run] {label}: 예상 처리량 {gb:.3f} GB")
    if gb > QUERY_COST_GUARD_GB:
        raise SystemExit(
            f"중단: {label} 예상 처리량 {gb:.2f}GB가 안전 한도 {QUERY_COST_GUARD_GB}GB를 초과했습니다. "
            f"쿼리가 실수로 파티션 안 된 테이블을 참조하고 있지는 않은지 확인하세요."
        )
    return gb


def main():
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M')}] KBTI Pipeline 시작")
    client = bigquery.Client(project=PROJECT_ID)

    _dry_run_check(client, EVENTS_QUERY, "events 쿼리")
    events_rows = list(client.query(EVENTS_QUERY).result())
    print(f"  events 쿼리 결과: {len(events_rows)}행")
    dates, domains = build_events_domains(events_rows)
    date_labels = [d[5:] for d in dates]
    events_stats = compute_events_stats(events_rows)

    _dry_run_check(client, GKG_QUERY, "GKG(사이버) 쿼리")
    gkg_rows = list(client.query(GKG_QUERY).result())
    print(f"  GKG(사이버) 쿼리 결과: {len(gkg_rows)}행")
    cyber = build_cyber_domain(gkg_rows, dates)
    cyber_stats = compute_cyber_stats(gkg_rows)

    stats = {
        "window_days": 30,
        "run_date": datetime.now().strftime("%Y-%m-%d"),
        "events": events_stats,
        "gkg_cyber": cyber_stats,
    }

    output = {
        "generated_at": datetime.now().isoformat(),
        "query_date": datetime.now().strftime("%Y-%m-%d"),
        "total_rows": len(events_rows),
        "dates": date_labels,
        "full_dates": dates,
        # 기존 필드 (하위 호환 — 전체 도메인과 동일한 값)
        "series": domains["overall"]["series"],
        "current": domains["overall"]["current"],
        # 도메인별 구조 (신규)
        "domains": {
            "overall": domains["overall"],
            "military": domains["military"],
            "supply": domains["supply"],
            "cyber": cyber,   # 방향 없음: series/current가 국가코드로 바로 매핑됨 (예: cyber.current.PRK)
        },
        # 방법론 기술용 통계치 — 논문에 "N건 검색, M건 유효매칭" 형태로 인용 가능
        "stats": stats,
        "partner_names": NAMES,
        "note": "KBTI = Goldstein(1992,JCR) x NumMentions weighted avg x (-1); cyber = GKG V2Tone-based proxy, no directionality"
    }

    with open("kbti_output.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"  저장 완료: kbti_output.json ({len(dates)}일)")
    print(f"  [통계] 30일 윈도우 기준")
    print(f"    전체 매칭 이벤트: {events_stats['total_events_matched']}건 (기사 언급 {events_stats['total_mentions_matched']}건)")
    print(f"    └ 군사 태깅: {events_stats['military_events']}건 / 공급망 태깅: {events_stats['supply_events']}건")
    print(f"    GKG 사이버 매칭 문서: {cyber_stats['total_documents_matched']}건")
    print(f"  현재 위협 지수 (전체, 상대→한국):")
    for p in PARTNERS:
        v = domains["overall"]["current"][f"{p}_threat"]
        level = "위급" if v > 2 else "높음" if v > 1 else "고조" if v > 0.5 else "보통" if v > -0.5 else "낮음"
        print(f"    {NAMES[p]}: {v:+.3f} [{level}]")


if __name__ == "__main__":
    main()
