"""
GAIS KBTI Pipeline — GitHub Actions 자동 실행용
매일 BigQuery GDELT 쿼리 → kbti_output.json 생성

[2026-09-14 업데이트]
- 도메인 3종 추가: 전체(overall, 기존과 동일) / 군사(military) / 공급망·경제(supply)
  → events 테이블 한 번의 쿼리에서 CASE 분기로 동시 계산 (비용 거의 증가 없음)
- 사이버(cyber) 도메인 추가: GKG 테이블 별도 쿼리, CYBER_ATTACK 테마 기반
  → GKG는 Actor1/Actor2 구조가 없어 Threat/Response 방향 구분 불가, 단일 긴장도 지수만 제공
- 정체성(identity) 도메인은 보류 (GDELT에 정확히 대응하는 테마가 없어 신호 품질 낮음)

[2026-09-15 업데이트 — LLM 감사(audit) 계층 추가]
- 실측 결과, 하루 지수를 흔드는 건 국가쌍당 최상위 |영향력| 소수(1~5건)의 이벤트였음
  (예: 북한 미사일 발사가 한일로 오분류, 완전히 긍정적인 협력기사가 EventCode 193으로 오분류)
- 이를 해결하기 위해 "이중 계층" 구조 도입:
  1) 기존 전체 이벤트 → 자동 집계(그대로 유지, 대량 처리)
  2) 국가쌍·방향·도메인별 그날 최상위 영향력 이벤트만 골라 원문 fetch 후
     Claude(Haiku)로 "실제로 두 국가 간 상호작용이 맞는지" 검증 → 부적합 판정 시 제외 후 재계산
- ANTHROPIC_API_KEY 환경변수가 없으면 감사 단계는 건너뛰고 기존 방식대로 동작 (하위 호환)
"""
import json, os, re, time
import concurrent.futures
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
    -- COALESCE로 각 컬럼의 NULL을 ''로 치환 -> 행위자 유형 코드가 NULL인 행에서
    -- "NULL OR FALSE = NULL"이 되어 is_military/is_diplomatic 자체가 NULL(빈 값)로
    -- 새는 것을 방지 (전부 NULL이면 False가 나와야 정상).
    (COALESCE(Actor1Type1Code,'') = 'MIL' OR COALESCE(Actor1Type2Code,'') = 'MIL' OR COALESCE(Actor1Type3Code,'') = 'MIL'
     OR COALESCE(Actor2Type1Code,'') = 'MIL' OR COALESCE(Actor2Type2Code,'') = 'MIL' OR COALESCE(Actor2Type3Code,'') = 'MIL') AS is_military,
    (EventCode IN ('163','1621','061','071')) AS is_supply,
    -- 외교: 군사와 같은 논리 — 이벤트 코드(협력코드만)로 정의하면 구조적으로 항상 긍정 쪽으로
    -- 쏠리는 반대 방향의 편향이 생김. 대신 '정부/외교 행위자(GOV)가 등장하는 모든 이벤트'로
    -- 정의해서, 그 채널이 그날 우호적이었는지 냉각됐는지를 있는 그대로 반영하게 함.
    (COALESCE(Actor1Type1Code,'') = 'GOV' OR COALESCE(Actor1Type2Code,'') = 'GOV' OR COALESCE(Actor1Type3Code,'') = 'GOV'
     OR COALESCE(Actor2Type1Code,'') = 'GOV' OR COALESCE(Actor2Type2Code,'') = 'GOV' OR COALESCE(Actor2Type3Code,'') = 'GOV') AS is_diplomatic
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
  SUM(IF(is_supply, GoldsteinScale * NumMentions, 0)) AS ws_sup,
  COUNTIF(is_diplomatic) AS e_dip,
  SUM(IF(is_diplomatic, NumMentions, 0)) AS m_dip,
  SUM(IF(is_diplomatic, GoldsteinScale * NumMentions, 0)) AS ws_dip
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
RECENCY_DECAY = 0.5   # 기본값(군사·외교): 하루 지날 때마다 영향력이 절반으로 줄어듦
                      # -> 메인 30일 시계열과 감사 보정 계산 둘 다 반드시 이 값을 공유해야
                      # 감사가 보정을 적용하는 순간 감쇠가 무시되는 일이 없음.
# 공급망은 표본이 워낙 적어(하루 1~2건도 흔함), 강한 감쇠를 걸면 이벤트 하나 터질 때마다
# 확 튀었다가 급격히 꺼지는 "톱니" 모양이 됨 -> 원래 7일 이동창을 도입한 취지(매끄럽게 하기)와
# 충돌. 그래서 공급망만 감쇠를 약하게(=이동창 성격을 유지) 별도 지정.
DOMAIN_DECAY = {"mil": 0.5, "dip": 0.5, "sup": 0.8}


def _decay_rate(dom_key):
    return DOMAIN_DECAY.get(dom_key, RECENCY_DECAY)


def _shrink(raw_value, event_count, k=CONFIDENCE_K):
    """이벤트 수가 적을수록 0에 가깝게 완화된 값을 반환. 이벤트가 0건이면 None."""
    if event_count <= 0:
        return None
    confidence = event_count / (event_count + k)
    return round(raw_value * confidence, 4)


# ══════════════════════════════════════════════════════════════════════════
# 상대(Z-score) 기반 경보 수준 — 고정 절대 임계값(v>2 등) 대신 PizzINT 방식으로
# "이 국가쌍 자신의 최근 히스토리 대비 지금이 통계적으로 얼마나 이례적인가"를 판단.
#
# 왜 고정 임계값을 버리는가:
#   - 기존의 v>2="위급" 같은 절대 컷오프는 Goldstein(1992) 원 논문에도, Pizza Index
#     방법론에도 근거가 없는 임의의 숫자였다 (Goldstein 스케일은 -10~+10 범위를 정의할
#     뿐 "2 이상이면 위급"이라는 해석 기준을 제시하지 않음).
#   - KBTI 값은 국가쌍마다, 도메인마다 평상시 변동 폭(분산)이 크게 다르다.
#     예를 들어 한국-일본 관계는 평소에도 사건이 잦아 변동성이 크고, 한국-러시아는
#     사건 자체가 드물어 평소엔 0 근처에 머물다가도 사건 하나로 크게 튄다.
#     동일한 "2"라는 절대값이 전자에선 평범한 하루일 수 있고 후자에선 극단적 이례치일
#     수 있는데, 고정 임계값은 이 차이를 무시한다.
#   - PizzINT 방법론의 핵심 아이디어는 "각 대상 자신의 과거 분포 대비 Z-score"로
#     이례성을 판단하는 것이다. 아래 _relative_level()이 이를 KBTI에 맞게 구현한다:
#     같은 국가쌍·같은 방향·같은 도메인의 최근 시계열(이미 계산되어 있는 30일 series)을
#     "정상 분포"로 삼아, 오늘 값이 그 분포에서 표준편차 몇 개만큼 벗어났는지로 판정한다.
def _relative_level(value, history, floor_std=0.6, min_abs_for_alert=2.0):
    """
    value: 오늘의 KBTI 값
    history: 같은 필드(국가쌍_방향)의 최근 시계열 (오늘 제외, None 포함 가능)
    floor_std: 표준편차 하한. 평소 변동이 거의 없는(=매우 조용한) 국가쌍이 아주 작은
               흔들림만으로도 "위급"으로 튀는 것을 막기 위한 안전장치.
    min_abs_for_alert: |value|가 이 값 미만이면, 설령 그 국가쌍 기준으론 통계적으로
               이례적이더라도 경보를 걸지 않는다(통계적 이례성과 실질적 위협 크기를 모두
               요구). 2.0으로 잡은 근거는 임의의 숫자가 아니라 Goldstein(1992) 논문
               Table 1 자체다 — 이 논문의 원 가중치 분포에서 "give warning"(-3.0)보다
               약한 사건들, 즉 "comment on situation"(-0.2)~"apologize"(1.8) 구간이
               실질적으로 Goldstein 패널이 "중립적 언급" 수준으로 매긴 값들이다. 이 구간
               (대략 -2~+2)에 머무는 값은, 그 국가쌍 자신의 과거 대비 아무리 통계적으로
               튀어도(z-score) 여전히 논문 원문 기준 "중립"이므로 alert을 걸지 않는다.
               (이전 버전은 0.3이었는데, 표본이 희소해 평소 변동폭 자체가 작은 국가쌍·
               도메인(예: 공급망)에서 floor_std가 자주 걸리면서, Goldstein 척도로는
               여전히 중립 수준인 -1 안팎의 값도 z>2로 튀어 "위급"이 되는 과잉경보가
               실제로 발생했다 — 이번에 실데이터로 확인.)
    반환: "crit"/"high"/"elev"/"mod"/"low" 중 하나, 판단 불가 시 None(표본 부족).
    """
    if value is None:
        return None
    valid = [x for x in history if x is not None]
    if len(valid) < 5:
        return None  # 자기 자신의 과거 분포가 아직 충분히 쌓이지 않음 -> 상대평가 보류
    mean = sum(valid) / len(valid)
    var = sum((x - mean) ** 2 for x in valid) / len(valid)
    std = max(var ** 0.5, floor_std)
    z = (value - mean) / std
    if abs(value) < min_abs_for_alert:
        z = 0.0
    if z > 2:
        return "crit"
    if z > 1:
        return "high"
    if z > 0.5:
        return "elev"
    if z > -0.5:
        return "mod"
    return "low"


LEVEL_LABEL_KR = {"crit": "위급", "high": "높음", "elev": "고조", "mod": "보통", "low": "낮음", None: "판단보류"}


def build_events_domains(rows):
    """events 쿼리 결과 -> 전체/군사/공급망/외교 4개 도메인의 current/series 구조로 변환"""
    daily = defaultdict(lambda: defaultdict(lambda: {
        "all": {"m": 0, "ws": 0, "e": 0},
        "mil": {"m": 0, "ws": 0, "e": 0},
        "sup": {"m": 0, "ws": 0, "e": 0},
        "dip": {"m": 0, "ws": 0, "e": 0},
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
        d["dip"]["m"] += int(r["m_dip"] or 0)
        d["dip"]["ws"] += float(r["ws_dip"] or 0)
        d["dip"]["e"] += int(r["e_dip"] or 0)

    dates = sorted(daily.keys())

    # 당일 유효 이벤트 수가 이 값 이상이면 창을 펼치지 않고 당일 것만 쓴다.
    # 0 으로 두면 패치 전 동작(항상 창)으로 돌아간다.
    WINDOW_SAME_DAY_MIN = 1

    def _agg(window_dates, field_key, dom_key, decay_rate):
        """창 안의 (언급합, 가중합, 유효건수)를 감쇠 가중으로 모은다."""
        m = ws = e = 0.0
        n = len(window_dates)
        for wd_idx, wd in enumerate(window_dates):
            days_ago = (n - 1) - wd_idx     # 0 = 그날(가장 최근)
            decay = (decay_rate ** days_ago) if n > 1 else 1.0
            c = daily[wd].get(field_key, {}).get(dom_key, {"m": 0, "ws": 0, "e": 0})
            m += c["m"] * decay
            ws += c["ws"] * decay
            e += c["e"] * decay
        return m, ws, e

    def domain_block(dom_key, window_days=1):
        """
        조건부 창 — 당일에 증거가 있으면 당일 것만 쓰고, 없을 때만 최근 N일을
        감쇠 가중으로 누적한다.

        창은 희소 채널이 비는 것을 막으려고 넣은 장치다. 당일 증거가 있는 날까지
        창을 펼치면, 전날 레코드가 절반 가중으로 다시 더해지면서 한 사건이
        여러 날에 걸쳐 중복 계산되고 정점이 사건 다음날로 밀린다. 그래서 창은
        '증거가 없을 때만' 쓴다.

        증거가 전혀 없는 날은 0.0 을 넣어 계열이 끊기지 않게 하되, measured 를
        False 로 표시해 z-score 기준선에서는 빼도록 한다.
        """
        series, current, measured = {}, {}, {}
        decay_rate = _decay_rate(dom_key)
        for p in PARTNERS:
            for direction in ("threat", "response"):
                field = f"{p}_{direction}"
                vals, flags = [], []
                for i, d in enumerate(dates):
                    # 1단계: 당일만
                    m, ws, e = _agg(dates[i:i + 1], field, dom_key, decay_rate)
                    # 2단계: 당일 증거가 부족하면 창을 펼친다
                    if e < WINDOW_SAME_DAY_MIN and window_days > 1:
                        lo = max(0, i - window_days + 1)
                        m, ws, e = _agg(dates[lo:i + 1], field, dom_key, decay_rate)
                    raw = (ws / m * -1.0) if m > 0 else 0.0
                    v = _shrink(raw, e)          # 이벤트 0건이면 None
                    flags.append(v is not None)
                    vals.append(v if v is not None else 0.0)
                series[field] = vals
                measured[field] = flags
                # 0 채움 덕분에 vals 에 None 이 없으므로 current 는 항상 '오늘' 값이다.
                # (패치 전에는 마지막 non-None 값이 올라와, 오늘이 비면 며칠 전
                #  값이 조용히 표시되는 숨은 이월이 있었다.)
                current[field] = round(vals[-1], 4) if vals else 0.0
        return series, current, measured

    def relative_levels(series, current, measured):
        """
        각 필드마다 자기 시계열을 히스토리로 삼아 상대(Z-score) 경보 수준을 계산.
        0 채움한 날(measured=False)은 '측정이 없었다'는 뜻이므로 기준선에서 뺀다.
        빼지 않으면 조용한 채널일수록 0 이 쌓여 표준편차가 무너지고, 작은 움직임도
        z 가 과도하게 커져 과잉경보가 난다.
        """
        levels = {}
        for field, cur_val in current.items():
            hist = series.get(field, [])
            flag = measured.get(field, [])
            hist_m = [v for v, ok in zip(hist, flag) if ok]
            hist_excl_today = hist_m[:-1] if len(hist_m) > 1 else hist_m
            # 오늘이 측정되지 않은 날이면 경보를 매기지 않는다 — 경보는 오늘의 증거를 요구한다.
            today_ok = flag[-1] if flag else False
            levels[field] = _relative_level(cur_val, hist_excl_today) if today_ok else None
        return levels

    out = {}
    # 전체는 표본이 충분해 일단위, 군사/공급망/외교는 희소하므로 7일 창을 '예비'로 둔다.
    # (조건부 — 당일 증거가 있으면 창은 펼쳐지지 않는다. domain_block 주석 참조)
    windows = {"all": 1, "mil": 7, "sup": 7, "dip": 7}
    for dom_key, dom_name in [("all", "overall"), ("mil", "military"), ("sup", "supply"), ("dip", "diplomatic")]:
        series, current, measured = domain_block(dom_key, windows[dom_key])
        level = relative_levels(series, current, measured)
        out[dom_name] = {"series": series, "current": current, "level": level,
                         "measured": measured}
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
    total_e_dip = total_m_dip = 0
    by_partner = {p: {"events_all": 0, "events_military": 0, "events_supply": 0, "events_diplomatic": 0} for p in PARTNERS}

    for r in rows:
        a1, a2 = r["from_country"], r["to_country"]
        partner = a2 if a1 == "KOR" else a1
        e_all = int(r["e_all"] or 0)
        e_mil = int(r["e_mil"] or 0)
        e_sup = int(r["e_sup"] or 0)
        e_dip = int(r["e_dip"] or 0)

        total_e_all += e_all
        total_m_all += int(r["m_all"] or 0)
        total_e_mil += e_mil
        total_m_mil += int(r["m_mil"] or 0)
        total_e_sup += e_sup
        total_m_sup += int(r["m_sup"] or 0)
        total_e_dip += e_dip
        total_m_dip += int(r["m_dip"] or 0)

        if partner in by_partner:
            by_partner[partner]["events_all"] += e_all
            by_partner[partner]["events_military"] += e_mil
            by_partner[partner]["events_supply"] += e_sup
            by_partner[partner]["events_diplomatic"] += e_dip

    return {
        "total_events_matched": total_e_all,
        "total_mentions_matched": total_m_all,
        "military_events": total_e_mil,
        "military_mentions": total_m_mil,
        "supply_events": total_e_sup,
        "supply_mentions": total_m_sup,
        "diplomatic_events": total_e_dip,
        "diplomatic_mentions": total_m_dip,
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


# ══════════════════════════════════════════════════════════════════════════
# LLM 감사(audit) 계층 — 국가쌍별 그날 최상위 |영향력| 이벤트만 원문 검증
# ══════════════════════════════════════════════════════════════════════════

RAW_AUDIT_QUERY = """
SELECT
  CAST(SQLDATE AS STRING) AS date_str,
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
  -- 군사/외교/공급망 도메인이 7일 이동창으로 집계되므로, 감사 대상도 1일이 아니라 7일을 봐야
  -- "오늘"보다 며칠 전에 나온 오염 기사를 놓치지 않는다 (전체/overall 도메인은 그중 최신 1일치만 씀).
  _PARTITIONTIME >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY))
  AND (
    (Actor1CountryCode='KOR' AND Actor2CountryCode IN ('PRK','JPN','CHN','USA','RUS'))
    OR (Actor2CountryCode='KOR' AND Actor1CountryCode IN ('PRK','JPN','CHN','USA','RUS'))
  )
"""

AUDIT_TOP_K = 10          # 전체(overall) 도메인: 국가쌍·방향별 검증할 최상위 |영향력| 이벤트 수
AUDIT_TOP_K_SPARSE = 25   # 군사/외교/공급망: 7일치가 한 버킷에 몰려 오염 규모가 더 클 수 있어 더 넓게 검증
AUDIT_MAX_CALLS = 200     # 실행당 '신규' LLM 호출 상한(비용/시간 안전장치).
                          # 캐시 적중분은 호출이 아니므로 이 상한에 포함되지 않는다.
AUDIT_CACHE_FILE = "audit_cache.json"
AUDIT_MAX_TOKENS = 400    # 조건별 서술에 필요한 여유. 즉답(10)일 때 정확도가 55%로 떨어졌다.
AUDIT_PROMPT_VERSION = 2  # 프롬프트가 바뀌면 올릴 것 -> 이전 버전으로 내린 판정 캐시를 폐기한다.


_COND_RE = re.compile(r"^\s*조건\s*([123])\s*[:：]\s*([^\n—–\-]{0,20})")


def _parse_conditions(text):
    """
    조건별 서술 응답에서 조건 1/2/3의 개별 판정을 뽑는다(2026-10-13 추가).

    왜 필요한가: 지금까지 로그에는 최종 판정만 남아, 어떤 레코드가 '이 국가쌍 사건이
    아니어서'(조건1) 제외됐는지 '논조를 못 믿어서'(조건2·3) 제외됐는지 구분할 수 없었다.
    둘은 처방이 달라야 한다 — 전자는 삭제가 맞고, 후자는 사건을 남기고 논조만 버리는
    것이 맞다. 그 분해 없이는 어느 쪽이 몇 건인지조차 모른다.

    판정 로직에는 쓰지 않는다. 지금은 기록만 한다 — 분해를 보고 규칙을 정하기 위함.

    반환: {"c1": True|False|None, "c2": ..., "c3": ...}
          None은 해당없음 또는 파싱 실패.
    """
    out = {"c1": None, "c2": None, "c3": None}
    for line in (text or "").splitlines():
        m = _COND_RE.match(line)
        if not m:
            continue
        seg = m.group(2)
        # 이유 문장에 '예'가 섞여 들어오지 않도록 줄표 앞 토막만 본다.
        if "예" in seg and "아니오" in seg:
            # 모델이 템플릿("예|아니오")을 그대로 되뱉은 경우. 판정이 아니다.
            # 조건1이 거짓이면 나머지를 평가하지 않고 이렇게 답하는 일이 실제로 있었다
            # (2026-10-13 꼬리 전수조사 11번 레코드).
            out[f"c{m.group(1)}"] = None
        elif "아니오" in seg:
            out[f"c{m.group(1)}"] = False
        elif "해당없음" in seg or "해당 없음" in seg:
            out[f"c{m.group(1)}"] = None
        elif "예" in seg:
            out[f"c{m.group(1)}"] = True
    return out


def _parse_verdict(text):
    """
    조건별 서술 응답에서 '최종:' 줄을 읽어 유지(True)/제외(False)를 돌려준다.

    형식을 못 지킨 응답은 보수적으로 유지(True)한다 — 기존 동작과 같은 방향이다.
    (파싱 실패를 제외로 처리하면, 형식 일탈 하나가 조용히 레코드를 지운다.)
    """
    for line in reversed((text or "").strip().splitlines()):
        t = line.strip()
        if t.startswith("최종"):
            if "아니오" in t:
                return False
            if "예" in t:
                return True
    t = (text or "").strip()
    if t.startswith("아니오"):
        return False
    if t.startswith("예"):
        return True
    print(f"    [audit] 판정 파싱 실패, 보수적으로 유지: {t[:60]!r}")
    return True

# 감사 계층의 실행 상태. _audit_with_llm()이 갱신하고 audit_and_correct()가 판정에 쓴다.
# 이것이 없으면 크레딧 소진·키 폐기·네트워크 장애가 전부 "보수적 유지"로 흡수되어,
# 감사가 한 건도 수행되지 않은 날에도 파이프라인이 초록색으로 끝난다(OPERATIONS.md 5절).
_AUDIT_STATS = {"api_calls": 0, "api_failures": 0}


def _cache_key(ev):
    """
    같은 URL이라도 코드·논조 부호가 다르면 판정이 달라질 수 있다.
    조건 3이 음수 코딩에만 걸리므로 부호는 반드시 키에 들어가야 한다.
    """
    return f"{ev['url']}|{ev['code']}|{'neg' if ev['goldstein'] < 0 else 'pos'}"


def _load_audit_cache(latest_date):
    """
    당일 판정 캐시만 유효하다. 감사는 '오늘' 값만 보정하므로 날짜가 바뀌면 버린다.
    이 캐시가 없으면 3시간마다 같은 기사를 같은 질문으로 하루 8번 다시 묻게 된다.
    """
    try:
        with open(AUDIT_CACHE_FILE, "r", encoding="utf-8") as f:
            c = json.load(f)
        if c.get("prompt_version") != AUDIT_PROMPT_VERSION:
            # 프롬프트가 바뀌면 이전 판정은 더 이상 같은 질문의 답이 아니다.
            print(f"  [audit] 프롬프트 버전 변경({c.get('prompt_version')} -> "
                  f"{AUDIT_PROMPT_VERSION}) — 캐시 폐기")
            return {}
        if c.get("date") == latest_date and isinstance(c.get("entries"), dict):
            return c["entries"]
        print(f"  [audit] 캐시 날짜 불일치({c.get('date')} != {latest_date}) — 새로 시작")
    except FileNotFoundError:
        pass
    except Exception as e:
        print(f"  [audit] 캐시 읽기 실패, 새로 시작: {e}")
    return {}


def _save_audit_cache(latest_date, entries):
    """저장 실패는 치명적이지 않다(다음 실행에서 다시 물어볼 뿐). 중단시키지 않는다."""
    try:
        with open(AUDIT_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump({"date": latest_date,
                       "prompt_version": AUDIT_PROMPT_VERSION,
                       "saved_at": datetime.now().isoformat(),
                       "entries": entries}, f, ensure_ascii=False, indent=2)
        print(f"  [audit] 판정 캐시 저장: {AUDIT_CACHE_FILE} ({len(entries)}건)")
    except Exception as e:
        print(f"  [audit] 캐시 저장 실패(무시): {e}")


def _extract_article_direct(url, max_chars=2500, timeout=8, _retry=True):
    """
    기사 원문을 직접 요청해서 구조화 추출: 제목 + 본문 문단(<article>/<p> 태그 우선).
    사이드바·관련기사 목록 같은 잡동사니가 같이 긁혀서 원문이 오염되는 것을 막기 위해,
    페이지 전체 텍스트를 그냥 이어붙이지 않고 실제 기사 본문 태그를 우선 사용한다.
    일부 언론사는 단순한 User-Agent를 가진 요청을 봇으로 간주해 차단하므로,
    실제 브라우저에 가까운 헤더를 사용하고, 실패 시 짧은 대기 후 한 번 재시도한다.
    반환: {"title": str|None, "paragraphs": [str,...] (최대 2개), "full_text": str} 또는 실패 시 None
    """
    try:
        import requests
        from bs4 import BeautifulSoup
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9,ko;q=0.8",
        }
        resp = requests.get(url, timeout=timeout, headers=headers)
        if resp.status_code >= 400:
            if _retry and resp.status_code in (403, 429, 503):
                # 일시적 차단/속도제한으로 보이는 상태코드만 한 번 재시도 (2초 대기 후)
                time.sleep(1)
                return _extract_article_direct(url, max_chars=max_chars, timeout=timeout, _retry=False)
            return None
        soup = BeautifulSoup(resp.text, "html.parser")
        for tag in soup(["script", "style", "nav", "header", "footer", "noscript", "aside"]):
            tag.decompose()

        title = soup.title.get_text(strip=True) if soup.title else None

        container = soup.find("article") or soup
        paragraphs = [p.get_text(" ", strip=True) for p in container.find_all("p")]
        paragraphs = [p for p in paragraphs if len(p) >= 20]  # 버튼/메뉴 등 짧은 텍스트 제외

        if paragraphs:
            full_text = " ".join(paragraphs)[:max_chars]
        else:
            # <p> 태그가 없는 페이지는 기존 방식(전체 텍스트)으로 폴백
            full_text = " ".join(soup.stripped_strings)[:max_chars]

        return {"title": title, "paragraphs": paragraphs[:2], "full_text": full_text}
    except Exception:
        if _retry:
            # 타임아웃/연결 오류 등도 일시적일 수 있으므로 한 번만 재시도
            time.sleep(1)
            return _extract_article_direct(url, max_chars=max_chars, timeout=timeout, _retry=False)
        return None


def _extract_article_via_reader_proxy(url, max_chars=2500, timeout=10):
    """
    직접 요청이 차단(예: 클라우드/데이터센터 IP 대역 자체를 막는 WAF, TLS 핑거프린팅)되어
    실패했을 때 쓰는 2차 경로. r.jina.ai(무료 Reader 프록시, 키 불필요)를 통해 페이지를
    텍스트로 변환해서 가져온다 -> 요청 발신 경로 자체가 달라 직접 차단을 우회할 수 있음.
    """
    try:
        import requests
        resp = requests.get("https://r.jina.ai/" + url, timeout=timeout)
        if resp.status_code >= 400 or not resp.text:
            return None
        text = resp.text.strip()
        lines = [l for l in text.split("\n") if l.strip()]
        title = None
        if lines and lines[0].lower().startswith("title:"):
            title = lines[0].split(":", 1)[1].strip()
        # 문단 구분이 명확하지 않은 포맷이라 paragraphs는 비워두고 full_text만 채움
        return {"title": title, "paragraphs": [], "full_text": text[:max_chars]}
    except Exception:
        return None


# 본문 자리에 기사가 아니라 차단·동의·고지·추천위젯이 들어온 것을 걸러내는 기준.
# 2026-10-13 실측: 감사 제외 150건 중 11건(7.3%)이 이런 경우였다 — 모델이 쓰레기를 보고
# "애매하면 아니오" 규칙대로 제외 판정을 냈다. 즉 추출 실패가 판정으로 둔갑했다.
ARTICLE_MIN_CHARS = 300          # 이보다 짧으면 기사 본문으로 보지 않는다
_NON_ARTICLE_PAT = re.compile(
    r"(just a moment|attention required|checking your browser|cloudflare|"
    r"enable javascript|자바스크립트를? (?:활성화|사용)|브라우저를? 확인|"
    r"access denied|forbidden|are you a robot|보안문자|captcha|"
    r"로그인 후 이용|구독자만|페이지를 찾을 수 없)", re.I)


def _looks_like_article(art):
    """
    추출 결과가 실제 기사 본문인지 판정. fetch_check.py의 분류기와 같은 기준을 쓴다.
    여기서 거짓이면 _extract_article이 None을 반환해, 감사의 보수적 기본값 경로로 넘어간다.
    """
    if not art:
        return False
    ft = (art.get("full_text") or "").strip()
    if len(ft) < ARTICLE_MIN_CHARS:
        return False
    # 제목 + 머리말에 차단/동의 페이지 표지가 있으면 본문이 아니다
    return not _NON_ARTICLE_PAT.search((art.get("title") or "") + " " + ft[:400])


def _extract_article(url, max_chars=2500, timeout=8):
    """
    기사 원문 추출 진입점: 직접 요청을 우선 시도하고, 실패하면 리더 프록시로 한 번 더 시도한다.

    버그 수정(2026-10-13): 직접 요청이 HTTP 200으로 '성공'했지만 돌아온 것이 기사가 아닌
    경우(클라우드플레어 확인 페이지, 저작권 고지와 기자 소개만 있는 페이지, 추천뉴스
    위젯만 남은 페이지)를 성공으로 취급하고 있었다. <p> 태그가 없으면 페이지 전체 텍스트로
    폴백하므로(534행) full_text가 비어 있지 않고, 그래서 _audit_with_llm()의
    `if not article.get("full_text"): return True` 보수적 기본값이 발동하지 않았다.
    모델은 쓰레기를 보고 "애매하면 아니오" 규칙대로 제외 판정을 냈다.
    2026-10-12 감사 180건 실측: 제외 150건 중 11건(7.3%)이 이 경우였다.

    반환값이 세 가지임에 주의:
      - 정상 추출            -> full_text가 찬 dict
      - 닿았지만 기사가 아님 -> full_text가 빈 dict (unparseable=True)
      - 아예 닿지 않음       -> None

    가운데를 None으로 돌려주면 안 된다. 호출부가 "원문을 끝내 못 가져온 경우"에 대해
    고위험 코드(150·182·190·193·194·195 — 북한 미사일·군사 코드)를 LLM 호출 없이 자동
    제외하기 때문이다. 그러면 TOI의 댓글정책 페이지 하나 때문에 실제 북한 탄도미사일 발사
    보도가 조용히 지워진다 — 2026-10-12에 고친 발사 실명이 그대로 되살아난다.
    빈 full_text로 돌려주면 _audit_with_llm()의 보수적 유지 경로를 그대로 타므로,
    프롬프트도 호출부도 고위험 코드 규칙도 손대지 않는다.
    """
    direct = _extract_article_direct(url, max_chars=max_chars, timeout=timeout)
    if _looks_like_article(direct):
        return direct
    proxy = _extract_article_via_reader_proxy(url, max_chars=max_chars, timeout=timeout + 2)
    if _looks_like_article(proxy):
        return proxy
    if direct is None and proxy is None:
        return None  # 두 경로 모두 응답 자체가 없었다 = 기존 의미의 fetch 실패
    # 닿기는 했으나 본문이 아니다. 더 긴 쪽이라도 돌려주고 싶은 유혹이 있지만, 그러면
    # 수정 전과 똑같이 모델이 쓰레기를 보고 제외 판정을 낸다 -> 본문 없음으로 넘긴다.
    got = direct or proxy or {}
    return {"title": got.get("title"), "paragraphs": [], "full_text": "",
            "unparseable": True, "raw_len": len(got.get("full_text") or "")}


def _audit_with_llm(anthropic_client, partner_name_ko, event, article, detail=None):
    """
    기사 원문(제목+본문)을 Claude(Haiku)에게 보여주고, (1) 실제로 한국-상대국 간 직접 상호작용이
    맞는지, (2) GoldsteinScale의 부호(논조)가 실제 내용과 맞는지, (3) 그 논조가 두 나라가 서로에게
    한 행위에서 나온 것인지(제3자의 반응·비판에서 나온 것이 아닌지) 엄격하게 검증.

    (3)은 '부정적으로 코딩된 기록'에만 적용한다. 이 조건의 논리 자체가 "부정적 논조를
    공급한 것이 제3자인가"를 묻는 것이므로 양수 코딩에는 성립하지 않는다. 게이트 없이
    전체에 적용했을 때 실측 결과가 명확했다: 한일 2026-09-07~13 창 100건에서 조건 (3)은
    갈등 기사 제외를 0건 늘린 반면(29→29), 협력 기사 제외를 5건 늘렸고(64→69), 그 5건이
    두 조건을 통과했던 협력 신호의 79.2%였다. 모두 한·미·일 3자 사안이었다. 협력 기록만
    골라 지우면 지수가 긴장 쪽으로 편향되므로, 조건의 취지와 정반대 효과가 난다.

    (3)을 2026-10-07에 추가한 이유: 행위자는 올바르게 추출됐는데 논조의 '주인'이 틀린 유형의
    오분류가 실측으로 확인됐다. 한국이 러시아로 연료를 수출한 사실을 우크라이나가 비판한 기사가
    Goldstein -8.0으로 코딩되어 한-러 공급망 채널에 긴장 +8.0으로 들어갔다. 한-러 사이에서 그
    행위는 거래이고, 부정적 논조의 주체는 제3국이다. 기존 두 조건은 둘 다 참이므로(한-러
    상호작용이 맞고, 기사 논조도 실제로 부정적) 이 유형을 통과시켰다.
    원문을 못 가져왔거나 API 호출이 실패하면 True(신뢰 유지, 보수적 기본값) 반환 —
    감사 기능 자체의 실패가 파이프라인을 망가뜨리거나 데이터를 과도하게 지우지 않도록 함.
    """
    if not article or not article.get("full_text"):
        return True
    goldstein = event["goldstein"]
    tone_word = "갈등적/부정적" if goldstein < 0 else "협력적/긍정적"
    title_line = f"기사 제목: {article['title']}\n" if article.get("title") else ""
    # 조건 3은 '부정적 논조의 출처'를 묻는 조건이므로 음수 코딩에만 건다.
    use_c3 = goldstein < 0
    cond3 = (
        f"3) 그 부정적 논조가 한국과 {partner_name_ko}이(가) 서로에게 한 행위에서 나온 "
        f"것인가, 아니면 그 행위에 대한 제3자(다른 나라·국제기구·국제여론)의 반응·비판에서 "
        f"나온 것인가. 두 나라 사이의 행위 자체는 거래·협력·접촉인데 기사의 부정적 논조가 "
        f"제3자의 비난에서 비롯된 것이라면, 그 논조는 이 두 나라 관계의 논조가 아니므로 "
        f"\"아니오\"로 답하세요. "
        f"(예: 한국이 러시아로 연료를 수출한 데 대해 우크라이나가 비판한 기사 — 한-러 "
        f"사이에서 그 행위는 거래이고, 비판하는 주체는 제3국입니다.)\n"
        f"   다만 다음 두 경우는 여기에 해당하지 않습니다. 첫째, 두 나라가 서로를 향해 "
        f"위협·제재·비난·충돌한 것을 제3국 언론이 단순히 '보도'한 경우 — 보도한 매체의 "
        f"국적이 아니라, 논조를 만들어낸 행위의 주체가 누구인지로 판단하세요. 둘째, 제3국이 "
        f"그 행위에 '함께 참여한' 경우(3자 연합훈련·3자 협의·3자 협정 등) — 이때 제3국은 "
        f"논평자가 아니라 참여자이므로, 그 행위는 두 나라 사이의 행위에 해당합니다.\n"
    ) if use_c3 else ""
    n_word = "세" if use_c3 else "두"
    prompt = (
        f"다음은 GDELT가 \"{event['a1']} -> {event['a2']}\" 간 이벤트로 자동 분류한 기사입니다.\n"
        f"분류된 CAMEO 코드: {event['code']}, GoldsteinScale: {goldstein:+.1f} "
        f"(이 점수는 이 상호작용이 \"{tone_word}\"이라는 뜻입니다. "
        f"음수=갈등/충돌/위협, 양수=협력/지원/우호).\n\n"
        f"{title_line}"
        f"기사 본문 일부: {article['full_text']}\n\n"
        f"아래 {n_word} 조건을 모두 엄격하게 확인하세요:\n"
        f"1) 이 기사가 실제로 한국과 {partner_name_ko} 두 국가(정부/국가급 행위자)의 "
        f"직접적인 상호작용을 다루고 있는가 (제3국 사건에 곁가지로 언급된 게 아니라)\n"
        f"2) 기사의 실제 논조가 \"{tone_word}\" 방향과 일치하는가. "
        f"예를 들어 의료지원·인도적 지원·경제협력·정상회담처럼 실제로는 우호적인 내용인데 "
        f"부정적 코드가 붙었거나, 반대로 위협·제재·충돌 내용인데 긍정적 코드가 붙었다면 "
        f"이건 불일치이므로 반드시 \"아니오\"로 답하세요.\n"
        f"{cond3}"
        f"\n{n_word} 조건이 모두 참일 때만 \"예\", 하나라도 거짓이거나 애매하면 "
        f"\"아니오\"입니다. "
        # 2026-10-12: 여기서 "다른 설명 없이 한 단어로만 답하라"고 지시하던 것을 걷어냈다.
        # 즉답을 강제하면 모델이 세 조건을 검토할 자리가 없어 "애매하면 아니오" 쪽으로
        # 쏠린다. 통제 시험(기사 2건 x 각 조건 10회, 총 60회):
        #   발사 보도(유지가 정답)  즉답 1/10 유지  ->  조건별 서술 10/10 유지
        #   지뢰 기사(제외가 정답)  즉답 10/10 제외 ->  조건별 서술 10/10 제외
        #   정답 일치율 55% -> 100% (Fisher p=0.0012), 분산도 0으로 떨어짐.
        # 판정 '기준'(세 조건 문구, "애매하면 아니오")은 한 글자도 바꾸지 않았다.
        # 바뀐 것은 답하는 형식뿐이다 — 부록 D 검증이 겨냥한 결정 규칙은 그대로다.
        f"각 조건을 차례로 검토한 뒤 마지막 줄에 최종 판정을 쓰세요. "
        f"아래 형식만 출력하고 다른 말은 쓰지 마세요.\n"
        f"조건1: 예|아니오 — (한 문장 이유)\n"
        f"조건2: 예|아니오 — (한 문장 이유)\n"
        + ("조건3: 예|아니오 — (한 문장 이유)\n" if use_c3 else "")
        + f"최종: 예|아니오"
    )
    try:
        _AUDIT_STATS["api_calls"] += 1
        resp = anthropic_client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=AUDIT_MAX_TOKENS,
            messages=[{"role": "user", "content": prompt}],
        )
        text = resp.content[0].text
        if detail is not None:
            # 판정에는 쓰지 않는다. 어느 조건이 걸렸는지 기록만 한다(2026-10-13).
            detail.update(_parse_conditions(text))
            detail["reason"] = " ".join(text.split())[:220]
        return _parse_verdict(text)
    except Exception as e:
        # 보수적 유지는 그대로 두되, '몇 건이 이렇게 유지됐는지'를 반드시 남긴다.
        # audit_and_correct()가 이 숫자를 보고 전면 실패면 실행을 중단시킨다.
        _AUDIT_STATS["api_failures"] += 1
        print(f"    [audit] LLM 호출 실패, 보수적으로 유지: {e}")
        return True


def fetch_raw_daily_events(client):
    """
    최근 7일치 원시 이벤트를 조회하고, (도메인,파트너,방향) 버킷으로 정리해서 반환.
    ANTHROPIC_API_KEY 유무와 무관하게 항상 실행 — CSV 저장과 대시보드용 주요기사 추출에 쓰임.
    - "all"(전체) 버킷: 최신 1일치만 사용 (전체 도메인은 1일 창이므로)
    - "mil"/"sup"/"dip" 버킷: 7일 전체 사용 (해당 도메인들이 7일 이동창으로 집계되므로,
      감사·근거기사 범위도 여기 맞춰야 "며칠 전" 오염 기사를 놓치지 않음)
    반환: (latest_date, today_rows, buckets) 또는 데이터 없으면 (None, [], {})
    today_rows는 CSV 저장용으로 최신 1일치만 담음(하루 1파일 원칙 유지).
    """
    _dry_run_check(client, RAW_AUDIT_QUERY, "원시 이벤트 쿼리(CSV 저장·감사·주요기사 공용, 7일)")
    raw_rows = list(client.query(RAW_AUDIT_QUERY, job_config=_job_config()).result())
    if not raw_rows:
        return None, [], {}

    latest_date_str = max(str(r["date_str"]) for r in raw_rows)
    latest_date = f"{latest_date_str[:4]}-{latest_date_str[4:6]}-{latest_date_str[6:]}"
    today_rows = [r for r in raw_rows if str(r["date_str"]) == latest_date_str]

    buckets = defaultdict(list)
    for r in raw_rows:
        a1, a2 = r["Actor1CountryCode"], r["Actor2CountryCode"]
        partner = a2 if a1 == "KOR" else a1
        if partner not in PARTNERS:
            continue
        direction = "response" if a1 == "KOR" else "threat"
        impact = float(r["GoldsteinScale"] or 0) * int(r["NumMentions"] or 0)
        ev = {
            "a1": r["Actor1Name"], "a2": r["Actor2Name"],
            "code": r["EventCode"], "goldstein": float(r["GoldsteinScale"] or 0),
            "mentions": int(r["NumMentions"] or 0), "impact": impact,
            "url": r["SOURCEURL"], "partner": partner, "direction": direction,
            "date": str(r["date_str"]),
        }
        is_latest_day = str(r["date_str"]) == latest_date_str
        if is_latest_day:
            buckets[("all", partner, direction)].append(ev)
        if r["is_military"]:
            buckets[("mil", partner, direction)].append(ev)
        if r["is_supply"]:
            buckets[("sup", partner, direction)].append(ev)
        if r["is_diplomatic"]:
            buckets[("dip", partner, direction)].append(ev)

    return latest_date, today_rows, buckets


def save_raw_daily_csv(latest_date, today_rows, path="raw_data"):
    """
    오늘 매칭된 전체 원시 이벤트(감사 대상 여부와 무관하게 전부)를 CSV로 저장.
    URL을 자르지 않고 그대로 보존 -> 나중에 재검증·재인용 가능. 날짜별 파일로 누적.
    """
    import csv
    os.makedirs(path, exist_ok=True)
    filepath = os.path.join(path, f"{latest_date}.csv")
    with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["date", "actor1", "actor2", "event_code", "goldstein", "mentions",
                    "is_military", "is_supply", "is_diplomatic", "url"])
        for r in today_rows:
            w.writerow([
                latest_date, r["Actor1Name"], r["Actor2Name"], r["EventCode"],
                r["GoldsteinScale"], r["NumMentions"],
                bool(r["is_military"]), bool(r["is_supply"]), bool(r["is_diplomatic"]),
                r["SOURCEURL"],
            ])
    print(f"  [raw-csv] {len(today_rows)}건 저장 완료: {filepath}")
    return filepath


def compute_top_articles(buckets, latest_date, top_n=2, exclude_urls=None):
    """
    도메인·파트너·방향별 "감쇠 가중 영향력" 최상위 top_n건을 대시보드 표시용으로 추출.
    원본(감쇠 없는) 영향력으로만 순위를 매기면, 지수 계산에선 이미 감쇠로 비중이 줄어든
    옛 기사가 "근거기사" 칸에는 계속 1등으로 남는 모순이 생긴다 -> 지수 계산(domain_block,
    audit 보정)과 동일한 RECENCY_DECAY를 여기서도 적용해 순위를 매긴다.
    exclude_urls가 주어지면(감사에서 오분류로 제외된 URL 집합), 그 URL들은
    "근거기사"로 다시 추천되지 않도록 후보에서 뺀다 -> 이미 값 계산에서 제외된 기사가
    "이 수치의 근거"라고 잘못 표시되는 걸 방지.
    (제목은 감사 단계에서 fetch된 것만 나중에 덧붙여짐; 여기서는 URL·행위자·수치만)
    """
    exclude_urls = exclude_urls or set()
    latest_dt = datetime.strptime(latest_date, "%Y-%m-%d")

    def decayed_abs_impact(e, decay_rate):
        ev_dt = datetime.strptime(e["date"], "%Y%m%d")
        days_ago = max(0, (latest_dt - ev_dt).days)
        return abs(e["impact"]) * (decay_rate ** days_ago)

    dom_key_map = {"all": "overall", "mil": "military", "sup": "supply", "dip": "diplomatic"}
    result = defaultdict(dict)  # dom_name -> field -> [ {url, goldstein, mentions, a1, a2}, ... ]
    for (dom_key, partner, direction), evs in buckets.items():
        decay_rate = _decay_rate(dom_key)
        candidates = [e for e in evs if e["url"] not in exclude_urls]
        candidates.sort(key=lambda e: decayed_abs_impact(e, decay_rate), reverse=True)
        seen_urls = set()
        top = []
        for e in candidates:
            if e["url"] in seen_urls:
                continue  # 같은 기사(URL)가 여러 행으로 중복 집계된 경우 근거기사 목록엔 한 번만
            seen_urls.add(e["url"])
            top.append(e)
            if len(top) >= top_n:
                break
        dom_name = dom_key_map[dom_key]
        field = f"{partner}_{direction}"
        result[dom_name][field] = [
            {"url": e["url"], "goldstein": e["goldstein"], "mentions": e["mentions"],
             "actor1": e["a1"], "actor2": e["a2"], "event_code": e["code"], "title": None,
             "days_ago": max(0, (latest_dt - datetime.strptime(e["date"], "%Y%m%d")).days)}
            for e in top
        ]
    return result


def audit_and_correct(client, output, dates, latest_date, today_rows, buckets, top_articles):
    """
    도메인별 '오늘' 값 중, 최상위 |영향력| 이벤트를 원문 검증해서 오분류로 판정되면
    제외하고 재계산 -> output의 domains.*.current / series 마지막 값을 보정.
    ANTHROPIC_API_KEY가 없으면 조용히 건너뜀(하위 호환).
    latest_date/today_rows/buckets는 fetch_raw_daily_events()에서 미리 조회한 것을 재사용
    (원시 쿼리를 두 번 돌리지 않기 위함).
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("  [audit] ANTHROPIC_API_KEY 없음 — 감사 단계 건너뜀")
        return {"enabled": False}

    try:
        import anthropic
        client_llm = anthropic.Anthropic(api_key=api_key)
    except Exception as e:
        print(f"  [audit] anthropic 클라이언트 초기화 실패, 건너뜀: {e}")
        return {"enabled": False}

    if not today_rows:
        print("  [audit] 감사 대상 원시 이벤트 없음")
        return {"enabled": True, "checked": 0, "flagged": 0}

    total_pool = sum(len(v) for v in buckets.values())
    print(f"  [audit] 감사 대상 날짜: {latest_date} (전체 {len(today_rows)}건 / 희소도메인은 최근 7일 {total_pool}건-버킷 풀에서 상위 이벤트만 검증)")

    # 검증 대상(상위 |impact|) 선정 + URL 중복 제거
    to_check = {}  # url -> event(대표 1건)
    for key, evs in buckets.items():
        dom_key = key[0]
        k = AUDIT_TOP_K if dom_key == "all" else AUDIT_TOP_K_SPARSE
        top = sorted(evs, key=lambda e: abs(e["impact"]), reverse=True)[:k]
        for ev in top:
            if ev["url"] not in to_check:
                to_check[ev["url"]] = ev

    _AUDIT_STATS["api_calls"] = 0
    _AUDIT_STATS["api_failures"] = 0
    cache = _load_audit_cache(latest_date)

    # 이미 오늘 판정한 (url, code, 부호)는 다시 묻지 않는다. 3시간 주기로 하루 8회 도는데
    # 같은 날짜를 감사하므로, 캐시가 없으면 호출의 7/8이 같은 질문의 반복이 된다.
    all_urls = list(to_check.keys())
    cached_urls = [u for u in all_urls if _cache_key(to_check[u]) in cache]
    fresh_urls = [u for u in all_urls if _cache_key(to_check[u]) not in cache]
    truncated = max(0, len(fresh_urls) - AUDIT_MAX_CALLS)
    fresh_urls = fresh_urls[:AUDIT_MAX_CALLS]
    urls = cached_urls + fresh_urls
    print(f"  [audit] 검증 대상 URL {len(all_urls)}건 (중복 제거 후) "
          f"— 캐시 적중 {len(cached_urls)}건, 신규 {len(fresh_urls)}건")
    if truncated:
        # 조용히 잘리면 "최고영향 레코드를 검증한다"는 주장 자체가 약해진다. 반드시 남긴다.
        print(f"  [audit] ::warning::상한 {AUDIT_MAX_CALLS}건에 걸려 "
              f"신규 {truncated}건이 검사되지 않았습니다 — AUDIT_MAX_CALLS 재검토 필요")

    # Section 4에서 실증한 "오분류 다발 코드" — 원문을 못 가져와 확인이 안 될 때,
    # 이 코드들은 일반 이벤트와 반대로 "보수적 제외"를 기본값으로 함(반대는 통과가 기본값).
    HIGH_RISK_EVENT_CODES = {"150", "182", "190", "193", "194", "195"}

    # 원문 fetch를 병렬로 먼저 전부 수행 -> 네트워크 I/O 대기(타임아웃·재시도·리더프록시)가
    # URL마다 순차적으로 누적되면 실행시간이 20분 이상으로 늘어나는 문제가 있었음.
    # fetch는 서로 독립적인 I/O 작업이라 동시에 진행해도 안전하며, 스레드풀로 병렬화하면
    # 전체 대기시간이 "URL 개수 x 개별시간"이 아니라 "가장 느린 URL 1~2개 시간" 수준으로 줄어듦.
    FETCH_WORKERS = 12
    articles_by_url = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=FETCH_WORKERS) as pool:
        future_to_url = {pool.submit(_extract_article, url): url for url in fresh_urls}
        for future in concurrent.futures.as_completed(future_to_url):
            u = future_to_url[future]
            try:
                articles_by_url[u] = future.result()
            except Exception:
                articles_by_url[u] = None

    fetched_titles = {}  # url -> title (top_articles에 나중에 덧붙이기 위함)
    bad_urls = set()
    checked = 0
    audit_log = []  # 통과("예")/제외("아니오") 전부 기록 — 나중에 재검토·논문 인용용
    for url in urls:
        ev = to_check[url]
        ck = _cache_key(ev)
        hit = cache.get(ck)
        detail = {}
        if hit is not None:
            # 오늘 이미 같은 질문에 답을 받았다. 판정과 보존해 둔 제목·앞 두 문단을 재사용.
            ok = hit["ok"]
            detail = hit.get("detail") or {}
            article = {"title": hit.get("title"),
                       "text_len": hit.get("text_len"),
                       "paragraphs": [p for p in (hit.get("paragraph_1"),
                                                  hit.get("paragraph_2")) if p]}
        else:
            article = articles_by_url.get(url)
            if article is not None:
                ok = _audit_with_llm(client_llm, NAMES.get(ev["partner"], ev["partner"]),
                                     ev, article, detail)
            else:
                # 원문을 끝내 못 가져온 경우: 일반 이벤트는 보수적으로 유지(통과)하되,
                # 실증된 오분류 다발 코드는 반대로 보수적 제외 -> LLM 호출 없이 바로 결정
                ok = ev["code"] not in HIGH_RISK_EVENT_CODES
            if article is not None and not article.get("unparseable"):
                # 원문을 가져온 건만 캐시한다. fetch 실패는 일시적일 수 있으므로
                # 캐시에 넣지 않고 다음 실행에서 다시 시도하게 둔다.
                # '닿았지만 본문이 아님'도 마찬가지다 — 차단 페이지는 대개 일시적이고,
                # 캐시에 넣으면 그날 하루 내내 같은 URL을 못 읽은 채로 굳는다.
                _p = article.get("paragraphs") or []
                cache[ck] = {"ok": ok, "article_fetched": True,
                             "title": article.get("title"),
                             "text_len": len(article.get("full_text") or ""),
                             "paragraph_1": _p[0] if len(_p) > 0 else None,
                             "paragraph_2": _p[1] if len(_p) > 1 else None,
                             "detail": detail or None}
        checked += 1
        if article and article.get("title"):
            fetched_titles[url] = article["title"]
        paras = (article or {}).get("paragraphs") or []
        audit_log.append({
            "url": url,
            "actor1": ev["a1"], "actor2": ev["a2"],
            "event_code": ev["code"], "goldstein": ev["goldstein"], "mentions": ev["mentions"],
            "partner": ev["partner"], "direction": ev["direction"],
            "verdict": "pass" if ok else "flagged",
            # 본문을 실제로 확보한 경우만 True. '닿았지만 기사가 아님'은 False —
            # 이걸 True로 적으면 로그상 검증된 것처럼 보이지만 모델은 본문을 못 봤다.
            "article_fetched": bool(article) and not article.get("unparseable"),
            # 추출 품질을 매 실행 기록한다. 이게 없어서 "추출 실패가 제외로 둔갑하는"
            # 문제를 2026-10-13에야 별도 도구로 발견했다. 이제 로그만 보면 보인다.
            "text_len": (article or {}).get("text_len",
                                            len((article or {}).get("full_text") or "")),
            # 조건별 판정(2026-10-13). 기록만 하고 판정에는 쓰지 않는다.
            # 조건1 = 이 국가쌍의 직접 상호작용인가 / 조건2 = 코딩된 논조가 내용과 맞는가 /
            # 조건3 = 그 부정적 논조가 제3자의 것은 아닌가(음수 코딩에만 적용).
            # 삭제가 맞는 건(조건1 실패)과 논조만 버려야 할 건(조건2·3 실패)을 구분하려면
            # 이 분해가 있어야 한다. 없어서 109건의 제외 사유를 추정만 하고 있었다.
            "cond": {k: detail.get(k) for k in ("c1", "c2", "c3")},
            "reason": detail.get("reason") if not ok else None,
            "unparseable": bool((article or {}).get("unparseable")),
            # 제목 + 본문 앞 2문단만 영구 보존(사이드바 등 잡동사니 제외) — link rot 대비
            "title": (article or {}).get("title"),
            "paragraph_1": paras[0] if len(paras) > 0 else None,
            "paragraph_2": paras[1] if len(paras) > 1 else None,
        })
        if not ok:
            bad_urls.add(url)
            print(f"    [audit] 제외: {ev['a1']}->{ev['a2']} code={ev['code']} G={ev['goldstein']} url={url}")

    print(f"  [audit] 검증 완료: {checked}건 확인, {len(bad_urls)}건 오분류로 제외 "
          f"(신규 호출 {_AUDIT_STATS['api_calls']}건)")
    _save_audit_cache(latest_date, cache)

    # ── 침묵 실패 가드 (OPERATIONS.md 5절) ──────────────────────────────
    # 크레딧 소진·키 폐기·네트워크 장애는 전부 _audit_with_llm()의 except로 들어가
    # "보수적 유지"가 된다. 그대로 두면 감사가 한 건도 수행되지 않은 날에도
    # 파이프라인이 정상 종료하고, 미검증 값이 daily_history.json에 영구히 박힌다.
    n_calls = _AUDIT_STATS["api_calls"]
    n_fail = _AUDIT_STATS["api_failures"]
    if n_fail:
        print(f"  [audit] ::error::LLM 호출 {n_fail}/{n_calls}건 실패 — "
              f"이 실행의 판독값은 부분적으로 미검증입니다")
    if n_calls and n_fail == n_calls:
        raise RuntimeError(
            f"감사 계층 전면 실패: LLM 호출 {n_fail}/{n_calls}건이 모두 실패했습니다. "
            f"미검증 값이 커밋되지 않도록 실행을 중단합니다. "
            f"크레딧 잔액과 ANTHROPIC_API_KEY를 확인하십시오 (OPERATIONS.md 5절)."
        )

    # 근거기사(top_articles)를 "제외된 URL 빼고" 다시 계산 -> 이미 값 계산에서 제외된 기사가
    # 여전히 "이 수치의 근거"로 표시되는 문제를 방지. (교체된 새 후보는 감사 대상이 아니었을 수
    # 있어 제목이 없을 수 있음 -> 프론트엔드가 "행위자1→행위자2(코드)" 형태로 대체 표시함)
    refreshed = compute_top_articles(buckets, latest_date, exclude_urls=bad_urls)
    top_articles.clear()
    top_articles.update(refreshed)
    for dom_name, fields in top_articles.items():
        for field, arts in fields.items():
            for a in arts:
                if a["url"] in fetched_titles:
                    a["title"] = fetched_titles[a["url"]]

    # 전체 감사 로그를 별도 파일로 저장 (URL 잘림 없이, 통과/제외 전부 포함)
    # -> 다음번엔 BigQuery를 다시 조회하지 않고 이 파일만 보면 재검토 가능
    with open("audit_log.json", "w", encoding="utf-8") as f:
        json.dump({
            "date_audited": latest_date,
            "generated_at": datetime.now().isoformat(),
            "total_checked": checked,
            "total_flagged": len(bad_urls),
            "entries": audit_log,
        }, f, ensure_ascii=False, indent=2)
    print(f"  [audit] 전체 감사 로그 저장 완료: audit_log.json ({len(audit_log)}건, URL 전체 포함)")

    # 제외 후 재계산 -> 해당 도메인·파트너·방향의 '오늘' 값만 보정
    dom_key_map = {"all": "overall", "mil": "military", "sup": "supply", "dip": "diplomatic"}
    corrected_count = 0
    carried_forward_count = 0
    # 2026-10-12: 북한 미사일 발사 당일, 외교 채널이 전량 제외되어 "전날값 x 0.5"가
    # 표시됐다. 숫자가 그럴듯해서 '측정이 없었다'는 사실 자체가 보이지 않았다.
    # 이월된 조합을 출력에 명시해 대시보드가 "미측정(이월)"로 표시할 수 있게 한다.
    carried_fields = []
    latest_dt = datetime.strptime(latest_date, "%Y-%m-%d")
    for (dom_key, partner, direction), evs in buckets.items():
        clean = [e for e in evs if e["url"] not in bad_urls]
        if len(clean) == len(evs):
            continue  # 이 조합에서 제외된 게 없으면 손댈 필요 없음
        # 메인 시계열(domain_block)과 동일하게, 오늘에서 며칠 지났는지에 따라 감쇠 가중치를 적용.
        # 이걸 빠뜨리면 감사가 뭔가 하나라도 제외 판정을 내릴 때마다, 어렵게 넣은 감쇠가
        # 무시되고 6일 전 사건이 오늘 사건과 동일한 무게로 재반영되는 문제가 생긴다.
        m_sum = e_count = 0.0
        weighted_impact = 0.0
        decay_rate = _decay_rate(dom_key)
        for e in clean:
            ev_dt = datetime.strptime(e["date"], "%Y%m%d")
            days_ago = max(0, (latest_dt - ev_dt).days)
            decay = decay_rate ** days_ago
            m_sum += e["mentions"] * decay
            weighted_impact += e["impact"] * decay
            e_count += decay
        corrected = _shrink((weighted_impact / m_sum * -1.0), e_count) if m_sum > 0 else None

        dom_name = dom_key_map[dom_key]
        field = f"{partner}_{direction}"
        series = output["domains"][dom_name]["series"].get(field)
        if not (series and dates and dates[-1] == latest_date):
            continue

        if corrected is None:
            # 감사로 이 버킷의 이벤트가 전부 제외됨 -> "오늘 믿을 만한 근거가 하나도 없다"는
            # 감사의 명확한 판정. 이 경우 오염된 원본값을 그대로 두는 것도, 무조건 0.0으로
            # 덮어쓰는 것도 둘 다 오해를 준다 -> "어제 값을 그대로 이어받는다"(전날과 같다고
            # 봄)가 가장 정직한 기본값. 다만 그냥 복사만 하면 감쇠가 하루 건너뛰는 셈이 되어
            # (며칠 연속 이어받기가 발생하면 감쇠 없이 같은 값이 반복됨), 이어받을 때도
            # 도메인별 하루치 감쇠를 한 번 적용한다 -> 근거 없는 날이 계속되면 값도 계속 옅어짐.
            #
            # 버그 수정(2026-09-21): "어제 값"(series[-2])도 None일 수 있다 — 그 국가쌍이
            # 어제도 이벤트가 0건이었던 경우(희소 도메인에서 드물지 않음). 이 경우
            # series[-2] * decay_rate가 TypeError로 파이프라인 전체를 죽였다(주말 6회 연속
            # 실패의 원인). 수정: 어제 하나만 보지 않고, series를 거슬러 올라가며 가장 최근의
            # 유효(non-None)값을 찾아, 그 값이 며칠 전 것인지에 비례해 감쇠를 적용한다.
            # 유효값이 시계열 전체에 하나도 없으면(정말 처음부터 데이터가 없던 쌍) 0.0으로 둔다.
            lookback_val, days_back = None, 0
            for idx in range(len(series) - 2, -1, -1):
                if series[idx] is not None:
                    lookback_val = series[idx]
                    days_back = (len(series) - 1) - idx
                    break
            corrected = round(lookback_val * (decay_rate ** days_back), 4) if lookback_val is not None else 0.0
            carried_forward_count += 1
            carried_fields.append(f"{dom_name}.{field}")
            output["domains"][dom_name].setdefault("carried_forward", {})[field] = True

        series[-1] = corrected
        output["domains"][dom_name]["current"][field] = corrected
        if dom_name == "overall":
            output["series"][field][-1] = corrected
            output["current"][field] = corrected
        corrected_count += 1

    print(f"  [audit] {corrected_count}개 (도메인,국가쌍,방향) 조합의 오늘 값을 보정했습니다"
          + (f" (그중 {carried_forward_count}개는 근거가 전부 제외되어 전날 값 이어받음)" if carried_forward_count else ""))
    if carried_fields:
        # 이월값은 '측정'이 아니다. 조용히 지나가면 사건 당일에도 평온한 숫자가 공개된다.
        print(f"  [audit] ::warning::이월(미측정) {len(carried_fields)}개: "
              + ", ".join(sorted(carried_fields)))

    # 보정으로 오늘 값(series 마지막 원소/current)이 바뀌었으므로, 상대(Z-score) 경보 수준도
    # 반드시 다시 계산해야 한다 -> 안 그러면 level이 감사 이전 값 기준으로 남아 화면과 어긋남.
    if corrected_count > 0:
        for dom_name in ("overall", "military", "supply", "diplomatic"):
            dom = output["domains"][dom_name]
            for field, cur_val in dom["current"].items():
                hist = dom["series"].get(field, [])
                hist_excl_today = hist[:-1] if len(hist) > 1 else hist
                dom["level"][field] = _relative_level(cur_val, hist_excl_today)
        output["level"] = output["domains"]["overall"]["level"]

    return {
        "enabled": True,
        "date_audited": latest_date,
        "checked": checked,
        "flagged": len(bad_urls),
        "flagged_urls": list(bad_urls),
        "corrections_applied": corrected_count,
        "audit_log_file": "audit_log.json",
        # 대시보드·사후 점검용. degraded=True인 날의 값은 일부 미검증이다.
        "degraded": _AUDIT_STATS["api_failures"] > 0,
        "api_calls": _AUDIT_STATS["api_calls"],
        "api_failures": _AUDIT_STATS["api_failures"],
        "cache_hits": len(cached_urls),
        "skipped_over_cap": truncated,
        # 아래 조합의 오늘 값은 계산된 것이 아니라 전날 값을 감쇠시켜 이어받은 것이다.
        "carried_forward_fields": sorted(carried_fields),
        "carried_forward_count": carried_forward_count,
    }

# ══════════════════════════════════════════════════════════════════════════
# 해설(commentary) 계층 — 지수와 그 지수를 움직인 기사만을 근거로 짧은 서술 생성
#
# 논문이 지적하는 위험이 여기에 그대로 적용된다. 공개된 긴장 지수는 정부·시장·언론이
# 읽으며, 코딩 오류에서 나온 수치가 그것이 관측한다고 주장하는 마찰을 실제로 만들어낼
# 수 있다. 숫자가 그렇다면 "긴장이 고조되고 있다"는 문장은 더하다. 인용되기 때문이다.
# 그래서 해설 계층에는 감사 계층과 같은 규율을 건다.
#
#   1) 외부 검색·사전지식 금지. 서술의 근거는 아래에서 전달하는 기사 목록뿐이다.
#   2) 모든 사실 주장은 전달된 기사 중 하나에 대응해야 하며, 모델은 자신이 쓴 기사의
#      '번호'를 돌려준다. URL을 직접 쓰게 하면 지어낼 수 있으므로 번호로만 받는다.
#   3) 예측·의도 추정 금지. "무엇이 움직였고 어떤 기사가 그것을 밀었는가"만 쓴다.
#   4) 설명할 수 없으면 설명할 수 없다고 쓴다. 억지 해석보다 공백이 낫다.
#
# 반드시 audit_and_correct() 이후에 호출할 것. 그래야 (a) 감사로 보정된 값을 해설하고,
# (b) top_articles에 감사 단계에서 fetch된 제목이 채워진 뒤에 읽는다.
# ══════════════════════════════════════════════════════════════════════════

COMMENTARY_MODEL = os.environ.get("COMMENTARY_MODEL", "claude-haiku-4-5-20251001")
COMMENTARY_DAYS = 7           # 해설이 참조하는 추세 길이
COMMENTARY_MAX_ARTICLES = 12  # 모델에 전달할 근거기사 수 상한


COMMENTARY_DOMAIN_KR = {"overall": "전체", "military": "군사",
                        "diplomatic": "외교", "supply": "공급망"}


def _commentary_inputs(output, dates, top_articles):
    """해설에 넘길 (수치 요약 문자열, 번호 매긴 근거기사 리스트)를 만든다."""
    tail = dates[-COMMENTARY_DAYS:]
    lines = []
    for p in PARTNERS:
        field = f"{p}_threat"
        ser = output["domains"]["overall"]["series"][field][-COMMENTARY_DAYS:]
        traj = " ".join("—" if v is None else f"{v:+.1f}" for v in ser)
        cur = output["domains"]["overall"]["current"].get(field)
        lvl = LEVEL_LABEL_KR.get(output["domains"]["overall"]["level"].get(field))
        mil = output["domains"]["military"]["current"].get(field)
        dip = output["domains"]["diplomatic"]["current"].get(field)
        sup = output["domains"]["supply"]["current"].get(field)
        fmt = lambda v: "—" if v is None else f"{v:+.2f}"
        lines.append(
            f"{NAMES[p]}: 최근 {len(tail)}일 전체 [{traj}] / "
            f"오늘 전체 {fmt(cur)} [{lvl}] / "
            f"군사 {fmt(mil)} · 외교 {fmt(dip)} · 공급망 {fmt(sup)}"
        )
    # 부호 규약을 반드시 명시한다. 이것이 없으면 모델은 '하락'을 관계 악화로 읽는다
    # (실측 확인: 한러 전체지수가 +2.71 -> -1.59로 내려간 날을 "급락, 악화"로 서술함).
    numbers = (
        f"관측일: {dates[-1]}\n"
        "부호 규약: 양수 = 긴장, 음수 = 협력. 값이 올라가면 긴장이 높아진 것이고, "
        "내려가면 협력 쪽으로 간 것입니다. 따라서 수치의 '하락'은 관계 악화가 아니라 "
        "개선이며, '상승'이 악화입니다. 이 규약을 반대로 읽지 마십시오.\n"
        + "\n".join(lines)
    )

    # 근거기사: 감사 단계에서 제목이 채워진 것만 쓴다(제목이 없으면 모델이 내용을 알 수 없음).
    arts = []
    for dom in ("overall", "military", "diplomatic", "supply"):
        for field, items in (top_articles.get(dom) or {}).items():
            for a in items or []:
                if a.get("title"):
                    arts.append({**a, "domain": dom, "field": field})

    seen, picked = set(), []
    for a in sorted(arts, key=lambda a: -abs(a["goldstein"] * a["mentions"])):
        if a["url"] in seen:
            continue
        seen.add(a["url"])
        picked.append(a)
        if len(picked) >= COMMENTARY_MAX_ARTICLES:
            break
    return numbers, picked


def _commentary_prompt(numbers, articles):
    # 기사마다 '어느 채널의 근거인지'와 '방향'을 반드시 붙인다. 이게 없으면 모델은
    # 공급망 채널 기사로 전체 지수의 움직임을 설명하는 범주 오류를 저지른다(실측 확인).
    # Goldstein 원점수 대신 '긴장 기여도'(= -G)로 바꿔 전달한다 -> 지수와 부호가 같아져
    # 모델이 부호를 두 번 뒤집는 일이 없어진다.
    rows = []
    for i, a in enumerate(articles):
        dom = COMMENTARY_DOMAIN_KR.get(a.get("domain"), a.get("domain"))
        direction = "상대→한국" if str(a.get("field", "")).endswith("_threat") else "한국→상대"
        rows.append(
            f"[{i+1}] ({dom} 채널 · {direction}) {a['actor1']} -> {a['actor2']} "
            f"(긴장 기여도 {-a['goldstein']:+.1f}, 보도 {a['mentions']}건, "
            f"{a['days_ago']}일 전) {a['title']}"
        )
    listing = "\n".join(rows)
    return (
        "당신은 한국의 대외관계 긴장지수(KBTI) 대시보드에 붙는 짧은 해설을 씁니다.\n"
        "아래 두 가지 외에는 어떤 지식도 사용하지 마십시오. 기억하고 있는 사건, "
        "일반적 배경지식, 추측은 모두 금지입니다.\n\n"
        f"=== 1. 오늘의 수치 ===\n{numbers}\n\n"
        f"=== 2. 이 수치를 움직인 기사 ===\n{listing}\n\n"
        "=== 작성 규칙 ===\n"
        "1. 수치가 '무엇을 했는지'와 '어떤 기사가 그것을 밀었는지'만 쓰십시오.\n"
        "2. 모든 사실 서술은 위 기사 중 하나에 대응해야 합니다. 대응하는 기사가 없는 "
        "내용은 한 글자도 쓰지 마십시오.\n"
        "3. 앞으로 어떻게 될지 예측하지 말고, 당사국의 의도를 추정하지 마십시오.\n"
        "4. 움직임의 이유를 기사에서 찾을 수 없으면, 찾을 수 없다고 쓰십시오. "
        "억지 해석보다 공백이 낫습니다.\n"
        "5. 한국어 3~4문장, 영어 3~4문장. 과장 없는 평서문으로.\n"
        "6. 가장 움직임이 큰 관계를 먼저 쓰고, 평온한 관계는 마지막에 한 문장으로 묶으십시오.\n"
        "7. 본문에 기사 번호를 쓰지 마십시오. 번호는 'used' 항목에만 넣고, 본문에서는 "
        "기사 내용을 풀어 쓰십시오(예: '(8번 기사)'가 아니라 '평양의 보복 경고 보도').\n"
        "8. 값이 내려간 관계를 '악화'로 쓰지 마십시오. 내려감 = 협력 쪽, 올라감 = 긴장 쪽입니다. "
        "서술의 방향은 기사 제목의 인상이 아니라 수치의 움직임을 따르십시오.\n"
        "9. 각 기사에 붙은 채널 표시를 지키십시오. 공급망 채널 기사로 전체 지수의 움직임을 "
        "설명하거나, 군사 채널 기사로 외교 수치를 설명하지 마십시오. 어느 채널이 움직였는지 "
        "밝히고 그 채널의 기사로만 설명하십시오.\n"
        "10. 서술 대상은 한국과 그 상대국 '사이'의 관계입니다. 기사에 제3국의 비판이나 반응이 "
        "담겨 있어도, 그 제3국의 시각은 지금 설명하는 두 나라 관계의 논조가 아닙니다. "
        "예: 한국이 러시아에 연료를 보낸 데 대해 우크라이나가 비판한 기사는, 한-러 관계에서는 "
        "거래이고 비판은 한-우크라이나 쪽의 일입니다.\n"
        "11. 기사 목록은 영향력의 '크기'(절대값) 순으로 뽑은 것이라, 어떤 채널에서는 나열된 "
        "기사가 그 채널의 최종 수치와 반대 방향일 수 있습니다. 그럴 때는 수치를 기준으로 "
        "서술하고, 반대 방향 기사는 '다만 …라는 보도도 있었다'처럼 덧붙이십시오. "
        "기사 쪽에 맞춰 수치의 방향을 바꿔 쓰지 마십시오.\n\n"
        "아래 JSON 형식으로만 답하십시오. 다른 말은 붙이지 마십시오.\n"
        '{"ko": "한국어 해설", "en": "English commentary", "used": [사용한 기사 번호]}'
    )


def build_commentary(output, dates, top_articles):
    """
    지수와 그 지수를 움직인 기사만을 근거로 짧은 해설을 생성해 output['commentary']에 넣는다.
    실패하면 조용히 건너뛴다 — 해설이 없다고 파이프라인이 망가지면 안 된다.
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("  [commentary] ANTHROPIC_API_KEY 없음 — 해설 생략")
        return None

    numbers, articles = _commentary_inputs(output, dates, top_articles)
    if len(articles) < 3:
        print(f"  [commentary] 제목이 확보된 근거기사 {len(articles)}건 — 너무 적어 생략")
        return None

    try:
        import anthropic
        client_llm = anthropic.Anthropic(api_key=api_key)
        resp = client_llm.messages.create(
            model=COMMENTARY_MODEL,
            max_tokens=900,
            messages=[{"role": "user", "content": _commentary_prompt(numbers, articles)}],
        )
        raw = resp.content[0].text.strip()
        # 모델이 코드펜스를 붙이는 경우 대비
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            raw = raw[raw.find("{"):]
        data = json.loads(raw[raw.find("{"): raw.rfind("}") + 1])
    except Exception as e:
        print(f"  [commentary] 생성 실패, 생략: {e}")
        return None

    # 모델이 돌려준 '번호'를 우리가 가진 기사로 환원한다. 번호가 범위를 벗어나면 버린다.
    used = []
    for n in data.get("used", []):
        try:
            a = articles[int(n) - 1]
        except (ValueError, TypeError, IndexError):
            continue
        used.append({"url": a["url"], "title": a["title"],
                     "actor1": a["actor1"], "actor2": a["actor2"]})

    commentary = {
        "ko": (data.get("ko") or "").strip(),
        "en": (data.get("en") or "").strip(),
        "sources": used,
        "model": COMMENTARY_MODEL,
        "generated_at": datetime.now().isoformat(),
        "basis": "index values and the audited records listed in sources; no external search",
    }
    if not commentary["ko"] or not commentary["en"]:
        print("  [commentary] 본문이 비어 생략")
        return None

    output["commentary"] = commentary
    print(f"  [commentary] 생성 완료 (근거기사 {len(used)}건 인용)")
    return commentary


# 안전장치: 매일 자동 실행되는 쿼리이므로, 실행 전 dry-run으로 예상 처리량을 먼저 확인하고
# 비정상적으로 커지면(=코드 실수 등) 자동 중단한다. 사람이 매번 비용을 신경 쓰지 않아도
# 시스템이 스스로 지키도록 하기 위함. (2026-09-14: 파티션 안 된 테이블을 잘못 참조해
# 무료 쿼터를 소진시킨 사고 이후 추가)
QUERY_COST_GUARD_GB = float(os.environ.get("QUERY_COST_GUARD_GB", "15"))


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


def _job_config():
    """
    실제 쿼리에 거는 하드 캡. _dry_run_check()는 '추정치'를 보지만 추정이 빗나갈 수 있고,
    2026-10-12에 개인 카드를 결제수단으로 등록한 뒤로는 한도를 넘어도 쿼리가 실패하지 않고
    '과금'된다. maximum_bytes_billed를 걸면 한도 초과 시 BigQuery가 쿼리를 거부하므로
    과금 자체가 발생하지 않는다.
    """
    return bigquery.QueryJobConfig(maximum_bytes_billed=int(QUERY_COST_GUARD_GB * 1e9))


GKG_CACHE_FILE = "cyber_cache.json"


def _get_cyber(client, dates):
    """
    GKG(사이버) 쿼리는 BigQuery 스캔량의 97%를 차지한다 — 2026-10-12 실측 12.703 GB/회로,
    2026-09-15의 2.60 GB에서 4.9배 커졌다. 하루 8회 돌리면 월 3.1 TB가 되어 무료 1 TiB를
    열흘 만에 넘긴다.

    사이버 채널은 부록 A.3대로 무방향이고 7일 이동창으로 풀링하며 방향별·채널별 비교에
    들어가지 않는다. 3시간마다 다시 뽑을 이유가 없다. 그래서 UTC 기준 하루 첫 실행에서만
    실제 쿼리하고, 같은 날의 나머지 7회는 직전 결과를 재사용한다 (월 스캔 3.1 TB -> 0.47 TB,
    무료 한도 안).

    날짜창(dates)이 바뀌면 캐시가 어긋나므로 그때는 무조건 다시 조회한다.
    """
    today = datetime.utcnow().strftime("%Y-%m-%d")
    cache = None
    try:
        with open(GKG_CACHE_FILE, "r", encoding="utf-8") as f:
            cache = json.load(f)
    except FileNotFoundError:
        pass
    except Exception as e:
        print(f"  [GKG] 캐시 읽기 실패, 새로 조회: {e}")

    if cache and cache.get("refreshed_date") == today and cache.get("dates") == dates:
        print(f"  [GKG] 오늘({today}) 이미 조회함 — 캐시 재사용, BigQuery 스캔 생략")
        return cache["cyber"], cache["cyber_stats"], False

    if cache and cache.get("refreshed_date") == today:
        print("  [GKG] 날짜창이 바뀌어 캐시를 쓸 수 없음 — 다시 조회")

    _dry_run_check(client, GKG_QUERY, "GKG(사이버) 쿼리")
    rows = list(client.query(GKG_QUERY, job_config=_job_config()).result())
    print(f"  GKG(사이버) 쿼리 결과: {len(rows)}행")
    cyber = build_cyber_domain(rows, dates)
    cyber_stats = compute_cyber_stats(rows)
    try:
        with open(GKG_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump({"refreshed_date": today, "saved_at": datetime.now().isoformat(),
                       "dates": dates, "cyber": cyber, "cyber_stats": cyber_stats},
                      f, ensure_ascii=False, indent=2)
        print(f"  [GKG] 캐시 저장: {GKG_CACHE_FILE}")
    except Exception as e:
        print(f"  [GKG] 캐시 저장 실패(무시, 다음 실행에서 다시 조회됨): {e}")
    return cyber, cyber_stats, True


# ── 감사값 영속화 (2026-10-14 추가) ────────────────────────────────────────
# 계열은 매 회차 BigQuery에서 전부 다시 만들어지고, 감사는 series[-1] 한 칸만
# 고친다. 다음 회차가 돌면 어제의 감사 결과는 사라진다. 아래 세 함수가
# 그것을 날짜별로 적어 두고, 계열을 만든 뒤 과거 날짜에 되살린다.
AUDITED_SERIES_PATH = "audited_series.json"


def _load_audited_series(path=AUDITED_SERIES_PATH):
    """누적 기록을 읽는다. 없거나 깨져 있으면 빈 dict — 패치 전과 같게 동작."""
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except (ValueError, OSError) as e:
        print(f"  ::warning::{path} 를 읽을 수 없습니다 ({e}) — 감사값 복원을 건너뜁니다")
        return {}


def snapshot_raw_series(output):
    """감사 전(측정 그대로) 계열을 series_raw 로 보존한다.

    audit_and_correct() 가 series[-1] 을 고치기 **전에** 불러야 한다.
    """
    for blk in (output.get("domains") or {}).values():
        ser = (blk or {}).get("series")
        if isinstance(ser, dict):
            blk["series_raw"] = {k: list(v) if isinstance(v, list) else v
                                 for k, v in ser.items()}


def record_audited_day(output, latest_date, path=AUDITED_SERIES_PATH):
    """그날 공개된 값(감사 반영 후)을 날짜별로 누적 보존한다.

    같은 날 여러 회차가 돌면 마지막 회차가 이긴다 — 대시보드가 그날 마지막에
    보여준 값과 일치시키기 위함이다.
    """
    if not latest_date:
        return {}
    store = _load_audited_series(path)
    day = {}
    for dom, blk in (output.get("domains") or {}).items():
        for field, val in ((blk or {}).get("current") or {}).items():
            if val is not None:
                day[f"{dom}|{field}"] = val
    if not day:
        return store

    store.setdefault(latest_date, {}).update(day)
    store["_meta"] = {
        "updated_at": datetime.now().isoformat(),
        "note": ("날짜별로 그날 감사를 거쳐 공개된 값. 계열은 매 회차 재계산되므로 "
                 "overlay_audited_series()가 이 기록으로 과거 날짜를 되살린다."),
    }
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(store, f, ensure_ascii=False, indent=1)
    except OSError as e:
        print(f"  ::warning::{path} 저장 실패 ({e}) — 계열 계산에는 영향 없음")
        return store
    n_days = len([k for k in store if not k.startswith("_")])
    print(f"  [audited] {latest_date} 값 {len(day)}개 보존 — {path} (누적 {n_days}일)")
    return store


def overlay_audited_series(output, dates, path=AUDITED_SERIES_PATH):
    """과거 날짜의 계열값을 '그날 감사를 거쳐 공개된 값'으로 되돌린다.

    기록이 없는 날짜(감사 도입 이전 등)는 측정값 그대로 둔다 — 없는 검증을
    있는 것처럼 보이게 하지 않는다.
    """
    store = _load_audited_series(path)
    if not store or not dates:
        return 0
    idx = {d: i for i, d in enumerate(dates)}
    n_cell = 0
    n_day = 0
    for day, vals in store.items():
        if day.startswith("_") or day not in idx or not isinstance(vals, dict):
            continue
        i = idx[day]
        hit = False
        for key, val in vals.items():
            dom, _, field = key.partition("|")
            ser = ((output.get("domains") or {}).get(dom) or {}).get("series") or {}
            arr = ser.get(field)
            if isinstance(arr, list) and len(arr) == len(dates) and arr[i] != val:
                arr[i] = val
                n_cell += 1
                hit = True
        n_day += hit
    # 하위 호환 별칭: output["series"]는 overall 계열과 같은 객체를 가리키므로
    # 위에서 이미 갱신됐지만, 혹시 끊어져 있을 경우를 대비해 다시 묶어 둔다.
    ov = ((output.get("domains") or {}).get("overall") or {}).get("series")
    if isinstance(ov, dict):
        output["series"] = ov
    if n_cell:
        print(f"  [audited] 과거 {n_day}일 / {n_cell}개 셀을 그날의 감사값으로 복원")
    return n_cell


def append_daily_history(output, path="daily_history.json"):
    """
    '전체(overall)' 도메인의 일별 값을 영구적으로 계속 누적하는 파일.
    kbti_output.json의 series는 30일 롤링창이라 오래된 날짜가 매일 밀려나가 사라지지만,
    이 파일은 한 번 기록된 날짜를 절대 지우지 않고 계속 쌓기만 한다.
    -> 대시보드의 30일/3개월/6개월 시간범위 버튼이 이 파일을 사용.
    같은 날짜에 재실행되면(수동 재실행 등) 그 날짜의 값만 최신으로 교체하고 새 항목을 追加하지 않는다.
    """
    dates = output.get("full_dates") or output.get("dates") or []
    series = output.get("series", {})
    if not dates:
        print("  [daily-history] 날짜 없음 — 건너뜀")
        return

    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            hist = json.load(f)
    else:
        hist = {"dates": [], "history": {}}
    hist.setdefault("dates", [])
    hist.setdefault("history", {})
    for k in series.keys():
        hist["history"].setdefault(k, [])

    if not hist["dates"]:
        # 최초 실행 -> 지금 가진 30일 전체를 시드로 채워서 바로 30일치를 확보
        hist["dates"] = list(dates)
        for k, vals in series.items():
            hist["history"][k] = list(vals)
    else:
        today = dates[-1]
        if today == hist["dates"][-1]:
            for k in series.keys():
                if hist["history"].get(k):
                    hist["history"][k][-1] = series[k][-1]
        elif today not in hist["dates"]:
            hist["dates"].append(today)
            for k in series.keys():
                hist["history"].setdefault(k, []).append(series[k][-1])

    hist["updated_at"] = datetime.now().isoformat()
    with open(path, "w", encoding="utf-8") as f:
        json.dump(hist, f, ensure_ascii=False, indent=2)
    print(f"  [daily-history] 누적 {len(hist['dates'])}일치 저장 완료: {path}")


def main():
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M')}] KBTI Pipeline 시작")
    client = bigquery.Client(project=PROJECT_ID)

    _dry_run_check(client, EVENTS_QUERY, "events 쿼리")
    events_rows = list(client.query(EVENTS_QUERY, job_config=_job_config()).result())
    print(f"  events 쿼리 결과: {len(events_rows)}행")
    dates, domains = build_events_domains(events_rows)
    date_labels = [d[5:] for d in dates]
    events_stats = compute_events_stats(events_rows)

    cyber, cyber_stats, gkg_queried = _get_cyber(client, dates)

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
        "level": domains["overall"]["level"],
        # 도메인별 구조 (신규)
        "domains": {
            "overall": domains["overall"],
            "military": domains["military"],
            "supply": domains["supply"],
            "diplomatic": domains["diplomatic"],
            "cyber": cyber,   # 방향 없음: series/current가 국가코드로 바로 매핑됨 (예: cyber.current.PRK)
        },
        # 방법론 기술용 통계치 — 논문에 "N건 검색, M건 유효매칭" 형태로 인용 가능
        "stats": stats,
        "partner_names": NAMES,
        "note": "KBTI = Goldstein(1992,JCR) x NumMentions weighted avg x (-1); cyber = GKG V2Tone-based proxy, no directionality"
    }

    # 감사 전(측정 그대로) 계열을 먼저 떠 둔다 — audit_and_correct()가 series[-1]을 고치기 전.
    snapshot_raw_series(output)

    # 원시(비집계) 오늘 이벤트 조회 — ANTHROPIC_API_KEY 유무와 무관하게 항상 실행.
    # CSV 영구저장 + 대시보드용 "주요 근거기사" 추출 + (키가 있으면) LLM 감사, 세 가지가 이 한 번의 쿼리 결과를 공유.
    latest_date, today_rows, buckets = fetch_raw_daily_events(client)
    if latest_date:
        save_raw_daily_csv(latest_date, today_rows)
        top_articles = compute_top_articles(buckets, latest_date)
        audit_result = audit_and_correct(client, output, dates, latest_date, today_rows, buckets, top_articles)
        record_audited_day(output, latest_date)   # 오늘 공개값을 날짜별 기록에 남긴다
        output["top_articles"] = top_articles
        build_commentary(output, dates, top_articles)
    else:
        print("  [raw] 오늘 매칭되는 원시 이벤트 없음 — CSV·주요기사·감사 모두 건너뜀")
        audit_result = {"enabled": False}
        output["top_articles"] = {}
    output["stats"]["audit"] = audit_result

    # 과거 날짜를 그날의 감사값으로 복원한다. 기록이 없는 날짜는 측정값 그대로.
    overlay_audited_series(output, dates)

    append_daily_history(output)

    with open("kbti_output.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"  저장 완료: kbti_output.json ({len(dates)}일)")
    print(f"  [통계] 30일 윈도우 기준")
    print(f"    전체 매칭 이벤트: {events_stats['total_events_matched']}건 (기사 언급 {events_stats['total_mentions_matched']}건)")
    print(f"    └ 군사 태깅: {events_stats['military_events']}건 / 공급망 태깅: {events_stats['supply_events']}건")
    print(f"    GKG 사이버 매칭 문서: {cyber_stats['total_documents_matched']}건")
    print(f"  현재 위협 지수 (전체, 상대→한국) — 경보 수준은 각 국가쌍 자신의 최근 30일 분포 대비 Z-score 기준:")
    for p in PARTNERS:
        field = f"{p}_threat"
        v = output["domains"]["overall"]["current"][field]
        lvl = output["domains"]["overall"]["level"].get(field)
        print(f"    {NAMES[p]}: {v:+.3f} [{LEVEL_LABEL_KR.get(lvl, lvl)}]")


if __name__ == "__main__":
    main()
