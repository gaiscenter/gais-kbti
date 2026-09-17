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
    (EventCode IN ('163','1621','061','071')) AS is_supply,
    -- 외교: 군사와 같은 논리 — 이벤트 코드(협력코드만)로 정의하면 구조적으로 항상 긍정 쪽으로
    -- 쏠리는 반대 방향의 편향이 생김. 대신 '정부/외교 행위자(GOV)가 등장하는 모든 이벤트'로
    -- 정의해서, 그 채널이 그날 우호적이었는지 냉각됐는지를 있는 그대로 반영하게 함.
    (Actor1Type1Code = 'GOV' OR Actor1Type2Code = 'GOV' OR Actor1Type3Code = 'GOV'
     OR Actor2Type1Code = 'GOV' OR Actor2Type2Code = 'GOV' OR Actor2Type3Code = 'GOV') AS is_diplomatic
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


def _shrink(raw_value, event_count, k=CONFIDENCE_K):
    """이벤트 수가 적을수록 0에 가깝게 완화된 값을 반환. 이벤트가 0건이면 None."""
    if event_count <= 0:
        return None
    confidence = event_count / (event_count + k)
    return round(raw_value * confidence, 4)


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
    # 전체는 표본이 충분해 일단위(1일) 유지, 군사/공급망/외교는 희소하므로 7일 이동창 적용
    windows = {"all": 1, "mil": 7, "sup": 7, "dip": 7}
    for dom_key, dom_name in [("all", "overall"), ("mil", "military"), ("sup", "supply"), ("dip", "diplomatic")]:
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
  (Actor1Type1Code='MIL' OR Actor1Type2Code='MIL' OR Actor1Type3Code='MIL'
   OR Actor2Type1Code='MIL' OR Actor2Type2Code='MIL' OR Actor2Type3Code='MIL') AS is_military,
  (EventCode IN ('163','1621','061','071')) AS is_supply,
  (Actor1Type1Code='GOV' OR Actor1Type2Code='GOV' OR Actor1Type3Code='GOV'
   OR Actor2Type1Code='GOV' OR Actor2Type2Code='GOV' OR Actor2Type3Code='GOV') AS is_diplomatic
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
AUDIT_MAX_CALLS = 200     # 하루 최대 LLM 호출 수 상한(비용/시간 안전장치) — 희소도메인 확대로 상향


def _extract_article(url, max_chars=2500, timeout=12, _retry=True):
    """
    기사 원문을 구조화해서 추출: 제목 + 본문 문단(<article>/<p> 태그 우선).
    사이드바·관련기사 목록 같은 잡동사니가 같이 긁혀서 원문이 오염되는 것을 막기 위해,
    페이지 전체 텍스트를 그냥 이어붙이지 않고 실제 기사 본문 태그를 우선 사용한다.
    일부 언론사는 단순한 User-Agent를 가진 요청을 봇으로 간주해 차단하므로,
    실제 브라우저에 가까운 헤더를 사용하고, 실패 시 짧은 대기 후 한 번 재시도한다.
    그래도 실패하면 None 반환(감사에서는 원문 없음 -> 보수적으로 통과 처리됨).
    반환: {"title": str|None, "paragraphs": [str,...] (최대 2개), "full_text": str}
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
                time.sleep(2)
                return _extract_article(url, max_chars=max_chars, timeout=timeout, _retry=False)
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
    except Exception as e:
        if _retry:
            # 타임아웃/연결 오류 등도 일시적일 수 있으므로 한 번만 재시도
            time.sleep(2)
            return _extract_article(url, max_chars=max_chars, timeout=timeout, _retry=False)
        return None


def _audit_with_llm(anthropic_client, partner_name_ko, event, article):
    """
    기사 원문(제목+본문)을 Claude(Haiku)에게 보여주고, 실제로 한국-상대국 간 직접 상호작용이 맞는지,
    그리고 GoldsteinScale의 부호(논조)가 실제 내용과 맞는지 엄격하게 검증.
    원문을 못 가져왔거나 API 호출이 실패하면 True(신뢰 유지, 보수적 기본값) 반환 —
    감사 기능 자체의 실패가 파이프라인을 망가뜨리거나 데이터를 과도하게 지우지 않도록 함.
    """
    if not article or not article.get("full_text"):
        return True
    goldstein = event["goldstein"]
    tone_word = "갈등적/부정적" if goldstein < 0 else "협력적/긍정적"
    title_line = f"기사 제목: {article['title']}\n" if article.get("title") else ""
    prompt = (
        f"다음은 GDELT가 \"{event['a1']} -> {event['a2']}\" 간 이벤트로 자동 분류한 기사입니다.\n"
        f"분류된 CAMEO 코드: {event['code']}, GoldsteinScale: {goldstein:+.1f} "
        f"(이 점수는 이 상호작용이 \"{tone_word}\"이라는 뜻입니다. "
        f"음수=갈등/충돌/위협, 양수=협력/지원/우호).\n\n"
        f"{title_line}"
        f"기사 본문 일부: {article['full_text']}\n\n"
        f"아래 두 조건을 모두 엄격하게 확인하세요:\n"
        f"1) 이 기사가 실제로 한국과 {partner_name_ko} 두 국가(정부/국가급 행위자)의 "
        f"직접적인 상호작용을 다루고 있는가 (제3국 사건에 곁가지로 언급된 게 아니라)\n"
        f"2) 기사의 실제 논조가 \"{tone_word}\" 방향과 일치하는가. "
        f"예를 들어 의료지원·인도적 지원·경제협력·정상회담처럼 실제로는 우호적인 내용인데 "
        f"부정적 코드가 붙었거나, 반대로 위협·제재·충돌 내용인데 긍정적 코드가 붙었다면 "
        f"이건 불일치이므로 반드시 \"아니오\"로 답하세요.\n\n"
        f"두 조건이 모두 참일 때만 \"예\", 하나라도 거짓이거나 애매하면 \"아니오\"로 답하세요. "
        f"다른 설명 없이 \"예\" 또는 \"아니오\"로만 답하세요."
    )
    try:
        resp = anthropic_client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=10,
            messages=[{"role": "user", "content": prompt}],
        )
        answer = resp.content[0].text.strip()
        return answer.startswith("예")
    except Exception as e:
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
    raw_rows = list(client.query(RAW_AUDIT_QUERY).result())
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
                r["GoldsteinScale"], r["NumMentions"], r["is_military"], r["is_supply"],
                r["is_diplomatic"], r["SOURCEURL"],
            ])
    print(f"  [raw-csv] {len(today_rows)}건 저장 완료: {filepath}")
    return filepath


def compute_top_articles(buckets, top_n=2, exclude_urls=None):
    """
    도메인·파트너·방향별 |영향력| 최상위 top_n건을 대시보드 표시용으로 추출.
    exclude_urls가 주어지면(감사에서 오분류로 제외된 URL 집합), 그 URL들은
    "근거기사"로 다시 추천되지 않도록 후보에서 뺀다 -> 이미 값 계산에서 제외된 기사가
    "이 수치의 근거"라고 잘못 표시되는 걸 방지.
    (제목은 감사 단계에서 fetch된 것만 나중에 덧붙여짐; 여기서는 URL·행위자·수치만)
    """
    exclude_urls = exclude_urls or set()
    dom_key_map = {"all": "overall", "mil": "military", "sup": "supply", "dip": "diplomatic"}
    result = defaultdict(dict)  # dom_name -> field -> [ {url, goldstein, mentions, a1, a2}, ... ]
    for (dom_key, partner, direction), evs in buckets.items():
        candidates = [e for e in evs if e["url"] not in exclude_urls]
        candidates.sort(key=lambda e: abs(e["impact"]), reverse=True)
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
             "actor1": e["a1"], "actor2": e["a2"], "event_code": e["code"], "title": None}
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

    urls = list(to_check.keys())[:AUDIT_MAX_CALLS]
    print(f"  [audit] 검증 대상 URL {len(urls)}건 (중복 제거 후, 최대 {AUDIT_MAX_CALLS}건)")

    fetched_titles = {}  # url -> title (top_articles에 나중에 덧붙이기 위함)
    bad_urls = set()
    checked = 0
    audit_log = []  # 통과("예")/제외("아니오") 전부 기록 — 나중에 재검토·논문 인용용
    for url in urls:
        ev = to_check[url]
        article = _extract_article(url)
        ok = _audit_with_llm(client_llm, NAMES.get(ev["partner"], ev["partner"]), ev, article)
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
            "article_fetched": article is not None,
            # 제목 + 본문 앞 2문단만 영구 보존(사이드바 등 잡동사니 제외) — link rot 대비
            "title": (article or {}).get("title"),
            "paragraph_1": paras[0] if len(paras) > 0 else None,
            "paragraph_2": paras[1] if len(paras) > 1 else None,
        })
        if not ok:
            bad_urls.add(url)
            print(f"    [audit] 제외: {ev['a1']}->{ev['a2']} code={ev['code']} G={ev['goldstein']} url={url}")

    print(f"  [audit] 검증 완료: {checked}건 확인, {len(bad_urls)}건 오분류로 제외")

    # 근거기사(top_articles)를 "제외된 URL 빼고" 다시 계산 -> 이미 값 계산에서 제외된 기사가
    # 여전히 "이 수치의 근거"로 표시되는 문제를 방지. (교체된 새 후보는 감사 대상이 아니었을 수
    # 있어 제목이 없을 수 있음 -> 프론트엔드가 "행위자1→행위자2(코드)" 형태로 대체 표시함)
    refreshed = compute_top_articles(buckets, exclude_urls=bad_urls)
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
    skipped_empty_count = 0
    for (dom_key, partner, direction), evs in buckets.items():
        clean = [e for e in evs if e["url"] not in bad_urls]
        if len(clean) == len(evs):
            continue  # 이 조합에서 제외된 게 없으면 손댈 필요 없음
        m_sum = sum(e["mentions"] for e in clean)
        e_count = len(clean)
        corrected = _shrink((sum(e["impact"] for e in clean) / m_sum * -1.0), e_count) if m_sum > 0 else None

        if corrected is None:
            # 감사로 이 버킷의 이벤트가 전부(또는 사실상 전부) 제외되어 "남은 값이 없는" 상태.
            # 이 경우 0.0(중립)으로 덮어쓰면 "데이터 없음"과 "긴장도 정확히 0"을 혼동시켜 오해를 준다.
            # -> 보정을 적용하지 않고 감사 전 원래 값을 그대로 둔다 (섣불리 0으로 만들지 않음).
            skipped_empty_count += 1
            continue

        dom_name = dom_key_map[dom_key]
        field = f"{partner}_{direction}"
        series = output["domains"][dom_name]["series"].get(field)
        if series and dates and dates[-1] == latest_date:
            series[-1] = corrected
            output["domains"][dom_name]["current"][field] = corrected
            if dom_name == "overall":
                output["series"][field][-1] = corrected
                output["current"][field] = corrected
            corrected_count += 1

    print(f"  [audit] {corrected_count}개 (도메인,국가쌍,방향) 조합의 오늘 값을 보정했습니다"
          + (f" ({skipped_empty_count}개는 감사 후 남은 이벤트가 없어 원래 값 유지)" if skipped_empty_count else ""))
    return {
        "enabled": True,
        "date_audited": latest_date,
        "checked": checked,
        "flagged": len(bad_urls),
        "flagged_urls": list(bad_urls),
        "corrections_applied": corrected_count,
        "audit_log_file": "audit_log.json",
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
            "diplomatic": domains["diplomatic"],
            "cyber": cyber,   # 방향 없음: series/current가 국가코드로 바로 매핑됨 (예: cyber.current.PRK)
        },
        # 방법론 기술용 통계치 — 논문에 "N건 검색, M건 유효매칭" 형태로 인용 가능
        "stats": stats,
        "partner_names": NAMES,
        "note": "KBTI = Goldstein(1992,JCR) x NumMentions weighted avg x (-1); cyber = GKG V2Tone-based proxy, no directionality"
    }

    # 원시(비집계) 오늘 이벤트 조회 — ANTHROPIC_API_KEY 유무와 무관하게 항상 실행.
    # CSV 영구저장 + 대시보드용 "주요 근거기사" 추출 + (키가 있으면) LLM 감사, 세 가지가 이 한 번의 쿼리 결과를 공유.
    latest_date, today_rows, buckets = fetch_raw_daily_events(client)
    if latest_date:
        save_raw_daily_csv(latest_date, today_rows)
        top_articles = compute_top_articles(buckets)
        audit_result = audit_and_correct(client, output, dates, latest_date, today_rows, buckets, top_articles)
        output["top_articles"] = top_articles
    else:
        print("  [raw] 오늘 매칭되는 원시 이벤트 없음 — CSV·주요기사·감사 모두 건너뜀")
        audit_result = {"enabled": False}
        output["top_articles"] = {}
    output["stats"]["audit"] = audit_result

    append_daily_history(output)

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
