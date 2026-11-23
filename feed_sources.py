# -*- coding: utf-8 -*-
"""GDELT 가 수집하지 않는 매체를 RSS 로 직접 받아 전수코딩에 넣는다.

왜 필요한가
-----------
GDELT 에 연합뉴스·로이터·AP·AFP·매경·뉴시스·중앙·동아가 **30일간 0건**이다.
BigQuery(events/mentions/GKG)와 DOC API 두 경로로 확인했다(2026-11-23).
연합은 한국 외교 기사의 1차 출처이고 다른 매체가 받아쓰는 곳이라, 빠지면
사건 자체를 놓친다 — 중국의 IAEA 핵잠 이의 제기가 그랬다.
조기경보 체계에서 통신사가 빠지는 것은 치명적이다.

아래로는 아무것도 바뀌지 않는다
-------------------------------
코드북도 집계도 건드리지 않는다. 전수코딩은 GDELT 의 EventCode·GoldsteinScale·
Actor 코딩을 쓰지 않고 **기사 본문에서 다시 코딩한다**(llm_coder 독스트링).
GDELT 에서 받는 것은 URL 과 mentions 둘뿐이다. 그래서 출처가 늘어도
집계 수식·코드북·build_index 는 한 줄도 바뀌지 않는다.

날짜 기준 — 여기가 제일 중요하다
--------------------------------
GDELT 의 하루는 **UTC** 다. RSS 는 발행시각을 현지 시간대로 준다.
변환하지 않고 날짜만 잘라 쓰면 KST 09:00 이전 기사가 하루 밀려 들어가
GDELT 자료와 다른 날에 쌓인다. 그래서 발행시각을 **UTC 로 변환한 뒤**
그날 것만 고른다.

mentions 를 1 로 두는 이유
--------------------------
GDELT 의 NumMentions 는 '그 사건을 보도한 매체 수'이고, 가중평균의 가중치로
들어간다. RSS 에는 그 숫자가 없다. 1 은 '우리가 관측한 보도 매체가 하나'라는
뜻이고 이것이 정직한 값이다. 증거 수 e 에는 온전히 1 을 더하므로 수축계수는
완화되고, 가중평균에서는 작게 잡힌다 — 과대평가가 생기지 않는 쪽이다.
같은 사건을 GDELT 쪽 매체도 보도했다면 URL 이 달라 각각 1건으로 센다.
이는 기존 설계와 같다(매체가 많을수록 증거가 많다).
"""
import email.utils
import os
import re
import time
import urllib.request
import xml.etree.ElementTree as ET
from datetime import timezone

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/125.0 Safari/537.36")
GAP = float(os.environ.get("FEED_GAP") or "0.8")
MENTIONS_DEFAULT = int(os.environ.get("FEED_MENTIONS") or "1")
# 한 회차에 본문을 받아 볼 최대 건수. 파이프라인 예산을 지키기 위한 상한이다.
MAX_FETCH = int(os.environ.get("FEED_MAX_FETCH") or "40")
ENABLED = {s.strip() for s in (os.environ.get("FEED_SOURCES")
                               or "yonhap").split(",") if s.strip()}

# (키, 표시이름, 피드 목록). harvest_probe.py 로 확인된 것만 넣는다.
# 주소를 기억으로 적지 말 것 — 연합 후보 9개 중 3개가 404 였다(실측).
# korea_implied: 한국 매체의 국제·북한·정치 지면은 **한국 독자에게 쓰는 글**이라
# 제목에 '한국' 을 적지 않는다. "서해서 '해양경찰' 내건 北함정" 이 그 예다.
# 한국어 매체에는 이 표시를 켜서, 상대국만 걸려도 받는다. 외신은 끈다 —
# 로이터의 'China, Japan hold talks' 까지 받으면 우리와 무관한 기사가 쏟아진다.
SOURCES = {
    "yonhap": ("연합뉴스", [
        "https://www.yna.co.kr/rss/international.xml",
        "https://www.yna.co.kr/rss/northkorea.xml",
        "https://www.yna.co.kr/rss/politics.xml",
    ], True),
}

# ── 상대국 선별 ──────────────────────────────────────────────────────────
# 연합은 하루 400건 가까이 내는데 대부분 국내 기사다. 전부 코딩하면 비용이
# 5배가 되면서 대부분을 버린다. 한국과 상대국이 **둘 다** 걸린 것만 남긴다.
PARTNER_WORDS = {
    "PRK": ["북한", "北", "평양", "김정은", "김여정", "북핵", "대북", "남북",
            "조선중앙", "노동신문", "North Korea", "N. Korea", "Pyongyang",
            "Kim Jong", "DPRK"],
    "JPN": ["일본", "日", "도쿄", "다카이치", "한일", "주한일본", "자위대",
            "Japan", "Tokyo", "Takaichi"],
    "CHN": ["중국", "中", "베이징", "시진핑", "왕이", "한중",
            "China", "Beijing", "Xi Jinping"],
    "USA": ["미국", "美", "워싱턴", "트럼프", "한미", "주한미군",
            "United States", "Washington", "Trump"],
    "RUS": ["러시아", "露", "모스크바", "푸틴", "라브로프", "한러",
            "Russia", "Moscow", "Putin"],
}
_SHORT = {"北", "日", "中", "美", "露"}
# 국호가 아닌 것이 분명한 한자 합성어. 한 글자 약칭을 찾기 전에 지운다.
_HANJA_NOISE = ["中企", "美術", "美容", "美食", "日程", "日課", "日常",
                "北上", "北極", "南北韓"]
# '한중·한일' 에는 '한국' 이 없다. 한국 기사에서 가장 흔한 표기라 넣어야 한다.
KOREA_WORDS = ["한국", "韓", "서울", "한중", "한일", "한미", "한러", "한반도",
               "주한", "방한", "대한민국", "Korea", "Korean", "Seoul", "ROK",
               # 남북 접경·해역. 국호 없이 이것만 쓰는 제목이 많다.
               "서해", "동해", "NLL", "판문점", "DMZ", "비무장지대", "휴전선",
               "연평", "백령", "개성", "금강산"]
# 'North Korea' 의 'Korea' 가 한국으로 잡힌다. 먼저 지우고 한국을 찾는다.
_NK_FORMS = ["North Korea's", "North Korean", "North Korea", "N. Korea",
             "N.Korea", "DPRK", "북한", "조선민주주의인민공화국"]


def partners_in(text):
    clean = text
    for n in _HANJA_NOISE:
        clean = clean.replace(n, " ")
    out = set()
    for p, ws in PARTNER_WORDS.items():
        for w in ws:
            if (w in clean) if w in _SHORT else (w in text):
                out.add(p)
                break
    return out


def mentions_korea(text):
    t = text
    for w in _NK_FORMS:
        t = t.replace(w, " ")
    return any(w in t for w in KOREA_WORDS)


def relevant(title, korea_implied=False):
    """상대국이 걸리고, 한국이 걸리거나(또는 한국 매체면) 받는다.

    korea_implied 는 비용과 누락 사이의 선택이다. 켜면 '김정은, 평양서 공장
    시찰' 같은 북한 국내 기사까지 모델에 가서 '관련 없음' 판정을 받는다 —
    한 건에 1센트도 안 된다. 끄면 "서해서 北함정" 같은 **진짜 양자 사건**을
    놓친다. 조기경보에서는 놓치는 쪽이 훨씬 비싸다.
    """
    if not partners_in(title):
        return False
    return korea_implied or mentions_korea(title)


# ── 피드 읽기 ────────────────────────────────────────────────────────────
def _get(url, timeout=20):
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read()
    except Exception:                                        # noqa: BLE001
        return None


def utc_date(pub):
    """RSS 발행시각 문자열 -> 'YYYY-MM-DD' (UTC). 못 읽으면 None.

    **시간대 변환이 핵심이다.** 'Mon, 23 Nov 2026 06:15:00 +0900' 은
    UTC 로는 11-22 다. 변환 없이 날짜만 자르면 GDELT 와 하루가 어긋난다.
    """
    if not pub:
        return None
    try:
        dt = email.utils.parsedate_to_datetime(pub)
    except (TypeError, ValueError):
        m = re.search(r"(\d{4})-(\d{2})-(\d{2})", pub)
        return m.group(0) if m else None
    if dt is None:
        return None
    if dt.tzinfo is None:                 # 시간대가 없으면 UTC 로 본다
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d")


def _parse(body):
    try:
        root = ET.fromstring(body)
    except ET.ParseError:
        return []
    out = []
    for it in root.iter():
        if it.tag.split("}")[-1] not in ("item", "entry"):
            continue
        t = l = d = None
        for ch in it:
            c = ch.tag.split("}")[-1]
            if c == "title":
                t = (ch.text or "").strip()
            elif c == "link":
                l = (ch.text or "").strip() or ch.attrib.get("href")
            elif c in ("pubDate", "published", "updated"):
                d = d or (ch.text or "").strip()
        if t and l:
            out.append({"title": t, "url": l.split("?")[0], "pub": d})
    return out


def collect(target_date, extract=None, exclude=None, max_fetch=None, verbose=True):
    """target_date(UTC, 'YYYY-MM-DD') 의 상대국 기사를 파이프라인 모양으로 돌려준다.

    extract 는 pipeline._extract_article_direct 를 넘긴다.
    exclude 는 이미 GDELT 로 받은 URL 집합 — 같은 기사를 두 번 코딩하지 않는다.
    """
    exclude = exclude or set()
    max_fetch = MAX_FETCH if max_fetch is None else max_fetch
    seen, cand = set(), []
    for key in sorted(ENABLED):
        if key not in SOURCES:
            if verbose:
                print(f"  [feed] ::warning::알 수 없는 출처 '{key}' — 건너뜀")
            continue
        name, feeds, korea_implied = SOURCES[key]
        n_items = n_day = n_rel = 0
        for url in feeds:
            body = _get(url)
            time.sleep(GAP)
            if not body:
                if verbose:
                    print(f"  [feed] {name} 피드 실패: {url}")
                continue
            for it in _parse(body):
                n_items += 1
                if it["url"] in seen or it["url"] in exclude:
                    continue
                d = utc_date(it.get("pub"))
                if d != target_date:          # UTC 기준 그날 것만
                    continue
                n_day += 1
                if not relevant(it["title"], korea_implied):
                    continue
                n_rel += 1
                seen.add(it["url"])
                it["source"] = key
                cand.append(it)
        if verbose:
            print(f"  [feed] {name}: 항목 {n_items}건 · {target_date}(UTC) {n_day}건 "
                  f"· 상대국 언급 {n_rel}건")

    if not extract:
        return cand

    out = []
    for it in cand[:max_fetch]:
        time.sleep(GAP)
        got = extract(it["url"])
        if not got or not (got.get("title") or "").strip():
            continue
        ps = (got.get("paragraphs") or []) + ["", ""]
        out.append({"url": it["url"], "title": got["title"],
                    "p1": ps[0], "p2": ps[1],
                    "mentions": MENTIONS_DEFAULT,
                    "published": got.get("published") or target_date,
                    "feed_source": it["source"]})
    if verbose:
        print(f"  [feed] 본문 확보 {len(out)}/{min(len(cand), max_fetch)}건 "
              f"→ 전수코딩에 추가")
    return out


def main():
    import sys
    from datetime import datetime
    target = sys.argv[1] if len(sys.argv) > 1 else \
        datetime.now(timezone.utc).strftime("%Y-%m-%d")
    print(f"외부 피드 수집 시험 — 대상 {target} (UTC) · 출처 {sorted(ENABLED)}\n")
    extract = None
    try:
        import pipeline
        extract = pipeline._extract_article_direct
        print(f"  pipeline 추출기 사용 (하한 {pipeline.ARTICLE_MIN_CHARS}자)\n")
    except Exception as e:                                   # noqa: BLE001
        print(f"  pipeline 을 못 불러왔다({type(e).__name__}) — 제목만 봅니다\n")
    arts = collect(target, extract=extract)
    print(f"\n{'='*70}\n  결과 {len(arts)}건\n{'='*70}")
    for a in arts:
        ps = sorted(partners_in(a["title"]))
        print(f"  [{','.join(ps):11s}] {a['title'][:56]}")


if __name__ == "__main__":
    main()
