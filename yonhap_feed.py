# -*- coding: utf-8 -*-
"""연합뉴스 RSS 에서 한국-상대국 기사를 골라 온다.

왜 필요한가
-----------
GDELT 에 연합뉴스가 30일간 **0건**이다. BigQuery(events/mentions/GKG)와 DOC API
두 경로로 확인했다(2026-11-23). 연합은 한국 외교 기사의 1차 출처이고 다른 매체가
받아쓰는 곳이라, 빠지면 사건 자체를 놓친다 — 중국의 IAEA 핵잠 이의 제기가 그랬다.

무엇을 하지 않는가
------------------
코드북도 집계도 건드리지 않는다. 전수코딩은 GDELT 코딩에 기대지 않고 기사에서
다시 코딩하므로(llm_coder 독스트링), 출처가 늘어도 아래로는 아무것도 바뀌지 않는다.
이 모듈은 **(url, title, p1, p2, mentions, published)** 를 만들어 넘길 뿐이다.

무엇을 고르나
-------------
국제·북한 피드만 본다. 연합은 하루 400건 가까이 내는데 대부분 국내 기사다
(에이티즈 빌보드, 공정위 제재, 부산 관광업). 전부 코딩하면 비용이 5배가 되면서
대부분을 버린다. 거기에 **상대국이 제목이나 머리글에 나오는 것**만 남긴다.

mentions 는 1 로 둔다
---------------------
GDELT 는 NumMentions(몇 개 매체가 보도했나)를 주고 그것이 지수의 증거량 e 로
들어간다. RSS 에는 그 숫자가 없다. 1 로 두면 수축계수 e/(e+5) 가 0.167 배로
세게 깎으므로 **과대평가는 생기지 않는다.** 연합 단독 보도가 과소평가되는 쪽인데,
둘 중에서는 이쪽이 안전하다. 바꾸려면 MENTIONS_DEFAULT 를 조정할 것.

쓰는 법
-------
    python yonhap_feed.py        # 무엇이 들어오는지 보고만 한다(수집·추출 시험)
    from yonhap_feed import collect
    arts = collect(extract=pipeline._extract_article_direct)
"""
import os
import re
import time
import urllib.request
import xml.etree.ElementTree as ET

FEEDS = [
    ("국제", "https://www.yna.co.kr/rss/international.xml"),
    ("북한", "https://www.yna.co.kr/rss/northkorea.xml"),
]
# 영문판도 같은 사건을 따로 낸다. 중복이 늘고 얻는 것은 적어 기본은 끈다.
FEEDS_EN = [("영문", "https://en.yna.co.kr/RSS/news.xml")]

MENTIONS_DEFAULT = int(os.environ.get("YONHAP_MENTIONS") or "1")
GAP = float(os.environ.get("YONHAP_GAP") or "1.0")
MAX_FETCH = int(os.environ.get("YONHAP_MAX") or "60")   # 한 회차 본문 수집 상한
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/125.0 Safari/537.36")

# 상대국 선별. 한국은 당연히 나오므로 **상대국**만 본다.
# 국호뿐 아니라 수도·지도자·통상 약칭까지 넣는다 — '시진핑 주석과 회담' 처럼
# 국호 없이 쓰는 제목이 흔하다.
PARTNER_WORDS = {
    "PRK": ["북한", "北", "평양", "김정은", "김여정", "조선중앙", "노동신문",
            "북핵", "북미", "대북", "남북",
            "North Korea", "Pyongyang", "Kim Jong"],
    "JPN": ["일본", "日", "도쿄", "다카이치", "자위대", "주한일본",
            "Japan", "Tokyo", "Takaichi"],
    "CHN": ["중국", "中", "베이징", "시진핑", "왕이", "한중",
            "China", "Beijing", "Xi Jinping"],
    "USA": ["미국", "美", "워싱턴", "트럼프", "주한미군", "한미",
            "United States", "Washington", "Trump"],
    "RUS": ["러시아", "露", "모스크바", "푸틴", "라브로프", "한러",
            "Russia", "Moscow", "Putin"],
}
# 한 글자 한자 약칭(北·日·中·美·露).
#
# 처음에는 '앞뒤가 한글이면 세지 않는다' 로 막았는데 **거꾸로였다.** 한국 기사에서
# 가장 흔한 표기가 바로 그 모양이다 — 北함정, 美개입, 日도, 中 핵잠. 그래서
# 'ᆨ서해서 北함정' 같은 핵심 제목이 통째로 걸러졌다(2026-11-23 시험).
# 반대로 내가 걱정한 오탐(미인대회·중이염)은 한자가 아니라 한글이라 애초에 안 걸린다.
#
# 그래서 인접 규칙은 버리고, 국호가 아닌 것이 분명한 한자 합성어만 먼저 지운다.
_SHORT = {"北", "日", "中", "美", "露"}
_HANJA_NOISE = ["中企", "中企부", "美術", "美容", "美食", "日程", "日課",
                "日常", "北上", "北極", "南北韓"]


def _get(url, timeout=20):
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read()
    except Exception:                                        # noqa: BLE001
        return None


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
                d = (ch.text or "").strip()
        if t and l:
            out.append({"title": t, "url": l.split("?")[0], "pub": d})
    return out


def partners_in(text):
    """이 글이 어느 상대국을 말하는가. 없으면 빈 집합."""
    clean = text
    for noise in _HANJA_NOISE:
        clean = clean.replace(noise, " ")
    hits = set()
    for p, words in PARTNER_WORDS.items():
        for w in words:
            if (w in clean) if w in _SHORT else (w in text):
                hits.add(p)
                break
    return hits


def _pub_date(s):
    """RSS 날짜에서 YYYY-MM-DD 만 뽑는다. 못 읽으면 None."""
    if not s:
        return None
    m = re.search(r"(\d{1,2})\s+(\w{3})\s+(\d{4})", s)
    if m:
        mon = {"jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
               "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11,
               "dec": 12}.get(m.group(2)[:3].lower())
        if mon:
            return f"{m.group(3)}-{mon:02d}-{int(m.group(1)):02d}"
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", s)
    return m.group(0) if m else None


def collect(extract=None, english=False, max_fetch=None, verbose=True):
    """상대국이 언급된 연합 기사를 모아 파이프라인이 쓰는 모양으로 돌려준다.

    extract 는 pipeline._extract_article_direct 를 넘긴다. 없으면 본문 없이
    제목만 돌려주므로 코딩에는 쓸 수 없다(수집량 확인용).
    """
    max_fetch = MAX_FETCH if max_fetch is None else max_fetch
    feeds = FEEDS + (FEEDS_EN if english else [])
    seen, cand = set(), []
    for name, url in feeds:
        body = _get(url)
        if not body:
            if verbose:
                print(f"  [yonhap] {name} 피드 실패: {url}")
            continue
        items = _parse(body)
        kept = 0
        for it in items:
            if it["url"] in seen:
                continue
            seen.add(it["url"])
            ps = partners_in(it["title"])
            if not ps:
                continue
            it["partners"] = sorted(ps)
            it["feed"] = name
            cand.append(it)
            kept += 1
        if verbose:
            print(f"  [yonhap] {name}: {len(items)}건 중 상대국 언급 {kept}건")
        time.sleep(GAP)

    if verbose:
        print(f"  [yonhap] 후보 {len(cand)}건 (상한 {max_fetch})")
    if not extract:
        return cand

    out = []
    for it in cand[:max_fetch]:
        time.sleep(GAP)
        got = extract(it["url"])
        if not got or not (got.get("title") or "").strip():
            continue
        ps = (got.get("paragraphs") or []) + ["", ""]
        out.append({
            "url": it["url"], "title": got["title"],
            "p1": ps[0], "p2": ps[1],
            "mentions": MENTIONS_DEFAULT,
            "published": got.get("published") or _pub_date(it.get("pub")),
            "source": "yonhap",
        })
    if verbose:
        print(f"  [yonhap] 본문 확보 {len(out)}/{min(len(cand), max_fetch)}건")
    return out


def main():
    print("연합 수집 시험 — 읽기 전용, LLM 호출 0건\n")
    extract = None
    try:
        import pipeline
        extract = pipeline._extract_article_direct
        print(f"  pipeline 추출기 사용 · 하한 {pipeline.ARTICLE_MIN_CHARS}자 "
              f"(타임스탬프 시작은 {pipeline.ARTICLE_MIN_CHARS_STAMPED}자)\n")
    except Exception as e:                                   # noqa: BLE001
        print(f"  pipeline 을 못 불러왔다({type(e).__name__}) — 제목만 봅니다\n")

    arts = collect(extract=extract)
    print(f"\n{'='*70}\n  들어올 기사\n{'='*70}")
    from collections import Counter
    c = Counter()
    for a in arts[:40]:
        ps = sorted(partners_in(a["title"]))
        for p in ps:
            c[p] += 1
        print(f"  [{','.join(ps):11s}] {a.get('published') or '?':10s} {a['title'][:52]}")
    if len(arts) > 40:
        print(f"  … 외 {len(arts)-40}건")
    print(f"\n  상대국별: {dict(c.most_common())}")
    print(f"  총 {len(arts)}건 · mentions 는 {MENTIONS_DEFAULT} 로 들어간다")


if __name__ == "__main__":
    main()
