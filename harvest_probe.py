# -*- coding: utf-8 -*-
"""여러 매체를 **한 번에** 재서, 어디를 붙일 수 있는지 표로 돌려준다. 읽기 전용.

왜
--
매체를 하나씩 시험하면 왕복이 너무 많다. 연합 하나에 오늘 아침을 다 썼다.
이 스크립트는 통신사·일본·중국·러시아·한국 매체를 한 회차에 모두 재고, 결과를
한 장의 표로 낸다. 그 표를 보고 **어디에 붙일지 한 번에 고른다.**

RSS 주소를 외워서 넣지 않는다
-----------------------------
내가 기억으로 적으면 틀린다(실측: 연합 후보 9개 중 3개가 404/URLError).
그래서 두 단계로 **찾게** 한다:
  1. 첫 화면의 <link rel="alternate" type="application/rss+xml"> 를 읽는다
  2. 없으면 흔한 경로(/rss, /feed, /rss.xml …)를 차례로 두드린다

무엇을 재나 (매체마다)
----------------------
  robots   : AI 크롤러를 명시적으로 막고 있는가 / Disallow 개수
  피드     : 살아 있는 RSS 주소와 그 안의 기사 수
  상대국   : 제목에 한국의 상대국(북·일·중·미·러)이 나오는 비율
  본문     : 파이프라인 추출기로 실제 본문이 잡히는가
이 넷이 다 되는 매체만 붙일 값어치가 있다.

쓰는 법
-------
    python harvest_probe.py                 # 전부
    PROBE_ONLY=kyodo,xinhua python ...       # 일부만
    PROBE_BODIES=0 python ...                # 본문 시험 생략(빠름)
"""
import os
import re
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/125.0 Safari/537.36")
GAP = float(os.environ.get("PROBE_GAP") or "0.8")
N_BODIES = int(os.environ.get("PROBE_BODIES") or "2")
ONLY = {s.strip().lower() for s in (os.environ.get("PROBE_ONLY") or "").split(",") if s.strip()}

# (키, 표시이름, 첫화면). 피드 주소는 적지 않는다 — 찾게 한다.
SOURCES = [
    # 대조군 — 피드가 있는 것이 확인된 두 곳. 이 둘이 '못 찾음' 으로 나오면
    # 탐침 자체가 고장난 것이므로 나머지 결과를 믿으면 안 된다.
    ("_ctl_yna",   "[대조] 연합뉴스", "https://www.yna.co.kr/"),
    ("_ctl_donga", "[대조] 동아일보", "https://www.donga.com/"),
    # 통신사
    ("ap",        "AP 통신",        "https://apnews.com/"),
    ("reuters",   "Reuters",       "https://www.reuters.com/"),
    ("afp",       "AFP",           "https://www.afp.com/en"),
    ("xinhua",    "신화통신(영문)",   "https://english.news.cn/"),
    ("kyodo",     "교도통신(영문)",   "https://english.kyodonews.net/"),
    ("tass",      "타스(영문)",      "https://tass.com/"),
    # 일본
    ("nhk",       "NHK(영문)",      "https://www3.nhk.or.jp/nhkworld/en/news/"),
    ("asahi",     "아사히(영문)",     "https://www.asahi.com/ajw/"),
    ("mainichi",  "마이니치(영문)",    "https://mainichi.jp/english/"),
    ("japantimes", "재팬타임스",      "https://www.japantimes.co.jp/"),
    # 중국
    ("globaltimes", "환구시보(영문)",  "https://www.globaltimes.cn/"),
    ("chinadaily",  "차이나데일리",    "https://www.chinadaily.com.cn/"),
    ("peoplecn",    "인민일보(영문)",  "http://en.people.cn/"),
    ("cgtn",        "CGTN",        "https://news.cgtn.com/"),
    # 한국 (GDELT 에 없는 것들)
    ("mk",        "매일경제",        "https://www.mk.co.kr/"),
    ("newsis",    "뉴시스",          "https://www.newsis.com/"),
    ("joongang",  "중앙일보",        "https://www.joongang.co.kr/"),
    ("donga",     "동아일보",        "https://www.donga.com/"),
    ("hankyung",  "한국경제",        "https://www.hankyung.com/"),
    ("khan",      "경향신문",        "https://www.khan.co.kr/"),
    ("news1",     "뉴스1",          "https://www.news1.kr/"),
    ("chosun",    "조선일보",        "https://www.chosun.com/"),
    ("kbs",       "KBS",           "https://news.kbs.co.kr/"),
    ("ytn",       "YTN",           "https://www.ytn.co.kr/"),
]

# **최상위만 보면 놓친다.** 1차 탐침(2026-11-23)에서 24곳 중 5곳만 피드를
# 찾았는데, NHK World 처럼 뉴스 섹션이 하위 경로에 있는 사이트는 거기 있는
# RSS 를 못 봤다. 그래서 (1) 최상위와 (2) 주어진 주소의 경로, 두 군데에
# 같은 후보를 대 본다.
COMMON = ["/rss", "/rss/", "/rss.xml", "/rss.html", "/rssindex.html",
          "/feed", "/feed/", "/feeds", "/feeds/", "/index.xml", "/atom.xml",
          "/rss/all.xml", "/rss/news.xml", "/rss/index.xml", "/feeds/all.xml",
          "/en/rss", "/rss/en", "/news/rss", "/rss/feed.xml"]

# RSS 목록을 따로 안내하는 페이지. 거기서 .xml 링크를 긁는다.
# 한국·일본 매체는 'RSS 서비스' 안내 페이지를 두는 곳이 많다.
# 회차 시간을 지키려고 4개로 줄였다. 24곳 × (안내페이지 + 후보) 가 금방 커진다.
HINT_PAGES = ["/rss", "/rss.html", "/rssindex.html", "/rss/index.html"]

AI_BOTS = ["GPTBot", "Google-Extended", "CCBot", "ClaudeBot", "anthropic-ai",
           "PerplexityBot", "Bytespider", "Applebot-Extended", "GoogleOther",
           "Claude-Web", "cohere-ai", "Omgilibot", "FacebookBot"]

# 상대국 낱말. yonhap_feed 와 같은 기준을 쓰되, 영문 매체가 많아 영문을 넉넉히 둔다.
PARTNER_WORDS = {
    # 외신은 'N. Korea' 로 줄여 쓴다. 이걸 빼면 "S. Korea, N. Korea exchange
    # fire" 같은 핵심 제목이 상대국 미검출로 버려진다(2026-11-23 시험).
    "PRK": ["북한", "北", "평양", "김정은", "북핵", "대북", "남북",
            "North Korea", "N. Korea", "N.Korea", "Pyongyang",
            "Kim Jong", "DPRK"],
    "JPN": ["일본", "日", "도쿄", "다카이치", "한일",
            "Japan", "Tokyo", "Takaichi"],
    "CHN": ["중국", "中", "베이징", "시진핑", "한중",
            "China", "Beijing", "Xi Jinping"],
    "USA": ["미국", "美", "워싱턴", "트럼프", "한미",
            "United States", "Washington", "Trump", "U.S."],
    "RUS": ["러시아", "露", "모스크바", "푸틴", "한러",
            "Russia", "Moscow", "Putin"],
}
_SHORT = {"北", "日", "中", "美", "露"}
_NOISE = ["中企", "美術", "美容", "日程", "日課", "北上", "北極"]
# 한국이 나오지 않는 기사는 우리 지수와 무관하다. 외신은 특히 그렇다.
#
# 두 가지를 먼저 처리해야 한다(2026-11-23 시험에서 둘 다 틀렸다):
#  1. 'North Korea' 의 'Korea' 가 한국으로 잡힌다. 북한 국내 기사는 양자 사건이
#     아니므로 북한 표기를 **먼저 지우고** 한국을 찾는다.
#  2. '한중 FTA 타결' 에는 '한국' 이 없다. 한국 기사에서 가장 흔한 표기가
#     '한중·한일·한미·한러' 라서, 이것을 넣지 않으면 핵심 제목이 통째로 빠진다.
KOREA_WORDS = ["한국", "韓", "서울", "한중", "한일", "한미", "한러", "한반도",
               "주한", "방한", "대한민국", "Korea", "Korean", "Seoul", "ROK"]
_NK_FORMS = ["North Korea's", "North Korean", "North Korea",
             "N. Korea", "N.Korea", "DPRK",
             "북한", "조선민주주의인민공화국"]


def _hits(text):
    clean = text
    for n in _NOISE:
        clean = clean.replace(n, " ")
    out = set()
    for p, ws in PARTNER_WORDS.items():
        for w in ws:
            if (w in clean) if w in _SHORT else (w in text):
                out.add(p)
                break
    return out


def _korea(text):
    t = text
    for w in _NK_FORMS:
        t = t.replace(w, " ")
    return any(w in t for w in KOREA_WORDS)


def get(url, timeout=15):
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read()
    except Exception as e:                                   # noqa: BLE001
        return getattr(e, "code", None) or type(e).__name__, None


def robots_summary(home):
    base = "{0.scheme}://{0.netloc}".format(urllib.parse.urlparse(home))
    st, body = get(base + "/robots.txt")
    if not body:
        return f"받지 못함({st})", []
    txt = body.decode("utf-8", "replace")
    blocked = [b for b in AI_BOTS if re.search(rf"User-agent:\s*{re.escape(b)}", txt, re.I)]
    n_dis = len(re.findall(r"^\s*Disallow:", txt, re.M))
    return f"Disallow {n_dis}개", blocked


def _xml_links(html, base):
    """문서 안의 .xml / rss / feed 링크를 긁는다."""
    out = []
    for m in re.finditer(r'href=["\']([^"\']+)["\']', html, re.I):
        h = m.group(1)
        low = h.lower()
        if low.endswith(".xml") or "/rss" in low or "/feed" in low:
            if not low.startswith(("javascript:", "mailto:")):
                out.append(urllib.parse.urljoin(base, h))
    return out


def find_feeds(home, limit=3, max_tries=16):
    """첫 화면·안내 페이지·흔한 경로(최상위와 하위 둘 다)에서 피드를 찾는다."""
    found, tried = [], set()
    p = urllib.parse.urlparse(home)
    base = f"{p.scheme}://{p.netloc}"
    # 주어진 주소의 경로. https://a.com/nhkworld/en/news/ -> /nhkworld/en/news
    sub = p.path.rstrip("/")

    cands = []
    st, body = get(home)
    if body:
        html = body.decode("utf-8", "replace")[:400000]
        # ① <link rel="alternate" type="application/rss+xml">
        for m in re.finditer(
                r'<link[^>]+type=["\']application/(?:rss|atom)\+xml["\'][^>]*>',
                html, re.I):
            h = re.search(r'href=["\']([^"\']+)["\']', m.group(0))
            if h:
                cands.append(urllib.parse.urljoin(home, h.group(1)))
        # ② 첫 화면의 rss/feed 링크 (상위 12개만)
        cands += _xml_links(html, home)[:12]

    # ③ 흔한 경로 — 최상위와 하위 경로 둘 다
    for pref in ([base] + ([base + sub] if sub else [])):
        cands += [pref + c for c in COMMON]

    # ④ RSS 안내 페이지를 열어 거기 걸린 .xml 을 긁는다 (최상위만 — 시간 때문)
    for pref in [base]:
        for hp in HINT_PAGES:
            if len(cands) > 60:
                break
            time.sleep(GAP / 2)
            _, b = get(pref + hp, timeout=10)
            if not b:
                continue
            txt = b.decode("utf-8", "replace")[:300000]
            if "<rss" in txt[:400].lower() or "<feed" in txt[:400].lower():
                cands.insert(0, pref + hp)      # 그 자체가 피드다
                continue
            cands += _xml_links(txt, pref + hp)[:20]

    seen_c = []
    for u in cands:                              # 순서 유지하며 중복 제거
        if u not in seen_c:
            seen_c.append(u)

    for u in seen_c:
        if u in tried or len(found) >= limit or len(tried) >= max_tries:
            continue
        tried.add(u)
        time.sleep(GAP / 2)
        st, b = get(u, timeout=12)
        if not b:
            continue
        items = parse_feed(b)
        if items:
            found.append((u, items))
    if not found:
        print(f"   (후보 {len(seen_c)}개 중 {len(tried)}개 시도 — 모두 피드 아님)")
    return found


def parse_feed(body):
    try:
        root = ET.fromstring(body)
    except ET.ParseError:
        return []
    out = []
    for it in root.iter():
        if it.tag.split("}")[-1] not in ("item", "entry"):
            continue
        t = l = None
        for ch in it:
            c = ch.tag.split("}")[-1]
            if c == "title":
                t = (ch.text or "").strip()
            elif c == "link":
                l = (ch.text or "").strip() or ch.attrib.get("href")
        if t and l:
            out.append((t, l.split("?")[0]))
    return out


def probe(key, name, home, extract):
    print(f"\n── {name}  ({home})", flush=True)
    rb, bots = robots_summary(home)
    print(f"   robots : {rb}" + (f" · AI 크롤러 차단: {', '.join(bots)}" if bots else ""))
    feeds = find_feeds(home)
    if not feeds:
        print("   피드   : 못 찾음")
        return {"name": name, "feed": 0, "items": 0, "kr": 0, "body": None, "bots": bots}
    items = []
    for u, its in feeds:
        print(f"   피드   : {u}  ({len(its)}건)")
        items += its
    seen, uniq = set(), []
    for t, l in items:
        if l not in seen:
            seen.add(l)
            uniq.append((t, l))
    rel = [(t, l) for t, l in uniq if _korea(t) and _hits(t)]
    print(f"   기사   : 고유 {len(uniq)}건 중 한국+상대국 언급 {len(rel)}건"
          f" ({len(rel)/max(1,len(uniq)):.0%})")
    for t, _ in rel[:3]:
        print(f"            · {t[:62]}")
    ok = None
    if extract and N_BODIES and rel:
        got_ok = 0
        for t, l in rel[:N_BODIES]:
            time.sleep(GAP)
            g = extract(l)
            good = bool(g and (g.get("full_text") or ""))
            got_ok += 1 if good else 0
            print(f"   본문   : {'✔' if good else '✘'} {len(g.get('full_text') or '') if g else 0:>5}자  {t[:44]}")
        ok = got_ok / min(N_BODIES, len(rel))
    return {"name": name, "feed": len(feeds), "items": len(uniq),
            "kr": len(rel), "body": ok, "bots": bots}


def main():
    print("매체 일괄 탐침 — 읽기 전용, LLM·BigQuery 호출 0건")
    extract = None
    try:
        import pipeline
        extract = pipeline._extract_article_direct
        print(f"pipeline 추출기 사용 (하한 {pipeline.ARTICLE_MIN_CHARS}자)")
    except Exception as e:                                   # noqa: BLE001
        print(f"pipeline 을 못 불러왔다({type(e).__name__}) — 본문 시험 생략")

    rows = []
    for key, name, home in SOURCES:
        if ONLY and key not in ONLY:
            continue
        try:
            rows.append(probe(key, name, home, extract))
        except Exception as e:                               # noqa: BLE001
            print(f"   오류: {type(e).__name__}: {str(e)[:70]}")
            rows.append({"name": name, "feed": 0, "items": 0, "kr": 0,
                         "body": None, "bots": []})

    print(f"\n{'='*78}\n  정리 — 붙일 수 있는 곳\n{'='*78}")
    print(f"  {'매체':16s}{'피드':>5s}{'기사':>7s}{'한국관련':>9s}{'본문':>7s}  AI크롤러차단")
    for r in sorted(rows, key=lambda x: -x["kr"]):
        body = "-" if r["body"] is None else f"{r['body']:.0%}"
        print(f"  {r['name']:16s}{r['feed']:>5d}{r['items']:>7d}{r['kr']:>9d}{body:>7s}"
              f"  {', '.join(r['bots'][:3]) if r['bots'] else ''}")
    usable = [r for r in rows if r["kr"] and (r["body"] is None or r["body"] > 0)]
    print(f"\n  한국 관련 기사가 실제로 나오는 매체: {len(usable)}곳 / 시험 {len(rows)}곳")
    print(f"  그 합계: 한 회차에 {sum(r['kr'] for r in usable)}건")
    print("\n  주의: 'AI크롤러차단' 이 있는 매체는 기술적으로 되더라도")
    print("        붙일지 말지를 사람이 정해야 한다. 학술 발표물의 자료 출처다.")


if __name__ == "__main__":
    main()
