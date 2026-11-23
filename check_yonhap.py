# -*- coding: utf-8 -*-
"""연합뉴스를 우리가 직접 받아올 수 있는지 **실제로** 시험한다. 읽기 전용.

왜
--
GDELT 에 연합뉴스가 30일간 0건임을 두 경로(BigQuery·DOC API)로 확인했다.
그러면 밖에서 받아오는 수밖에 없는데, GDELT 가 못 긁는 이유가 차단이라면
우리 수집기도 똑같이 막힌다. 수집기를 다 만든 뒤에 못 쓰는 것을 아는 일은
피한다 — 먼저 잰다.

무엇을 재나
-----------
1. robots.txt — 받아와서 **그대로 보여준다**. 학술 발표물에 들어갈 자료이므로
   사이트가 무엇을 허용하는지 사람이 직접 읽고 판단해야 한다.
2. RSS 주소 — 외워서 넣으면 틀린다. 후보를 여러 개 두고 **되는 것을 찾는다.**
3. 본문 추출 — 파이프라인이 이미 쓰는 _extract_article_direct 를 그대로 불러
   쓴다. 새로 만들면 실제 성공률과 달라진다.
4. 하루 몇 건이 들어오는지 — 붙일 값어치가 있는지 판단할 숫자.

쓰는 법
-------
    python check_yonhap.py            # 전부 시험
    YONHAP_BODIES=5 python ...        # 본문 추출 시험 건수(기본 3)
"""
import os
import re
import sys
import time
import urllib.request
import xml.etree.ElementTree as ET
from collections import Counter

N_BODIES = int(os.environ.get("YONHAP_BODIES") or "3")
GAP = float(os.environ.get("YONHAP_GAP") or "1.5")
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/125.0 Safari/537.36")

# 후보 주소. 되는 것을 찾는 것이 목적이므로 넉넉히 넣는다.
FEEDS = [
    ("한국어·전체",   "https://www.yna.co.kr/rss/news.xml"),
    ("한국어·정치",   "https://www.yna.co.kr/rss/politics.xml"),
    ("한국어·국제",   "https://www.yna.co.kr/rss/international.xml"),
    ("한국어·북한",   "https://www.yna.co.kr/rss/northkorea.xml"),
    ("한국어·경제",   "https://www.yna.co.kr/rss/economy.xml"),
    ("한국어(rss.)",  "https://rss.yna.co.kr/rss/news.xml"),
    ("영문·전체",     "https://en.yna.co.kr/RSS/news.xml"),
    ("영문·북한",     "https://en.yna.co.kr/RSS/northkorea.xml"),
    ("영문·정치",     "https://en.yna.co.kr/RSS/politics.xml"),
]

ROBOTS = ["https://www.yna.co.kr/robots.txt", "https://en.yna.co.kr/robots.txt"]


def get(url, timeout=20):
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read()
    except Exception as e:                                   # noqa: BLE001
        code = getattr(e, "code", None)
        return (code or type(e).__name__), None


def show_robots():
    print(f"\n{'='*74}\n  1. robots.txt — 사이트가 무엇을 허용하는지\n{'='*74}")
    for u in ROBOTS:
        st, body = get(u)
        print(f"\n  ── {u}  (HTTP {st}) ──")
        if not body:
            print("     받지 못했다")
            continue
        txt = body.decode("utf-8", "replace")
        for line in txt.splitlines()[:40]:
            if line.strip():
                print("     " + line.rstrip()[:96])
        if len(txt.splitlines()) > 40:
            print("     … (생략)")


def parse_feed(body):
    """RSS/Atom 에서 (제목, 링크, 발행일) 을 뽑는다."""
    try:
        root = ET.fromstring(body)
    except ET.ParseError as e:
        return None, f"XML 파싱 실패: {str(e)[:60]}"
    items = []
    for it in root.iter():
        tag = it.tag.split("}")[-1]
        if tag not in ("item", "entry"):
            continue
        t = l = d = None
        for ch in it:
            c = ch.tag.split("}")[-1]
            if c == "title":
                t = (ch.text or "").strip()
            elif c == "link":
                l = (ch.text or "").strip() or ch.attrib.get("href")
            elif c in ("pubDate", "published", "updated", "date"):
                d = (ch.text or "").strip()
        if t and l:
            items.append((t, l, d))
    return items, None


def check_feeds():
    print(f"\n{'='*74}\n  2. RSS 주소 — 어느 것이 살아 있나\n{'='*74}")
    working = []
    for name, url in FEEDS:
        time.sleep(GAP)
        st, body = get(url)
        if body is None:
            print(f"  ✘ {name:14s} {url:46s} 실패 ({st})")
            continue
        items, err = parse_feed(body)
        if err:
            print(f"  ✘ {name:14s} {url:46s} {err}")
            continue
        print(f"  ✔ {name:14s} {url:46s} {len(items):>3}건")
        if items:
            print(f"       최신: {items[0][0][:58]}")
            print(f"       날짜: {items[0][2]}")
            working.append((name, url, items))
    return working


def check_bodies(working):
    print(f"\n{'='*74}\n  3. 본문 추출 — 파이프라인의 추출기를 그대로 써 본다\n{'='*74}")
    try:
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        import pipeline
    except Exception as e:                                   # noqa: BLE001
        print(f"  pipeline 을 불러오지 못했다: {type(e).__name__}: {str(e)[:80]}")
        print("  (저장소 루트에서 돌려야 한다)")
        return
    extract = getattr(pipeline, "_extract_article_direct", None)
    looks = getattr(pipeline, "_looks_like_article", None)
    if not extract:
        print("  _extract_article_direct 를 찾지 못했다")
        return
    print(f"  ARTICLE_MIN_CHARS = {getattr(pipeline, 'ARTICLE_MIN_CHARS', '?')}\n")
    ok = fail = 0
    for name, _, items in working:
        for title, link, _ in items[:N_BODIES]:
            time.sleep(GAP)
            got = extract(link)
            if not got:
                fail += 1
                print(f"  ✘ [{name}] 추출 실패  {title[:46]}")
                continue
            ft = got.get("full_text") or ""
            passed = looks(got) if looks else True
            ok += 1 if passed else 0
            fail += 0 if passed else 1
            print(f"  {'✔' if passed else '✘'} [{name}] {len(ft):>5}자 "
                  f"{'본문으로 인정' if passed else '본문 아님으로 판정'}"
                  f"  {(got.get('title') or title)[:40]}")
            if passed and ft:
                print(f"       {ft[:100].replace(chr(10),' ')}…")
    tot = ok + fail
    print(f"\n  성공 {ok}/{tot}" + (f" ({ok/tot:.0%})" if tot else ""))


def estimate_volume(working):
    print(f"\n{'='*74}\n  4. 하루 몇 건이 들어오나\n{'='*74}")
    urls = set()
    dates = Counter()
    for _, _, items in working:
        for t, l, d in items:
            urls.add(l.split("?")[0])
            m = re.search(r"(\d{1,2}\s+\w{3}\s+\d{4})", d or "")
            if m:
                dates[m.group(1)] += 1
    print(f"  살아 있는 피드가 담고 있는 고유 기사: {len(urls)}건")
    if dates:
        print("  발행일 분포:")
        for d, n in list(dates.most_common())[:6]:
            print(f"    {d}  {n}건")
    print("\n  주의: RSS 는 '최근 N건'만 보여주는 창이다. 하루 총량을 알려면")
    print("        하루에 여러 번 받아서 합쳐야 한다. 위 숫자는 한 번 받은 양이다.")


def main():
    print("연합뉴스 수집 가능성 시험 — 읽기 전용, API 비용 0")
    show_robots()
    working = check_feeds()
    if not working:
        print("\n살아 있는 RSS 가 없다. 주소 후보를 더 찾아야 한다.")
        return
    check_bodies(working)
    estimate_volume(working)
    print(f"\n{'='*74}\n  읽는 법\n{'='*74}")
    print("  robots.txt 가 막고 있다면 — 기술적으로 되더라도 거기서 멈출 것")
    print("  RSS 는 되는데 본문 추출이 실패한다면 — 기사 페이지만 차단된 것이다")
    print("  둘 다 된다면 — 붙일 수 있다. 다음은 mentions 가중치를 정하는 일이다")


if __name__ == "__main__":
    main()
