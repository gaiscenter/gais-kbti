# -*- coding: utf-8 -*-
"""저장소 안에 흩어져 있는 기사 본문을 모아 llm_text/ 샤드로 영구화한다.

왜 필요한가
-----------
LLM 전면 코딩은 기사 본문을 읽어야 하는데, 파이프라인은 본문을 버려 왔다.
감사 계층이 쓰고 난 audit_log.json 과 audit_cache.json 은 매 실행 덮이고,
2026-10-23 에 llm_text/ 를 도입하기 전까지는 어디에도 남지 않았다.

그런데 **덮인 판본은 전부 커밋돼 있다.** 그래서 과거 본문은 사라진 것이 아니라
커밋 히스토리 안에 흩어져 있을 뿐이다. 이 스크립트가 그것을 모은다.

출처 셋을 합친다
----------------
  1. 루트의 2026-*.json 37개   — 2026-10-21 에 한 번 올린 정적 아카이브
  2. audit_log.json / audit_cache.json 의 **전 커밋 판본**
  3. testset_key.json 180건    — 사람 코더에게 준 시험 세트

같은 URL 이 여러 판본에 있으면 **본문이 가장 많이 들어온 판본**을 남긴다.
(제목만 있는 판본과 제목+2문단 판본이 섞여 있다.)

API 호출은 0건이다. 네트워크도 쓰지 않는다. git 과 저장소 파일만 읽는다.

출력
----
  llm_text/<YYYY-MM>.json   파이프라인의 save_llm_text() 와 같은 형식.
                            즉 다음 실행부터 shadow_llm_coding() 이 그대로 읽는다.
"""
import collections
import csv
import glob
import json
import os
import subprocess
import sys

POP_CSV = os.environ.get("POP_CSV", "")          # 비우면 paper_archive 에서 최신을 고른다
TEXT_DIR = os.environ.get("LLM_TEXT_DIR", "llm_text")
HIST_FILES = ("audit_log.json", "audit_cache.json")


def _best(a, b):
    """본문이 더 많이 들어온 쪽을 남긴다."""
    if a is None:
        return b
    return b if sum(map(bool, b)) > sum(map(bool, a)) else a


def _take(store, url, title, p1, p2):
    t = (title or "").strip()
    if not url or not t:
        return
    store[url] = _best(store.get(url), (t, (p1 or "").strip(), (p2 or "").strip()))


def from_static(store):
    """루트의 날짜별 JSON 37개."""
    n = 0
    for f in sorted(glob.glob("2026-*.json")):
        try:
            d = json.load(open(f, encoding="utf-8"))
        except (ValueError, OSError):
            continue
        for x in d.get("entries", []):
            if isinstance(x, dict):
                _take(store, x.get("url"), x.get("title"),
                      x.get("paragraph_1"), x.get("paragraph_2"))
                n += 1
    print(f"  [정적] 날짜 JSON {len(glob.glob('2026-*.json'))}개 · 항목 {n:,}")


def from_history(store):
    """audit_log.json / audit_cache.json 의 전 커밋 판본.

    얕은 클론(--depth 1)에서는 커밋이 하나뿐이라 아무것도 못 캔다.
    워크플로에서 fetch-depth: 0 을 반드시 줘야 한다.
    """
    for path in HIST_FILES:
        shas = subprocess.run(["git", "log", "--format=%H", "--", path],
                              capture_output=True, text=True).stdout.split()
        n = 0
        for sha in shas:
            o = subprocess.run(["git", "show", f"{sha}:{path}"],
                               capture_output=True, text=True)
            if o.returncode:
                continue
            try:
                d = json.loads(o.stdout)
            except ValueError:
                continue
            e = d.get("entries")
            if isinstance(e, list):
                items = [(x.get("url"), x) for x in e if isinstance(x, dict)]
            elif isinstance(e, dict):
                items = [(k, v) for k, v in e.items() if isinstance(v, dict)]
            else:
                continue
            for u, x in items:
                _take(store, u, x.get("title"),
                      x.get("paragraph_1"), x.get("paragraph_2"))
                n += 1
        print(f"  [히스토리] {path} 커밋 {len(shas)}개 · 항목 {n:,}")
        if len(shas) <= 1:
            print(f"  ::warning::{path} 커밋이 {len(shas)}개뿐이다 — "
                  f"얕은 클론이면 fetch-depth: 0 을 확인하라")


def from_testset(store):
    try:
        d = json.load(open("testset_key.json", encoding="utf-8"))
    except (ValueError, OSError):
        return
    for x in d:
        _take(store, x.get("url"), x.get("title"), x.get("p1"), x.get("p2"))
    print(f"  [시험세트] testset_key.json 항목 {len(d):,}")


def population():
    """모집단 CSV 에서 url -> 최초 event_date 를 읽는다."""
    p = POP_CSV
    if not p:
        cand = sorted(glob.glob("paper_archive/matched_events_*.csv"))
        if not cand:
            sys.exit("모집단 CSV가 없다. archive_paper_window.py 를 먼저 돌려라.")
        # 파일명이 matched_events_<시작>_to_<종료>.csv 이므로 **종료일**로 고른다.
        # 크기로 고르면 안 된다 — 8/17~9/30 창이 9/16~10/27 창보다 행이 많아서
        # 엉뚱한 파일을 집는다(실제로 한 번 그렇게 골랐다).
        def _end(path):
            stem = os.path.basename(path)[len("matched_events_"):-len(".csv")]
            return stem.split("_to_")[-1]
        p = max(cand, key=_end)
    print(f"  모집단: {p}")
    day = {}
    with open(p, encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            u, d = r.get("url"), (r.get("event_date") or "")[:10]
            if not u or not d:
                continue
            if u not in day or d < day[u]:
                day[u] = d
    return day


def write_shards(day, store):
    """달 단위 샤드에 병합 저장. 기존 항목은 덮지 않는다."""
    by_month = collections.defaultdict(dict)
    for u, d in day.items():
        if u in store:
            by_month[d[:7]][u] = (d, store[u])
    os.makedirs(TEXT_DIR, exist_ok=True)
    total = 0
    for month, items in sorted(by_month.items()):
        path = os.path.join(TEXT_DIR, f"{month}.json")
        try:
            cur = json.load(open(path, encoding="utf-8"))
            entries = cur.get("entries") if isinstance(cur, dict) else None
        except (ValueError, OSError):
            entries = None
        if not isinstance(entries, dict):
            entries = {}
        added = 0
        for u, (d, (t, p1, p2)) in items.items():
            if u in entries:
                continue            # 먼저 들어온 판본을 남긴다
            entries[u] = {"title": t, "p1": p1, "p2": p2, "first_seen": d}
            added += 1
        obj = {"entries": entries,
               "_meta": {"note": ("LLM 코딩에 쓴 기사 본문. 코드북이 바뀌어도 지우지 않는다 — "
                                  "과거 날짜를 새 코드북으로 다시 코딩하려면 이 본문이 "
                                  "있어야 하고, 기사는 시간이 지나면 내려간다."),
                         "harvested": True}}
        with open(path, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=1)
        print(f"  [샤드] {path} — 총 {len(entries):,} (이번에 추가 {added:,})")
        total += len(entries)
    return total


def report(day, store):
    g = {0: "없음", 1: "제목만", 2: "제목+1문단", 3: "완비"}
    per = collections.defaultdict(lambda: [0, 0])
    grades = collections.Counter()
    for u, d in day.items():
        per[d][0] += 1
        if u in store:
            per[d][1] += 1
            grades[g[sum(map(bool, store[u]))]] += 1
    tot = sum(v[0] for v in per.values())
    cov = sum(v[1] for v in per.values())
    print(f"\n모집단 {tot:,} · 본문 있음 {cov:,} ({100*cov/tot:.1f}%) · 없음 {tot-cov:,}")
    print("  " + " · ".join(f"{k} {v:,}" for k, v in sorted(grades.items())))
    print("\n날짜        기사   본문    비율")
    for d in sorted(per):
        n, h = per[d]
        print(f"{d}  {n:4d}  {h:4d}   {100*h/n if n else 0:5.1f}%")
    miss = [u for u in day if u not in store]
    with open("need_fetch.json", "w", encoding="utf-8") as f:
        json.dump(sorted(miss), f)
    print(f"\n재수집 대상 {len(miss):,}건 → need_fetch.json")


def main():
    print("기사 본문 수확 시작 (API 호출 0건)")
    store = {}
    from_static(store)
    from_history(store)
    from_testset(store)
    print(f"  합계: 제목 보유 고유 URL {len(store):,}")
    day = population()
    write_shards(day, store)
    report(day, store)


if __name__ == "__main__":
    main()
