# -*- coding: utf-8 -*-
"""이미 저장된 기사의 **발행일**을 수집해, 사건일과 어긋난 기사를 찾아낸다.

왜 필요한가
-----------
GDELT 가 옛 기사를 다시 긁어 오늘 사건으로 매기는 일이 있다. 2026-11-08 에
2016년 한일 GSOMIA 기사가 그날 유일한 일본 기사로 들어와, JPN 칸과 그날 해설을
통째로 만들었다. 채널 칸의 41%가 증거 1건이라 이런 기사 하나가 그날 판독을
결정한다.

파이프라인은 2026-11-09 부터 발행일을 저장하고 오래된 기사를 거르지만, 그 전에
쌓인 5,000여 건에는 발행일이 없다. 이 스크립트가 그것을 뒤늦게 채운다.

하는 일
-------
1) llm_text/ 샤드에서 발행일이 없는 URL 을 모아 **다시 받는다** (HTTP 만, API 0)
2) 받은 발행일을 샤드에 **영구 저장**한다 (본문은 건드리지 않는다)
3) llm_coded/ 를 읽어 각 기사가 어느 날짜 사건으로 쓰였는지 맞춰 보고,
   사건일보다 STALE_ARTICLE_DAYS 넘게 오래된 것을 보고한다
4) 그 기사를 빼면 어느 날 어느 칸이 바뀌는지 계산한다 — **고치지는 않는다.**
   무엇을 다시 코딩할지는 사람이 보고 정한다.

여러 번 돌려도 안전하다. 이미 발행일이 있는 URL 은 건너뛴다.
"""
import collections
import concurrent.futures
import glob
import json
import os
import sys
import time

import pipeline as P

WORKERS = int(os.environ.get("SCAN_WORKERS") or "12")
LIMIT = int(os.environ.get("SCAN_LIMIT") or "0")          # 0 = 전부
RETRY_FAILED = os.environ.get("SCAN_RETRY_FAILED", "") == "1"
STALE_DAYS = int(os.environ.get("STALE_ARTICLE_DAYS") or "45")
OUT = os.environ.get("SCAN_OUT") or "published_scan.json"
CODED_DIR = os.environ.get("CODED_DIR") or "llm_coded"


def load_shards():
    """샤드 경로 -> 그 안의 entries dict."""
    out = {}
    for p in sorted(glob.glob(os.path.join(P.LLM_TEXT_DIR, "*.json"))):
        try:
            with open(p, encoding="utf-8") as f:
                d = json.load(f)
        except (ValueError, OSError) as e:
            print(f"  ::warning::{p} 읽기 실패 — 건너뜀: {e}")
            continue
        if isinstance(d.get("entries"), dict):
            out[p] = d
    return out


def event_days():
    """url -> 그 URL 이 쓰인 사건 날짜들(가장 이른 것 하나면 충분하지만 전부 본다)."""
    out = collections.defaultdict(set)
    for p in sorted(glob.glob(os.path.join(CODED_DIR, "*.json"))):
        try:
            with open(p, encoding="utf-8") as f:
                d = json.load(f)
        except (ValueError, OSError):
            continue
        day = d.get("date")
        if not day:
            continue
        for u in (d.get("articles") or {}):
            out[u].add(day)
    return out


def fetch_one(url):
    try:
        got = P._extract_article_direct(url)
    except Exception as e:                                   # noqa: BLE001
        return url, None, f"{type(e).__name__}"
    if not got:
        return url, None, "응답 없음"
    return url, got.get("published"), None if got.get("published") else "발행일 태그 없음"


def main():
    t0 = time.time()
    shards = load_shards()
    if not shards:
        sys.exit(f"{P.LLM_TEXT_DIR}/ 에 본문 샤드가 없다.")

    have, need = 0, []
    for p, d in shards.items():
        for u, r in d["entries"].items():
            if not isinstance(r, dict):
                continue
            if r.get("published"):
                have += 1
            else:
                need.append(u)

    prev = {}
    if os.path.exists(OUT):
        try:
            with open(OUT, encoding="utf-8") as f:
                prev = json.load(f).get("no_date", {})
        except (ValueError, OSError):
            prev = {}
    if prev and not RETRY_FAILED:
        before = len(need)
        need = [u for u in need if u not in prev]
        print(f"  지난 실행에서 발행일을 못 찾은 {before-len(need):,}건은 건너뜀 "
              "(SCAN_RETRY_FAILED=1 로 재시도)")

    need = list(dict.fromkeys(need))
    print(f"본문 보관소 {have+len(need)+len(prev):,}건 · 발행일 있음 {have:,} · 받을 것 {len(need):,}")
    if LIMIT:
        need = need[:LIMIT]
        print(f"  이번 실행 상한 {LIMIT:,}건")

    found, nodate = {}, dict(prev)
    if need:
        done = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as pool:
            for u, pub, err in pool.map(fetch_one, need):
                done += 1
                if pub:
                    found[u] = pub
                else:
                    nodate[u] = err or "?"
                if done % 300 == 0:
                    print(f"    {done:,}/{len(need):,} · 발행일 확보 {len(found):,}", flush=True)

        # 샤드에 되쓴다. 본문은 건드리지 않는다.
        for p, d in shards.items():
            touched = 0
            for u, r in d["entries"].items():
                if isinstance(r, dict) and u in found and not r.get("published"):
                    r["published"] = found[u]
                    touched += 1
            if touched:
                with open(p, "w", encoding="utf-8") as f:
                    json.dump(d, f, ensure_ascii=False, indent=1)
                print(f"    {os.path.basename(p)} 에 발행일 {touched:,}건 기록")

    # ── 사건일과 대조 ────────────────────────────────────────────────
    pub_all = {}
    for p, d in load_shards().items():
        for u, r in d["entries"].items():
            if isinstance(r, dict) and r.get("published"):
                pub_all[u] = r["published"]
    days = event_days()

    stale = []          # (사건일, 발행일, url, 제목)
    titles = {}
    for p in sorted(glob.glob(os.path.join(CODED_DIR, "*.json"))):
        try:
            with open(p, encoding="utf-8") as f:
                d = json.load(f)
        except (ValueError, OSError):
            continue
        for u, r in (d.get("articles") or {}).items():
            titles[u] = r.get("title") or ""
    for u, pub in pub_all.items():
        for day in days.get(u, ()):
            if pub < P._n_days_before(day, STALE_DAYS):
                stale.append((day, pub, u, titles.get(u, "")))
    stale.sort()

    print(f"\n발행일 확보 {len(pub_all):,} / 기사 {len(pub_all)+len(nodate):,} "
          f"({100*len(pub_all)/max(1,len(pub_all)+len(nodate)):.0f}%)")
    print(f"사건일보다 {STALE_DAYS}일 넘게 오래된 기사: {len(stale)}건")
    for day, pub, u, ti in stale[:30]:
        print(f"  사건 {day}  발행 {pub}  {ti[:60]}")

    # ── 빼면 무엇이 바뀌나 ──────────────────────────────────────────
    import llm_coder as L
    K = L.CONFIDENCE_K
    bad_by_day = collections.defaultdict(set)
    for day, pub, u, ti in stale:
        bad_by_day[day].add(u)

    print(f"\n그 기사를 빼면 바뀌는 칸 (영향 받는 날짜 {len(bad_by_day)}일)")
    changes = []
    for day, bad in sorted(bad_by_day.items()):
        p = os.path.join(CODED_DIR, f"{day}.json")
        if not os.path.exists(p):
            continue
        with open(p, encoding="utf-8") as f:
            d = json.load(f)

        def cells(skip):
            rows = collections.defaultdict(list)
            for u, r in (d.get("articles") or {}).items():
                if u in skip:
                    continue
                t = r.get("tags") or {}
                if t.get("v1") != "yes":
                    continue
                g = L.GOLDSTEIN.get(t.get("v5"))
                if g is None:
                    continue
                rows[f"{t.get('v2')}_{t.get('v3')}"].append((g, r.get("mentions", 1)))
            out = {}
            for k, rs in rows.items():
                den = sum(m for _, m in rs)
                if not den:
                    continue
                e = len(rs)
                out[k] = (-1.0 * sum(g * m for g, m in rs) / den * e / (e + K), e)
            return out

        a, b = cells(set()), cells(bad)
        for k in sorted(set(a) | set(b)):
            va, vb = a.get(k), b.get(k)
            if va == vb:
                continue
            fa = f"{va[0]:+.2f}(e={va[1]})" if va else "사라짐"
            fb = f"{vb[0]:+.2f}(e={vb[1]})" if vb else "사라짐"
            changes.append((day, k, fa, fb))
            print(f"  {day}  {k:16s} {fa:>16s}  ->  {fb}")
    if not changes:
        print("  없음")

    with open(OUT, "w", encoding="utf-8") as f:
        json.dump({"scanned_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
                   "stale_days_threshold": STALE_DAYS,
                   "n_with_date": len(pub_all), "n_no_date": len(nodate),
                   "stale": [{"event_date": d, "published": p, "url": u, "title": t}
                             for d, p, u, t in stale],
                   "cell_changes": [{"date": d, "cell": c, "before": a, "after": b}
                                    for d, c, a, b in changes],
                   "no_date": nodate}, f, ensure_ascii=False, indent=1)
    print(f"\n저장: {OUT} · 소요 {time.time()-t0:.0f}초")
    if stale:
        print("\n다시 코딩할 날짜: " + ", ".join(sorted(bad_by_day)))
        print("  (이 스크립트는 고치지 않는다. 무엇을 다시 코딩할지는 보고 정한다.)")


if __name__ == "__main__":
    main()
