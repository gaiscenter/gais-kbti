# -*- coding: utf-8 -*-
"""본문이 없는 기사를 다시 받아 llm_text/ 샤드에 채운다.

왜 필요한가
-----------
harvest_text.py 가 저장소 안에서 긁어모을 수 있는 본문은 모집단 4,328건 중
2,874건(66.4%)이다. 나머지 1,454건은 감사 계층이 한 번도 보지 않은 기사다 —
감사는 (도메인×국가쌍×방향)마다 |영향력| 상위 10~25건만 보기 때문이고,
그 선정은 결정적이라 저언급 기사는 42일 내내 한 번도 뽑히지 않는다.

그 1,454건을 지금 받는다. **표본을 뽑아 소멸률을 추정하지 않는다** — 전수를
받아 보면 성공/실패가 곧 소멸률이다. 추정할 이유가 없다.

중요한 성질
-----------
실패를 **기록한다.** 기사가 내려간 것, 차단된 것, 타임아웃을 구분해 남긴다.
그 비율이 백필 가능 범위를 정하고, 논문의 자료 한계 절에 그대로 들어간다.
조용히 건너뛰면 "왜 이 날은 기사가 적은가"를 나중에 설명할 수 없게 된다.

추출기는 파이프라인 것을 그대로 쓴다(pipeline._extract_article_direct).
따로 만들면 수확한 본문과 새로 받은 본문의 형태가 갈린다.

여러 번 돌려도 안전하다. 이미 본문이 있는 URL 은 건드리지 않고, 실패한 것만
다음 실행에서 다시 시도한다(--retry-failed).
"""
import collections
import concurrent.futures
import csv
import glob
import json
import os
import sys
import time

LIMIT = int(os.environ.get("FETCH_LIMIT", "0"))          # 0 = 전부
WORKERS = int(os.environ.get("FETCH_WORKERS", "12"))     # 감사 계층과 같은 값
RETRY_FAILED = os.environ.get("RETRY_FAILED", "") == "1"
FAIL_FILE = "fetch_failures.json"

import pipeline   # noqa: E402  — 모듈 수준 부작용 없음(함수·상수 정의뿐)


def population():
    p = os.environ.get("POP_CSV", "")
    if p:
        if not os.path.exists(p):
            sys.exit(f"지정한 모집단 CSV 가 없다: {p}")
        print(f"  모집단: {p} (지정)")
        return p
    cand = sorted(glob.glob("paper_archive/matched_events_*.csv"))
    if not cand:
        sys.exit("모집단 CSV가 없다. archive_paper_window.py 를 먼저 돌려라.")

    def _end(p):
        stem = os.path.basename(p)[len("matched_events_"):-len(".csv")]
        return stem.split("_to_")[-1]

    # 종료일이 가장 늦은 창. 2015년 같은 과거 창을 쓰려면 POP_CSV 로 지정한다 —
    # 안 그러면 늘 최신 창이 뽑혀 과거 구간 작업이 조용히 엉뚱한 모집단을 쓴다.
    p = max(cand, key=_end)
    print(f"  모집단: {p}")
    day, ment = {}, {}
    with open(p, encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            u, d = r.get("url"), (r.get("event_date") or "")[:10]
            if not u or not d:
                continue
            if u not in day or d < day[u]:
                day[u] = d
            try:
                ment[u] = max(ment.get(u, 0), int(r.get("mentions") or 0))
            except ValueError:
                pass
    return day, ment


def have_text():
    got = set()
    for p in sorted(glob.glob(os.path.join(pipeline.LLM_TEXT_DIR, "*.json"))):
        try:
            d = json.load(open(p, encoding="utf-8"))
        except (ValueError, OSError):
            continue
        for u, r in (d.get("entries") or {}).items():
            if isinstance(r, dict) and (r.get("title") or "").strip():
                got.add(u)
    return got


def main():
    t0 = time.time()
    print("본문 재수집 시작 (LLM 호출 0건, HTTP 요청만)")
    day, ment = population()
    got = have_text()
    todo = [u for u in day if u not in got]
    print(f"  모집단 {len(day):,} · 본문 있음 {len(got & set(day)):,} · 대상 {len(todo):,}")

    prev = {}
    if os.path.exists(FAIL_FILE):
        try:
            prev = json.load(open(FAIL_FILE, encoding="utf-8")).get("failures", {})
        except (ValueError, OSError):
            prev = {}
    if prev and not RETRY_FAILED:
        todo = [u for u in todo if u not in prev]
        print(f"  지난 실행 실패 {len(prev):,}건은 건너뜀 (RETRY_FAILED=1 로 재시도)")

    todo.sort(key=lambda u: -ment.get(u, 0))      # 언급 많은 것부터 — 중간에 끊겨도 영향 큰 것부터 확보
    if LIMIT:
        todo = todo[:LIMIT]
        print(f"  이번 실행 상한 {LIMIT:,}건")
    if not todo:
        print("  받을 것이 없다.")
        return

    ok, fail = {}, {}
    done = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futs = {pool.submit(pipeline._extract_article_direct, u): u for u in todo}
        for fut in concurrent.futures.as_completed(futs):
            u = futs[fut]
            done += 1
            try:
                got1 = fut.result()
            except Exception as e:                       # noqa: BLE001
                fail[u] = f"{type(e).__name__}: {str(e)[:120]}"
                got1 = None
            if got1 and (got1.get("title") or "").strip():
                ps = (got1.get("paragraphs") or []) + ["", ""]
                ok[u] = {"url": u, "title": got1["title"], "p1": ps[0], "p2": ps[1]}
            elif u not in fail:
                fail[u] = "제목 없음 또는 응답 없음"
            if done % 200 == 0:
                print(f"    {done:,}/{len(todo):,} · 성공 {len(ok):,} · 실패 {len(fail):,}",
                      flush=True)

    # 날짜별 샤드에 저장. 파이프라인과 같은 함수를 써서 형식이 갈리지 않게 한다.
    by_day = collections.defaultdict(list)
    for u, a in ok.items():
        by_day[day[u]].append(a)
    for d in sorted(by_day):
        pipeline.save_llm_text(d, by_day[d])

    with open(FAIL_FILE, "w", encoding="utf-8") as f:
        json.dump({"checked_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
                   "n_attempted": len(todo), "n_ok": len(ok), "n_failed": len(fail),
                   "failures": fail}, f, ensure_ascii=False, indent=1)

    n = len(todo)
    print(f"\n시도 {n:,} · 성공 {len(ok):,} ({100*len(ok)/n:.1f}%) · "
          f"실패 {len(fail):,} ({100*len(fail)/n:.1f}%) · {time.time()-t0:.0f}초")

    print("\n실패 사유")
    for r, c in collections.Counter(
            v.split(":")[0] for v in fail.values()).most_common(8):
        print(f"  {c:5d}  {r[:70]}")

    # 소멸률이 언급수·시점에 따라 다른지 본다. 저언급·오래된 기사가 더 많이
    # 죽는다면 백필한 과거는 체계적으로 고언급 쪽으로 기운다 — 보고해야 한다.
    print("\n언급수대별 성공률")
    band = lambda m: "1-2" if m <= 2 else ("3-5" if m <= 5 else ("6-15" if m <= 15 else "16+"))
    agg = collections.defaultdict(lambda: [0, 0])
    for u in todo:
        b = band(ment.get(u, 0))
        agg[b][0] += 1
        agg[b][1] += (u in ok)
    for b in ("1-2", "3-5", "6-15", "16+"):
        a, s = agg[b]
        if a:
            print(f"  {b:>5}  시도 {a:5d}  성공 {s:5d}  {100*s/a:5.1f}%")

    print("\n월별 성공률")
    agg2 = collections.defaultdict(lambda: [0, 0])
    for u in todo:
        m = day[u][:7]
        agg2[m][0] += 1
        agg2[m][1] += (u in ok)
    for m in sorted(agg2):
        a, s = agg2[m]
        print(f"  {m}  시도 {a:5d}  성공 {s:5d}  {100*s/a:5.1f}%")

    after = have_text() & set(day)
    print(f"\n모집단 {len(day):,} · 본문 {len(after):,} ({100*len(after)/len(day):.1f}%) "
          f"· 아직 없음 {len(day)-len(after):,}")


if __name__ == "__main__":
    main()
