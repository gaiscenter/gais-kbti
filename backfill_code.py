# -*- coding: utf-8 -*-
"""과거 날짜를 LLM 으로 전수 코딩하고 날짜별 지수를 만든다.

이것이 세 팔 비교의 마지막 조각이다.
  ① GDELT 원시      paper_archive CSV 에 이미 있다
  ② GDELT + 감사    audited_series.json 과 감사 히스토리에 있다
  ③ LLM 전수 코딩   ← 이 스크립트가 만든다

섀도 계층(shadow_llm_coding)과 다른 점
--------------------------------------
섀도는 '오늘'만 코딩하고 CODER_MAX_CALLS=250 상한이 걸려 있다. 일일 운영용이다.
백필은 날짜 범위를 받아 그 구간 전체를 코딩하고, 상한은 따로 받는다.

출력 셋
-------
  llm_coded/<날짜>.json   기사별 태그 — **영구 기록**. 코드북이 바뀌어도 안 지운다.
                          llm_coder_cache.json 은 지문이 바뀌면 폐기되므로
                          논문이 인용할 코딩 결과를 거기 두면 안 된다.
  llm_series.json         날짜별 지수. 대시보드 2계열과 논문 그림이 읽을 파일.
  (llm_coder_cache.json)  재실행 시 같은 기사를 다시 묻지 않기 위한 캐시.

안전장치
--------
이미 코딩된 날짜는 건너뛴다(--force 로 덮어쓰기). 중간에 끊겨도 날짜 단위로
저장되므로 다시 돌리면 남은 날짜부터 이어간다.
"""
import collections
import concurrent.futures
import csv
import glob
import json
import os
import sys
import time

import llm_coder as L

START = os.environ.get("BACKFILL_START", "")
END = os.environ.get("BACKFILL_END", "")
WORKERS = int(os.environ.get("CODE_WORKERS", "6"))
MAX_CALLS = int(os.environ.get("CODE_MAX_CALLS", "0"))   # 0 = 무제한
FORCE = os.environ.get("CODE_FORCE", "") == "1"
CODED_DIR = os.environ.get("CODED_DIR", "llm_coded")
TEXT_DIR = os.environ.get("LLM_TEXT_DIR", "llm_text")   # pipeline.LLM_TEXT_DIR 과 같아야 한다


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
    # URL 당 언급수는 레코드 **최댓값**을 쓴다. 합치면 같은 기사를 여러 번 세는
    # 것이 되고, 그것이 GDELT 지수가 증거량을 부풀리는 바로 그 방식이다.
    by_day = collections.defaultdict(dict)
    for r in csv.DictReader(open(p, encoding="utf-8-sig")):
        u, d = r.get("url"), (r.get("event_date") or "")[:10]
        if not u or not d:
            continue
        try:
            m = int(r.get("mentions") or 1)
        except ValueError:
            m = 1
        by_day[d][u] = max(by_day[d].get(u, 0), m)
    return by_day


def text_store():
    out = {}
    for p in sorted(glob.glob(os.path.join(TEXT_DIR, "*.json"))):
        try:
            d = json.load(open(p, encoding="utf-8"))
        except (ValueError, OSError):
            continue
        for u, r in (d.get("entries") or {}).items():
            if isinstance(r, dict) and (r.get("title") or "").strip():
                out[u] = (r["title"], r.get("p1") or "", r.get("p2") or "")
    return out


def main():
    t0 = time.time()
    if not START or not END:
        sys.exit("BACKFILL_START / BACKFILL_END 를 지정하라.")
    print(f"LLM 전수 코딩 — {START} ~ {END}")

    key = os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        sys.exit("ANTHROPIC_API_KEY 가 없다.")
    import anthropic
    client = anthropic.Anthropic(api_key=key)
    L._check_sdk(anthropic)

    codebook = L._codebook_text()
    fp = L.prompt_fingerprint(codebook)
    system = L.SYSTEM_TMPL.format(codebook=codebook)
    print(f"  코드북 지문 {fp} · 코드집 {len(L.GOLDSTEIN)}+{len(L.UNPRICED)}개")

    by_day = population()
    text = text_store()
    days = [d for d in sorted(by_day) if START <= d <= END]
    if not days:
        sys.exit("해당 구간에 날짜가 없다.")
    os.makedirs(CODED_DIR, exist_ok=True)

    cache = L._load_cache(fp)
    for k in L._STATS:
        L._STATS[k] = 0
    calls_left = MAX_CALLS if MAX_CALLS else float("inf")
    grand = collections.Counter()

    for d in days:
        out_path = os.path.join(CODED_DIR, f"{d}.json")
        prev = {}
        if os.path.exists(out_path) and not FORCE:
            try:
                prev = json.load(open(out_path, encoding="utf-8"))
            except (ValueError, OSError):
                prev = {}
            # **완료된 날만 건너뛴다.** 예전에는 파일이 있기만 하면 건너뛰어서,
            # 크레딧이 떨어져 0건 코딩된 날이 '완료'로 굳어 영원히 안 채워졌다
            # (2026-10-27: 10/04~10/16 13일이 그렇게 비었다).
            if prev.get("complete"):
                print(f"  {d}  이미 코딩됨 — 건너뜀")
                continue
            if prev:
                print(f"  {d}  미완({prev.get('n_coded',0)}/{prev.get('n_articles',0)}) — 빠진 것만 이어서")
        urls = by_day[d]
        todo = [(u, m) for u, m in urls.items() if u in text]
        notext = len(urls) - len(todo)
        # 지난 실행에서 성공한 기사는 캐시에 있으므로 재호출되지 않는다.
        for u, r in (prev.get("articles") or {}).items():
            if u not in cache and isinstance(r, dict) and r.get("tags"):
                cache[u] = r["tags"]

        fresh = [(u, m) for u, m in todo if u not in cache]
        if len(fresh) > calls_left:
            print(f"  ::warning::호출 상한에 걸려 {d} 에서 중단 "
                  f"(남은 호출 {int(calls_left)}, 필요 {len(fresh)})")
            break

        coded = {}
        for u, m in todo:
            if u in cache:
                coded[u] = {"tags": cache[u], "mentions": m,
                            "title": text[u][0], "cached": True}

        if fresh:
            def work(item):
                u, m = item
                t, p1, p2 = text[u]
                return u, m, L.code_article(client, system, t, p1, p2)

            with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as pool:
                for u, m, tags in pool.map(work, fresh):
                    if tags is None:
                        continue
                    cache[u] = tags
                    coded[u] = {"tags": tags, "mentions": m,
                                "title": text[u][0], "cached": False}
            calls_left -= len(fresh)

        index = L.build_index(coded)
        chans = L.build_channels(coded)
        rel = sum(1 for r in coded.values() if r["tags"].get("v1") == "yes")
        missing = [u for u, _ in todo if u not in coded]
        payload = {"date": d, "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
                   "codebook_fingerprint": fp, "mode": "backfill", "model": L.CODER_MODEL,
                   "n_population": len(urls), "n_articles": len(todo),
                   "n_no_text": notext, "n_coded": len(coded), "n_relevant": rel,
                   # 이 날이 끝났는지를 파일 안에 적는다. 파일 존재 여부로 판단하면
                   # 실패한 날이 완료로 굳는다.
                   "complete": not missing, "n_missing": len(missing),
                   "missing": missing[:500],
                   "articles": {u: {"tags": r["tags"], "mentions": r["mentions"],
                                    "title": r["title"]} for u, r in coded.items()}}
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=1)
        L.record_day(payload, index, channels=chans)
        L._save_cache(fp, cache)

        cells = " ".join(f"{k}={v['value']:+.2f}(e={v['e']})" for k, v in sorted(index.items()))
        print(f"  {d}  모집단 {len(urls):3d} · 본문 {len(todo):3d} · 코딩 {len(coded):3d} "
              f"· 관련 {rel:3d} · 신규호출 {len(fresh):3d}"
              + (f" · ::warning::미코딩 {len(missing)}" if missing else ""))
        if cells:
            print(f"        {cells}")

        # 집계는 중단 판정 **앞에서** 갱신한다. 뒤에 두면 중단된 회차의
        # 마지막 날이 요약에서 통째로 빠져 "모집단 0"으로 찍힌다.
        grand["pop"] += len(urls); grand["coded"] += len(coded)
        grand["rel"] += rel; grand["fresh"] += len(fresh); grand["notext"] += notext

        # 되돌릴 수 없는 오류는 즉시 멈춘다. 계속 돌아봐야 실패만 쌓이고,
        # 남은 날짜가 빈 채로 저장된다.
        bad, why = L.fatal()
        if bad:
            print(f"\n  ::error::중단 — 재시도해도 풀리지 않는 오류입니다.\n  {why}")
            print("  이 날짜까지의 결과는 저장됐고, 해결 후 같은 워크플로를 "
                  "다시 돌리면 빠진 날짜부터 이어갑니다.")
            break

    s = L._STATS
    tot = s["cache_write"] + s["cache_read"] + s["input_uncached"]
    print(f"\n날짜 {len(days)}일 · 모집단 {grand['pop']:,} · 본문없음 {grand['notext']:,} "
          f"· 코딩 {grand['coded']:,} · 관련 {grand['rel']:,}")
    kinds = L.fail_summary()
    print(f"신규 호출 {grand['fresh']:,} · 실패 {s['api_failures']} · 파싱불가 {s['unparseable']}"
          + (f" · 세부코드를 루트로 접음 {s['folded']}" if s["folded"] else ""))
    if kinds:
        # 실패를 종류별로 찍는다. "실패 1187" 만으로는 속도 제한인지 크레딧
        # 소진인지 알 수 없어 로그 수백 줄을 눈으로 뒤져야 했다.
        print("실패 내역: " + " · ".join(f"{k} {v}" for k, v in sorted(kinds.items())))
    left = [d for d in days
            if not (json.load(open(os.path.join(CODED_DIR, f"{d}.json"), encoding="utf-8"))
                    .get("complete") if os.path.exists(os.path.join(CODED_DIR, f"{d}.json")) else False)]
    if left:
        print(f"아직 안 끝난 날짜 {len(left)}일: {left[0]} ~ {left[-1]}")
    print(f"토큰 — 캐시 쓰기 {s['cache_write']:,} · 읽기 {s['cache_read']:,} "
          f"· 비캐시 {s['input_uncached']:,} (적중 {100*s['cache_read']/tot if tot else 0:.0f}%)")
    if grand["fresh"]:
        print(f"기사당 평균 {tot/grand['fresh']:,.0f} 토큰 "
              f"→ 전체 4,193건 환산 약 {tot/grand['fresh']*4193/1e6:.1f}M 토큰")
    print(f"소요 {time.time()-t0:.0f}초"
          + (f" → 전체 환산 약 {(time.time()-t0)/grand['fresh']*4193/60:.0f}분"
             if grand["fresh"] else ""))


if __name__ == "__main__":
    main()
