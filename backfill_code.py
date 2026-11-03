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
TEXT_DIR = os.environ.get("LLM_TEXT_DIR", "llm_text")   # pipeline.LLM_TEXT_DIR 과 같아야 한다

# 날짜를 콤마로 직접 받는다. 사건군 7일처럼 흩어진 날짜는 start~end 로 못 집는다.
DAYS = [d.strip() for d in os.environ.get("BACKFILL_DAYS", "").split(",") if d.strip()]

# --- 재검사 모드 -------------------------------------------------------------
# 같은 코드북으로 다시 코딩해 **날짜별 지수**가 얼마나 흔들리는지 잰다.
# 기사 단위 잡음(3.9~4.4%)은 시험세트로 쟀지만, 그것이 지수를 얼마나 움직이는지는
# 따로 재야 한다 — 채널 칸의 41%가 e=1 이라 한 기사가 그날 값을 크게 민다.
#
# **출력 경로를 손으로 맞추게 하지 않는다.** 모드를 켜면 코딩 결과·지수·캐시가
# 모두 따로 가게 자동으로 갈린다. 입력을 셋씩 맞추게 하면 하나를 빠뜨려
# 운영 말뭉치를 덮어쓴다 — 이번 작업에서 파일명 하나로 그럴 뻔했다.
RETEST = os.environ.get("CODE_RETEST", "") == "1"
if RETEST:
    CODED_DIR = os.environ.get("CODED_DIR") or "llm_coded_retest"
    SERIES_FILE = os.environ.get("SERIES_FILE") or "llm_series_retest.json"
    if CODED_DIR == "llm_coded" or SERIES_FILE == L.SERIES_FILE:
        sys.exit("재검사 모드가 운영 경로를 가리킨다. CODED_DIR / SERIES_FILE 을 확인하라.")
else:
    CODED_DIR = os.environ.get("CODED_DIR", "llm_coded")
    SERIES_FILE = os.environ.get("SERIES_FILE") or L.SERIES_FILE


# raw_data/ 를 모집단으로 쓰도록 **명시적으로** 켜는 스위치.
# raw_data 는 그날 마지막 실행 시점의 행만 담는데 GDELT 는 지난 날짜에 기사를
# 계속 덧붙인다. 실측(2026-10-27) 결과 같은 구간에서 paper_archive 대비 12.8%가
# 빠져 있었고, 빠진 것은 무작위가 아니라 '늦게 올라온 기사'다.
# 대시보드 구멍을 메우는 데는 충분하지만 **논문 말뭉치로는 못 쓴다.**
# 그래서 기본값은 꺼짐이고, 쓴 날짜는 파일에 표시한다.
ALLOW_RAW = os.environ.get("ALLOW_RAW_POP", "") == "1"
RAW_DIR = os.environ.get("RAW_DIR") or "raw_data"


def _raw_day(day):
    """raw_data/<날짜>.csv 를 {url: mentions} 로 읽는다. 없으면 빈 dict."""
    f = os.path.join(RAW_DIR, f"{day}.csv")
    if not os.path.exists(f):
        return {}
    out = {}
    try:
        with open(f, encoding="utf-8-sig") as fh:
            for r in csv.DictReader(fh):
                u = r.get("url")
                if not u:
                    continue
                try:
                    m = int(r.get("mentions") or 1)
                except ValueError:
                    m = 1
                out[u] = max(out.get(u, 0), m)
    except OSError:
        return {}
    return out


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
    if not DAYS and (not START or not END):
        sys.exit("BACKFILL_START / BACKFILL_END 또는 BACKFILL_DAYS 를 지정하라.")
    if RETEST:
        print(f"::재검사 모드:: 코딩 → {CODED_DIR}/ · 지수 → {SERIES_FILE} · 캐시 미사용")
    print("LLM 전수 코딩 — " + (", ".join(DAYS) if DAYS else f"{START} ~ {END}"))

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
    if DAYS:
        days = [d for d in DAYS if d in by_day]
        miss = [d for d in DAYS if d not in by_day]
    else:
        days = [d for d in sorted(by_day) if START <= d <= END]
        miss = []

    # 모집단 CSV 에 없는 날짜를 raw_data 로 메운다 — **켠 경우에만.**
    raw_days = set()

    # 아카이브 창의 **마지막 날은 잘려 있다.** 그날 추출 시점까지의 기사만 들어가서다
    # (실측: 2026-10-27 이 CSV 26건 대 raw_data 74건). 이런 날은 raw_data 가 더 크므로
    # 켜져 있을 때만, 그리고 **1.5배 이상 차이 날 때만** 바꿔 쓴다. 일상적인 날에
    # raw_data 를 끌어오지 않기 위해 문턱을 둔다.
    if ALLOW_RAW:
        swapped = []
        for d in list(days):
            rows = _raw_day(d)
            if rows and len(rows) >= 1.5 * max(1, len(by_day.get(d) or {})):
                swapped.append((d, len(by_day.get(d) or {}), len(rows)))
                by_day[d] = rows
                raw_days.add(d)
        if swapped:
            print("  ::warning::모집단 CSV 가 잘린 날은 raw_data 로 바꿔 씀 — "
                  + ", ".join(f"{d}({a}→{b}건)" for d, a, b in swapped))

    if miss:
        if ALLOW_RAW:
            for d in miss:
                rows = _raw_day(d)
                if rows:
                    by_day[d] = rows
                    days.append(d)
                    raw_days.add(d)
            days.sort()
            still = [d for d in miss if d not in raw_days]
            print(f"  ::warning::모집단 CSV 에 없어 raw_data 로 대체한 날짜 "
                  f"{len(raw_days)}일: {sorted(raw_days)}")
            print("  raw_data 는 GDELT 소급 수집분이 빠져 모집단이 7~13% 작다. "
                  "대시보드용으로는 충분하지만 논문 말뭉치로 쓰면 안 된다.")
            if still:
                print(f"  ::warning::raw_data 에도 없는 날짜 {still}")
        else:
            print(f"  ::warning::모집단 CSV 에 없는 날짜 {miss} — 건너뜀. "
                  "raw_data 로 메우려면 ALLOW_RAW_POP=1 로 켜라 "
                  "(모집단이 7~13% 작아진다).")
    if not days:
        sys.exit("코딩할 날짜가 없다.")
    os.makedirs(CODED_DIR, exist_ok=True)

    # 재검사는 캐시를 읽지 않는다 — 저장된 답이 돌아오면 같은 결과가 나와
    # 잡음을 못 잰다. 쓰지도 않는다 — 재검사 결과가 운영 캐시에 들어가면
    # 다음 전수 코딩이 그걸 집어 쓴다.
    cache = {} if RETEST else L._load_cache(fp)
    for k in L._STATS:
        L._STATS[k] = 0
    calls_left = MAX_CALLS if MAX_CALLS else float("inf")
    grand = collections.Counter()
    stale = []          # 옛 코드북으로 코딩돼 있어 다시 돌린 날짜

    for d in days:
        out_path = os.path.join(CODED_DIR, f"{d}.json")
        prev = {}
        if os.path.exists(out_path) and not FORCE and not RETEST:
            try:
                prev = json.load(open(out_path, encoding="utf-8"))
            except (ValueError, OSError):
                prev = {}
            # **완료된 날만 건너뛴다.** 예전에는 파일이 있기만 하면 건너뛰어서,
            # 크레딧이 떨어져 0건 코딩된 날이 '완료'로 굳어 영원히 안 채워졌다
            # (2026-10-27: 10/04~10/16 13일이 그렇게 비었다).
            #
            # **어느 코드북으로 완료했는지도 본다.** 지문을 안 보면 두 가지가 터진다
            # (2026-10-28에 실측): ① 옛 판본으로 '완료'된 날은 영원히 건너뛰어
            # 말뭉치에 v1.3 날짜가 섞인 채 남는다(10/14·10/16). ② 옛 판본의
            # 미완인 날은 이어가기가 **옛 태그를 현재 캐시에 주입하고** 새 파일에
            # 현재 지문을 찍어, 한 날 안에서 판본이 섞이고 캐시까지 오염된다(10/15).
            prev_fp = prev.get("codebook_fingerprint")
            if prev and prev_fp != fp:
                stale.append((d, prev_fp or "?", prev.get("n_coded", 0)))
                print(f"  {d}  판본 다름({(prev_fp or '?')[:8]} ≠ {fp[:8]}) — 전부 다시 코딩")
                prev = {}          # 옛 태그를 캐시에 넣지 않는다
            elif prev.get("complete"):
                print(f"  {d}  이미 코딩됨 — 건너뜀")
                continue
            elif prev:
                print(f"  {d}  미완({prev.get('n_coded',0)}/{prev.get('n_articles',0)}) — 빠진 것만 이어서")
        urls = by_day[d]
        todo = [(u, m) for u, m in urls.items() if u in text]
        notext = len(urls) - len(todo)
        # 지난 실행에서 성공한 기사는 캐시에 있으므로 재호출되지 않는다.
        # prev 는 위에서 지문이 일치할 때만 남아 있다.
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
                   # 어느 모집단에서 나온 날인지 파일 안에 적는다. 로그는 사라지고
                   # 파일은 남는다 — 논문이 인용하기 전에 여기를 보면 된다.
                   "population_source": ("raw_data" if d in raw_days
                                         else "paper_archive"),
                   "articles": {u: {"tags": r["tags"], "mentions": r["mentions"],
                                    "title": r["title"]} for u, r in coded.items()}}
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=1)
        L.record_day(payload, index, path=SERIES_FILE, channels=chans)
        if not RETEST:
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
    if stale:
        print(f"옛 판본이라 다시 코딩한 날짜 {len(stale)}일: "
              + ", ".join(f"{d}({f[:8]}, {n}건)" for d, f, n in stale))

    # 말뭉치 전체가 한 판본인지 **구간 밖까지** 센다. 논문이 인용하는 수치는
    # 전 기간이 같은 코드북이어야 하는데, 구간만 보면 밖에 남은 옛 판본을 놓친다.
    seen = collections.Counter()
    odd = []
    for p in sorted(glob.glob(os.path.join(CODED_DIR, "*.json"))):
        try:
            o = json.load(open(p, encoding="utf-8"))
        except (ValueError, OSError):
            continue
        f2 = o.get("codebook_fingerprint", "?")
        seen[f2] += 1
        if f2 != fp:
            odd.append((os.path.basename(p)[:10], f2))
    src = collections.Counter()
    for p2 in sorted(glob.glob(os.path.join(CODED_DIR, "*.json"))):
        try:
            src[json.load(open(p2, encoding="utf-8"))
                .get("population_source", "미표기")] += 1
        except (ValueError, OSError):
            pass
    print("\n모집단 출처 — " + " · ".join(f"{k} {v}일" for k, v in src.most_common()))
    if src.get("raw_data"):
        print(f"  ::warning::raw_data 기반 {src['raw_data']}일 — 논문 수치에 쓰기 전에 "
              "archive_window 로 모집단을 다시 뽑고 그 날짜만 재코딩하라.")
    print("\n말뭉치 판본 — " + " · ".join(f"{k[:8]} {v}일" for k, v in seen.most_common()))
    if odd:
        print(f"  ::warning::현재 코드북과 다른 날짜 {len(odd)}일 — "
              + ", ".join(f"{d}({f[:8]})" for d, f in odd[:20]))
        print("  이 날짜들을 구간에 넣어 다시 돌리면 자동으로 재코딩됩니다.")
    elif RETEST:
        print(f"  재검사 {sum(seen.values())}일이 현재 코드북입니다.")
    else:
        print(f"  전 기간 {sum(seen.values())}일이 같은 코드북입니다.")
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
