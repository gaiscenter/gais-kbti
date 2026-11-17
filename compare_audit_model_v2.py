# -*- coding: utf-8 -*-
"""감사 계층의 **모델을 바꿨을 때 판정이 달라지는가**를 잰다.

왜 이 설계인가
--------------
"Haiku 4.5 와 5.5 의 판정이 몇 % 다르다"만 재면 답이 안 나온다. 같은 모델을
두 번 돌려도 판정은 달라지기 때문이다 — 코더에서 이미 측정된 바 있다(시험세트
180건을 같은 코드북으로 두 번 돌리면 3.9~4.4% 가 달라진다, 2026-10-28).
그러니 모델 간 차이는 **모델 내 흔들림을 기준선으로** 읽어야 한다.

세 번 돌린다:
    A1 = 현행 모델 1회차
    A2 = 현행 모델 2회차   -> A1 vs A2 = 모델 내 비결정성 (기준선)
    B  = 후보 모델 1회차   -> A1 vs B  = 모델 간 차이

A1-B 불일치가 A1-A2 와 구별되지 않으면 교체는 재실행 한 번과 같은 정도의
변화다. 유의하게 크면 불일치 건을 사람이 봐야 한다. 방향이 한쪽으로 쏠리면
(후보가 체계적으로 더/덜 제외하면) 지수가 편향되므로 McNemar 로 따로 본다.

2026-11-17 1차 실행에서 배운 것
--------------------------------
후보 모델의 152건 중 144건이 실패했는데도 남은 8건으로 "차이 없음"이라는
결론을 찍었다. 원인은 출력 상한이었다 — Haiku 5.5 가 조건별 서술을 더 길게
써서 AUDIT_MAX_TOKENS=400 에서 잘렸고, "최종:" 줄에 닿지 못해 파싱이 깨졌다.
(건당 출력 토큰 A1 217 / B 361, 상한 400.)

그래서 세 가지를 넣었다:
  1) 후보 모델에 출력 상한을 따로 준다. 상한은 판정 '기준'이 아니라 답을 끝낼
     여유이므로, 모델마다 충분히 주는 쪽이 공정하다. 잘린 답을 세는 것은
     모델을 비교하는 게 아니라 상한을 비교하는 것이다.
  2) stop_reason 을 모아서 잘림을 드러낸다.
  3) 라운드 간 성공률이 벌어지면 **결론을 내지 않고 멈춘다.**

본문과 라운드별 판정은 캐시에 남긴다. 같은 감사 기록을 다시 돌릴 때 바뀐
라운드만 호출하면 되고, 본문이 회차마다 달라져 모델 차이와 섞이는 일도 없다.

판정 함수는 pipeline 에서 그대로 가져다 쓴다. 프롬프트를 여기에 베껴 쓰면
원본이 바뀌었을 때 조용히 어긋난다 — 코드집 스트리퍼에서 이미 겪은 사고다.
모델과 상한은 pipeline 안에 적혀 있으므로 **클라이언트를 감싸서** 바꾼다.
pipeline.py 는 한 글자도 고치지 않는다 — 논문이 걸린 코드라서 시험 도구가
운영 코드를 건드리면 안 된다.

쓰는 법
-------
    CANDIDATE_MODEL=claude-haiku-5-5 python compare_audit_model_v2.py
"""
import concurrent.futures
import json
import os
import sys
import threading
import time
from datetime import datetime

import pipeline as P

LOG = os.environ.get("AUDIT_LOG", "audit_log.json")
OUT = os.environ.get("COMPARE_OUT", "audit_model_compare.json")
CACHE = os.environ.get("COMPARE_CACHE", "audit_ab_cache.json")
BASE_MODEL = os.environ.get("BASE_MODEL", "claude-haiku-4-5-20251001")
CAND_MODEL = os.environ.get("CANDIDATE_MODEL", "claude-haiku-5-5")
BASE_MAX_TOKENS = int(os.environ.get("BASE_MAX_TOKENS") or P.AUDIT_MAX_TOKENS)
CAND_MAX_TOKENS = int(os.environ.get("CAND_MAX_TOKENS") or "1200")
LIMIT = int(os.environ.get("LIMIT") or "0")          # 0 = 전부
WORKERS = int(os.environ.get("WORKERS") or "6")
NO_CACHE = os.environ.get("NO_CACHE", "") == "1"

# 라운드 성공률이 이만큼 벌어지면 비교를 포기한다. 1차 실행에서 152 대 8로
# 벌어졌는데도 결론을 냈다. 그 결론은 8건짜리였다.
MIN_ROUND_RATIO = 0.80


# ── 통계 ────────────────────────────────────────────────────────
def _c(n, k):
    r = 1
    for i in range(k):
        r = r * (n - i) // (i + 1)
    return r


def mcnemar_exact(b, c):
    """불일치 쌍이 한쪽으로 쏠렸는지만 보는 양측 정확검정. scipy 없이."""
    n = b + c
    if n == 0:
        return 1.0
    lo = min(b, c)
    tail = sum(_c(n, i) for i in range(lo + 1)) * (0.5 ** n)
    return min(1.0, 2 * tail)


# ── 모델·상한 치환 ──────────────────────────────────────────────
class ModelSwap:
    """messages.create 의 model 과 max_tokens 만 갈아끼우는 얇은 껍데기.

    프롬프트·파싱은 운영과 완전히 같고, 바뀌는 것은 어느 모델이 얼마나 긴 답을
    쓸 수 있느냐뿐이다. 응답의 stop_reason 을 모아 두어, 잘려서 파싱이 깨지는
    일이 조용히 지나가지 않게 한다.
    """

    def __init__(self, client, model, max_tokens):
        self._client = client
        self._model = model
        self._max_tokens = max_tokens
        self.stop_reasons = {}
        self.no_text = 0
        self._lock = threading.Lock()

    @property
    def messages(self):
        outer = self

        class _M:
            @staticmethod
            def create(**kw):
                kw["model"] = outer._model
                kw["max_tokens"] = outer._max_tokens
                resp = outer._client.messages.create(**kw)
                sr = getattr(resp, "stop_reason", None) or "?"
                has_text = any(getattr(b, "type", None) == "text"
                               or hasattr(b, "text")
                               for b in (getattr(resp, "content", None) or []))
                with outer._lock:
                    outer.stop_reasons[sr] = outer.stop_reasons.get(sr, 0) + 1
                    if not has_text:
                        outer.no_text += 1
                return resp

        return _M()


# ── 캐시 ────────────────────────────────────────────────────────
def load_cache(log_date):
    if NO_CACHE or not os.path.exists(CACHE):
        return {"audit_log_date": log_date, "bodies": {}, "rounds": {}}
    try:
        c = json.load(open(CACHE, encoding="utf-8"))
    except (ValueError, OSError):
        return {"audit_log_date": log_date, "bodies": {}, "rounds": {}}
    if c.get("audit_log_date") != log_date:
        print(f"  [캐시] 감사 기록 날짜가 바뀌었다 "
              f"({c.get('audit_log_date')} -> {log_date}) — 폐기")
        return {"audit_log_date": log_date, "bodies": {}, "rounds": {}}
    return c


def save_cache(c):
    try:
        json.dump(c, open(CACHE, "w", encoding="utf-8"),
                  ensure_ascii=False, indent=1)
    except OSError as e:
        print(f"  [캐시] 저장 실패: {e}")


# ── 본문 ────────────────────────────────────────────────────────
def fetch_bodies(entries, cache):
    """세 회차가 함께 쓸 본문. 캐시에 있으면 받지 않는다."""
    have = cache["bodies"]
    need = [e for e in entries if e["url"] not in have]
    print(f"   캐시 {len(have)}건 · 새로 받을 것 {len(need)}건")
    if not need:
        return have
    lock = threading.Lock()

    def one(e):
        art = P._extract_article(e["url"])
        if art and art.get("full_text"):
            with lock:
                have[e["url"]] = {"title": art.get("title"),
                                  "full_text": art["full_text"]}

    t0 = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as ex:
        list(ex.map(one, need))
    print(f"   수집 {time.time()-t0:.0f}초")
    return have


# ── 한 회차 ─────────────────────────────────────────────────────
def run_round(client, entries, bodies, model, max_tokens, label, cache):
    """한 모델로 전부 판정한다. 반환: ({key: (verdict, detail)}, 사용량)"""
    slot = f"{label}|{model}|{max_tokens}"
    if slot in cache["rounds"]:
        got = {k: (v[0], {"reason": v[1], "c1": v[2], "c2": v[3], "c3": v[4]})
               for k, v in cache["rounds"][slot].items() if k in {e["_key"] for e in entries}}
        print(f"\n━━ {label}: {model} (max_tokens={max_tokens}) ━━")
        print(f"   캐시에서 {len(got)}건 재사용 — 호출 없음")
        return got, {"api_calls": 0, "api_failures": 0,
                     "input_tokens": 0, "output_tokens": 0}, None

    print(f"\n━━ {label}: {model} (max_tokens={max_tokens}) ━━")
    t0 = time.time()
    before = dict(P._AUDIT_STATS)
    swapped = ModelSwap(client, model, max_tokens)
    res, lock = {}, threading.Lock()

    def one(e):
        art = bodies.get(e["url"])
        if not art:
            return
        ev = {"a1": e["actor1"], "a2": e["actor2"], "code": e["event_code"],
              "goldstein": e["goldstein"]}
        detail = {}
        ok = P._audit_with_llm(
            swapped, P.NAMES.get(e["partner"], e["partner"]),
            ev, art, detail=detail)
        # detail 이 비면 호출 뒤 파싱이 깨진 것이다 — 집계에서 뺀다.
        if not detail.get("reason"):
            return
        with lock:
            res[e["_key"]] = (bool(ok), detail)

    with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as ex:
        list(ex.map(one, entries))

    used = {k: P._AUDIT_STATS[k] - before.get(k, 0)
            for k in ("api_calls", "api_failures", "input_tokens", "output_tokens")}
    n = max(1, used["api_calls"])
    print(f"   판정 {len(res)}/{len(entries)}건 · 호출 {used['api_calls']} "
          f"· 실패 {used['api_failures']} · 건당 출력 {used['output_tokens']/n:.0f} 토큰")
    print(f"   stop_reason: {swapped.stop_reasons}"
          + (f" · text 블록 없음 {swapped.no_text}건" if swapped.no_text else ""))
    if swapped.stop_reasons.get("max_tokens"):
        print(f"   ::warning::{swapped.stop_reasons['max_tokens']}건이 출력 상한에서 "
              f"잘렸다. max_tokens 를 올려야 한다 (지금 {max_tokens}).")

    cache["rounds"][slot] = {k: [v[0], v[1].get("reason"), v[1].get("c1"),
                                 v[1].get("c2"), v[1].get("c3")]
                             for k, v in res.items()}
    print(f"   {time.time()-t0:.0f}초")
    return res, used, swapped.stop_reasons


# ── 비교 ────────────────────────────────────────────────────────
def compare(x, y, nx, ny):
    keys = sorted(set(x) & set(y))
    diff = [k for k in keys if x[k][0] != y[k][0]]
    b = sum(1 for k in diff if x[k][0] and not y[k][0])
    c = len(diff) - b
    p = mcnemar_exact(b, c)
    pct = 100 * len(diff) / len(keys) if keys else 0
    print(f"  {nx} vs {ny:<18s} n={len(keys):>3d}  불일치 {len(diff):>3d}건 ({pct:5.1f}%)  "
          f"[{nx}유지→{ny}제외 {b} · 반대 {c}]  McNemar p={p:.3f}")
    return {"a": nx, "b": ny, "n": len(keys), "disagree": len(diff),
            "pct": round(pct, 1), "a_keep_b_drop": b, "a_drop_b_keep": c,
            "mcnemar_p": round(p, 4), "keys": diff}


def main():
    if not os.path.exists(LOG):
        sys.exit(f"{LOG} 가 없습니다.")
    log = json.load(open(LOG, encoding="utf-8"))
    log_date = log.get("date_audited")
    entries = [e for e in (log.get("entries") or []) if e.get("url")]
    seen, uniq = set(), []
    for e in entries:
        e["_key"] = f"{e['url']}|{e['event_code']}|{e['partner']}|{e['direction']}"
        if e["_key"] in seen:
            continue
        seen.add(e["_key"])
        uniq.append(e)
    entries = uniq[:LIMIT] if LIMIT else uniq

    print(f"감사 기록 {LOG} · 대상 {len(entries)}건 ({log_date})")
    print(f"기준 모델 {BASE_MODEL} (max_tokens={BASE_MAX_TOKENS})")
    print(f"후보 모델 {CAND_MODEL} (max_tokens={CAND_MAX_TOKENS})")

    key = os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        sys.exit("ANTHROPIC_API_KEY 가 없습니다.")
    import anthropic
    client = anthropic.Anthropic(api_key=key)

    cache = load_cache(log_date)
    print(f"\n━━ 본문 ━━")
    bodies = fetch_bodies(entries, cache)
    entries = [e for e in entries if e["url"] in bodies]
    save_cache(cache)
    print(f"   본문 확보 {len(entries)}건 — 못 받은 건은 제외")
    if len(entries) < 20:
        sys.exit("본문을 20건도 못 받았습니다. 링크가 죽었거나 추출이 막혔습니다.")

    rounds = [("A1", BASE_MODEL, BASE_MAX_TOKENS),
              ("A2", BASE_MODEL, BASE_MAX_TOKENS),
              ("B", CAND_MODEL, CAND_MAX_TOKENS)]
    got, used, stops = {}, {}, {}
    for label, model, mt in rounds:
        r, u, s = run_round(client, entries, bodies, model, mt, label, cache)
        got[label], used[label], stops[label] = r, u, s
        save_cache(cache)

    # ── 라운드가 고르게 끝났는지 먼저 본다. 아니면 결론을 내지 않는다.
    ns = {k: len(v) for k, v in got.items()}
    print(f"\n━━ 라운드별 판정 수 ━━\n  {ns}")
    lo, hi = min(ns.values()), max(ns.values())
    if hi == 0 or lo / hi < MIN_ROUND_RATIO:
        print(f"\n::error::라운드 간 판정 수가 고르지 않다 ({lo}/{hi} = {lo/max(1,hi):.0%}). "
              f"비교하지 않는다 — 적은 쪽 표본으로 낸 결론은 읽을 수 없다.")
        for label in ("A1", "A2", "B"):
            if stops.get(label):
                print(f"  {label} stop_reason: {stops[label]}")
        print("  max_tokens 가 모자라 잘렸다면 CAND_MAX_TOKENS 를 올려서 다시 돌리십시오.")
        json.dump({"generated_at": datetime.now().isoformat(timespec="seconds"),
                   "aborted": "라운드 간 판정 수 불균형", "round_sizes": ns,
                   "stop_reasons": stops,
                   "base_model": BASE_MODEL, "candidate_model": CAND_MODEL,
                   "base_max_tokens": BASE_MAX_TOKENS,
                   "candidate_max_tokens": CAND_MAX_TOKENS},
                  open(OUT, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
        sys.exit(1)

    print(f"\n━━ 결과 ━━")
    base = compare(got["A1"], got["A2"], "A1", "A2(같은 모델)")
    cand = compare(got["A1"], got["B"], "A1", "B(후보)")
    cand2 = compare(got["A2"], got["B"], "A2", "B(후보)")

    print(f"\n━━ 읽는 법 ━━")
    print(f"  모델 내 흔들림(기준선) : {base['pct']:.1f}%  (n={base['n']})")
    print(f"  모델 간 차이           : {cand['pct']:.1f}% / {cand2['pct']:.1f}%  (n={cand['n']})")
    worse = max(cand["pct"], cand2["pct"])
    if worse <= max(base["pct"] * 1.5, base["pct"] + 1.0):
        print("  -> 모델 간 차이가 재실행 한 번과 구별되지 않는다. 교체해도 무방해 보인다.")
    else:
        print("  -> 모델 간 차이가 기준선보다 크다. 아래 불일치 건을 사람이 봐야 한다.")
    if min(cand["mcnemar_p"], cand2["mcnemar_p"]) < 0.05:
        print("  -> 불일치가 한쪽으로 쏠렸다(McNemar p<0.05). 후보 모델이 체계적으로")
        print("     더(또는 덜) 제외한다는 뜻이므로 지수가 한 방향으로 움직인다.")
    else:
        print("  -> 불일치의 방향은 대칭이다(p>=0.05). 쏠림 증거는 없다.")
    if base["n"] < 100:
        print(f"  주의: 표본 {base['n']}건이다. 2%p 안팎의 차이는 가릴 검정력이 없다.")

    idx = {e["_key"]: e for e in entries}
    review = []
    for k in cand["keys"]:
        e = idx[k]
        review.append({
            "url": e["url"], "title": e.get("title"),
            "actor1": e["actor1"], "actor2": e["actor2"],
            "event_code": e["event_code"], "goldstein": e["goldstein"],
            "partner": e["partner"], "direction": e["direction"],
            "A1": "유지" if got["A1"][k][0] else "제외",
            "A2": "유지" if got["A2"].get(k, (None,))[0] else "제외",
            "B": "유지" if got["B"][k][0] else "제외",
            "A1_reason": got["A1"][k][1].get("reason"),
            "B_reason": got["B"][k][1].get("reason"),
        })
    print(f"\n  사람이 볼 불일치 {len(review)}건 — {OUT} 의 review 항목")
    for r in review[:6]:
        print(f"    · [{r['A1']}→{r['B']}] {r['event_code']} {r['goldstein']:+.1f} "
              f"{(r['title'] or r['url'])[:66]}")

    def cost(u, i, o):
        return u["input_tokens"] / 1e6 * i + u["output_tokens"] / 1e6 * o
    spent = (cost(used["A1"], 1.0, 5.0) + cost(used["A2"], 1.0, 5.0)
             + cost(used["B"], 0.10, 0.50))
    print(f"\n  이번 실행에 쓴 금액 ≈ ${spent:.2f} (캐시 재사용분은 0)")

    json.dump({
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "base_model": BASE_MODEL, "candidate_model": CAND_MODEL,
        "base_max_tokens": BASE_MAX_TOKENS,
        "candidate_max_tokens": CAND_MAX_TOKENS,
        "n_entries": len(entries), "audit_log_date": log_date,
        "round_sizes": ns, "stop_reasons": stops,
        "baseline_within_model": base, "between_model": cand,
        "between_model_round2": cand2,
        "tokens": used, "estimated_cost_usd": round(spent, 3),
        "review": review,
        "_how_to_read": ("A1-A2 는 같은 모델 두 회차라 모델 내 비결정성이다. "
                         "A1-B 가 그보다 크지 않으면 모델 교체는 재실행 한 번과 "
                         "같은 정도의 변화다. McNemar p<0.05 는 불일치가 한쪽으로 "
                         "쏠렸다는 뜻이고, 그때는 지수가 편향된다. "
                         "round_sizes 가 고르지 않으면 어떤 결론도 읽지 말 것."),
    }, open(OUT, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print(f"  저장: {OUT} · 캐시: {CACHE}")


if __name__ == "__main__":
    main()
