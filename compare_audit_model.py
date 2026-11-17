# -*- coding: utf-8 -*-
"""감사 계층의 **모델을 바꿨을 때 판정이 달라지는가**를 잰다.

왜 이 설계인가
--------------
"Haiku 4.5 와 5.5 의 판정이 몇 % 다르다"만 재면 답이 안 나온다. 같은 모델을
두 번 돌려도 판정은 달라지기 때문이다 — 코더에서 이미 측정된 바 있다(시험세트
180건을 같은 코드북으로 두 번 돌리면 3.9~4.4% 가 달라진다, 2026-10-28).
그러니 모델 간 차이는 **모델 내 흔들림을 기준선으로** 읽어야 한다.

그래서 세 번 돌린다:
    A1 = 현행 모델 1회차
    A2 = 현행 모델 2회차   -> A1 vs A2 = 모델 내 비결정성 (기준선)
    B  = 후보 모델 1회차   -> A1 vs B  = 모델 간 차이

A1-B 불일치가 A1-A2 불일치와 구별되지 않으면, 모델 교체는 재실행 한 번과
같은 정도의 변화다. 유의하게 크면 불일치 건을 사람이 봐야 한다.

방향도 본다. 불일치가 대칭이면 그냥 흔들림이지만, 한쪽으로 쏠리면(후보 모델이
체계적으로 더 많이 제외하거나 덜 제외하면) 지수가 편향된다. McNemar 정확검정이
그 쏠림만 본다.

무엇을 입력으로 쓰나
--------------------
audit_log.json 에 그날 감사한 건의 입력(행위자·코드·Goldstein·파트너·URL)이
전부 남아 있다. 본문은 저장돼 있지 않으므로(문단 둘뿐) URL 에서 다시 받는다.
받은 본문은 **세 회차가 공유한다** — 회차마다 다시 받으면 본문이 달라져서
모델 차이와 본문 차이가 섞인다.

판정 함수는 pipeline 에서 그대로 가져다 쓴다. 프롬프트를 여기에 베껴 쓰면
원본이 바뀌었을 때 조용히 어긋난다 — 코드집 스트리퍼에서 이미 겪은 종류의 사고다.

모델은 pipeline 안에 하드코딩돼 있으므로, **클라이언트를 감싸서** 바꾼다.
pipeline.py 는 한 글자도 고치지 않는다 — 논문이 걸린 코드라서 시험 도구가
운영 코드를 건드리면 안 된다.

쓰는 법
-------
    LIMIT=60 CANDIDATE_MODEL=claude-haiku-5-5 python compare_audit_model.py
"""
import concurrent.futures
import json
import os
import sys
import time
from datetime import datetime

import pipeline as P

LOG = os.environ.get("AUDIT_LOG", "audit_log.json")
OUT = os.environ.get("COMPARE_OUT", "audit_model_compare.json")
BASE_MODEL = os.environ.get("BASE_MODEL", "claude-haiku-4-5-20251001")
CAND_MODEL = os.environ.get("CANDIDATE_MODEL", "claude-haiku-5-5")
LIMIT = int(os.environ.get("LIMIT") or "0")          # 0 = 전부
WORKERS = int(os.environ.get("WORKERS") or "6")


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


# ── 모델 치환 ───────────────────────────────────────────────────
class ModelSwap:
    """messages.create 의 model 인자만 갈아끼우는 얇은 껍데기.

    pipeline._audit_with_llm 은 모델명을 안에 적어 두었다. 그 줄을 고치는 대신
    클라이언트를 감싼다. 프롬프트·파라미터·파싱은 운영과 완전히 같고, 바뀌는
    것은 어느 모델이 답하느냐뿐이다.
    """

    def __init__(self, client, model):
        self._client = client
        self._model = model

    @property
    def messages(self):
        client, model = self._client, self._model

        class _M:
            @staticmethod
            def create(**kw):
                kw["model"] = model
                return client.messages.create(**kw)

        return _M()


# ── 본문 ────────────────────────────────────────────────────────
def fetch_bodies(entries):
    """세 회차가 함께 쓸 본문을 한 번만 받는다."""
    out, lock = {}, __import__("threading").Lock()

    def one(e):
        art = P._extract_article(e["url"])
        if art and art.get("full_text"):
            with lock:
                out[e["url"]] = art

    with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as ex:
        list(ex.map(one, entries))
    return out


# ── 한 회차 ─────────────────────────────────────────────────────
def run_round(client, entries, bodies, model, label):
    """한 모델로 전부 판정한다. 반환: {key: (verdict, detail)}"""
    print(f"\n━━ {label}: {model} ━━")
    t0 = time.time()
    before = dict(P._AUDIT_STATS)
    swapped = ModelSwap(client, model)
    res, lock = {}, __import__("threading").Lock()

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
        # detail 이 비면 호출이 실패해 보수적 True 가 돌아온 것이다 — 집계에서 뺀다.
        if not detail.get("reason"):
            return
        with lock:
            res[e["_key"]] = (bool(ok), detail)

    with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as ex:
        list(ex.map(one, entries))

    used = {k: P._AUDIT_STATS[k] - before.get(k, 0)
            for k in ("api_calls", "api_failures", "input_tokens", "output_tokens")}
    print(f"   판정 {len(res)}건 · 호출 {used['api_calls']} · 실패 {used['api_failures']} "
          f"· 입력 {used['input_tokens']:,} · 출력 {used['output_tokens']:,} "
          f"· {time.time()-t0:.0f}초")
    return res, used


# ── 비교 ────────────────────────────────────────────────────────
def compare(x, y, nx, ny):
    keys = sorted(set(x) & set(y))
    diff = [k for k in keys if x[k][0] != y[k][0]]
    # b = x 는 유지인데 y 가 제외, c = 그 반대
    b = sum(1 for k in diff if x[k][0] and not y[k][0])
    c = len(diff) - b
    p = mcnemar_exact(b, c)
    pct = 100 * len(diff) / len(keys) if keys else 0
    print(f"  {nx} vs {ny:<22s} n={len(keys):>3d}  불일치 {len(diff):>3d}건 ({pct:5.1f}%)  "
          f"[{nx}유지→{ny}제외 {b} · 반대 {c}]  McNemar p={p:.3f}")
    return {"a": nx, "b": ny, "n": len(keys), "disagree": len(diff),
            "pct": round(pct, 1), "a_keep_b_drop": b, "a_drop_b_keep": c,
            "mcnemar_p": round(p, 4), "keys": diff}


def main():
    if not os.path.exists(LOG):
        sys.exit(f"{LOG} 가 없습니다.")
    log = json.load(open(LOG, encoding="utf-8"))
    entries = [e for e in (log.get("entries") or []) if e.get("url")]
    for i, e in enumerate(entries):
        e["_key"] = f"{e['url']}|{e['event_code']}|{e['partner']}|{e['direction']}"
    # 같은 키가 여러 번 나오면 첫 건만
    seen, uniq = set(), []
    for e in entries:
        if e["_key"] in seen:
            continue
        seen.add(e["_key"])
        uniq.append(e)
    entries = uniq[:LIMIT] if LIMIT else uniq

    print(f"감사 기록 {LOG} · 대상 {len(entries)}건 ({log.get('date_audited')})")
    print(f"기준 모델 {BASE_MODEL}\n후보 모델 {CAND_MODEL}")

    key = os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        sys.exit("ANTHROPIC_API_KEY 가 없습니다.")
    import anthropic
    client = anthropic.Anthropic(api_key=key)

    print(f"\n━━ 본문 재수집 ━━")
    t0 = time.time()
    bodies = fetch_bodies(entries)
    entries = [e for e in entries if e["url"] in bodies]
    print(f"   {len(bodies)}건 확보 ({time.time()-t0:.0f}초) — 못 받은 건은 제외")
    if len(entries) < 20:
        sys.exit("본문을 20건도 못 받았습니다. 링크가 대부분 죽었거나 추출이 막혔습니다.")

    a1, u1 = run_round(client, entries, bodies, BASE_MODEL, "A1 기준 1회차")
    a2, u2 = run_round(client, entries, bodies, BASE_MODEL, "A2 기준 2회차")
    bb, u3 = run_round(client, entries, bodies, CAND_MODEL, "B  후보")

    print(f"\n━━ 결과 ━━")
    base = compare(a1, a2, "A1", "A2(같은 모델)")
    cand = compare(a1, bb, "A1", "B(후보)")
    cand2 = compare(a2, bb, "A2", "B(후보)")

    print(f"\n━━ 읽는 법 ━━")
    print(f"  모델 내 흔들림(기준선) : {base['pct']:.1f}%")
    print(f"  모델 간 차이           : {cand['pct']:.1f}% / {cand2['pct']:.1f}%")
    worse = max(cand["pct"], cand2["pct"])
    if worse <= base["pct"] * 1.5:
        print("  -> 모델 간 차이가 재실행 한 번과 구별되지 않는다. 교체해도 무방해 보인다.")
    else:
        print("  -> 모델 간 차이가 기준선보다 크다. 아래 불일치 건을 사람이 봐야 한다.")
    if min(cand["mcnemar_p"], cand2["mcnemar_p"]) < 0.05:
        print("  -> 불일치가 한쪽으로 쏠렸다(McNemar p<0.05). 후보 모델이 체계적으로")
        print("     더(또는 덜) 제외한다는 뜻이므로, 지수가 한 방향으로 움직인다.")
    else:
        print("  -> 불일치의 방향은 대칭이다(p>=0.05). 쏠림 증거는 없다.")

    # 사람이 볼 불일치 건
    idx = {e["_key"]: e for e in entries}
    review = []
    for k in cand["keys"]:
        e = idx[k]
        review.append({
            "url": e["url"], "title": e.get("title"),
            "actor1": e["actor1"], "actor2": e["actor2"],
            "event_code": e["event_code"], "goldstein": e["goldstein"],
            "partner": e["partner"], "direction": e["direction"],
            "A1": "유지" if a1[k][0] else "제외",
            "A2": "유지" if a2.get(k, (None,))[0] else "제외",
            "B": "유지" if bb[k][0] else "제외",
            "A1_reason": a1[k][1].get("reason"),
            "B_reason": bb[k][1].get("reason"),
        })
    print(f"\n  사람이 볼 불일치 {len(review)}건 — {OUT} 의 review 항목")
    for r in review[:5]:
        print(f"    · [{r['A1']}→{r['B']}] {r['event_code']} {r['goldstein']:+.1f} "
              f"{(r['title'] or r['url'])[:70]}")

    # 비용
    def cost(u, inp, outp):
        return u["input_tokens"] / 1e6 * inp + u["output_tokens"] / 1e6 * outp
    spent = cost(u1, 1.0, 5.0) + cost(u2, 1.0, 5.0) + cost(u3, 0.10, 0.50)
    print(f"\n  이 시험에 쓴 금액 ≈ ${spent:.2f}")

    json.dump({
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "base_model": BASE_MODEL, "candidate_model": CAND_MODEL,
        "n_entries": len(entries), "audit_log_date": log.get("date_audited"),
        "baseline_within_model": base, "between_model": cand,
        "between_model_round2": cand2,
        "tokens": {"A1": u1, "A2": u2, "B": u3},
        "estimated_cost_usd": round(spent, 3),
        "review": review,
        "_how_to_read": ("A1-A2 는 같은 모델 두 회차라 모델 내 비결정성이다. "
                         "A1-B 가 그보다 크지 않으면 모델 교체는 재실행 한 번과 "
                         "같은 정도의 변화다. McNemar p<0.05 는 불일치가 한쪽으로 "
                         "쏠렸다는 뜻이고, 그때는 지수가 편향된다."),
    }, open(OUT, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print(f"  저장: {OUT}")


if __name__ == "__main__":
    main()
