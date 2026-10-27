# -*- coding: utf-8 -*-
"""시험 세트 180건을 현재 코드북으로 다시 코딩한다.

왜 필요한가
-----------
논문이 싣는 신뢰도(kappa)는 **v1.1 시점에 봉인한 LLM 태그**로 계산한 값이다.
그 뒤 코드북이 두 번 바뀌었다.

  v1.2  세부코드 15개 삭제 — V5 신뢰도를 **올리려고** 한 변경인데 재측정하지 않았다
  v1.3  V1 에 국가 간 경제 행위 포함 — V1 일치도가 바뀐다

즉 지금 배포 중인 코더(v1.3)의 신뢰도를 아무도 모른다. v1.2 의 근거가
"세부코드를 빼면 V5 kappa 가 오른다"였는데, 정말 올랐는지 확인된 바 없다.

무엇을 하지 않는가
------------------
**봉인본을 덮지 않는다.** testset_llm_tags.json 은 그대로 두고 새 파일에 쓴다.
판본 간 비교가 논문의 결과이므로 옛 값이 남아 있어야 한다.

사람 코딩은 다시 받지 않는다. V2·V3 는 개정과 무관하고, V5 는 사람 코딩을
루트로 접으면 되며, **V1 만 경계가 움직였다.** 그래서 V1 이 갈린 건만 사람이
다시 보면 된다. 이 스크립트는 그 목록도 함께 낸다.
"""
import collections
import json
import os
import sys
import time
import concurrent.futures

import llm_coder as L

KEY = os.environ.get("TESTSET_KEY", "testset_key.json")
OUT = os.environ.get("TESTSET_OUT", "testset_llm_tags_v13.json")
SEALED = os.environ.get("TESTSET_SEALED", "testset_llm_tags.json")
WORKERS = int(os.environ.get("CODE_WORKERS", "6"))


def main():
    t0 = time.time()
    key = os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        sys.exit("ANTHROPIC_API_KEY 가 없다.")
    import anthropic
    client = anthropic.Anthropic(api_key=key)
    L._check_sdk(anthropic)

    codebook = L._codebook_text()
    fp = L.prompt_fingerprint(codebook)
    system = L.SYSTEM_TMPL.format(codebook=codebook)
    print(f"시험 세트 재코딩 — 코드북 지문 {fp} · 코드집 {len(L.GOLDSTEIN)}+{len(L.UNPRICED)}개")

    arts = json.load(open(KEY, encoding="utf-8"))
    print(f"  기사 {len(arts)}건")
    cache = L._load_cache(fp)
    for k in L._STATS:
        L._STATS[k] = 0

    fresh = [a for a in arts if a["url"] not in cache]
    print(f"  캐시 적중 {len(arts)-len(fresh)} · 신규 호출 {len(fresh)}")

    def work(a):
        return a, L.code_article(client, system, a.get("title"), a.get("p1"), a.get("p2"))

    if fresh:
        with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as pool:
            for a, tags in pool.map(work, fresh):
                if tags:
                    cache[a["url"]] = tags
        L._save_cache(fp, cache)

    out = {}
    miss = 0
    for a in arts:
        t = cache.get(a["url"])
        if t:
            out[str(a["id"])] = t
        else:
            miss += 1
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=1)
    print(f"  저장: {OUT} ({len(out)}건, 미코딩 {miss})")

    bad, why = L.fatal()
    if bad:
        print(f"  ::error::중단 — {why}")
        return 1

    # 봉인본과 대조 — 판본이 무엇을 바꿨는지 여기서 바로 보인다
    try:
        old = json.load(open(SEALED, encoding="utf-8"))
    except (ValueError, OSError):
        old = None
    if old:
        flip = collections.Counter()
        v5 = collections.Counter()
        v1_changed = []
        for i, t3 in out.items():
            t1 = old.get(i)
            if not t1:
                continue
            if t1.get("v1") != t3.get("v1"):
                flip[f"{t1.get('v1')} → {t3.get('v1')}"] += 1
                v1_changed.append(i)
            if t1.get("v1") == "yes" == t3.get("v1") and t1.get("v5") != t3.get("v5"):
                v5[f"{t1.get('v5')}→{t3.get('v5')}"] += 1
        print(f"\n봉인본(v1.1) 대비")
        print(f"  V1 이 바뀐 건 {sum(flip.values())}건: {dict(flip)}")
        print(f"  V5 가 바뀐 건 {sum(v5.values())}건 (상위 {dict(v5.most_common(5))})")
        print(f"\n사람이 다시 볼 ID {len(v1_changed)}건:")
        print("  " + ", ".join(v1_changed))
        with open("testset_v1_changed.json", "w", encoding="utf-8") as f:
            json.dump(v1_changed, f)

    s = L._STATS
    tot = s["cache_write"] + s["cache_read"] + s["input_uncached"]
    print(f"\n신규 호출 {len(fresh)} · 실패 {s['api_failures']} · 파싱불가 {s['unparseable']}"
          + (f" · 루트로 접음 {s['folded']}" if s["folded"] else ""))
    print(f"토큰 {tot:,} (적중 {100*s['cache_read']/tot if tot else 0:.0f}%) · {time.time()-t0:.0f}초")
    return 0


if __name__ == "__main__":
    sys.exit(main())
