#!/usr/bin/env python3
"""
꼬리 제외율 측정기 — 감사가 보지 않는 레코드의 오류율을 실측한다.

배경 (2026-10-13):
  감사는 (도메인,파트너,방향) 버킷마다 |영향력| 상위 K건만 검증한다(전체 도메인 K=10).
  7일치 원자료 1,847건으로 계산해 보니 이 규칙은 레코드의 38.6%를 검증하면서
  |Goldstein x Mentions| 총량의 70.7%만 덮는다. 레코드가 많은 칸일수록 더 나쁘다
  (9/12 PRK threat N=61 -> 57.2%). 즉 미검증 꼬리가 지수를 얼마나 움직일 수 있는지가
  원고의 핵심 수치에 직접 걸린다.

  그런데 지금까지 쓴 제외율 82.7%는 '영향력으로 선별된 상위권'에서 측정된 값이다.
  꼬리의 오류율은 다를 수 있다 — 부수적 동시언급이 많아 더 높을 수도, 논조가 밋밋해
  더 낮을 수도 있다. 가정으로 두면 원고의 잔여 한계가 전부 가정 위에 선다.

이 스크립트가 하는 일:
  1) pipeline.fetch_raw_daily_events()로 운영과 동일한 버킷을 만든다.
  2) 전체 도메인 버킷에서 상위 K건(=감사가 본 것)을 URL 단위로 제외한 나머지 = 꼬리.
  3) 꼬리에서 무작위 표본을 뽑아 운영 감사 경로(_extract_article + _audit_with_llm)를
     그대로 태운다. 프롬프트도 판정 규칙도 한 글자 건드리지 않는다.
  4) 비가중 제외율과 영향력 가중 제외율(비율추정량)을 신뢰구간과 함께 보고하고,
     실측값으로 칸별 잔여 이동폭을 다시 계산한다.

  추출 실패는 '유지'가 아니라 '검증 불가'로 따로 집계한다. 운영은 보수적으로 True를
  반환하지만, 잔여 한계를 따질 때 검증 불가는 검증된 것이 아니다.

사용법:
  python tail_audit.py                 # 꼬리에서 40건 표본
  python tail_audit.py --n 80
  python tail_audit.py --k 10 --seed 7

환경변수: ANTHROPIC_API_KEY, GOOGLE_APPLICATION_CREDENTIALS (GitHub Actions 시크릿)
"""
import argparse
import datetime
import json
import math
import os
import random
import sys
from collections import defaultdict

import pipeline  # 버킷 구성·추출·프롬프트가 운영과 100% 동일하도록 운영 모듈을 그대로 쓴다


class _RecordingClient:
    """운영 _audit_with_llm()에 넘길 클라이언트. 판정은 진짜로 하되 응답 원문을 남긴다."""

    def __init__(self, real):
        self._real = real
        self.last_text = None
        self.calls = 0
        self.failures = 0
        outer = self

        class _Messages:
            @staticmethod
            def create(**kw):
                outer.calls += 1
                try:
                    r = outer._real.messages.create(**kw)
                except Exception:
                    outer.failures += 1
                    raise
                try:
                    outer.last_text = r.content[0].text
                except Exception:
                    outer.last_text = None
                return r

        self.messages = _Messages()


def wilson(k, n, z=1.96):
    """Wilson 점수 구간 — n이 작고 비율이 극단일 때 정규근사보다 정직하다."""
    if n == 0:
        return (0.0, 0.0, 0.0)
    p = k / n
    d = 1 + z * z / n
    c = (p + z * z / (2 * n)) / d
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return (p, max(0.0, c - h), min(1.0, c + h))


def _k_for(dom_key, k_override):
    """운영과 동일: 전체 도메인은 AUDIT_TOP_K, 희소 도메인은 AUDIT_TOP_K_SPARSE."""
    if k_override is not None:
        return k_override
    return pipeline.AUDIT_TOP_K if dom_key == "all" else pipeline.AUDIT_TOP_K_SPARSE


def build_tail(buckets, k_override=None):
    """
    네 도메인 전부에서 감사 대상(버킷별 상위 K)을 URL 단위로 걷어낸 나머지를 돌려준다.

    운영 감사는 버킷을 가로질러 하나의 URL 사전으로 중복 제거한다(pipeline 812-819행).
    같은 기사가 여러 이벤트 행으로 들어오고 여러 버킷에 걸치므로, 행 단위로 세면
    꼬리를 크게 과대평가한다. 어느 버킷에서든 상위 K에 든 URL은 '이미 본 것'이다.
    """
    audited_urls = set()
    cells = {}
    for (dom_key, partner, direction), evs in buckets.items():
        ranked = sorted(evs, key=lambda e: abs(e["impact"]), reverse=True)
        for e in ranked[: _k_for(dom_key, k_override)]:
            audited_urls.add(e["url"])
        cells[(dom_key, partner, direction)] = ranked

    tail, seen = [], set()
    for (dom_key, partner, direction), ranked in cells.items():
        for rank, e in enumerate(ranked, 1):
            if rank <= _k_for(dom_key, k_override) or e["url"] in audited_urls or e["url"] in seen:
                continue
            seen.add(e["url"])
            tail.append(dict(e, _rank=rank, _dom=dom_key))
    return cells, tail, audited_urls


def residual(cells, audited_urls, rate, latest_date, k_override=None, draws=400, rng=None):
    """
    측정된 제외율을 꼬리에 적용했을 때 칸별 지수가 얼마나 움직이는지.

    pipeline 962-979행과 동일하게 감쇠 가중과 신뢰도 수축을 적용한다. 이걸 빼면
    희소 도메인(7일 창)에서 6일 전 사건이 오늘과 같은 무게로 들어가 이동폭이 과장된다.
    """
    rng = rng or random.Random(11)
    latest_dt = datetime.datetime.strptime(latest_date, "%Y-%m-%d")
    out = {}

    def index_of(evs, decay_rate):
        m_sum = w_imp = e_cnt = 0.0
        for e in evs:
            days = max(0, (latest_dt - datetime.datetime.strptime(e["date"], "%Y%m%d")).days)
            d = decay_rate ** days
            m_sum += e["mentions"] * d
            w_imp += e["impact"] * d
            e_cnt += d
        if m_sum <= 0:
            return None
        return pipeline._shrink((w_imp / m_sum) * -1.0, e_cnt)

    for (dom_key, partner, direction), ranked in cells.items():
        k = _k_for(dom_key, k_override)
        tail = [e for i, e in enumerate(ranked, 1) if i > k and e["url"] not in audited_urls]
        if not tail:
            continue
        decay_rate = pipeline._decay_rate(dom_key)
        base = index_of(ranked, decay_rate)
        if base is None:
            continue
        shifts = []
        for _ in range(draws):
            drop = {e["url"] for e in tail if rng.random() < rate}
            cur = index_of([e for e in ranked if e["url"] not in drop], decay_rate)
            if cur is not None:
                shifts.append(cur - base)
        if shifts:
            shifts.sort()
            out[(dom_key, partner, direction)] = {
                "n": len(ranked), "tail_n": len(tail), "base": base,
                "mean_shift": sum(shifts) / len(shifts),
                "lo": shifts[int(0.025 * len(shifts))],
                "hi": shifts[int(0.975 * len(shifts))],
            }
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=40, help="꼬리에서 뽑을 표본 수 (= LLM 호출 수)")
    ap.add_argument("--k", type=int, default=None, help="감사 상위 K (기본: pipeline.AUDIT_TOP_K)")
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--out", default="tail_audit_report.json")
    args = ap.parse_args()

    if args.n > 200:
        print("--n 200 초과는 비용 보호를 위해 막습니다.", file=sys.stderr)
        sys.exit(1)

    key = os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        print("ANTHROPIC_API_KEY가 없습니다.", file=sys.stderr)
        sys.exit(1)
    import anthropic
    from google.cloud import bigquery

    rng = random.Random(args.seed)

    print("원시 이벤트 조회 중 (운영과 동일한 쿼리·버킷) …")
    bq = bigquery.Client(project=pipeline.PROJECT_ID)
    latest_date, today_rows, buckets = pipeline.fetch_raw_daily_events(bq)
    if not buckets:
        print("데이터 없음 — 중단", file=sys.stderr)
        sys.exit(1)

    cells, tail, audited_urls = build_tail(buckets, args.k)
    all_urls = {e["url"] for v in buckets.values() for e in v}

    print(f"\n기준일 {latest_date} · K = 전체 {pipeline.AUDIT_TOP_K} / 희소 "
          f"{pipeline.AUDIT_TOP_K_SPARSE}" + (f"  (--k {args.k}로 덮어씀)" if args.k else ""))
    print(f"  고유 URL {len(all_urls)}건 중 감사가 본 것 {len(audited_urls)}건 / 꼬리 {len(tail)}건")
    print("\n  도메인별 신호질량 커버리지 (감쇠 미적용, 절대 영향력 기준)")
    for dom in ("all", "mil", "dip", "sup"):
        ms = sum(abs(e["impact"]) for kk, v in buckets.items() if kk[0] == dom for e in v)
        mt = sum(abs(e["impact"]) for kk, v in buckets.items() if kk[0] == dom
                 for e in v if e["url"] in audited_urls)
        if ms > 0:
            print(f"    {dom:4s} {mt / ms:>6.1%}   (미검증 {1 - mt / ms:.1%})")

    if not tail:
        print("꼬리가 비어 있습니다 — 모든 레코드가 이미 감사 대상입니다.")
        return

    # 도메인별 층화. 꼬리는 희소 도메인(특히 외교)에 몰려 있어서 단순무작위로 뽑으면
    # 표본 대부분이 한 칸에서 나온다 -> 도메인별로 최소 구색을 보장하고 나머지를 비례 배분.
    by_dom = defaultdict(list)
    for e in tail:
        by_dom[e["_dom"]].append(e)
    budget = min(args.n, len(tail))
    floor = min(6, budget // max(1, len(by_dom)))
    alloc = {d: min(floor, len(v)) for d, v in by_dom.items()}
    left = budget - sum(alloc.values())
    rest = sum(max(0, len(v) - alloc[d]) for d, v in by_dom.items())
    for d, v in by_dom.items():
        if left <= 0 or rest <= 0:
            break
        extra = min(len(v) - alloc[d], round(left * (len(v) - alloc[d]) / rest))
        alloc[d] += extra
    short = budget - sum(alloc.values())
    for d in sorted(by_dom, key=lambda x: -len(by_dom[x])):  # 잔여는 큰 도메인부터
        while short > 0 and alloc[d] < len(by_dom[d]):
            alloc[d] += 1
            short -= 1
    sample = [e for d, v in by_dom.items() for e in rng.sample(v, alloc[d])]
    rng.shuffle(sample)
    print("  표본 배분: " + ", ".join(f"{d}={alloc[d]}/{len(by_dom[d])}" for d in sorted(by_dom)))
    print(f"\n꼬리 무작위 표본 {len(sample)}건 검증 시작 (호출 {len(sample)}회)\n" + "=" * 78)

    client = _RecordingClient(anthropic.Anthropic(api_key=key))
    rows = []
    for i, e in enumerate(sample, 1):
        ev = {"a1": e["a1"], "a2": e["a2"], "code": e["code"],
              "goldstein": e["goldstein"], "mentions": e["mentions"], "url": e["url"]}
        art = pipeline._extract_article(e["url"])
        if art is None or not (art.get("full_text") or ""):
            status, keep, reason = "추출실패", None, None
        else:
            client.last_text = None
            try:
                keep = pipeline._audit_with_llm(client, pipeline.NAMES[e["partner"]], ev, art)
                status = "유지" if keep else "제외"
            except Exception as exc:
                status, keep, reason = f"호출실패({type(exc).__name__})", None, None
            else:
                reason = (client.last_text or "").strip().replace("\n", " ")[:160]
        print(f"[{i:>3}] {e['_dom']:3s} {e['partner']}/{e['direction']:8s} rank={e['_rank']:>3} "
              f"G={e['goldstein']:+6.1f} m={e['mentions']:>3} |imp|={abs(e['impact']):>7.1f}  {status}")
        if reason:
            print(f"      {reason}")
        rows.append({"url": e["url"], "domain": e["_dom"], "partner": e["partner"],
                     "direction": e["direction"], "rank": e["_rank"],
                     "goldstein": e["goldstein"], "mentions": e["mentions"],
                     "impact": e["impact"], "status": status, "keep": keep, "reason": reason})

    # ── 집계 ────────────────────────────────────────────────────────────────
    judged = [r for r in rows if r["keep"] is not None]
    excluded = [r for r in judged if r["keep"] is False]
    unverifiable = [r for r in rows if r["keep"] is None]

    p, lo, hi = wilson(len(excluded), len(judged))
    mass_j = sum(abs(r["impact"]) for r in judged)
    mass_x = sum(abs(r["impact"]) for r in excluded)
    pw = mass_x / mass_j if mass_j else 0.0

    print("\n" + "=" * 78)
    print(f"판정 완료 {len(judged)}건 / 검증 불가 {len(unverifiable)}건 "
          f"(추출실패·호출실패)")
    print(f"\n  꼬리 제외율 (비가중)      {p:.1%}   95% CI [{lo:.1%}, {hi:.1%}]  "
          f"({len(excluded)}/{len(judged)})")
    print(f"  꼬리 제외율 (영향력 가중) {pw:.1%}   비율추정량")
    print(f"  상위 K 제외율 (기존 실측) 82.7%  ← 비교 기준")

    delta = p - 0.827
    if abs(delta) < 0.10:
        verdict = "상위권과 비슷하다 — 기존 잔여 추정이 대체로 타당. 적응적 K가 필요하다."
    elif delta < 0:
        verdict = ("꼬리가 더 깨끗하다 — 잔여 위험이 추정보다 작다. "
                   "원고는 한계 문단으로 처리 가능할 수 있다.")
    else:
        verdict = "꼬리가 더 더럽다 — 잔여 위험이 추정보다 크다. 적응적 K가 시급하다."
    print(f"\n  => {verdict}")

    # ── 실측값으로 잔여 재계산 ──────────────────────────────────────────────
    print("\n" + "-" * 78)
    print(f"실측 제외율 {p:.1%}를 꼬리에 적용했을 때 칸별 이동 "
          f"(감쇠·수축 적용, 중립 게이트 |KBTI|<2)")
    res = residual(cells, audited_urls, p, latest_date, args.k, rng=random.Random(args.seed + 1))
    shown = sorted(res.items(), key=lambda x: -abs(x[1]["mean_shift"]))[:15]
    for (dom, partner, direction), v in shown:
        flag = ""
        if abs(v["base"]) < 2.0 <= abs(v["base"] + v["mean_shift"]):
            flag = "   <<< 게이트 통과"
        print(f"  {dom:3s} {partner} {direction:8s} N={v['n']:>3} 꼬리={v['tail_n']:>3} "
              f"지수={v['base']:+6.2f} → {v['base'] + v['mean_shift']:+6.2f} "
              f"(95% {v['lo']:+.2f}~{v['hi']:+.2f}){flag}")
    if len(res) > len(shown):
        print(f"  … 외 {len(res) - len(shown)}칸 (전체는 보고서 JSON 참조)")

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump({
            "generated_at": datetime.datetime.now().isoformat(),
            "latest_date": latest_date, "seed": args.seed,
            "k_all": pipeline.AUDIT_TOP_K, "k_sparse": pipeline.AUDIT_TOP_K_SPARSE,
            "k_override": args.k,
            "unique_urls": len(all_urls), "audited_urls": len(audited_urls), "tail_n": len(tail),
            "mass_coverage": {
                dom: (lambda ms, mt: (mt / ms) if ms else None)(
                    sum(abs(e["impact"]) for kk, v in buckets.items() if kk[0] == dom for e in v),
                    sum(abs(e["impact"]) for kk, v in buckets.items() if kk[0] == dom
                        for e in v if e["url"] in audited_urls))
                for dom in ("all", "mil", "dip", "sup")},
            "sample_n": len(sample), "judged": len(judged),
            "unverifiable": len(unverifiable),
            "tail_exclusion_rate": p, "ci95": [lo, hi],
            "tail_exclusion_rate_weighted": pw,
            "topk_exclusion_rate_reference": 0.827,
            "api_calls": client.calls, "api_failures": client.failures,
            "rows": rows,
            "residual": {f"{d}_{a}_{b}": v for (d, a, b), v in res.items()},
        }, f, ensure_ascii=False, indent=2)
    print(f"\n보고서 저장: {args.out}  (LLM 호출 {client.calls}회, 실패 {client.failures}회)")


if __name__ == "__main__":
    main()
