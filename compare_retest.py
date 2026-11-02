# -*- coding: utf-8 -*-
"""같은 코드북 두 회차를 비교해 **날짜별 지수의 흔들림**을 잰다.

왜 필요한가
-----------
코더는 결정적이지 않다. 시험세트 180건을 같은 코드북으로 두 번 돌리면 기사의
3.9~4.4%가 달라진다(2026-10-28 실측, v1.5a·v1.5b 각 2회차).

그런데 논문이 드는 숫자는 기사가 아니라 **날짜별 채널 지수**다. 그 지수가 얼마나
흔들리는지는 기사 단위 비율로 환산되지 않는다 — 채널 칸의 41%가 e=1 이고
57.8%가 e≤2 라서, 기사 하나가 흔들리면 그날 칸이 통째로 바뀌거나 사라진다.
반대로 e가 큰 칸은 기사 몇 개가 흔들려도 거의 움직이지 않는다.

이 스크립트가 그 폭을 센다. 출력이 논문 그림의 오차 범위와 자료 한계 절에 들어간다.

쓰는 법
-------
    python compare_retest.py                  # llm_coded 와 llm_coded_retest 비교
    CODED_A=... CODED_B=... python compare_retest.py
"""
import collections
import glob
import json
import os
import statistics as st
import sys

A = os.environ.get("CODED_A", "llm_coded")
B = os.environ.get("CODED_B", "llm_coded_retest")

try:
    import llm_coder as L
except ImportError:
    sys.exit("llm_coder.py 가 같은 폴더에 있어야 한다.")

K = L.CONFIDENCE_K


def cells_for_day(path):
    """하루치 파일 -> {셀: (수축값, 원시값, e)} 와 기사별 태그."""
    try:
        d = json.load(open(path, encoding="utf-8"))
    except (ValueError, OSError):
        return None, None, None
    rows = collections.defaultdict(list)
    tags = {}
    for u, r in (d.get("articles") or {}).items():
        t = r.get("tags") or {}
        tags[u] = tuple(t.get(v) or "—" for v in ("v1", "v2", "v3", "v4", "v5"))
        if t.get("v1") != "yes":
            continue
        g = L.GOLDSTEIN.get(t.get("v5"))
        if g is None:
            continue
        rows[f"{t.get('v2')}_{t.get('v3')}"].append((g, r.get("mentions", 1)))
    out = {}
    for k, rs in rows.items():
        num = sum(g * m for g, m in rs)
        den = sum(m for _, m in rs)
        if not den:
            continue
        raw = -1.0 * num / den
        e = len(rs)
        out[k] = (raw * e / (e + K), raw, e)
    return out, tags, d.get("codebook_fingerprint")


def main():
    days = sorted(os.path.basename(p)[:10]
                  for p in glob.glob(os.path.join(B, "*.json")))
    if not days:
        sys.exit(f"{B}/ 에 비교할 날짜가 없다. 재검사 모드로 먼저 돌려라.")
    print(f"두 회차 비교 — {A}/ 대 {B}/ · {len(days)}일\n")

    art_tot = art_chg = 0
    fps = set()
    deltas = collections.defaultdict(list)     # 셀 -> [차이]
    appeared, vanished = [], []
    rows = []

    for day in days:
        ca, ta, fa = cells_for_day(os.path.join(A, f"{day}.json"))
        cb, tb, fb = cells_for_day(os.path.join(B, f"{day}.json"))
        if ca is None or cb is None:
            print(f"  {day}  한쪽이 없어 건너뜀")
            continue
        fps |= {fa, fb}
        shared = set(ta) & set(tb)
        art_tot += len(shared)
        art_chg += sum(1 for u in shared if ta[u] != tb[u])

        for k in sorted(set(ca) | set(cb)):
            va, vb = ca.get(k), cb.get(k)
            if va and vb:
                d = vb[0] - va[0]
                deltas[k].append(d)
                rows.append((day, k, va[0], vb[0], d, va[2], vb[2]))
            elif vb:
                appeared.append((day, k, vb[0], vb[2]))
            else:
                vanished.append((day, k, va[0], va[2]))

    if len(fps) > 1:
        print(f"  ::warning::지문이 섞여 있다 {sorted(x or '?' for x in fps)} — "
              f"같은 코드북이 아니면 이 비교는 잡음이 아니라 코드북 효과를 잰다\n")

    print(f"기사 단위 — 공통 {art_tot:,}건 중 {art_chg:,}건 달라짐 "
          f"({100*art_chg/art_tot if art_tot else 0:.1f}%)\n")

    # 가장 많이 움직인 칸부터. e 와 함께 봐야 한다 — e=1 칸이 크게 흔들리는 것이
    # 이 측정의 요점이다.
    rows.sort(key=lambda r: -abs(r[4]))
    print("가장 많이 움직인 칸 (상위 20)")
    print(f"  {'날짜':12s}{'셀':16s}{'1회차':>10s}{'2회차':>10s}{'차이':>9s}{'e':>9s}")
    for day, k, a, b, d, ea, eb in rows[:20]:
        print(f"  {day:12s}{k:16s}{a:+10.2f}{b:+10.2f}{d:+9.2f}   {ea}→{eb}")

    print(f"\ne 별 흔들림 — |차이| 평균과 최대")
    by_e = collections.defaultdict(list)
    for _, _, _, _, d, ea, _ in rows:
        band = "1" if ea == 1 else ("2" if ea == 2 else ("3-5" if ea <= 5 else
               ("6-15" if ea <= 15 else "16+")))
        by_e[band].append(abs(d))
    print(f"  {'e':>5s}{'칸 수':>8s}{'평균':>10s}{'중위':>10s}{'최대':>10s}")
    for b in ("1", "2", "3-5", "6-15", "16+"):
        v = by_e.get(b)
        if v:
            print(f"  {b:>5s}{len(v):>8d}{st.mean(v):>10.3f}"
                  f"{st.median(v):>10.3f}{max(v):>10.3f}")

    allд = [abs(r[4]) for r in rows]
    if allд:
        q = sorted(allд)
        print(f"\n전체 {len(q)}칸 — |차이| 평균 {st.mean(q):.3f} · "
              f"중위 {st.median(q):.3f} · 95분위 {q[int(.95*(len(q)-1))]:.3f} · 최대 {max(q):.3f}")

    print(f"\n한쪽에만 나타난 칸 — 2회차에 새로 {len(appeared)}개 · 사라진 것 {len(vanished)}개")
    for day, k, v, e in (appeared + vanished)[:12]:
        where = "새로" if (day, k, v, e) in appeared else "사라짐"
        print(f"  {day}  {k:16s} {v:+.2f} (e={e})  {where}")

    print("\n채널 셀별 흔들림 (칸이 2개 이상인 것만)")
    print(f"  {'셀':16s}{'칸 수':>7s}{'|차이| 평균':>12s}{'최대':>9s}")
    for k in sorted(deltas, key=lambda k: -st.mean([abs(x) for x in deltas[k]]) if deltas[k] else 0):
        v = [abs(x) for x in deltas[k]]
        if len(v) >= 2:
            print(f"  {k:16s}{len(v):>7d}{st.mean(v):>12.3f}{max(v):>9.3f}")


if __name__ == "__main__":
    main()
