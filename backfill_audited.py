#!/usr/bin/env python3
"""audited_series.json 백필 — 저장된 감사 판정을 현재 창 규칙으로 다시 집계한다.

왜
--
audited_series.json 에 쌓인 30일치는 '옛 창 규칙(항상 7일 이동창)'으로 공개된
값이다. pipeline.py 가 조건부 창으로 바뀐 뒤에도 overlay_audited_series() 가
그 옛 값을 새 계열 위에 덮어쓰므로, 공개 계열의 88.6%(1,236/1,395 셀)가 옛
규칙으로 남는다. 규칙이 둘 섞인 계열로는 논문을 쓸 수 없다.

무엇을 하는가
-------------
LLM 을 다시 돌리지 않는다. 그날 감사가 내린 제외 판정(URL 목록)을 그대로
쓰면서, 집계 규칙만 현재 pipeline.py 의 조건부 창으로 통일해
audited_series.json 을 다시 만든다.

  - 감사 판정 : git 이력의 audit_log.json 에서 날짜별 마지막 회차를 꺼낸다.
  - 모집단    : pipeline.RAW_AUDIT_QUERY 를 import 해서 기간만 늘린다.
                (베끼지 않으므로 pipeline.py 와 영원히 어긋나지 않는다)
  - 집계      : pipeline.py 와 같은 조건부 창 + 같은 _shrink.

무엇을 하지 않는가
------------------
감사 로그가 없는 날짜(2026-09-15 이전)는 건드리지 않는다. 없는 검증을 있는
것처럼 만들지 않는다.

자기검증 — 반드시 먼저
----------------------
    python backfill_audited.py --self-test

제외를 적용하지 않고 계산해 kbti_output.json 의 series_raw 와 셀 단위로
대조한다. 최대 오차가 0 이어야 집계 로직이 파이프라인과 동일하다는 뜻이다.
0 이 아니면 백필을 돌리지 마라 — 로직이 어긋난 것이다.

쓰기
----
    python backfill_audited.py                 # audited_series.json 을 새로 쓴다
    python backfill_audited.py --days 30       # BigQuery 조회 기간 (기본 30)
    python backfill_audited.py --dry-run       # 계산만 하고 쓰지 않는다

기존 파일은 audited_series.json.bak 으로 남긴다.

의미가 바뀐다 — 중요
--------------------
백필 전 audited_series.json = "그날 그 기기가 실제로 공개한 값"
백필 후 audited_series.json = "그날의 감사 판정을, 현재 집계 규칙으로 재적용한 값"

둘은 다른 추정량이다. 논문에서 섞어 쓰면 안 된다. 파일의 _meta.semantics 에
적어 둔다. 백필 전 값은 .bak 과 git 이력에 남는다.
"""
import argparse
import json
import os
import pathlib
import shutil
import subprocess
import sys
from collections import defaultdict
from datetime import datetime

import pipeline as P   # 쿼리·상수·_shrink 를 베끼지 않고 그대로 쓴다

AUDIT_LOG_PATH = "audit_log.json"
DEFAULT_OUT = "audited_series.json"

# pipeline.py 의 domain_block 과 같은 값이어야 한다. 다르면 계열이 어긋난다.
WINDOW_SAME_DAY_MIN = 1
WINDOWS = {"all": 1, "mil": 7, "sup": 7, "dip": 7}
DOMAIN_NAMES = [("all", "overall"), ("mil", "military"),
                ("sup", "supply"), ("dip", "diplomatic")]


# ── 1) 감사 판정을 git 이력에서 꺼낸다 ────────────────────────────────────
def _git(*args):
    r = subprocess.run(["git", *args], capture_output=True, text=True)
    if r.returncode != 0:
        raise SystemExit(f"git {' '.join(args[:2])} 실패: {r.stderr.strip()[:300]}")
    return r.stdout


def collect_audit_logs():
    """audit_log.json 의 모든 과거 판본을 읽어 (감사일 -> flagged URL 집합)을 만든다.

    같은 날짜에 여러 회차가 있으면 가장 늦은 회차가 이긴다 — audited_series.json
    이 원래 쓰던 규칙("같은 날 여러 회차가 돌면 마지막 회차가 이긴다")과 같다.

    감사일은 로그의 date_audited 를 쓴다. 이 필드는 나중에 추가되었으므로,
    없는 초기 로그는 일단 None 으로 두고 뒤에서 레코드 날짜로 추론한다.
    """
    out = _git("log", "--format=%H\t%cI", "--", AUDIT_LOG_PATH)
    commits = [ln.split("\t") for ln in out.strip().splitlines() if "\t" in ln]
    if not commits:
        raise SystemExit(
            f"{AUDIT_LOG_PATH} 의 git 이력이 없습니다. "
            "checkout 에 fetch-depth: 0 이 설정돼 있는지 확인하십시오.")

    best = {}        # date_audited -> (committed_at, {url: verdict})
    undated = []     # (committed_at, {url: verdict})
    skipped = 0
    for sha, committed_at in commits:
        try:
            blob = _git("show", f"{sha}:{AUDIT_LOG_PATH}")
            log = json.loads(blob)
        except (SystemExit, ValueError):
            skipped += 1
            continue
        verdicts = {e["url"]: e["verdict"]
                    for e in (log.get("entries") or [])
                    if e.get("url") and e.get("verdict")}
        if not verdicts:
            continue
        stamp = log.get("generated_at") or committed_at
        day = log.get("date_audited")
        if day:
            if day not in best or stamp > best[day][0]:
                best[day] = (stamp, verdicts)
        else:
            undated.append((stamp, verdicts))

    print(f"  감사 로그 커밋 {len(commits)}개 중 판정 있는 것 "
          f"{len(best) + len(undated)}건 (날짜 확정 {len(best)}, 추론 필요 {len(undated)}"
          + (f", 읽기 실패 {skipped}" if skipped else "") + ")")
    return best, undated


def resolve_undated(undated, url_dates, best):
    """date_audited 가 없는 초기 로그의 감사일을 레코드 날짜에서 추론한다.

    감사 풀은 7일이므로 로그에는 여러 날짜의 레코드가 섞여 있다. 그 회차가
    '공개한 날'은 풀에서 가장 늦은 날짜다 — 파이프라인의 latest_date 와 같다.
    커밋 시각으로 추측하지 않는다(새벽 회차는 전날을 공개한다).
    """
    added = 0
    for stamp, verdicts in undated:
        days = {d for u in verdicts for d in url_dates.get(u, ())}
        if not days:
            continue
        day = max(days)
        if day not in best or stamp > best[day][0]:
            best[day] = (stamp, verdicts)
            added += 1
    if undated:
        print(f"  레코드 날짜로 감사일을 추론한 로그 {added}건")
    return best


# ── 2) 모집단을 가져온다 ──────────────────────────────────────────────────
def fetch_records(client, days):
    """pipeline.RAW_AUDIT_QUERY 를 기간만 늘려 레코드 단위로 가져온다."""
    needle = "INTERVAL 7 DAY"
    if needle not in P.RAW_AUDIT_QUERY:
        raise SystemExit(
            "RAW_AUDIT_QUERY 에서 'INTERVAL 7 DAY' 를 찾지 못했습니다. "
            "pipeline.py 가 바뀌었으니 이 스크립트를 함께 고쳐야 합니다.")
    query = P.RAW_AUDIT_QUERY.replace(needle, f"INTERVAL {days} DAY")
    P._dry_run_check(client, query, f"백필 레코드 조회({days}일)")
    rows = list(client.query(query, job_config=P._job_config()).result())
    print(f"  레코드 {len(rows):,}건")
    return rows


def normalize(rows):
    """BigQuery 행 -> 가벼운 dict 목록. 파트너 배정은 pipeline 과 같은 규칙."""
    recs = []
    url_dates = defaultdict(set)
    for r in rows:
        a1, a2 = r["Actor1CountryCode"], r["Actor2CountryCode"]
        partner = a2 if a1 == "KOR" else a1
        if partner not in P.PARTNERS:
            continue
        ds = str(r["date_str"])
        date = f"{ds[:4]}-{ds[4:6]}-{ds[6:]}"
        url = r["SOURCEURL"]
        recs.append({
            "date": date,
            "field": f"{partner}_{'response' if a1 == 'KOR' else 'threat'}",
            "m": int(r["NumMentions"] or 0),
            "g": float(r["GoldsteinScale"] or 0),
            "mil": bool(r["is_military"]),
            "sup": bool(r["is_supply"]),
            "dip": bool(r["is_diplomatic"]),
            "url": url,
        })
        if url:
            url_dates[url].add(date)
    return recs, url_dates


# ── 3) 집계 — pipeline.py 의 조건부 창과 같아야 한다 ──────────────────────
def build_daily(recs, dates, flagged_by_date):
    """(날짜 -> 필드 -> 도메인 -> {m,ws,e}). flagged URL 은 그 날짜에서 뺀다.

    날짜축은 '제외 전' 전체 레코드로 만든다(인수 dates). 제외로 어떤 날짜가
    통째로 비어도 축이 밀리면 안 된다 — 밀리면 창 계산이 어긋난다.
    """
    daily = {d: defaultdict(lambda: {k: {"m": 0.0, "ws": 0.0, "e": 0.0}
                                     for k in ("all", "mil", "sup", "dip")})
             for d in dates}
    dropped = 0
    for r in recs:
        drop = flagged_by_date.get(r["date"])
        if drop and r["url"] in drop:
            dropped += 1
            continue
        d = daily[r["date"]][r["field"]]
        contrib = [("all", True), ("mil", r["mil"]), ("sup", r["sup"]), ("dip", r["dip"])]
        for key, on in contrib:
            if not on:
                continue
            d[key]["m"] += r["m"]
            d[key]["ws"] += r["g"] * r["m"]
            d[key]["e"] += 1
    return daily, dropped


def _agg(daily, window_dates, field, dom_key, decay_rate):
    """창 안의 (언급합, 가중합, 유효건수)를 감쇠 가중으로 모은다."""
    m = ws = e = 0.0
    n = len(window_dates)
    for idx, wd in enumerate(window_dates):
        days_ago = (n - 1) - idx
        decay = (decay_rate ** days_ago) if n > 1 else 1.0
        c = daily[wd].get(field, {}).get(dom_key, {"m": 0, "ws": 0, "e": 0})
        m += c["m"] * decay
        ws += c["ws"] * decay
        e += c["e"] * decay
    return m, ws, e


def domain_series(daily, dates, dom_key, window_days):
    """pipeline.py 의 domain_block 과 같은 조건부 창 계산."""
    series, measured = {}, {}
    decay_rate = P._decay_rate(dom_key)
    fields = sorted({f for d in dates for f in daily[d]})
    for field in fields:
        vals, flags = [], []
        for i, _ in enumerate(dates):
            m, ws, e = _agg(daily, dates[i:i + 1], field, dom_key, decay_rate)
            if e < WINDOW_SAME_DAY_MIN and window_days > 1:
                lo = max(0, i - window_days + 1)
                m, ws, e = _agg(daily, dates[lo:i + 1], field, dom_key, decay_rate)
            raw = (ws / m * -1.0) if m > 0 else 0.0
            v = P._shrink(raw, e)
            flags.append(v is not None)
            vals.append(v if v is not None else 0.0)
        series[field] = vals
        measured[field] = flags
    return series, measured


def compute_all(daily, dates):
    """{도메인이름: (series, measured)}"""
    return {name: domain_series(daily, dates, key, WINDOWS[key])
            for key, name in DOMAIN_NAMES}


# ── 4) 자기검증 ───────────────────────────────────────────────────────────
def self_test(blocks, dates, out_path="kbti_output.json"):
    """제외를 끄고 계산한 결과를 파이프라인의 series_raw 와 셀 단위로 대조."""
    if not os.path.exists(out_path):
        raise SystemExit(f"{out_path} 가 없어 대조할 수 없습니다.")

    with open(out_path, encoding="utf-8") as f:
        live = json.load(f)
    live_dates = live.get("full_dates") or []
    overlap = [d for d in dates if d in live_dates]
    if not overlap:
        raise SystemExit("날짜가 겹치지 않아 대조할 수 없습니다.")
    li = {d: live_dates.index(d) for d in overlap}
    mi = {d: dates.index(d) for d in overlap}

    # 두 축의 시작이 어긋나면 맨 앞 며칠은 창이 다르게 잘린다(희소 채널만).
    # 그건 로직 차이가 아니므로 '경계'로 따로 센다.
    edge_n = max(WINDOWS.values()) - 1
    edge = set(overlap[:edge_n]) if (dates[0] != live_dates[0]) else set()

    # 마지막 날짜는 '움직이는 값'이다. GDELT 는 당일 레코드를 하루 종일 계속
    # 채우므로, 파이프라인이 출력을 만든 시각과 이 스크립트가 쿼리한 시각
    # 사이에 모집단이 커진다. 로직 차이가 아니므로 따로 센다.
    moving = {live_dates[-1]} if live_dates else set()
    moving -= edge

    print(f"\n  대조 구간 {overlap[0]} ~ {overlap[-1]} ({len(overlap)}일)")
    if edge:
        print(f"  날짜축 시작이 다릅니다(내 축 {dates[0]} / 파이프라인 {live_dates[0]}) — "
              f"맨 앞 {len(edge)}일은 창 절단 차이이므로 경계로 분류합니다")
    if moving:
        print(f"  파이프라인 출력 시각 {live.get('generated_at')} — 마지막 날짜 "
              f"{live_dates[-1]} 는 GDELT 가 계속 채우는 중이라 '당일'로 분류합니다")
    print(f"  {'도메인':12} {'셀':>7} {'최대오차':>10} {'내부불일치':>11} "
          f"{'경계':>6} {'당일':>6}")
    worst = 0.0
    total_bad = total_edge = total_moving = 0
    for dom, (series, _measured) in blocks.items():
        ref = ((live.get("domains") or {}).get(dom) or {}).get("series_raw") or {}
        cells = bad = bad_edge = bad_moving = 0
        dmax = 0.0
        detail = []
        for field, mine in series.items():
            theirs = ref.get(field)
            if not isinstance(theirs, list):
                continue
            for d in overlap:
                a, b = mine[mi[d]], theirs[li[d]]
                if b is None:
                    b = 0.0
                cells += 1
                diff = abs(a - b)
                if d in edge:
                    if diff > 1e-4:
                        bad_edge += 1
                    continue
                if d in moving:
                    if diff > 1e-4:
                        bad_moving += 1
                    continue
                if diff > dmax:
                    dmax = diff
                if diff > 1e-4:
                    bad += 1
                    if len(detail) < 5:
                        detail.append(f"{d} {field} 내값 {a:+.4f} / 파이프라인 {b:+.4f}")
        worst = max(worst, dmax)
        total_bad += bad
        total_edge += bad_edge
        total_moving += bad_moving
        print(f"  {dom:12} {cells:>7} {dmax:>10.6f} {bad:>11} "
              f"{bad_edge:>6} {bad_moving:>6}")
        for line in detail:
            print(f"      {line}")

    print()
    print("  (사이버 도메인은 별도 경로이므로 대조에 포함되지 않습니다.)")
    if total_bad == 0:
        print(f"  통과 — 내부 최대 오차 {worst:.2e}. 집계 로직이 파이프라인과 동일합니다.")
        if total_edge:
            print(f"  경계 불일치 {total_edge}개는 날짜축 절단 차이입니다 — 정상입니다.")
        if total_moving:
            print(f"  당일 불일치 {total_moving}개는 GDELT 가 그 사이 레코드를 더 채운 것입니다"
                  " — 정상입니다. 백필은 당일을 기록하지 않습니다.")
        print("  이제 제외를 적용해 백필을 돌려도 됩니다: mode=write")
        return True
    print(f"  실패 — 내부 불일치 {total_bad}개 셀. 백필을 돌리지 마십시오.")
    return False


# ── 5) 쓰기 ───────────────────────────────────────────────────────────────
def write_store(blocks, dates, audited_days, out_path, days):
    """감사 로그가 있는 날짜만 기록한다. 0.0 도 명시적으로 쓴다.

    0 을 빼면 안 된다 — 감사가 그날 그 채널을 통째로 비운 경우(모든 레코드
    제외) 값은 0 이어야 하는데, 생략하면 overlay 가 '감사 전 값'을 그대로
    남겨 더 큰 값이 공개된다.
    """
    store = {}
    for d in dates:
        if d not in audited_days:
            continue
        day = {}
        for dom, (series, _m) in blocks.items():
            i = dates.index(d)
            for field, vals in series.items():
                day[f"{dom}|{field}"] = round(vals[i], 4)
        if day:
            store[d] = day

    store["_meta"] = {
        "updated_at": datetime.now().isoformat(),
        "source": "backfill_audited.py",
        "rule": f"conditional window (WINDOW_SAME_DAY_MIN={WINDOW_SAME_DAY_MIN}), "
                f"windows={WINDOWS}, k={P.CONFIDENCE_K}",
        "query_days": days,
        "semantics": (
            "그날의 감사 판정(제외 URL)을 현재 집계 규칙으로 재적용한 값. "
            "'그날 실제로 공개된 값'이 아니다 — 그것은 백필 전 판본과 git 이력에 있다. "
            "두 값은 다른 추정량이므로 논문에서 섞어 쓰면 안 된다."),
        "audited_days": sorted(audited_days),
        "note": ("overlay_audited_series()가 이 기록으로 과거 날짜를 되살린다. "
                 "감사 로그가 없는 날짜는 기록하지 않으므로 측정값이 그대로 남는다."),
    }

    path = pathlib.Path(out_path)
    if path.exists():
        shutil.copy2(path, str(path) + ".bak")
        print(f"  기존 파일을 {path}.bak 으로 보존")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(store, f, ensure_ascii=False, indent=1)
    n_cell = sum(len(v) for k, v in store.items() if not k.startswith("_"))
    print(f"  {path} 작성 — {len(store) - 1}일 / {n_cell:,}개 셀")
    return store


# ── 진단 — 역전이 감사 때문인가 창 규칙 때문인가, 그리고 기사 수 ──────────
def _series_one(daily, dates, field, dom_key, rule):
    """rule: 'old' = 항상 창,  'new' = 당일 증거가 있으면 당일만."""
    decay_rate = P._decay_rate(dom_key)
    w = WINDOWS[dom_key]
    out = []
    for i, _ in enumerate(dates):
        m, ws, e = _agg(daily, dates[i:i + 1], field, dom_key, decay_rate)
        expand = (w > 1) and (rule == "old" or e < WINDOW_SAME_DAY_MIN)
        if expand:
            lo = max(0, i - w + 1)
            m, ws, e = _agg(daily, dates[lo:i + 1], field, dom_key, decay_rate)
        raw = (ws / m * -1.0) if m > 0 else 0.0
        v = P._shrink(raw, e)
        out.append(0.0 if v is None else v)
    return out


def diagnose(recs, dates, flagged_by_date, episodes, checked_by_date=None,
             partner="PRK"):
    """사건별 2x2 분해와 그날의 기사 수를 국가코드 기준으로 정확히 집계한다."""
    d_no = build_daily(recs, dates, {})[0]
    d_au = build_daily(recs, dates, flagged_by_date)[0]

    print("\n" + "=" * 78)
    print(f"진단 A — {partner} military threat, 발사 당일(D) 대 다음날(D+1)")
    print("=" * 78)
    field = f"{partner}_threat"
    header = "".join(f"{lbl:>22}" for lbl, _ in episodes)
    print(f"  {'조합':22}{header}")
    for label, daily, rule in (("옛 규칙 x 감사 전", d_no, "old"),
                               ("옛 규칙 x 감사 후", d_au, "old"),
                               ("새 규칙 x 감사 전", d_no, "new"),
                               ("새 규칙 x 감사 후", d_au, "new")):
        s = _series_one(daily, dates, field, "mil", rule)
        cells = []
        for _lbl, d0 in episodes:
            i = dates.index(d0) if d0 in dates else None
            if i is None or i + 1 >= len(dates):
                cells.append(f"{'—':>22}")
                continue
            a, b = s[i], s[i + 1]
            cells.append(f"{a:>+8.3f}{b:>+9.3f}{'역전' if b > a else '정상':>5}")
        print(f"  {label:22}" + "".join(cells))
    print("\n  각 칸은 'D값  D+1값  판정'. 네 조합을 비교하면 무엇이 역전을 없앴는지 보인다.")

    print("\n" + "=" * 78)
    print(f"진단 B — 창이 열린 빈도 (당일 증거 0건이라 7일 창으로 넘어간 셀)")
    print("=" * 78)
    for dom_key, name in DOMAIN_NAMES:
        w = WINDOWS[dom_key]
        tot = opened = 0
        fields = sorted({f for d in dates for f in d_au[d]})
        for f in fields:
            decay_rate = P._decay_rate(dom_key)
            for i, _ in enumerate(dates):
                _m, _ws, e = _agg(d_au, dates[i:i + 1], f, dom_key, decay_rate)
                tot += 1
                if w > 1 and e < WINDOW_SAME_DAY_MIN:
                    lo = max(0, i - w + 1)
                    _m2, _ws2, e2 = _agg(d_au, dates[lo:i + 1], f, dom_key, decay_rate)
                    if e2 > 0:
                        opened += 1
        pct = 100 * opened / tot if tot else 0
        tail = ("창이 없으면 이 셀들이 빈다" if opened else "창이 한 번도 열리지 않음")
        print(f"  {name:11} {opened:>4} / {tot:>4} 셀 ({pct:>5.1f}%)   <- {tail}")

    checked_by_date = checked_by_date or {}

    print("\n" + "=" * 78)
    print(f"진단 C — 사건일 기사 수 ({partner} 쌍, threat + response)")
    print("=" * 78)
    print("  도달률 = 검증/레코드 (감사가 들여다본 비율).  판정제외율 = 제외/검증")
    print("  (감사 풀은 7일이고 상위 K개만 검증하므로, 그날 레코드의 일부만 도달한다)")
    for _lbl, day in episodes:
        rs = [r for r in recs
              if r["date"] == day and r["field"].startswith(partner + "_")]
        if not rs:
            print(f"\n  {day}: 레코드 없음")
            continue
        drop = flagged_by_date.get(day, set())
        seen = checked_by_date.get(day, set())
        print(f"\n  {day}")
        print(f"    {'':24} {'레코드':>7} {'기사':>6} | {'검증':>6} {'검증기사':>9}"
              f" {'도달률':>8} | {'제외':>6} {'제외기사':>9} {'판정제외율':>11}")
        rows = [
            ("전체", lambda r: True),
            ("  military 분류", lambda r: r["mil"]),
            ("  diplomatic 분류", lambda r: r["dip"]),
            ("  supply 분류", lambda r: r["sup"]),
            ("  채널 태그 없음", lambda r: not (r["mil"] or r["dip"] or r["sup"])),
        ]
        for label, sel in rows:
            sub = [r for r in rs if sel(r)]
            if not sub:
                print(f"    {label:24} {0:>7}")
                continue
            ck = [r for r in sub if r["url"] in seen]
            dsub = [r for r in sub if r["url"] in drop]
            cov = 100 * len(ck) / len(sub)
            jud = (100 * len(dsub) / len(ck)) if ck else float("nan")
            jud_s = f"{jud:>10.1f}%" if ck else f"{'—':>11}"
            print(f"    {label:24} {len(sub):>7} {len({r['url'] for r in sub}):>6}"
                  f" | {len(ck):>6} {len({r['url'] for r in ck}):>9} {cov:>7.1f}%"
                  f" | {len(dsub):>6} {len({r['url'] for r in dsub}):>9} {jud_s}")
        thr = [r for r in rs if r["field"].endswith("_threat")]
        res = [r for r in rs if r["field"].endswith("_response")]
        print(f"    (방향별: threat {len(thr)}건 / response {len(res)}건)")

    print("\n" + "=" * 78)
    print(f"진단 D — 날짜별 감사 도달률 ({partner} 쌍 전체)")
    print("=" * 78)
    print(f"  {'날짜':12} {'레코드':>7} {'검증':>6} {'도달률':>8} {'제외':>6}"
          f" {'판정제외율':>11} {'전체제외율':>11}")
    agg_rec = agg_ck = agg_dr = 0
    for d in dates:
        if d not in checked_by_date and d not in flagged_by_date:
            continue
        rs = [r for r in recs
              if r["date"] == d and r["field"].startswith(partner + "_")]
        if not rs:
            continue
        seen = checked_by_date.get(d, set())
        drop = flagged_by_date.get(d, set())
        ck = [r for r in rs if r["url"] in seen]
        dr = [r for r in rs if r["url"] in drop]
        agg_rec += len(rs); agg_ck += len(ck); agg_dr += len(dr)
        jud = f"{100*len(dr)/len(ck):>10.1f}%" if ck else f"{'—':>11}"
        print(f"  {d:12} {len(rs):>7} {len(ck):>6} {100*len(ck)/len(rs):>7.1f}%"
              f" {len(dr):>6} {jud} {100*len(dr)/len(rs):>10.1f}%")
    if agg_rec:
        jud = f"{100*agg_dr/agg_ck:>10.1f}%" if agg_ck else f"{'—':>11}"
        print(f"  {'합계':12} {agg_rec:>7} {agg_ck:>6} {100*agg_ck/agg_rec:>7.1f}%"
              f" {agg_dr:>6} {jud} {100*agg_dr/agg_rec:>10.1f}%")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=30, help="BigQuery 조회 기간(기본 30)")
    ap.add_argument("--diagnose", default="",
                    help="쉼표로 구분한 사건 날짜. 예: 2026-09-20,2026-10-03,2026-10-12")
    ap.add_argument("--self-test", action="store_true",
                    help="제외를 끄고 계산해 파이프라인의 series_raw 와 대조만 한다")
    ap.add_argument("--dry-run", action="store_true", help="계산만 하고 쓰지 않는다")
    ap.add_argument("--out", default=DEFAULT_OUT)
    args = ap.parse_args()

    from google.cloud import bigquery

    print("1) 감사 판정 수집")
    best, undated = ({}, []) if args.self_test else collect_audit_logs()

    print("2) 모집단 조회")
    client = bigquery.Client(project=P.PROJECT_ID)
    recs, url_dates = normalize(fetch_records(client, args.days))
    if not recs:
        raise SystemExit("레코드가 없습니다.")
    dates = sorted({r["date"] for r in recs})
    print(f"  날짜축 {dates[0]} ~ {dates[-1]} ({len(dates)}일)")

    flagged_by_date, checked_by_date, audited_days = {}, {}, set()
    if not args.self_test:
        best = resolve_undated(undated, url_dates, best)
        for day, (_stamp, verdicts) in best.items():
            if day not in dates:
                continue
            audited_days.add(day)
            flagged_by_date[day] = {u for u, v in verdicts.items() if v == "flagged"}
            # pass 판정도 '검증했다'는 기록이다 — 도달률 계산에 쓴다.
            checked_by_date[day] = set(verdicts)
        # 당일은 백필하지 않는다. GDELT 는 당일 레코드를 하루 종일 계속 채우므로
        # 지금 계산한 값은 그날의 부분 집계다. 그것을 기록하면 overlay 가 이후
        # 회차마다 그 부분값으로 당일을 되돌려, 하루가 중간에 멈춘 채 공개된다.
        # 당일은 파이프라인이 매 회차 record_audited_day() 로 스스로 기록한다.
        today = dates[-1]
        if today in audited_days:
            audited_days.discard(today)
            print(f"  당일 {today} 은 기록하지 않습니다 — GDELT 가 아직 채우는 중이고,"
                  " 파이프라인이 매 회차 스스로 기록합니다")
        if audited_days:
            print(f"  날짜축과 겹치는 감사일 {len(audited_days)}일"
                  f" ({min(audited_days)} ~ {max(audited_days)})")
        else:
            raise SystemExit(
                "  날짜축과 겹치는 감사일이 없습니다. 백필할 것이 없으므로 중단합니다.")

    if args.diagnose:
        eps = [(d.strip(), d.strip()) for d in args.diagnose.split(",") if d.strip()]
        missing = [d for _l, d in eps if d not in dates]
        if missing:
            print(f"  경고: 날짜축에 없는 날짜 {missing} — 조회 기간(--days)을 늘리십시오")
        eps = [(l, d) for l, d in eps if d in dates]
        if not eps:
            raise SystemExit("진단할 날짜가 날짜축에 없습니다.")
        diagnose(recs, dates, flagged_by_date, eps, checked_by_date)
        print("\n진단만 수행했습니다 — 아무 파일도 쓰지 않았습니다.")
        return

    print("3) 집계")
    daily, dropped = build_daily(recs, dates, flagged_by_date)
    if not args.self_test:
        print(f"  감사 판정으로 제외한 레코드 {dropped:,}건 / {len(recs):,}건"
              f" ({100 * dropped / len(recs):.1f}%)")
    blocks = compute_all(daily, dates)

    if args.self_test:
        print("4) 자기검증 — 제외 없이 파이프라인과 대조")
        sys.exit(0 if self_test(blocks, dates) else 1)

    print("4) 쓰기")
    if args.dry_run:
        print("  --dry-run 이므로 쓰지 않습니다. 표본만 보여드립니다:")
        mil_series = blocks["military"][0].get("PRK_threat")
        for d in sorted(audited_days)[-5:]:
            if mil_series:
                print(f"    {d} military|PRK_threat = {mil_series[dates.index(d)]:+.4f}")
        return
    write_store(blocks, dates, audited_days, args.out, args.days)
    print("\n완료. daily.yml 다음 회차가 이 기록으로 과거 날짜를 되살립니다.")


if __name__ == "__main__":
    main()
