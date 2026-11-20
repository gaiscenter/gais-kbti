# -*- coding: utf-8 -*-
"""llm_coded/ 를 다시 집계해 llm_series.json 을 새로 쓴다. **LLM 호출 0건.**

왜 필요한가
-----------
2026-11-19 에 집계 규칙이 바뀌었다 — 협력적 상호행위(CAMEO 040·050·060)를
양방향에 기록한다(llm_coder.MUTUAL_CODES 참조). 코딩 결과(llm_coded/)는 그대로
쓸 수 있고 바뀐 것은 집계뿐이므로, 기사를 다시 읽을 이유가 없다.

기사 코딩은 손대지 않는다. 코드북 지문도 그대로다 — V1~V5 판정은 하나도
바뀌지 않았고, 따라서 맹검 코더와의 일치도(kappa)도 그대로 유효하다.

집계 함수는 llm_coder 에서 그대로 가져다 쓴다. 여기에 베껴 쓰면 다음에 규칙이
바뀔 때 조용히 어긋난다.

쓰는 법
-------
    python rebuild_series.py            # 전부 다시 집계
    DRY_RUN=1 python rebuild_series.py  # 바뀌는 것만 보고 쓰지 않음
"""
import glob
import json
import os
import sys
from datetime import datetime

import llm_coder

CODED_DIR = os.environ.get("CODED_DIR") or "llm_coded"
SERIES_FILE = os.environ.get("SERIES_FILE") or "llm_series.json"
DRY_RUN = os.environ.get("DRY_RUN", "") == "1"

# 날짜별 기록에 함께 남기는 메타. record_day() 가 쓰는 것과 같은 키를 쓴다.
META_KEYS = ("n_articles", "n_coded", "n_relevant",
             "codebook_fingerprint", "model", "mode")


def main():
    files = sorted(glob.glob(os.path.join(CODED_DIR, "*.json")))
    if not files:
        sys.exit(f"{CODED_DIR}/ 에 코딩 결과가 없습니다.")

    try:
        with open(SERIES_FILE, encoding="utf-8") as f:
            old = json.load(f)
        if not isinstance(old, dict):
            old = {}
    except (OSError, ValueError):
        old = {}

    print(f"코딩 결과 {len(files)}일 · 기존 계열 {len(old)}일\n")
    store, moved, same = {}, [], 0
    for path in files:
        try:
            with open(path, encoding="utf-8") as f:
                doc = json.load(f)
        except (OSError, ValueError) as e:
            print(f"  건너뜀 {path}: {e}")
            continue
        day = doc.get("date")
        arts = doc.get("articles") or {}
        if not day or not arts:
            continue

        rec = {k: doc.get(k) for k in META_KEYS}
        rec["cells"] = llm_coder.build_index(arts)
        rec["channels"] = llm_coder.build_channels(arts)
        rec["generated_at"] = datetime.now().isoformat(timespec="seconds")
        # 앞서 저장돼 있던 키 중 우리가 다시 만들지 않는 것은 보존한다.
        prev = old.get(day) or {}
        for k, v in prev.items():
            rec.setdefault(k, v)
        store[day] = rec

        # 무엇이 달라졌는지 센다 — 값 자체보다 '칸이 생겼는가'가 중요하다.
        before = set((prev.get("cells") or {}).keys())
        after = set(rec["cells"].keys())
        if before != after:
            moved.append((day, sorted(after - before), sorted(before - after)))
        else:
            same += 1

    # llm_coded 가 없는 날은 재집계 대상이 아니다. 기존 기록을 그대로 둔다 —
    # 여기서 빠뜨리면 계열에서 그 날이 통째로 사라진다.
    kept = [d for d in old if d not in store]
    for d in kept:
        store[d] = old[d]
    if kept:
        print(f"코딩 결과가 없어 기존 기록을 유지한 날 {len(kept)}일: "
              + ", ".join(sorted(kept)[:6]) + (" …" if len(kept) > 6 else ""))

    print(f"칸 구성이 바뀐 날 {len(moved)}일 · 그대로인 날 {same}일")
    for day, added, removed in moved[:12]:
        bits = []
        if added:
            bits.append("추가 " + ", ".join(added))
        if removed:
            bits.append("제거 " + ", ".join(removed))
        print(f"  {day}  {' · '.join(bits)}")
    if len(moved) > 12:
        print(f"  … 외 {len(moved)-12}일")

    # 파트너별로 '값이 있는 날' 수가 어떻게 달라졌는지
    print("\n파트너별 값이 있는 날 수 (threat / response)")
    print(f"  {'':5s} {'이전':>14s} {'이후':>14s}")
    for p in llm_coder.PARTNERS:
        row = []
        for src in (old, store):
            n_t = sum(1 for d in src.values()
                      if isinstance(d, dict) and f"{p}_threat" in (d.get("cells") or {}))
            n_r = sum(1 for d in src.values()
                      if isinstance(d, dict) and f"{p}_response" in (d.get("cells") or {}))
            row.append(f"{n_t:3d} / {n_r:3d}")
        print(f"  {p:5s} {row[0]:>14s} {row[1]:>14s}")

    if DRY_RUN:
        print("\nDRY_RUN=1 — 파일을 쓰지 않았습니다.")
        return

    tmp = SERIES_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(store, f, ensure_ascii=False, indent=1)
    os.replace(tmp, SERIES_FILE)
    print(f"\n저장: {SERIES_FILE} ({len(store)}일)")


if __name__ == "__main__":
    main()
