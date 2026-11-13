# -*- coding: utf-8 -*-
"""날짜 × 상대국별 해설을 만들어 llm_commentary.json 에 넣는다.

왜 따로 만드는가
----------------
파이프라인의 build_commentary 는 **오늘치 한 건**을 kbti_output.json 에 쓰고 매
실행 덮는다. 그리고 그 글은 **감사 계열(GDELT+LLM 감사) 수치**를 설명한다.
대시보드 그래프가 LLM 전수코딩만 그리게 바뀌었으므로, 그 해설을 선 밑에 붙이면
화면에 없는 값을 설명하는 글이 된다.

또 하나. 그 해설은 5개국을 한 글에 담는다. 패널은 **고른 나라의 그래프 아래**에
붙으므로 나라별로 갈라야 한다 — 북한 그래프를 보는데 중국 얘기가 섞이면 안 된다.

쓰는 법
-------
    python build_day_commentary.py                 # 안 쓴 칸만
    COMMENTARY_FORCE=1 python build_day_commentary.py   # 전부 다시

출력
----
    llm_commentary.json
      {"generated_at":..., "codebook_fingerprint":...,
       "days": {"2026-09-28": {"PRK": {"ko":..., "en":..., "sources":[...]}}}}

지문이 바뀌면 그 칸은 다시 쓴다 — 코드북이 바뀌면 수치가 바뀌고, 옛 수치를
설명하는 글이 남으면 그래프와 어긋난다.
"""
import collections
import concurrent.futures
import glob
import json
import os
import re
import sys
import time

import llm_coder as L

CODED_DIR = os.environ.get("CODED_DIR") or "llm_coded"
OUT = os.environ.get("COMMENTARY_OUT") or "llm_commentary.json"
# .get(k, 기본값) 을 쓰면 안 된다 — 워크플로가 **빈 문자열**을 넘기므로
# 기본값이 안 먹고 모델 이름이 비어 호출이 통째로 실패한다. or 로 받는다.
MODEL = os.environ.get("COMMENTARY_MODEL") or "claude-haiku-4-5-20251001"
WORKERS = int(os.environ.get("COMMENTARY_WORKERS") or "6")
FORCE = os.environ.get("COMMENTARY_FORCE", "") == "1"
MAX_CELLS = int(os.environ.get("COMMENTARY_MAX") or "0")    # 0 = 전부
TRAJ_DAYS = 7          # 궤적을 몇 일 보여줄 것인가
TOP_ARTICLES = 10      # 한 칸에 넘길 근거 기사 수

K = L.CONFIDENCE_K
CHANNELS = {"military": "군사", "diplomatic": "외교", "economic": "경제"}
COUNTRY_KR = {"PRK": "북한", "USA": "미국", "CHN": "중국", "JPN": "일본", "RUS": "러시아"}
COUNTRY_EN = {"PRK": "North Korea", "USA": "the United States", "CHN": "China",
              "JPN": "Japan", "RUS": "Russia"}


def _shrunk(rows):
    """[(goldstein, mentions)] -> (수축값, 원시값, 증거수). 비면 None."""
    if not rows:
        return None
    den = sum(m for _, m in rows)
    if not den:
        return None
    e = len(rows)
    raw = -1.0 * sum(g * m for g, m in rows) / den
    return raw * e / (e + K), raw, e


def load_days():
    """날짜 -> {상대국 -> {'cells': {...}, 'articles': [...]}}"""
    out = {}
    for p in sorted(glob.glob(os.path.join(CODED_DIR, "*.json"))):
        try:
            d = json.load(open(p, encoding="utf-8"))
        except (ValueError, OSError):
            continue
        day = d.get("date")
        if not day:
            continue
        rows = collections.defaultdict(lambda: collections.defaultdict(list))
        arts = collections.defaultdict(list)
        for u, r in (d.get("articles") or {}).items():
            t = r.get("tags") or {}
            if t.get("v1") != "yes":
                continue
            g = L.GOLDSTEIN.get(t.get("v5"))
            if g is None:
                continue
            p2, v3, v4 = t.get("v2"), t.get("v3"), t.get("v4")
            if p2 not in COUNTRY_KR:
                continue
            m = r.get("mentions", 1)
            rows[p2][v3].append((g, m))
            ch = next((k for k, kr in CHANNELS.items()
                       if v4 in (k, kr)), None)
            if ch:
                rows[p2][f"{v3}:{ch}"].append((g, m))
            arts[p2].append({"url": u, "title": r.get("title") or "",
                             "code": t.get("v5"), "g": g, "m": m,
                             "dir": v3, "ch": v4})
        day_block = {p2: {"cells": {k: _shrunk(v) for k, v in rows[p2].items()},
                          "articles": arts[p2],
                          "n_day_coded": d.get("n_coded") or 0,
                          "fingerprint": d.get("codebook_fingerprint")}
                     for p2 in rows}
        # **사건이 하나도 없던 쌍도 기록한다.** 완성된 하루에 증거가 없으면
        # 지수는 0 인데, 해설이 아예 없으면 화면에 "해설이 아직 없습니다"가 떠
        # '자료가 빠졌다'로 읽힌다. 실제로는 "N건을 봤는데 그 쌍의 사건이
        # 없었다"이고, 둘은 전혀 다른 말이다 (2026-11-09 PRK 가 그랬다).
        for p2 in COUNTRY_KR:
            if p2 not in day_block:
                day_block[p2] = {"cells": {}, "articles": [],
                                 "n_day_coded": d.get("n_coded") or 0,
                                 "fingerprint": d.get("codebook_fingerprint")}
        out[day] = day_block
    return out


def numbers_block(days, day, partner):
    """수치 요약. **궤적을 반드시 함께 준다** — 오늘 값만 주면 모델이 방향을
    지어낸다(파이프라인에서 실제로 '하락'을 '상승'이라 쓴 일이 있다)."""
    ordered = sorted(days)
    i = ordered.index(day)
    win = ordered[max(0, i - TRAJ_DAYS + 1):i + 1]
    kr = COUNTRY_KR.get(partner, partner)
    lines = [f"한국-{kr} · {day} (왼쪽이 과거, 오른쪽이 오늘)"]

    def traj(key):
        vals = []
        for dd in win:
            c = (days[dd].get(partner) or {}).get("cells") or {}
            v = c.get(key)
            vals.append("—" if not v else f"{v[0]:+.1f}")
        return " ".join(vals)

    cur = (days[day].get(partner) or {}).get("cells") or {}
    for dirn, dl in (("threat", f"{kr} → 한국"), ("response", f"한국 → {kr}")):
        v = cur.get(dirn)
        head = (f"  [{dl}] 전체 [{traj(dirn)}] 오늘 "
                + ("증거 없음" if not v else f"{v[0]:+.2f} (근거 {v[2]}건)"))
        lines.append(head)
        for ch, chkr in CHANNELS.items():
            cv = cur.get(f"{dirn}:{ch}")
            if cv:
                lines.append(f"      {chkr}: [{traj(f'{dirn}:{ch}')}] "
                             f"오늘 {cv[0]:+.2f} (근거 {cv[2]}건)")
            else:
                lines.append(f"      {chkr}: 오늘 증거 없음 — 값을 말하지 말 것")
    return "\n".join(lines)


def articles_block(cell):
    """영향력 큰 순으로 번호 매긴 근거 기사."""
    arts = sorted(cell["articles"], key=lambda a: -abs(a["g"] * a["m"]))[:TOP_ARTICLES]
    out = []
    for n, a in enumerate(arts, 1):
        out.append(f"[{n}] ({a['dir']}/{a['ch']}/{a['code']}, G={a['g']:+.1f}, "
                   f"언급 {a['m']}) {a['title'][:150]}")
    return "\n".join(out), arts


# 지명 표기 통일. 프롬프트에도 넣었지만 모형이 놓칠 수 있어 출력에서 한 번 더 거른다.
# **생성된 해설 문장에만 적용한다** — 기사 제목·원문은 그대로 둔다.
# 이미 올바른 형태로 쓴 것을 다시 감싸지 않도록 앞뒤를 살핀다.
_TERMS = [
    (re.compile(r"다케시마"), "독도"),
    (re.compile(r"\bTakeshima\b", re.I), "Dokdo"),
    (re.compile(r"\bJapan Sea\b", re.I), "East Sea (Sea of Japan)"),
    # 이미 '동해(일본해)' / 'East Sea (Sea of Japan)' 인 것은 건드리지 않는다.
    (re.compile(r"(?<!동해\()(?<!동해\(\s)일본해"), "동해(일본해)"),
    (re.compile(r"(?<!East Sea \()\bSea of Japan\b", re.I), "East Sea (Sea of Japan)"),
]


def normalize_terms(text):
    if not text:
        return text
    for pat, rep in _TERMS:
        text = pat.sub(rep, text)
    return text

# 근거가 이보다 적은 칸에는 **해석을 쓰지 않는다.**
# 지수는 수축 e/(e+5) 로 얇은 증거를 깎는다(e=1 이면 0.167배). 그런데 글에는
# 수축이 없어서, 기사 한 건으로도 "−0.17의 긴장도 하락을 보였습니다" 같은
# 단정문이 나온다. 숫자는 겸손한데 문장은 아니다.
#
# 실측(2026-11-09): 해설 칸 149개 중 **40개(27%)가 기사 한 건**, 52개(35%)가
# 두 건 이하다. 대부분 일본·중국·러시아로, 그날 실제로 사건이 없었고 GDELT 가
# 곁가지 기사를 하나 올린 경우다.
#
# 대신 **사실만** 적는다 — 기사 제목을 그대로 보이고 해석하지 않는다.
# 보는 사람은 그 점이 무엇 때문인지 알 수 있고, 없는 이야기는 안 만들어진다.
# API 호출도 들지 않는다.
MIN_ARTICLES = int(os.environ.get("COMMENTARY_MIN_ARTICLES") or "3")


def thin_note(day, partner, cell):
    """근거가 얇은 칸의 사실 기록. 모델을 부르지 않는다."""
    arts = sorted(cell["articles"], key=lambda a: -abs(a["g"] * a["m"]))
    kr = COUNTRY_KR.get(partner, partner)
    en = COUNTRY_EN.get(partner, partner)
    n = len(arts)
    if n == 0:
        nd = cell.get("n_day_coded") or 0
        return {"ko": f"이 날 기사 {nd:,}건을 읽었고, 그중 한국-{kr} 사이의 사건은 "
                      f"없었습니다. 지수 0 은 '자료가 없다'가 아니라 "
                      f"'사건이 확인되지 않았다'는 뜻입니다.",
                "en": f"{nd:,} articles were read for this day and none reported an "
                      f"event between Korea and {en}. A value of zero means no event "
                      f"was found, not that data is missing.",
                "model": "none (no events)",
                "codebook_fingerprint": cell["fingerprint"],
                "n_articles": 0, "thin": True, "empty": True,
                "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"), "sources": []}
    # **제목을 본문에 넣지 않는다.** 아래 '근거 기사' 에 링크로 같은 제목이
    # 붙어서 두 번 보이고, 위쪽 글자만 본 사람은 링크가 없다고 읽는다.
    ko = (f"근거 기사가 {n}건뿐이라 한국-{kr} 관계의 움직임으로 해석하지 "
          f"않습니다. 아래 근거 기사를 직접 보십시오.")
    eng = (f"Only {n} article{'s' if n != 1 else ''} — too thin to read as a movement "
           f"in Korea–{en} relations. See the source article"
           f"{'s' if n != 1 else ''} below.")
    return {"ko": ko, "en": eng, "model": "none (thin evidence)",
            "codebook_fingerprint": cell["fingerprint"],
            "n_articles": n, "thin": True,
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "sources": [{"url": a["url"], "title": a["title"]} for a in arts[:3]]}



PROMPT = """아래는 한국과 {kr}({en}) 사이의 {day} 하루 긴장도 지수와, 그 지수를 만든 기사입니다.

{numbers}

근거 기사 (영향력 순)
{articles}

이 둘만 근거로 2~4문장의 짧은 해설을 쓰십시오. 규칙:

1. **주어진 기사에 없는 사실을 쓰지 마십시오.** 배경지식·추측·다른 날 사건을 끌어오지 마십시오.
2. **어느 채널이 움직였는지 밝히고, 그 채널의 기사로만 설명하십시오.** 군사 지수를 경제 기사로 설명하는 것은 오류입니다.
3. **방향을 지어내지 마십시오.** 궤적에 근거해서만 상승·하락을 말하고, '증거 없음'인 칸은 값도 상태도 말하지 마십시오.
4. 근거가 1~2건뿐이면 그렇게 쓰십시오 — "근거 1건이라 단정하기 어렵다"가 억지 해석보다 낫습니다.
5. 지수 숫자를 그대로 나열하지 말고, 그날 **무슨 일이 있었는지**를 먼저 쓰십시오. 패널을 보는 사람은 숫자를 이미 보고 있습니다.
6. 제3국의 시각이 기사에 담겨 있어도, 그것은 지금 설명하는 두 나라 관계의 논조가 아닙니다.
7. **지명 표기.** 기사 원문이 무엇이라 쓰든, 해설에서는 다음 표기를 씁니다.
   한국어: 일본해 → **동해(일본해)** · 다케시마 → **독도**
   영어:   Sea of Japan / Japan Sea → **East Sea (Sea of Japan)** · Takeshima → **Dokdo**

출력은 JSON 한 덩어리뿐입니다. 설명을 붙이지 마십시오.
{{"ko": "한국어 해설", "en": "English commentary", "used": [사용한 기사 번호]}}"""


def write_one(client, days, day, partner):
    cell = days[day][partner]
    ab, arts = articles_block(cell)
    msg = PROMPT.format(kr=COUNTRY_KR.get(partner, partner),
                        en=COUNTRY_EN.get(partner, partner), day=day,
                        numbers=numbers_block(days, day, partner), articles=ab)
    try:
        resp = client.messages.create(
            model=MODEL, max_tokens=700,
            messages=[{"role": "user", "content": msg}])
        txt = "".join(b.text for b in resp.content if getattr(b, "type", "") == "text")
    except Exception as e:                                   # noqa: BLE001
        return day, partner, None, f"{type(e).__name__}: {str(e)[:120]}"
    try:
        s = txt[txt.index("{"):txt.rindex("}") + 1]
        j = json.loads(s)
    except (ValueError, IndexError):
        return day, partner, None, "JSON 파싱 불가"
    ko = normalize_terms((j.get("ko") or "").strip())
    en = normalize_terms((j.get("en") or "").strip())
    if not ko and not en:
        return day, partner, None, "본문 비어 있음"
    used = [arts[n - 1] for n in (j.get("used") or []) if 1 <= n <= len(arts)]
    return day, partner, {
        "ko": ko, "en": en, "model": MODEL,
        "codebook_fingerprint": cell["fingerprint"],
        # 이 해설이 몇 건을 보고 쓰였는지. 나중에 기사가 늘면 다시 쓰기 위해서다.
        "n_articles": len(cell["articles"]),
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "sources": [{"url": a["url"], "title": a["title"]} for a in used[:5]],
    }, None


def main():
    t0 = time.time()
    key = os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        sys.exit("ANTHROPIC_API_KEY 가 없다.")
    import anthropic
    client = anthropic.Anthropic(api_key=key)

    days = load_days()
    if not days:
        sys.exit(f"{CODED_DIR}/ 에 코딩 결과가 없다.")

    prev = {}
    if os.path.exists(OUT):
        try:
            prev = (json.load(open(OUT, encoding="utf-8")).get("days") or {})
        except (ValueError, OSError):
            prev = {}

    # 이미 쓴 글에도 지명 규칙을 적용한다. **API 호출이 없다** — 규칙을 나중에
    # 넣었으므로 옛 글에는 안 걸려 있고, 전체를 다시 생성하면 비결정적 모형이라
    # 손댈 이유가 없는 글까지 바뀐다.
    fixed = 0
    for day, ps in prev.items():
        for pt, rec in ps.items():
            for f in ("ko", "en"):
                before = rec.get(f) or ""
                after = normalize_terms(before)
                if after != before:
                    rec[f] = after
                    fixed += 1
    if fixed:
        print(f"  지명 표기 교정 {fixed}개 필드 (호출 없음)")

    # **오늘(UTC)은 매번 다시 쓴다.** GDELT 의 하루는 UTC 09:00(한국시간)에 시작해
    # 하루 내내 기사가 들어온다. 첫 실행은 20~30건만 보고 쓰는데, 건너뛰기만 하면
    # 그 글이 그대로 굳어 저녁에도 아침 몇 건짜리 해설이 걸려 있다
    # (2026-11-03 04:10 에 26건만 보고 쓴 글이 그랬다).
    today_utc = time.strftime("%Y-%m-%d", time.gmtime())

    # **없어진 칸의 옛 해설을 지운다.** 지금까지는 더하기만 했다. 기사가 빠져
    # 그 쌍의 칸이 사라져도 옛 해설이 남아, 그래프에 없는 값을 설명하는 글이
    # 화면에 걸린다 (2026-11-08: 2016년 GSOMIA 기사가 빠진 뒤 JPN 해설이 그랬다).
    pruned = []
    for day in list(prev):
        live = days.get(day) or {}
        for partner in list(prev[day]):
            if partner not in live:
                del prev[day][partner]
                pruned.append(f"{day}/{partner}")
        if not prev[day]:
            del prev[day]
    if pruned:
        print(f"  근거가 사라져 해설을 지운 칸 {len(pruned)}개: " + ", ".join(pruned[:10]))

    todo, redo = [], []
    for day in sorted(days):
        for partner, cell in days[day].items():
            have = (prev.get(day) or {}).get(partner)
            if not have or FORCE:
                todo.append((day, partner))
                continue
            # 지문이 다르면 다시 쓴다 — 옛 수치를 설명하는 글이 남으면 안 된다.
            if have.get("codebook_fingerprint") != cell["fingerprint"]:
                todo.append((day, partner))
                continue
            # 아직 끝나지 않은 하루이거나, 기사가 늘었으면 다시 쓴다.
            n_now = len(cell["articles"])
            # 얇은 칸이 두꺼워졌거나 그 반대면 형식이 바뀌어야 한다
            was_thin, is_thin = bool(have.get("thin")), n_now < MIN_ARTICLES
            # 사실기록·무사건은 모델을 부르지 않으므로 매번 다시 쓴다 — 공짜다.
            # 문구를 고쳐도 옛 글이 남지 않는다 (2026-11-13: 제목 중복 제거).
            if have.get("thin") or have.get("empty"):
                todo.append((day, partner))
                continue
            if day >= today_utc or have.get("n_articles") not in (None, n_now) \
                    or was_thin != is_thin:
                todo.append((day, partner))
                redo.append((day, partner, have.get("n_articles"), n_now))
    if redo:
        same = [r for r in redo if r[0] >= today_utc]
        grew = [r for r in redo if r[0] < today_utc]
        if same:
            print(f"  오늘({today_utc}) {len(same)}칸 — 하루가 끝나지 않아 다시 씀")
        if grew:
            print("  기사가 늘어 다시 쓰는 칸: "
                  + ", ".join(f"{d}/{p} {a}→{b}건" for d, p, a, b in grew[:8]))
    fps = {c["fingerprint"] for d in days.values() for c in d.values()}
    print(f"날짜 {len(days)}일 · 칸 {sum(len(v) for v in days.values())}개 · "
          f"쓸 것 {len(todo)}개 · 지문 {sorted(x or '?' for x in fps)}")
    if len(fps) > 1:
        print("  ::warning::말뭉치에 지문이 섞여 있다 — 해설이 서로 다른 코드북의 수치를 설명하게 된다")
    if MAX_CELLS:
        todo = todo[:MAX_CELLS]
        print(f"  이번 실행 상한 {MAX_CELLS}개")
    if not todo:
        if fixed or pruned:
            # 교정만 했어도 저장해야 한다. 안 그러면 다음 실행에서 또 교정한다.
            out = {d: dict(v) for d, v in prev.items()}
            with open(OUT, "w", encoding="utf-8") as f:
                json.dump({"generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
                           "model": MODEL, "n_days": len(out),
                           "n_cells": sum(len(v) for v in out.values()),
                           "days": out}, f, ensure_ascii=False, indent=1)
            print(f"  저장: {OUT} — 교정·삭제만 반영")
        else:
            print("  쓸 것이 없다.")
        return

    out = {d: dict(v) for d, v in prev.items()}
    fails = collections.Counter()

    # 얇은 칸은 모델을 부르지 않고 사실만 적는다.
    thin = [(d, p) for d, p in todo if len(days[d][p]["articles"]) < MIN_ARTICLES]
    for d, p in thin:
        out.setdefault(d, {})[p] = thin_note(d, p, days[d][p])
    if thin:
        print(f"  근거 {MIN_ARTICLES}건 미만이라 해석 없이 사실만 적은 칸 {len(thin)}개 "
              f"(호출 없음)")
    todo = [x for x in todo if x not in set(thin)]
    if not todo:
        print("  모델을 부를 칸이 없다.")

    done = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futs = [pool.submit(write_one, client, days, d, p) for d, p in todo]
        for f in concurrent.futures.as_completed(futs):
            day, partner, rec, err = f.result()
            done += 1
            if rec:
                out.setdefault(day, {})[partner] = rec
            else:
                fails[err.split(":")[0]] += 1
            if done % 25 == 0:
                print(f"    {done}/{len(todo)} · 실패 {sum(fails.values())}", flush=True)

    with open(OUT, "w", encoding="utf-8") as f:
        json.dump({"generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
                   "model": MODEL, "n_days": len(out),
                   "n_cells": sum(len(v) for v in out.values()),
                   "days": out}, f, ensure_ascii=False, indent=1)

    n = sum(len(v) for v in out.values())
    print(f"\n저장: {OUT} — {len(out)}일 {n}칸")
    if fails:
        print("실패: " + " · ".join(f"{k} {v}" for k, v in fails.most_common()))
    print(f"소요 {time.time()-t0:.0f}초")


if __name__ == "__main__":
    main()
