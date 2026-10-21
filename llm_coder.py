#!/usr/bin/env python3
"""LLM 코딩 계층 — 그림자 모드.

GDELT 를 검색 계층으로만 쓰고, 쌍·방향·주제·사건코드는 기사 원문에서 LLM 이 코딩한다.
Goldstein 점수는 모형이 만들지 않고 공표 척도에서 가져온다(코드북 8장).

그림자 모드.
    이 모듈은 llm_coding.json 과 llm_index.json 에만 쓴다.
    kbti_output.json, audited_series.json, daily_history.json 은 건드리지 않는다.
    대시보드는 전환 결정이 날 때까지 기존 값을 그대로 보여준다.

프롬프트는 코드북 파일에서 생성한다.
    사람 코더와 LLM 코더가 같은 문서를 받아야 일치도(kappa)가 지침 차이가 아니라
    과제의 난이도를 재게 된다. 프롬프트를 손으로 따로 쓰면 반드시 갈라진다.

ANTHROPIC_API_KEY 가 없으면 조용히 건너뛴다(기존 감사 계층과 같은 규약).
"""
from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import datetime

CODEBOOK_FILE = "codebook.md"
CODER_CACHE_FILE = "llm_coder_cache.json"
CODING_FILE = "llm_coding.json"
INDEX_FILE = "llm_index.json"

CODER_MODEL = "claude-haiku-4-5-20251001"   # 감사 계층과 같은 모형
CODER_MAX_TOKENS = 300
CODER_MAX_CALLS = 250        # 실행당 신규 호출 상한. 30일 실측 고유 URL 약 94건/일.
CODER_MIN_CHARS = 0          # 코드북 9장 — 제목만으로 충분하면 코딩한다. 길이로 거르지 않는다.

PARTNERS = ["PRK", "JPN", "CHN", "USA", "RUS"]
CONFIDENCE_K = 5             # pipeline.py 와 같은 값이어야 한다

# 코드북 7.2 — 루트 20 + 세부 15. 170·200 은 공표표에서 아직 확인하지 못했다(코드북 8장).
GOLDSTEIN = {
    "010": 0.0, "020": 3.0, "030": 4.0, "040": 1.0, "050": 3.5, "060": 6.0,
    "070": 7.0, "080": 5.0, "090": -2.0, "100": -5.0, "110": -2.0, "120": -4.0,
    "130": -4.4, "140": -6.5, "150": -7.2, "160": -4.0, "180": -9.0, "190": -10.0,
    "016": -2.0, "025": -0.3, "045": 5.0, "046": 7.0, "054": 6.0, "055": 7.0,
    "057": 8.0, "084": 7.0, "085": 7.0, "087": 9.0, "0871": 9.0, "0874": 10.0,
    "138": -7.0, "163": -8.0, "164": -7.0,
}
UNPRICED = {"170", "200"}    # 코드집에는 있으나 Goldstein 미확보 — 집계에서 빼고 세어 둔다

_STATS = {"api_calls": 0, "api_failures": 0, "unparseable": 0, "unpriced": 0}


# ---------------------------------------------------------------- 프롬프트

def _codebook_text(path=CODEBOOK_FILE):
    """코드북 원문. 코더에게 주면 안 되는 절은 뺀다.

    변경 이력과 '맹검 코더가 ...' 식의 주석에는 개발 세트 기사의 제목과 정답이
    들어 있다. 그대로 주면 그 기사들에 대해서만 답을 알려주는 꼴이 된다.
    """
    with open(path, "r", encoding="utf-8") as f:
        txt = f.read()
    txt = re.split(r"^##\s*12\.\s*변경 이력", txt, flags=re.M)[0].rstrip()
    txt = "\n".join(ln for ln in txt.split("\n")
                    if not (ln.startswith(">") and ("맹검" in ln or "v1.0" in ln)))
    return re.sub(r"\n{3,}", "\n\n", txt)


SYSTEM_TMPL = """당신은 내용 분석 코더다. 아래 코드북을 그대로 따른다.
다른 자료를 참조하지 않고, 주어진 제목과 두 단락만 읽고 판정한다.

{codebook}

---

출력은 JSON 한 덩어리뿐이다. 설명을 붙이지 않는다.

{{"v1":"...","v2":"...","v3":"...","v4":"...","v5":"...","conf":"..."}}

  v1   yes | no | unjudgeable
  v2   PRK | JPN | CHN | USA | RUS | unjudgeable
  v3   threat | response | unjudgeable
  v4   military | diplomatic | economic | other | unjudgeable
  v5   7.2 코드집의 코드 하나 | unjudgeable
  conf high | low   (판정불가로 돌릴까 망설였으면 low)

v1 이 no 또는 unjudgeable 이면 {{"v1":"..."}} 만 낸다.
7.2 코드집에 없는 코드는 루트로 접는다. 세 자리를 그대로 적으면 오류다."""

ALLOWED = {
    "v1": {"yes", "no", "unjudgeable"},
    "v2": set(PARTNERS) | {"unjudgeable"},
    "v3": {"threat", "response", "unjudgeable"},
    "v4": {"military", "diplomatic", "economic", "other", "unjudgeable"},
    "v5": set(GOLDSTEIN) | UNPRICED | {"unjudgeable"},
    "conf": {"high", "low"},
}


def prompt_fingerprint(codebook):
    """코드북이 바뀌면 캐시를 버리기 위한 지문. 감사 계층의 prompt_version 과 같은 역할."""
    return hashlib.sha256(codebook.encode("utf-8")).hexdigest()[:16]


# ---------------------------------------------------------------- 코딩

def _parse(text):
    """모형 출력에서 JSON 을 꺼내 허용값으로 검증한다.

    파싱 실패나 허용값 밖은 None 을 돌려 '코딩 안 됨'으로 남긴다.
    감사 계층은 파싱 실패를 '보수적 유지'로 흡수했지만, 여기서는 그러면 안 된다 —
    코딩은 유지/제외가 아니라 값을 만드는 일이므로, 잘못 만든 값이 지수에 들어간다.
    """
    m = re.search(r"\{.*?\}", text, re.S)
    if not m:
        return None
    try:
        d = json.loads(m.group(0))
    except (ValueError, TypeError):
        return None
    if not isinstance(d, dict) or d.get("v1") not in ALLOWED["v1"]:
        return None
    if d["v1"] != "yes":
        return {"v1": d["v1"]}
    out = {"v1": "yes"}
    for k in ("v2", "v3", "v4", "v5", "conf"):
        v = d.get(k)
        if v not in ALLOWED[k]:
            return None
        out[k] = v
    return out


def code_article(client, system, title, p1, p2):
    """기사 하나를 코딩한다. 실패하면 None."""
    body = "\n\n".join(x for x in (p1 or "", p2 or "") if x.strip())
    user = f"제목: {title or ''}\n\n{body}".strip()
    try:
        _STATS["api_calls"] += 1
        resp = client.messages.create(
            model=CODER_MODEL,
            max_tokens=CODER_MAX_TOKENS,
            system=system,
            messages=[{"role": "user", "content": user}],
        )
        out = _parse("".join(b.text for b in resp.content if b.type == "text"))
    except Exception as e:                                   # noqa: BLE001
        _STATS["api_failures"] += 1
        print(f"    [coder] 호출 실패: {type(e).__name__}: {e}")
        return None
    if out is None:
        _STATS["unparseable"] += 1
    return out


# ---------------------------------------------------------------- 집계

def build_index(coded):
    """코딩 결과에서 지수를 만든다. 파이프라인과 같은 수식.

        원값   = -sum(G x 언급수) / sum(언급수)
        표시값 = 원값 x e/(e+CONFIDENCE_K),  e = 기사 수

    단위가 기사이므로 e 는 레코드 수가 아니라 기사 수다. GDELT 가 기사 하나를
    평균 2.87개 레코드로 쪼개 증거량을 부풀리던 것이 여기서 사라진다.
    """
    cells, skipped = {}, 0
    for rec in coded.values():
        t = rec.get("tags") or {}
        if t.get("v1") != "yes":
            continue
        p, d, c = t.get("v2"), t.get("v3"), t.get("v5")
        if p not in PARTNERS or d not in ("threat", "response"):
            continue
        if c in UNPRICED:
            skipped += 1
            continue
        g = GOLDSTEIN.get(c)
        if g is None:
            continue
        cells.setdefault(f"{p}_{d}", []).append((g, max(1, int(rec.get("mentions") or 1))))
    _STATS["unpriced"] = skipped
    out = {}
    for key, vals in cells.items():
        e = len(vals)
        wsum = sum(w for _, w in vals)
        raw = -sum(g * w for g, w in vals) / wsum
        out[key] = {"value": round(raw * e / (e + CONFIDENCE_K), 4),
                    "raw": round(raw, 4), "e": e, "mentions": wsum}
    return out


# ---------------------------------------------------------------- 실행

def _load_cache(fingerprint):
    try:
        with open(CODER_CACHE_FILE, "r", encoding="utf-8") as f:
            c = json.load(f)
    except FileNotFoundError:
        return {}
    except Exception as e:                                   # noqa: BLE001
        print(f"  [coder] 캐시 읽기 실패, 새로 시작: {e}")
        return {}
    if c.get("fingerprint") != fingerprint:
        print("  [coder] 코드북이 바뀌었다 — 캐시 폐기")
        return {}
    return c.get("entries", {}) if isinstance(c.get("entries"), dict) else {}


def _save_cache(fingerprint, entries):
    try:
        with open(CODER_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump({"fingerprint": fingerprint, "saved_at": datetime.now().isoformat(),
                       "entries": entries}, f, ensure_ascii=False, indent=1)
    except Exception as e:                                   # noqa: BLE001
        print(f"  [coder] 캐시 저장 실패(무시): {e}")


def shadow_run(articles, latest_date, codebook_file=CODEBOOK_FILE, client=None):
    """그림자 실행.

    articles : [{"url","title","p1","p2","mentions"}] — 중복 URL 없이
    반환     : 코딩 결과와 지수. 두 파일에만 쓰고 대시보드 산출물은 건드리지 않는다.
    """
    codebook = _codebook_text(codebook_file)
    fp = prompt_fingerprint(codebook)
    system = SYSTEM_TMPL.format(codebook=codebook)
    cache = _load_cache(fp)

    if client is None:
        key = os.environ.get("ANTHROPIC_API_KEY")
        if not key:
            print("  [coder] ANTHROPIC_API_KEY 없음 — LLM 코딩 건너뜀")
            return None
        try:
            import anthropic
            client = anthropic.Anthropic(api_key=key)
        except Exception as e:                               # noqa: BLE001
            print(f"  [coder] 클라이언트 초기화 실패, 건너뜀: {e}")
            return None

    for k in _STATS:
        _STATS[k] = 0

    coded, fresh, hits = {}, 0, 0
    for a in articles:
        url = a.get("url")
        if not url:
            continue
        if url in cache:
            hits += 1
            coded[url] = {"tags": cache[url], "mentions": a.get("mentions", 1),
                          "title": a.get("title", ""), "cached": True}
            continue
        if fresh >= CODER_MAX_CALLS:
            continue
        fresh += 1
        tags = code_article(client, system, a.get("title"), a.get("p1"), a.get("p2"))
        if tags is None:
            continue
        cache[url] = tags
        coded[url] = {"tags": tags, "mentions": a.get("mentions", 1),
                      "title": a.get("title", ""), "cached": False}

    over = max(0, len([a for a in articles if a.get("url") not in cache]) - CODER_MAX_CALLS)
    if over:
        print(f"  [coder] ::warning::상한 {CODER_MAX_CALLS}건 초과 — {over}건 미코딩")
    # 호출이 전부 무위로 끝난 날을 초록색으로 넘기지 않는다. 키 폐기·크레딧 소진은
    # 호출 실패로, 프롬프트·모형 변경은 파싱 불가로 나타나므로 둘을 같이 본다.
    dead = _STATS["api_failures"] + _STATS["unparseable"]
    if _STATS["api_calls"] and dead == _STATS["api_calls"]:
        print(f"  [coder] ::error::신규 호출 {_STATS['api_calls']}건이 전부 무위 "
              f"(실패 {_STATS['api_failures']}, 파싱불가 {_STATS['unparseable']}) "
              f"— 키·크레딧·프롬프트 확인 필요")

    _save_cache(fp, cache)
    index = build_index(coded)

    payload = {
        "date": latest_date,
        "generated_at": datetime.now().isoformat(),
        "codebook_fingerprint": fp,
        "mode": "shadow",
        "model": CODER_MODEL,
        "n_articles": len(articles),
        "n_coded": len(coded),
        "n_relevant": sum(1 for r in coded.values() if r["tags"].get("v1") == "yes"),
        "stats": dict(_STATS),
        "articles": {u: {"tags": r["tags"], "mentions": r["mentions"],
                         "title": r["title"]} for u, r in coded.items()},
    }
    for path, data in ((CODING_FILE, payload),
                       (INDEX_FILE, {"date": latest_date, "mode": "shadow",
                                     "codebook_fingerprint": fp, "cells": index})):
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=1)

    print(f"  [coder] 기사 {len(articles)}건 중 {len(coded)}건 코딩 "
          f"(신규 호출 {fresh}, 캐시 적중 {hits}), 관련 {payload['n_relevant']}건")
    print(f"  [coder] 실패 {_STATS['api_failures']} · 파싱불가 {_STATS['unparseable']} "
          f"· Goldstein 미확보로 제외 {_STATS['unpriced']}")
    for k in sorted(index):
        c = index[k]
        print(f"    {k:<16} {c['value']:+.4f}  (e={c['e']}, raw={c['raw']:+.4f})")
    print(f"  [coder] 그림자 모드 — {CODING_FILE}, {INDEX_FILE} 에만 기록. "
          f"대시보드 산출물은 건드리지 않았다.")
    return payload, index
