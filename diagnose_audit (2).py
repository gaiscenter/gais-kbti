#!/usr/bin/env python3
"""
감사 계층 진단기 — 특정 레코드가 '왜' 제외됐는지 조건별로 확인한다.

배경 (2026-10-12):
  북한 미사일 발사가 연합뉴스 톱기사인 날, KBTI의 북한 3개 채널이 모두 전날 값의
  정확히 절반(=전량 제외로 인한 이월 감쇠)을 표시했다. 감사 로그를 보니 북한 레코드
  38건이 100% 제외됐고, 그중에는 합참 발표를 인용한 발사 보도 2건이 포함돼 있었다.
  원고 부록 D.4가 "모델의 관련성 기준이 다섯 코더보다 보수적으로 적용되어 긴장을
  과소보고하는 쪽으로 편향"이라고 적어둔 바로 그 유형이다.

이 스크립트가 하는 일:
  1) 실제 파이프라인의 _extract_article()로 원문을 다시 가져와 어느 경로로 들어왔는지
     (직접 요청 / 리더 프록시), 본문 길이가 얼마인지 보여준다.
  2) 실제 파이프라인의 _audit_with_llm()이 만드는 '운영 프롬프트 원문'을 가로채서
     그대로 호출한다 -> 라이브와 동일한 판정을 재현한다.
  3) 같은 프롬프트에 '조건별로 나누어 답하라'는 지시만 덧붙여 한 번 더 호출한다
     -> 조건 1(직접 상호작용) / 2(논조 일치) / 3(제3자 논조) 중 무엇이 걸렸는지 본다.

  (3)은 진단 전용이다. 운영 프롬프트 자체는 한 글자도 바꾸지 않는다 —
  부록 D의 κ 검증이 그 문구에 대한 검증이기 때문이다.

사용법:
  python diagnose_audit.py                    # audit_log.json에서 PRK 제외건 상위 5개
  python diagnose_audit.py --partner RUS --top 3
  python diagnose_audit.py --url https://... --url https://...
  python diagnose_audit.py --all-flagged --top 10   # 파트너 무관, 제외건 전체에서

환경변수: ANTHROPIC_API_KEY (GitHub Actions에서는 시크릿으로 주입)
"""
import argparse
import json
import os
import sys
from types import SimpleNamespace

import pipeline  # 추출·프롬프트가 운영과 100% 동일하도록 운영 모듈을 그대로 쓴다

MODEL = "claude-haiku-4-5-20251001"
_CAP = {"prompt": None}


class _PromptSpy:
    """_audit_with_llm()이 조립한 프롬프트를 가로채기만 한다(실제 호출 없음)."""

    class messages:
        @staticmethod
        def create(**kw):
            _CAP["prompt"] = kw["messages"][0]["content"]
            return SimpleNamespace(content=[SimpleNamespace(text="예")])


DIAG_SUFFIX = (
    "\n\n---\n"
    "위 판정을 조건별로 나누어 설명하세요. 다른 말 없이 아래 형식만 그대로 출력합니다.\n"
    "조건1: 예|아니오 — (한 문장 이유)\n"
    "조건2: 예|아니오 — (한 문장 이유)\n"
    "조건3: 예|아니오|해당없음 — (한 문장 이유)\n"
    "최종: 예|아니오\n"
)


def pick_entries(args):
    """진단 대상 선정: --url로 직접 주거나, audit_log.json에서 고른다."""
    try:
        with open("audit_log.json", "r", encoding="utf-8") as f:
            log = json.load(f)
    except FileNotFoundError:
        print("audit_log.json이 없습니다. 저장소 루트에서 실행하십시오.", file=sys.stderr)
        sys.exit(1)

    entries = log.get("entries", [])
    print(f"감사 로그: {log.get('date_audited')} 기준, "
          f"{log.get('total_checked')}건 검사 / {log.get('total_flagged')}건 제외\n")

    if args.url:
        by_url = {e["url"]: e for e in entries}
        out = []
        for u in args.url:
            if u in by_url:
                out.append(by_url[u])
            else:  # 로그에 없는 URL도 최소 정보로 진단 가능하게 한다
                out.append({"url": u, "actor1": "PRK", "actor2": "KOR", "event_code": "193",
                            "goldstein": -10.0, "mentions": 1, "partner": "PRK",
                            "direction": "threat", "verdict": "(로그에 없음)"})
        return out

    pool = [e for e in entries if e["verdict"] == "flagged"]
    if not args.all_flagged:
        pool = [e for e in pool if e.get("partner") == args.partner]
    pool.sort(key=lambda e: -abs(e["goldstein"] * e["mentions"]))
    return pool[: args.top]


def ask(client, prompt, max_tokens, temperature=None):
    try:
        kw = {"model": MODEL, "max_tokens": max_tokens,
              "messages": [{"role": "user", "content": prompt}]}
        if temperature is not None:
            kw["temperature"] = temperature
        r = client.messages.create(**kw)
        return r.content[0].text.strip()
    except Exception as e:
        return f"[호출 실패] {e}"


def verdict_of(text):
    """추론 응답의 '최종:' 줄에서 판정을 뽑는다. 못 찾으면 전체 텍스트로 판단."""
    for line in reversed(text.splitlines()):
        if line.strip().startswith("최종"):
            return "아니오" if "아니오" in line else ("예" if "예" in line else "?")
    t = text.strip()
    return "아니오" if t.startswith("아니오") else ("예" if t.startswith("예") else "?")


def repeat_test(client, e, n, temperature=None):
    """
    같은 기사에 세 조건으로 각 n회 호출해 판정 분포를 본다.

      A 운영 현행 : 운영 프롬프트 + AUDIT_MAX_TOKENS + pipeline._parse_verdict
                    (= 지금 배포된 것과 동일. 이 팔의 분산이 곧 지수의 재현불가능성이다)
      B 구버전    : 운영 프롬프트 + max_tokens=10 + 구 파서(startswith)
                    (2026-10-12 이전 설정. 역사적 비교용)
      C 진단      : 운영 프롬프트 + 조건별 서술 접미사 + 400 + 최종줄 파싱

    수정(2026-10-13): A가 v4 프롬프트를 구 파서로 읽어 10/10 '판독불가'를 내고 있었다.
    두 팔이 아무것도 측정하지 못한 채, 자동 해석은 그걸 '제외'로 보고했다.
    A는 이제 운영 설정을 그대로 가져온다 -> pipeline 쪽을 바꾸면 여기도 같이 따라간다.
    """
    ev = {"a1": e["actor1"], "a2": e["actor2"], "code": e["event_code"],
          "goldstein": e["goldstein"], "mentions": e["mentions"], "url": e["url"]}
    art = pipeline._extract_article(e["url"])
    if art is None:
        print("원문 추출 실패 — 반복 시험 불가"); return None
    if art.get("unparseable") or not (art.get("full_text") or ""):
        # 2026-10-13 패치 이후: 닿았지만 기사가 아닌 페이지는 빈 본문으로 온다.
        # 운영은 이 건을 LLM 없이 보수적 유지하므로, 반복 시험할 판정 자체가 없다.
        print("본문 확보 실패(차단·고지 페이지) — 운영은 LLM 없이 보수적 유지합니다. "
              "반복 시험 불가"); return None
    if art.get("unparseable") or not (art.get("full_text") or ""):
        # 2026-10-13 패치 이후: 닿았지만 기사가 아닌 페이지는 빈 본문으로 온다.
        # 이 경우 운영은 LLM을 부르지 않고 보수적 유지하므로, 반복 시험할 판정 자체가 없다.
        print("본문 확보 실패(차단·고지 페이지) — 운영은 이 건을 LLM 없이 보수적 유지합니다. "
              "반복 시험 불가"); return None
    _CAP["prompt"] = None
    pipeline._audit_with_llm(_PromptSpy, pipeline.NAMES.get(e["partner"], e["partner"]), ev, art)
    prod = _CAP["prompt"]
    if not prod:
        print("프롬프트 가로채기 실패 — 반복 시험 불가"); return None
    if not prod:
        print("프롬프트 가로채기 실패 — 반복 시험 불가"); return None

    print(f"반복 시험 n={n}  (총 {n*3}회 호출)")
    print(f"  대상: {e['partner']}/{e.get('direction')} code={ev['code']} G={ev['goldstein']:+.1f}")
    print(f"        {(art.get('title') or '')[:80]}")
    print(f"        본문 {len(art.get('full_text') or '')}자\n")

    def old_parse(r):   # 2026-10-12 이전 파서
        return "아니오" if r.startswith("아니오") else "예" if r.startswith("예") else "?"

    def prod_parse(r):  # 지금 운영이 쓰는 파서
        return "예" if pipeline._parse_verdict(r) else "아니오"

    arms = [
        ("A 운영 현행", prod, pipeline.AUDIT_MAX_TOKENS, prod_parse),
        ("B 구버전(즉답 10토큰)", prod, 10, old_parse),
        ("C 진단(조건별 서술)", prod + DIAG_SUFFIX, 400, verdict_of),
    ]
    out = {}
    for label, prompt, mt, parse in arms:
        tally = {"예": 0, "아니오": 0, "?": 0, "실패": 0}
        for _ in range(n):
            r = ask(client, prompt, mt, temperature)
            tally["실패" if r.startswith("[호출 실패]") else parse(r)] += 1
        print(f"  {label:24} 유지 {tally['예']:>2} / 제외 {tally['아니오']:>2}"
              + (f" / 판독불가 {tally['?']}" if tally["?"] else "")
              + (f" / 호출실패 {tally['실패']}" if tally["실패"] else ""))
        out[label] = tally

    # 임계값 한 줄로 원인을 단정하지 않는다. 2026-10-13에 이 자리의 자동 판정이
    # 두 번 틀렸다(판독불가를 '제외'로 읽음). 사실만 적고 해석은 사람이 한다.
    a = out["A 운영 현행"]
    n_eff = n - a["실패"]
    print()
    if n_eff:
        keep, drop = a["예"], a["아니오"]
        print(f"  운영 현행 설정의 재현성: {n_eff}회 중 유지 {keep} / 제외 {drop}"
              + (f" / 판독불가 {a['?']}" if a["?"] else ""))
        if keep and drop:
            p = min(keep, drop) / n_eff
            print(f"  => 같은 입력에 판정이 갈립니다(소수쪽 {p:.0%}). 지수가 재현되지 않습니다.")
            print("     temperature를 0으로 고정하고 다시 보십시오(--temperature 0).")
        else:
            print("  => 이 기사에 대해서는 운영 설정이 일관됩니다(분산 0).")
            print("     라이브 판정이 이와 다르다면 원인은 이 호출 바깥에 있습니다"
                  " — 캐시, 다른 이벤트행, 또는 더 드문 확률.")
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--partner", default="PRK")
    ap.add_argument("--top", type=int, default=5)
    ap.add_argument("--url", action="append")
    ap.add_argument("--all-flagged", action="store_true")
    ap.add_argument("--repeat", type=int, default=0,
                    help="반복 시험: 같은 기사에 세 조건으로 각 N회 호출해 판정 분포를 본다")
    ap.add_argument("--temperature", type=float, default=None,
                    help="샘플링 온도 고정(예: 0). 비우면 API 기본값 = 운영과 동일")
    ap.add_argument("--out", default="diagnose_audit_report.json")
    args = ap.parse_args()

    key = os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        print("ANTHROPIC_API_KEY가 없습니다.", file=sys.stderr)
        sys.exit(1)
    import anthropic
    client = anthropic.Anthropic(api_key=key)

    targets = pick_entries(args)

    if args.repeat:
        res = repeat_test(client, targets[0], args.repeat, args.temperature)
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump({"mode": "repeat", "n": args.repeat, "url": targets[0]["url"],
                       "temperature": args.temperature,
                       "prod_max_tokens": pipeline.AUDIT_MAX_TOKENS,
                       "result": res}, f, ensure_ascii=False, indent=2)
        print(f"\n보고서 저장: {args.out}")
        return

    print(f"진단 대상 {len(targets)}건 (호출 {len(targets) * 2}회)\n" + "=" * 78)

    report = []
    for i, e in enumerate(targets, 1):
        ev = {"a1": e["actor1"], "a2": e["actor2"], "code": e["event_code"],
              "goldstein": e["goldstein"], "mentions": e["mentions"], "url": e["url"]}
        partner_ko = pipeline.NAMES.get(e["partner"], e["partner"])

        print(f"\n[{i}] {e['partner']}/{e.get('direction')}  "
              f"{ev['a1']}->{ev['a2']}  code={ev['code']}  G={ev['goldstein']:+.1f}  "
              f"m={ev['mentions']}  라이브판정={e['verdict']}")
        print(f"    {e['url'][:110]}")

        # ── 1) 원문 재추출 ────────────────────────────────────────────────
        art = pipeline._extract_article(e["url"])
        if art is None:
            print("    [추출] 실패 — 원문 없음. LLM 호출 없이 코드 기반으로 판정됨.")
            report.append({"url": e["url"], "extract": None})
            continue
        ft = art.get("full_text") or ""
        # 리더 프록시 경로는 paragraphs를 의도적으로 비워둔다(파이프라인 531행)
        route = "리더 프록시" if not art.get("paragraphs") else "직접 요청"
        print(f"    [추출] 경로={route}  제목={(art.get('title') or '(없음)')[:60]}")
        print(f"           본문 {len(ft)}자 / 문단 스냅샷 {len(art.get('paragraphs') or [])}개")
        print(f"           머리말: {ft[:160]}".replace("\n", " "))

        # ── 2) 운영 프롬프트 그대로 재현 ──────────────────────────────────
        _CAP["prompt"] = None
        pipeline._audit_with_llm(_PromptSpy, partner_ko, ev, art)  # 프롬프트만 가로챔
        prod_prompt = _CAP["prompt"]
        if not prod_prompt:
            print("    [프롬프트] 가로채기 실패 — 본문이 비어 조기 반환됐을 수 있음")
            continue
        verdict = ask(client, prod_prompt, pipeline.AUDIT_MAX_TOKENS, args.temperature)
        keep = pipeline._parse_verdict(verdict)
        print(f"    [운영 판정] max_tokens={pipeline.AUDIT_MAX_TOKENS}, _parse_verdict "
              f"-> {'유지' if keep else '제외'}")
        for line in verdict.splitlines():
            if line.strip():
                print("      " + line.strip())

        # ── 3) 조건별 진단 ────────────────────────────────────────────────
        diag = ask(client, prod_prompt + DIAG_SUFFIX, 400, args.temperature)
        print("    [조건별 진단]")
        for line in diag.splitlines():
            if line.strip():
                print("      " + line.strip())

        report.append({
            "url": e["url"], "partner": e["partner"], "direction": e.get("direction"),
            "event_code": ev["code"], "goldstein": ev["goldstein"], "mentions": ev["mentions"],
            "live_verdict": e["verdict"], "replayed_verdict": verdict,
            "extract_route": route, "title": art.get("title"),
            "full_text_len": len(ft), "paragraph_count": len(art.get("paragraphs") or []),
            "diagnosis": diag,
        })

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump({"generated_at": __import__("datetime").datetime.now().isoformat(),
                   "model": MODEL, "results": report}, f, ensure_ascii=False, indent=2)
    print("\n" + "=" * 78)
    print(f"보고서 저장: {args.out} ({len(report)}건)")

    n_keep = sum(1 for r in report if str(r.get("replayed_verdict", "")).startswith("예"))
    print(f"재현 결과: 유지 {n_keep}건 / 제외 {len(report) - n_keep}건")
    print("라이브와 재현 판정이 다르면 모델 응답의 비결정성을 의심할 것(부록 C.5).")


if __name__ == "__main__":
    main()
