#!/usr/bin/env python3
"""pipeline.py 에 LLM 코딩 계층(그림자 모드)을 붙인다.

그림자 모드다. 대시보드 산출물(kbti_output.json, audited_series.json,
daily_history.json)은 한 줄도 바뀌지 않는다. 새 파일 두 개에만 쓴다.

붙이는 것
  - import llm_coder
  - shadow_llm_coding() — 오늘 고유 URL을 모아 코딩에 넘긴다
  - main() 안 audit_and_correct() 직후의 호출 한 줄

기사 본문은 감사 계층이 이미 받아온 것을 재사용하고(audit_log.json),
받지 않은 URL만 _extract_article_direct() 로 추가로 받는다. 감사는 상위 K건만
보므로 30일 실측 커버리지가 50.1% 였다. 코딩은 전수를 봐야 하므로 나머지를 채운다.

사용:  python3 patch_shadow_coder.py [pipeline.py]
"""
import hashlib
import re
import sys

PATH = sys.argv[1] if len(sys.argv) > 1 else "pipeline.py"

IMPORT_ANCHOR = "from google.cloud import bigquery"

IMPORT_BLOCK = '''from google.cloud import bigquery

# LLM 코딩 계층(그림자). 모듈이 없어도 파이프라인은 그대로 돌아야 한다.
try:
    import llm_coder
except ImportError:
    llm_coder = None
'''

FUNC_ANCHOR = "def main():"

FUNC_BLOCK = '''def shadow_llm_coding(latest_date, today_rows):
    """LLM 코딩을 그림자로 돌린다. 실패해도 파이프라인을 멈추지 않는다.

    GDELT 는 검색 계층으로만 쓰고, 쌍·방향·주제·사건코드는 기사에서 다시 코딩한다.
    단위가 레코드가 아니라 기사이므로, 기사 하나가 평균 2.87개 레코드로 쪼개져
    증거량이 부풀던 문제가 여기서 사라진다.

    본문은 감사 계층이 방금 쓴 audit_log.json 에서 재사용하고, 거기 없는 URL만
    새로 받는다. 감사는 상위 K건만 보므로 그대로 두면 절반만 코딩된다.
    """
    if llm_coder is None:
        print("  [coder] llm_coder 모듈 없음 — 건너뜀")
        return None
    if not os.environ.get("ANTHROPIC_API_KEY"):
        print("  [coder] ANTHROPIC_API_KEY 없음 — LLM 코딩 건너뜀")
        return None

    # URL 당 언급수는 레코드 최댓값을 쓴다. 합치면 같은 기사를 여러 번 세는 것이 되고,
    # 그것이 지금 지수가 증거량을 부풀리는 바로 그 방식이다.
    best = {}
    for r in today_rows:
        # BigQuery 행의 필드 이름은 쿼리에 쓴 그대로다 — SOURCEURL, NumMentions.
        # raw_data CSV 에서는 url/mentions 로 바뀌어 저장되므로 헷갈리기 쉽다.
        u = r["SOURCEURL"]
        if not u:
            continue
        try:
            m = int(r["NumMentions"] or 1)
        except (TypeError, ValueError, KeyError):
            m = 1
        best[u] = max(best.get(u, 0), m)

    cached = {}
    try:
        with open("audit_log.json", "r", encoding="utf-8") as f:
            for e in json.load(f).get("entries", []):
                if e.get("url") and e.get("title"):
                    cached[e["url"]] = (e.get("title"), e.get("paragraph_1"),
                                        e.get("paragraph_2"))
    except FileNotFoundError:
        pass
    except Exception as e:                                   # noqa: BLE001
        print(f"  [coder] 감사 로그 읽기 실패(무시): {e}")

    articles, refetched = [], 0
    for u, m in sorted(best.items(), key=lambda kv: -kv[1]):
        if u in cached:
            t, p1, p2 = cached[u]
        else:
            if refetched >= llm_coder.CODER_MAX_CALLS:
                continue
            got = _extract_article_direct(u)
            if not got or not (got.get("title") or "").strip():
                continue
            refetched += 1
            ps = (got.get("paragraphs") or []) + ["", ""]
            t, p1, p2 = got["title"], ps[0], ps[1]
        articles.append({"url": u, "title": t, "p1": p1, "p2": p2, "mentions": m})

    print(f"  [coder] 오늘 고유 URL {len(best)}건 중 본문 확보 {len(articles)}건 "
          f"(감사 재사용 {len(articles)-refetched}, 새로 받음 {refetched})")
    try:
        return llm_coder.shadow_run(articles, latest_date)
    except Exception as e:                                   # noqa: BLE001
        print(f"  [coder] ::warning::LLM 코딩 실패(그림자이므로 계속): "
              f"{type(e).__name__}: {e}")
        return None


def main():'''

CALL_ANCHOR = ("        record_audited_day(output, latest_date)   "
               "# 오늘 공개값을 날짜별 기록에 남긴다")

CALL_BLOCK = (CALL_ANCHOR + "\n"
              "        # 그림자: 대시보드 산출물은 건드리지 않고 llm_coding.json "
              "/ llm_index.json 에만 쓴다.\n"
              "        shadow_llm_coding(latest_date, today_rows)")


def main():
    with open(PATH, "r", encoding="utf-8") as f:
        src = f.read()
    before = hashlib.md5(src.encode()).hexdigest()[:8]
    n0 = src.count("\n")

    if "llm_coder" in src:
        print("이미 적용되어 있다. 아무것도 하지 않는다.")
        return 1

    for name, anchor, block in (("import", IMPORT_ANCHOR, IMPORT_BLOCK),
                                ("함수", FUNC_ANCHOR, FUNC_BLOCK),
                                ("호출", CALL_ANCHOR, CALL_BLOCK)):
        if src.count(anchor) != 1:
            print(f"중단: {name} 앵커가 {src.count(anchor)}번 나온다 (1번이어야 한다)")
            print(f"  앵커: {anchor[:70]!r}")
            return 2
        src = src.replace(anchor, block)

    # os / json 은 이미 import 되어 있어야 한다 — 새 코드가 둘 다 쓴다.
    # pipeline.py 는 "import json, os, re, time" 처럼 한 줄에 묶어 쓰므로
    # 모듈명 단위로 찾는다.
    imported = set()
    for line in re.findall(r"^import +(.+)$", src, re.M):
        imported |= {n.strip().split(".")[0].split(" as ")[0]
                     for n in line.split(",")}
    missing = sorted({"os", "json"} - imported)
    if missing:
        print(f"중단: {', '.join(missing)} 가 import 되어 있지 않다")
        return 3

    compile(src, PATH, "exec")          # 문법 검사 — 깨진 파일을 쓰지 않는다

    with open(PATH, "w", encoding="utf-8") as f:
        f.write(src)
    after = hashlib.md5(src.encode()).hexdigest()[:8]
    print(f"적용 완료: {PATH}")
    print(f"  md5 {before} -> {after}")
    print(f"  {n0} -> {src.count(chr(10))} 줄 (+{src.count(chr(10))-n0})")
    print()
    print("남은 일")
    print("  1. llm_coder.py 와 codebook.md 를 저장소에 함께 올린다")
    print("  2. ANTHROPIC_API_KEY 는 이미 Actions 시크릿에 있다 — 감사 계층과 같은 키를 쓴다")
    print("  3. daily.yml 에 llm_coding.json, llm_index.json, llm_coder_cache.json 을")
    print("     커밋 대상에 추가한다")
    print("  4. 대시보드는 그대로다. 전환은 검증이 끝난 뒤 별도 결정.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
