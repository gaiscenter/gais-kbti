# -*- coding: utf-8 -*-
"""통신사가 GDELT 에 정말 없는지 **제대로** 확인한다. 읽기 전용.

check_sources.py 를 대체한다. 그 스크립트의 결론("통신사 전무")은 아래 두 가지
결함 때문에 믿을 수 없다. 결론을 먼저 고치지 말고 측정을 먼저 고친다.

고친 것
-------
1. **대소문자.** BigQuery 의 LIKE 는 대소문자를 구분한다. '%yonhap%' 으로
   찾았으므로 'Yonhap' 으로 저장돼 있으면 못 잡는다. 전부 LOWER() 로 맞춘다.

2. **패턴이 좁았고 동시에 허술했다.** '%reuters.com%' 은 'Reuters' 나
   'reutersagency.com' 을 못 잡는다. 반대로 '%apnews.com%' 은
   'kelownacapnews.com' 에 걸려 AP 가 220건 있는 것처럼 보였다(실제 0건).
   그래서 두 갈래로 나눈다:
     · 엄격 조회 : 도메인 '정확히 일치 또는 서브도메인'. 오탐이 안 난다.
     · 발견 조회 : 'reuters' 같은 낱말이 든 출처를 **그대로 보여준다**.
                   어떤 형태로 저장돼 있는지 모르니 단정하지 말고 눈으로 본다.

3. **DOC API 교차검증.** BigQuery 와 완전히 다른 경로다. GDELT 쪽 안내가
   권하는 domain: 연산자를 그대로 쓴다. 둘이 어긋나면 그 사실 자체가 결과다.

쓰는 법
-------
    python check_wires.py           # dry-run (BigQuery 처리량) + DOC API 는 바로 조회
    RUN=1 python check_wires.py     # 전부 조회
    WIRE_DAYS=30 RUN=1 python ...   # 더 긴 기간. '예전엔 있었는데 끊겼나' 를 본다
"""
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

PROJECT_ID = os.environ.get("GCP_PROJECT")
DAYS = int(os.environ.get("WIRE_DAYS") or "7")
GUARD_GB = float(os.environ.get("QUERY_COST_GUARD_GB") or "5")
RUN = os.environ.get("RUN") == "1"

# (표시이름, 정확히 찾을 도메인들, 발견 조회용 낱말들)
WIRES = [
    ("연합뉴스",     ["yna.co.kr", "en.yna.co.kr", "yonhapnews.co.kr"], ["yonhap", "yna."]),
    ("Reuters",     ["reuters.com"],                                   ["reuters"]),
    ("AP",          ["apnews.com", "ap.org"],                          ["apnews", "associatedpress"]),
    ("AFP",         ["afp.com"],                                       ["afp."]),
    ("Bloomberg",   ["bloomberg.com"],                                 ["bloomberg"]),
    ("매일경제",     ["mk.co.kr"],                                      ["mk.co.kr"]),
    ("뉴시스",       ["newsis.com"],                                    ["newsis"]),
    ("중앙일보",     ["joongang.co.kr", "koreajoongangdaily.joins.com"], ["joongang", "joins."]),
    ("동아일보",     ["donga.com"],                                     ["donga"]),
    ("한국경제",     ["hankyung.com"],                                  ["hankyung"]),
    ("경향신문",     ["khan.co.kr"],                                    ["khan.co.kr"]),
    ("뉴스1",        ["news1.kr"],                                      ["news1."]),
    ("[대조] 코리아헤럴드", ["koreaherald.com"],                          ["koreaherald"]),
    ("[대조] 한겨레",  ["hani.co.kr"],                                    ["hani.co.kr"]),
]

_EXACT = sorted({d for _, ds, _ in WIRES for d in ds})
_WORDS = sorted({w for _, _, ws in WIRES for w in ws})


def _exact_pred(col):
    """도메인 정확 일치 또는 서브도메인. 'kelownacapnews.com' 같은 오탐이 안 난다."""
    c = f"LOWER({col})"
    return " OR ".join(
        f"({c} = '{d}' OR {c} LIKE '%.{d}')" for d in _EXACT)


def _word_pred(col):
    c = f"LOWER({col})"
    return " OR ".join(f"{c} LIKE '%{w}%'" for w in _WORDS)


def _q(table, col, pred):
    return f"""
SELECT {col} AS source, COUNT(*) AS n
FROM `gdelt-bq.gdeltv2.{table}`
WHERE _PARTITIONTIME >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL {DAYS} DAY))
  AND ({pred})
GROUP BY source ORDER BY n DESC
"""


QUERIES = [
    ("GKG · 엄격",          _q("gkg_partitioned", "SourceCommonName", _exact_pred("SourceCommonName"))),
    ("GKG · 발견",          _q("gkg_partitioned", "SourceCommonName", _word_pred("SourceCommonName"))),
    ("mentions · 엄격",     _q("eventmentions_partitioned", "MentionSourceName", _exact_pred("MentionSourceName"))),
    ("mentions · 발견",     _q("eventmentions_partitioned", "MentionSourceName", _word_pred("MentionSourceName"))),
]


# ── DOC API ───────────────────────────────────────────────────────────────
# BigQuery 와 다른 경로. GDELT 안내가 권하는 domain: 연산자를 그대로 쓴다.
#
# **천천히 던진다.** 처음에는 18개를 쉬지 않고 보냈다가 2건 만에 429 를 맞고
# 나머지가 전부 실패했다(2026-11-23). 그 2건은 대조군이 돌기 전이라 0건인지
# 막힌 것인지도 알 수 없었다. 측정이 망가지면 결론도 없다.
DOC_GAP = float(os.environ.get("DOC_GAP") or "6")   # 호출 사이 간격(초)
DOC_RETRY = 3


def doc_api(domain, timespan_days):
    url = ("https://api.gdeltproject.org/api/v2/doc/doc?"
           + urllib.parse.urlencode({
               "query": f"domain:{domain}",
               "mode": "artlist", "format": "json",
               "timespan": f"{timespan_days}d", "maxrecords": "20"}))
    req = urllib.request.Request(url, headers={
        "User-Agent": "gais-kbti-source-check/1.0 (research; contact via github.com/gaiscenter)"})
    wait = DOC_GAP
    for attempt in range(DOC_RETRY):
        try:
            with urllib.request.urlopen(req, timeout=40) as r:
                body = r.read().decode("utf-8", "replace")
            break
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < DOC_RETRY - 1:
                wait *= 2
                print(f"       (429 — {wait:.0f}초 쉬고 재시도)")
                time.sleep(wait)
                continue
            return None, f"HTTP {e.code}"
        except Exception as e:                               # noqa: BLE001
            return None, f"{type(e).__name__}: {str(e)[:70]}"
    else:
        return None, "429 반복"
    body = body.strip()
    if not body:
        return 0, "빈 응답"
    try:
        arts = (json.loads(body).get("articles") or [])
        return len(arts), (arts[0].get("title", "")[:58] if arts else "")
    except ValueError:
        return None, f"JSON 아님: {body[:80]}"


def run_doc_api():
    print(f"\n{'='*72}\n  DOC API 교차검증 (BigQuery 와 다른 경로, 최근 {DAYS}일)")
    print(f"  호출 간격 {DOC_GAP:.0f}초 — 대조군을 **먼저** 돌린다. 대조군이 0건이면"
          f"\n  그 아래 결과는 전부 무의미하므로 거기서 멈춘다.\n{'='*72}")
    ctrl = [w for w in WIRES if w[0].startswith("[대조]")]
    rest = [w for w in WIRES if not w[0].startswith("[대조]")]
    ctrl_ok = False
    for i, (name, domains, _) in enumerate(ctrl + rest):
        for d in domains:
            if i or d != domains[0]:
                time.sleep(DOC_GAP)
            n, note = doc_api(d, DAYS)
            if n is None:
                print(f"  ?  {name:18s} {d:28s} 조회 실패 — {note}")
            else:
                print(f"  {'✔' if n else '✘'}  {name:18s} {d:28s} {n:>3}건  {note}")
                if name.startswith("[대조]") and n:
                    ctrl_ok = True
        if ctrl and name == ctrl[-1][0] and not ctrl_ok:
            print("\n  중단: 대조군이 0건이다. DOC API 조회 자체가 안 된 것이므로"
                  "\n        아래를 돌려도 의미가 없다. DOC_GAP 을 늘려 다시 볼 것.")
            return


def run_bq():
    from google.cloud import bigquery
    if not PROJECT_ID:
        print("GCP_PROJECT 가 없어 BigQuery 는 건너뜁니다.")
        return
    client = bigquery.Client(project=PROJECT_ID)
    total = 0.0
    for label, q in QUERIES:
        j = client.query(q, job_config=bigquery.QueryJobConfig(
            dry_run=True, use_query_cache=False))
        gb = j.total_bytes_processed / 1e9
        total += gb
        print(f"  [dry-run] {label}: {gb:.3f} GB")
    print(f"  [dry-run] 합계 {total:.3f} GB (상한 {GUARD_GB} GB)")
    if total > GUARD_GB:
        sys.exit("중단: 상한 초과. WIRE_DAYS 를 줄이십시오.")
    if not RUN:
        print("\nRUN=1 이 아니어서 BigQuery 조회는 하지 않았습니다.")
        return

    for label, q in QUERIES:
        rows = list(client.query(q, job_config=bigquery.QueryJobConfig(
            maximum_bytes_billed=int(GUARD_GB * 1e9))).result())
        print(f"\n{'='*72}\n  {label} — 최근 {DAYS}일\n{'='*72}")
        if "발견" in label:
            # 어떤 형태로 저장돼 있는지 그대로 보여준다. 단정하지 않는다.
            if not rows:
                print("  해당 낱말이 든 출처가 하나도 없다")
            for r in rows[:40]:
                print(f"    {str(r['source'])[:46]:48s}{r['n']:>8,}건")
            if len(rows) > 40:
                print(f"    … 외 {len(rows)-40}곳")
            continue
        by = {str(r["source"] or "").lower(): r["n"] for r in rows}
        for name, domains, _ in WIRES:
            hit = {s: n for s, n in by.items()
                   if any(s == d or s.endswith("." + d) for d in domains)}
            if hit:
                print(f"  ✔ {name:18s}{sum(hit.values()):>8,}건   {list(hit)[:3]}")
            else:
                print(f"  ✘ {name:18s}       없음")


def main():
    print(f"최근 {DAYS}일 · 상한 {GUARD_GB} GB")
    run_bq()
    run_doc_api()
    print("\n── 읽는 법 ──")
    print("  [대조] 두 곳이 모두 ✔ 가 아니면 결과를 믿지 말 것 — 조회 쪽 문제다")
    print("  '발견' 에 reuters/yonhap 류가 보이면 저장 형태가 달랐던 것이다")
    print("  BigQuery 와 DOC API 가 어긋나면, 어긋난다는 사실 자체를 보고할 것")


if __name__ == "__main__":
    main()
