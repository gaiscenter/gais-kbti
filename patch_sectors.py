#!/usr/bin/env python3
"""pipeline.py 패치 — 군사·외교 채널을 '영역(sector)' 기준으로 다시 정의한다.

왜
--
두 채널은 행위자 유형(MIL / GOV)으로만 정의돼 있었다. 그런데 GDELT에는 영역
필드가 없고 CAMEO는 '주제'가 아니라 '발화 행위'를 코딩한다. 북한 관영매체의
미사일 발사 보도는 행위자가 '북한 정부'로 코딩되어 군사 채널에 들어가지
못했다 — 미사일 보도의 74%가 누락됐고, 2026-10-15에는 군사 태그가 붙은 북한
레코드가 0건이라 표시된 지수가 7일 창에서 끌어온 이월값이었다.

행위 코드로 넓히되 한쪽 부호만 고르면 채널의 부호가 선택 규칙에 의해
결정된다. 그래서 영역으로 정의하고 양쪽 부호를 모두 넣는다. 군사 영역에는
무력충돌·무력시위·군사위협과 함께 군사협력·군사원조·평화유지·휴전·철수가
들어간다. 결과가 갈등 쪽으로 치우치면 그것은 규칙이 아니라 관계의 성질이다.

무엇을 넣고 무엇을 뺐나
-----------------------
군사 = MIL 행위자  OR  군사 영역 코드
  협력 쪽  062 군사협력 · 072 군사원조 · 074 평화유지 · 087x 긴장완화(휴전·
           봉쇄완화·해산·철수) · 0861 평화유지군 수용 · 02xx/03xx 해당 호소·의사
  갈등 쪽  138x 군사력 위협 · 15x 무력시위 · 19x 교전 · 20x 대량폭력 ·
           1123/1124 침략·전쟁범죄 비난 · 1012/1032/1034/1056 요구 ·
           1212/1222/1224/1246 거부 · 1622/1661 지원중단·평화유지군 축출
  180·182·183 만  18x 중에서. CAMEO 18x(ASSAULT)는 개인 범죄 폭력을 포함한다.
           제목 확인: 182·183 은 DMZ 폭발·연락사무소 폭파 배상(국가 간 사건),
           181 납치 · 1821 성폭행 · 186 암살은 국가 간 군사 행위가 아니다.
  154 포함  어제는 정밀도 기준으로 뺐으나 영역 기준에서는 들어간다.
           제목 7개 중 5개가 실제 군사(북한군 러시아 증파·주한미군 이동·호르무즈).

외교 = GOV 행위자  OR  외교 영역 코드
  협력 쪽  02x 호소 · 03x 의사표명 · 04x 협의 · 05x 외교적 협력 · 082 정치적 양보
  갈등 쪽  10x 요구 · 11x 비판 · 12x 거부 · 13x 위협(비군사) · 16x 관계 축소
  01x 제외  '공개 성명' 994건(12.7%)은 평균 G −0.04 이고 74.2%가 정확히 0.0 이다.
           주제를 담지 않는 잔여 코드라 지수를 0 쪽으로 희석시킬 뿐이다.
           10x 요구(49건)는 전부 −5.0 이지만 유지한다 — 음수만 빼는 것은
           바로 이 패치가 피하려는 '한쪽 부호 배제'다.

측정 (raw_data 7,815건, 2026-09-16 ~ 10-14, 고유 사건 코드 118개)
-----------------------------------------------------------------
                              건수    비중   평균G    음수    양수  협력코드
  military  = MIL actor        368   4.7%  −1.43  39.9%  40.8%   38.6%
            = 합집합           783  10.0%  −2.57  55.9%  35.0%   34.0%
  diplomatic= GOV actor        793  10.1%  +2.15  16.0%  75.2%   66.7%
            = 합집합         5,158  66.0%  +2.21  18.8%  79.8%   79.0%
  어느 채널에도 없음        85.7% -> 26.2%      두 채널 중복 174건 (2.2%)

일별 계열 (전수 기준 진단값 — 실제 지수는 국가쌍별이라 돌려봐야 안다):
                     평균   표준편차  측정일   overall과 r
  overall           −1.00    1.08    29/29        —
  military  현재    +1.37    3.68    24/29     +0.117
            영역    +2.21    3.40    27/29     +0.401
  diplomatic 현재   −1.50    1.83    28/29     +0.361
            영역    −1.96    0.95    29/29     +0.461
  채널 분리(mil−dip) 현재 +2.92 (SD 3.86, 24/29) -> 영역 +4.34 (SD 3.48, 27/29)
  => 외교가 66%를 덮어도 overall 과 중복되지 않고(r 0.461), 분리는 넓어지고
     산포는 줄고, 측정일수는 늘었다. 이월로 메우는 날이 줄어든다는 뜻이다.

민감도 (같은 데이터):
  DROP_NOISY=False  military 1,030건(13.2%) 평균G −4.34, 일별 +4.55(SD 3.28,
                    28/29), overall과 r +0.368, 채널 분리 +6.65(SD 3.34)
  DIP_NARROW=False  diplomatic 6,057건(77.5%) 평균G +1.88

근거: GDELT CAMEO taxonomy
  https://docs.gdeltcloud.com/reference/codes-political-cooperation
  https://docs.gdeltcloud.com/reference/codes-political-conflict

조정 가능한 두 가지
-------------------
DROP_NOISY : 190·192·193 을 뺄지. 세 코드 모두 CAMEO 정의상 군사 영역에
             속하지만 GDELT 파서가 비유적·비군사적 동사에 붙인 사례가 섞여
             있다. 제목을 읽은 결과 190·193 은 진짜와 잡음이 혼재하고(북한
             유도탄 시험·괌 핵공격 시뮬레이션·DMZ 지뢰 폭발 vs 나홍진 영화·
             양궁 금메달·소니 TV 리뷰), 192 는 확인한 4건 전부 잡음이다
             (보스턴다이내믹스, 삼성 갤러리). 혼재율을 엄밀히 재지 않았으므로
             판정이 아니라 민감도 모수로 둔다. True 를 본문, False 를 부록으로
             양쪽 보고하는 것을 권한다.
DIP_NARROW : 외교에서 01x 공개 성명을 뺄지. True 권장(위 근거).
             False 면 6,057건(77.5%)까지 넓어진다.

되돌리기
--------
    git checkout <직전 커밋> -- pipeline.py
"""
import sys, hashlib, pathlib

SRC = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else "pipeline.py")
EXPECT = "97427f40f9a65302a28a691da268b828"   # 감사 v3 배포본

DROP_NOISY = True
DIP_NARROW = True

# ── 군사 영역 ──────────────────────────────────────────────────────────────
MIL_COOP = [
    "062", "072", "074",
    "087", "0871", "0872", "0873", "0874", "0861",
    "0212", "0232", "0234", "0312", "0332", "0334", "0256", "0356",
]
MIL_CONFLICT = [
    "138", "1381", "1382", "1383", "1384", "1385",
    "15", "150", "151", "152", "153", "154",
    "180", "182", "183",            # 18x 중 국가 간 사건만 (181·1821·186 제외)
    "1123", "1124",
    "1012", "1032", "1034", "1056",
    "1212", "1222", "1224", "1246",
    "1622", "1661",
]
MIL_PREFIX = ["19", "20"]           # 교전 / 대량폭력
NOISY = ["190", "192", "193"]       # 파서 오염이 섞인 코드 (민감도 모수)

# ── 외교 영역 ──────────────────────────────────────────────────────────────
DIP_COOP = [
    "02", "020", "022", "024", "0241", "0242", "0243", "0244",
    "025", "0252", "026", "027", "028",
    "03", "030", "032", "034", "0341", "0342", "0343", "0344",
    "035", "0352", "036", "037", "038", "039",
    "04", "040", "041", "042", "043", "044", "045", "046",
    "05", "050", "051", "052", "053", "054", "055", "056", "057",
    "082",
]
DIP_CONFLICT = [
    "10", "100", "102", "104", "1041", "1042", "1043", "1044",
    "105", "1052", "1055", "106", "107", "108",
    "11", "110", "111", "112", "1121", "1122", "113", "114",
    "12", "120", "123", "1231", "1232", "1233", "1234",
    "124", "1242", "1245", "125", "126", "127", "129",
    "13", "130", "131", "1313", "133", "134", "135", "136", "139",
    "161", "164", "165", "1662",
]
DIP_STATEMENTS = [                  # DIP_NARROW=False 일 때만 더한다
    "01", "010", "011", "012", "013", "014", "015", "016", "017", "018", "019",
]


def quote(codes):
    return ", ".join(f"'{c}'" for c in sorted(set(codes)))


def wrap(sql, ind, width=96):
    """IN (...) 목록을 읽을 수 있는 폭으로 접는다."""
    out, line = [], ind
    for tok in sql.split(" "):
        if len(line) + len(tok) + 1 > width and line.strip():
            out.append(line.rstrip())
            line = ind + "   "
        line += tok + " "
    out.append(line.rstrip())
    return "\n".join(out)


def mil_clause(ind):
    codes = MIL_COOP + MIL_CONFLICT
    pre = " OR ".join(f"SUBSTR(COALESCE(EventCode,''), 1, 2) = '{p}'" for p in MIL_PREFIX)
    body = (wrap(f"COALESCE(EventCode,'') IN ({quote(codes)})", ind)
            + f"\n{ind} OR ({pre})")
    if DROP_NOISY:
        body = (f"((\n{body}\n{ind} ) AND COALESCE(EventCode,'') "
                f"NOT IN ({quote(NOISY)}))")
    return body


def dip_clause(ind):
    codes = DIP_COOP + DIP_CONFLICT + ([] if DIP_NARROW else DIP_STATEMENTS)
    return wrap(f"COALESCE(EventCode,'') IN ({quote(codes)})", ind)


HEAD = (
    "    -- 채널은 '영역'으로 정의한다 — 행위자 유형 또는 그 영역이 주제인 사건 코드.\n"
    "    -- CAMEO 는 주제가 아니라 발화 행위를 코딩하므로 행위자 유형만으로는 미사일\n"
    "    -- 보도의 74%를 놓친다(관영매체 발사 보도는 행위자가 '북한 정부'로 코딩된다).\n"
    "    -- 코드 집합에는 갈등과 협력이 모두 들어간다. 한쪽만 고르면 채널의 부호가\n"
    "    -- 선택 규칙에 의해 결정되기 때문이다(군사 채널 협력 코드 비중 34.0%).\n"
    "    -- 자세한 근거와 측정치: patch_sectors.py 머리말.\n"
)

OLD_A = """    (COALESCE(Actor1Type1Code,'') = 'MIL' OR COALESCE(Actor1Type2Code,'') = 'MIL' OR COALESCE(Actor1Type3Code,'') = 'MIL'
     OR COALESCE(Actor2Type1Code,'') = 'MIL' OR COALESCE(Actor2Type2Code,'') = 'MIL' OR COALESCE(Actor2Type3Code,'') = 'MIL') AS is_military"""

OLD_A_DIP = """    (COALESCE(Actor1Type1Code,'') = 'GOV' OR COALESCE(Actor1Type2Code,'') = 'GOV' OR COALESCE(Actor1Type3Code,'') = 'GOV'
     OR COALESCE(Actor2Type1Code,'') = 'GOV' OR COALESCE(Actor2Type2Code,'') = 'GOV' OR COALESCE(Actor2Type3Code,'') = 'GOV') AS is_diplomatic"""

OLD_B = """  (COALESCE(Actor1Type1Code,'')='MIL' OR COALESCE(Actor1Type2Code,'')='MIL' OR COALESCE(Actor1Type3Code,'')='MIL'
   OR COALESCE(Actor2Type1Code,'')='MIL' OR COALESCE(Actor2Type2Code,'')='MIL' OR COALESCE(Actor2Type3Code,'')='MIL') AS is_military"""

OLD_B_DIP = """  (COALESCE(Actor1Type1Code,'')='GOV' OR COALESCE(Actor1Type2Code,'')='GOV' OR COALESCE(Actor1Type3Code,'')='GOV'
   OR COALESCE(Actor2Type1Code,'')='GOV' OR COALESCE(Actor2Type2Code,'')='GOV' OR COALESCE(Actor2Type3Code,'')='GOV') AS is_diplomatic"""


# 패치 후에는 코드와 반대되는 말이 되는 기존 주석. 함께 고친다.
STALE = """    -- 외교: 군사와 같은 논리 — 이벤트 코드(협력코드만)로 정의하면 구조적으로 항상 긍정 쪽으로
    -- 쏠리는 반대 방향의 편향이 생김. 대신 '정부/외교 행위자(GOV)가 등장하는 모든 이벤트'로
    -- 정의해서, 그 채널이 그날 우호적이었는지 냉각됐는지를 있는 그대로 반영하게 함."""

FRESH = """    -- 외교: 군사와 같은 논리. 협력 코드만으로 정의하면 채널이 구조적으로 항상 긍정
    -- 쪽으로 쏠린다 — 그래서 GOV 행위자에 더해, 외교 영역 코드를 갈등 쪽까지 함께
    -- 넣는다 (02x 호소 · 03x 의사 · 04x 협의 · 05x 협력 · 082 양보 / 10x 요구 ·
    -- 11x 비판 · 12x 거부 · 13x 위협 · 16x 관계축소). 01x 공개 성명은 제외했다 —
    -- 994건(12.7%)의 평균 G 가 −0.04 이고 74.2%가 정확히 0.0 인 잔여 코드라
    -- 지수를 0 쪽으로 희석시킬 뿐 영역을 가리지 않는다."""


def build(old, ind, kind):
    """기존 행위자 조건을 유지한 채 영역 코드 조건을 OR 로 덧붙인다."""
    tail = f") AS is_{kind}"
    body = old.rstrip()
    assert body.endswith(tail), body[-40:]
    body = body[: -len(tail)]
    clause = mil_clause(ind) if kind == "military" else dip_clause(ind)
    # tail 에 이미 닫는 괄호가 있다 — 여기서 또 넣으면 괄호가 하나 남는다.
    return f"{body}\n{ind} OR\n{clause}\n{ind}{tail}"


def main():
    src = SRC.read_text(encoding="utf-8")
    md5 = hashlib.md5(SRC.read_bytes()).hexdigest()
    print(f"대상: {SRC}  md5 {md5}  {src.count(chr(10)) + 1}줄")
    if md5 != EXPECT:
        print(f"::warning::기대한 배포본이 아닙니다 (기대 {EXPECT}). 그래도 진행합니다.")
    if "채널은 '영역'으로 정의한다" in src:
        sys.exit("이미 패치된 파일입니다.")

    jobs = [("집계 쿼리 military",   OLD_A,     "    ", "military"),
            ("집계 쿼리 diplomatic", OLD_A_DIP, "    ", "diplomatic"),
            ("원시 쿼리 military",   OLD_B,     "  ",   "military"),
            ("원시 쿼리 diplomatic", OLD_B_DIP, "  ",   "diplomatic")]
    for label, old, ind, kind in jobs:
        n = src.count(old)
        if n != 1:
            sys.exit(f"{label} 앵커를 찾지 못했습니다 ({n}회).")
        src = src.replace(old, build(old, ind, kind), 1)
        print(f"  {label} 교체")

    anchor = "    (COALESCE(Actor1Type1Code,'') = 'MIL'"
    if src.count(anchor) != 1:
        sys.exit(f"머리말 주석 앵커가 {src.count(anchor)}회입니다.")
    src = src.replace(anchor, HEAD + anchor, 1)

    if src.count(STALE) != 1:
        sys.exit(f"외교 쪽 기존 주석을 찾지 못했습니다 ({src.count(STALE)}회).")
    src = src.replace(STALE, FRESH, 1)
    print("  외교 쪽 기존 주석 갱신 (패치 후 코드와 반대되는 설명이었다)")

    SRC.write_text(src, encoding="utf-8")
    print(f"패치 완료 — {src.count(chr(10)) + 1}줄  "
          f"md5 {hashlib.md5(SRC.read_bytes()).hexdigest()}")
    print(f"  DROP_NOISY={DROP_NOISY} (190·192·193 제외)  DIP_NARROW={DIP_NARROW} (01x 제외)")


if __name__ == "__main__":
    main()
