#!/usr/bin/env python3
"""pipeline.py 패치 — 채널을 영역(sector)으로 정의하고, 외교를 군사에 대해 배타로 만든다.

왜 (1) — 행위자 기준만으로는 영역이 안 잡힌다
----------------------------------------------
두 채널은 행위자 유형(MIL / GOV)으로만 정의돼 있었다. GDELT에는 영역 필드가
없고 CAMEO는 '주제'가 아니라 '발화 행위'를 코딩한다. 북한 관영매체의 미사일
발사 보도는 행위자가 '북한 정부'로 코딩되지 군 조직으로 코딩되지 않아,
미사일 보도의 74%가 군사 채널에 안 잡혔다. 2026-10-15에는 군사 태그가 붙은
북한 레코드가 0건이라 표시된 지수가 7일 창에서 끌어온 이월값이었다.

왜 (2) — 그런데 외교 코드는 주제를 가리키지 않는다
---------------------------------------------------
여기서 군사와 외교의 성격이 갈린다.

  군사 코드(15x 무력시위, 19x 교전, 20x 대량폭력, 138 군사위협)는 **물질적
  행위**다. 이 코드가 붙었으면 주제가 군사다.

  외교 코드(02x 호소, 03x 의사, 04x 협의, 05x 협력, 10x~13x 요구·비판·거부·
  위협)는 **발화 행위**다. "비난했다"는 무엇에 대해서든 가능하다. 미사일을
  비난해도 112다. 즉 외교 코드는 외교적 *주제*가 아니라 외교적 *양식*만
  가려낸다.

실측이 이를 보여준다 — 주제가 명백히 군사인 레코드(MIL 행위자) 368건 중
161건(43.8%)이 외교 코드를 달고 있다. 제목을 읽으면 전부 미사일·DMZ 폭발·
핵무력·정전협정 위반이다(042 "Seoul: NKorea Fires 2 Projectiles", 020
"Explosion injures 3 South Korean military officers inside DMZ", 112 "North
Korea says it test-fired an intermediate-range missile").

그래서 외교를 코드로만 넓히면 군사 사건이 외교 채널로 흘러든다.

실제로 2026-09-20 외교 채널의 값 +5.4545는 **사건 코드 194(포병·전차 교전)
짜리 미사일 기사 한 건**이 'Seoul'을 정부 행위자로 달았다는 이유로 외교
채널에 들어가 만든 값이었다. 외교적 행위를 잰 것이 아니라 같은 미사일 사건을
한 번 더 센 것이다.

무엇을 하나
-----------
  is_military   = MIL 행위자  OR  군사 영역 코드            (변경 없음)
  is_diplomatic = (GOV 행위자 OR 외교 코드)  AND NOT is_military

군사 주제가 외교 채널에 들어오지 못하게 막는다. 두 채널은 군사에 대해
배타가 되고, 외교 채널에는 '군사 주제가 아닌 외교 행위'만 남는다.

측정 (raw_data 7,815건, 2026-09-16 ~ 10-14)
-------------------------------------------
                                   건수    비중   평균G    음수    양수
  military  현재 (MIL 행위자)        368   4.7%  −1.43  39.9%  40.8%
            영역 (변경 없음)         783  10.0%  −2.57  55.9%  35.0%
  diplomatic 현재 (GOV 행위자)       793  10.1%  +2.15  16.0%  75.2%
            영역 합집합(배타 전)   5,158  66.0%  +2.21  18.8%  79.8%
            영역 + 배타            4,984  63.8%  +2.25  18.6%  80.0%

배타로 빠지는 레코드 174건(외교의 3.4%). 두 채널 중복 0건, 분류 안 됨 26.2%.

일별 계열(전수 기준 진단값 — 실제 지수는 국가쌍별이라 돌려봐야 안다):
  overall    평균 −1.00  SD 1.08  29/29
  military   평균 +2.21  SD 3.40  27/29   overall과 r +0.401
  diplomatic 평균 −2.00  SD 0.95  29/29   overall과 r +0.423
  채널 분리(mil−dip) +4.38 (SD 3.50)  — 현재 정의는 +2.92 (SD 3.86, 24/29)

검증 — 2026-09-20 외교 PRK_threat:
  옛 정의(GOV 행위자만)        +5.4545   <- 194 미사일 레코드가 만든 오염
  영역 합집합(배타 전) 12건    −1.7316
  영역 + 배타          9건    −2.8800   <- 194·042·043 제거됨
  미사일 발사 당일 군사는 치솟고 외교는 협력 쪽에 머문다.

근거: GDELT CAMEO taxonomy
  https://docs.gdeltcloud.com/reference/codes-political-cooperation
  https://docs.gdeltcloud.com/reference/codes-political-conflict

조정 가능한 세 가지
-------------------
DROP_NOISY : 190·192·193 제외 여부. 세 코드 모두 CAMEO 정의상 군사 영역이지만
             GDELT 파서가 비유적·비군사적 동사에 붙인 사례가 섞여 있다(190·193은
             북한 유도탄 시험과 나홍진 영화가 함께, 192는 확인한 4건 전부 잡음).
             혼재율을 엄밀히 재지 않았으므로 판정이 아니라 민감도 모수로 둔다.
DIP_NARROW : 외교에서 01x 공개 성명 제외 여부. 994건(12.7%)의 평균 G가 −0.04이고
             74.2%가 정확히 0.0인 잔여 코드다. True 권장.
DIP_EXCLUSIVE : 위 배타 조건. False 로 두면 합집합(배타 전)이 된다 — 부록
             민감도 분석용.

되돌리기
--------
    git checkout <직전 커밋> -- pipeline.py
"""
import sys, hashlib, pathlib

SRC = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else "pipeline.py")
EXPECT = "97427f40f9a65302a28a691da268b828"   # 감사 v3 배포본

DROP_NOISY = True
DIP_NARROW = True
DIP_EXCLUSIVE = True

# ── 군사 영역 ──────────────────────────────────────────────────────────────
MIL_COOP = [
    "062", "072", "074",
    "087", "0871", "0872", "0873", "0874", "0861",
    "0212", "0232", "0234", "0312", "0332", "0334", "0256", "0356",
]
MIL_CONFLICT = [
    "138", "1381", "1382", "1383", "1384", "1385",
    "15", "150", "151", "152", "153", "154",
    "180", "182", "183",            # 18x 중 국가 간 사건만 (181 납치·1821 성폭행·186 암살 제외)
    "1123", "1124",
    "1012", "1032", "1034", "1056",
    "1212", "1222", "1224", "1246",
    "1622", "1661",
]
MIL_PREFIX = ["19", "20"]           # 교전 / 대량폭력
NOISY = ["190", "192", "193"]

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
DIP_STATEMENTS = [
    "01", "010", "011", "012", "013", "014", "015", "016", "017", "018", "019",
]


def quote(codes):
    return ", ".join(f"'{c}'" for c in sorted(set(codes)))


def wrap(sql, ind, width=92):
    out, line = [], ind
    for tok in sql.split(" "):
        if len(line) + len(tok) + 1 > width and line.strip():
            out.append(line.rstrip())
            line = ind + "   "
        line += tok + " "
    out.append(line.rstrip())
    return "\n".join(out)


def actors(kind, ind, spaced):
    """행위자 유형 조건 6개. 두 쿼리의 등호 공백 스타일이 달라 맞춰준다."""
    eq = " = " if spaced else "="
    parts = [f"COALESCE(Actor{a}Type{t}Code,''){eq}'{kind}'" for a in (1, 2) for t in (1, 2, 3)]
    return (f"{ind}   " + f"\n{ind}   OR ".join(parts))


def mil_codes(ind):
    codes = MIL_COOP + MIL_CONFLICT
    pre = " OR ".join(f"SUBSTR(COALESCE(EventCode,''), 1, 2) = '{p}'" for p in MIL_PREFIX)
    body = (wrap(f"COALESCE(EventCode,'') IN ({quote(codes)})", ind + "   ")
            + f"\n{ind}   OR ({pre})")
    if DROP_NOISY:
        body = (f"{ind}   ((\n{body}\n{ind}    ) AND COALESCE(EventCode,'') "
                f"NOT IN ({quote(NOISY)}))")
    return body


def dip_codes(ind):
    codes = DIP_COOP + DIP_CONFLICT + ([] if DIP_NARROW else DIP_STATEMENTS)
    return wrap(f"COALESCE(EventCode,'') IN ({quote(codes)})", ind + "   ")


def mil_expr(ind, spaced):
    """is_military 의 전체 불리언. 외교 쪽에서 부정해 재사용한다."""
    return (f"{ind}(\n{actors('MIL', ind, spaced)}\n"
            f"{ind}   OR\n{mil_codes(ind)}\n{ind})")


def dip_expr(ind, spaced):
    inner = (f"{ind}   (\n{actors('GOV', ind + '   ', spaced)}\n"
             f"{ind}      OR\n{dip_codes(ind + '   ')}\n{ind}   )")
    if not DIP_EXCLUSIVE:
        return f"{ind}(\n{inner}\n{ind})"
    return (f"{ind}(\n{inner}\n"
            f"{ind}   -- 군사 주제를 외교 채널에서 뺀다. 외교 코드는 발화 '양식'만\n"
            f"{ind}   -- 가리키므로(\"비난했다\"는 미사일에 대해서도 112다) 이 조건이\n"
            f"{ind}   -- 없으면 군사 사건이 외교 채널로 흘러든다 — 2026-09-20의 +5.4545가\n"
            f"{ind}   -- 코드 194 미사일 기사 한 건이 만든 값이었다.\n"
            f"{ind}   AND NOT\n{mil_expr(ind + '   ', spaced)}\n{ind})")


HEAD = (
    "    -- 채널은 '영역(sector)'으로 정의한다 — 행위자 유형 또는 그 영역이 주제인 사건 코드.\n"
    "    -- CAMEO 는 주제가 아니라 발화 행위를 코딩하므로 행위자 유형만으로는 미사일 보도의\n"
    "    -- 74%를 놓친다(관영매체 발사 보도는 행위자가 '북한 정부'로 코딩된다).\n"
    "    -- 군사 코드 집합에는 갈등과 협력이 모두 들어간다(협력 비중 34.0%). 한쪽만 고르면\n"
    "    -- 채널의 부호가 선택 규칙에 의해 결정되기 때문이다.\n"
    "    -- 외교는 추가로 군사에 대해 배타다 — 근거는 patch_sectors2.py 머리말.\n"
)

OLD_A = """    (COALESCE(Actor1Type1Code,'') = 'MIL' OR COALESCE(Actor1Type2Code,'') = 'MIL' OR COALESCE(Actor1Type3Code,'') = 'MIL'
     OR COALESCE(Actor2Type1Code,'') = 'MIL' OR COALESCE(Actor2Type2Code,'') = 'MIL' OR COALESCE(Actor2Type3Code,'') = 'MIL') AS is_military"""

OLD_A_DIP = """    (COALESCE(Actor1Type1Code,'') = 'GOV' OR COALESCE(Actor1Type2Code,'') = 'GOV' OR COALESCE(Actor1Type3Code,'') = 'GOV'
     OR COALESCE(Actor2Type1Code,'') = 'GOV' OR COALESCE(Actor2Type2Code,'') = 'GOV' OR COALESCE(Actor2Type3Code,'') = 'GOV') AS is_diplomatic"""

OLD_B = """  (COALESCE(Actor1Type1Code,'')='MIL' OR COALESCE(Actor1Type2Code,'')='MIL' OR COALESCE(Actor1Type3Code,'')='MIL'
   OR COALESCE(Actor2Type1Code,'')='MIL' OR COALESCE(Actor2Type2Code,'')='MIL' OR COALESCE(Actor2Type3Code,'')='MIL') AS is_military"""

OLD_B_DIP = """  (COALESCE(Actor1Type1Code,'')='GOV' OR COALESCE(Actor1Type2Code,'')='GOV' OR COALESCE(Actor1Type3Code,'')='GOV'
   OR COALESCE(Actor2Type1Code,'')='GOV' OR COALESCE(Actor2Type2Code,'')='GOV' OR COALESCE(Actor2Type3Code,'')='GOV') AS is_diplomatic"""

STALE = """    -- 외교: 군사와 같은 논리 — 이벤트 코드(협력코드만)로 정의하면 구조적으로 항상 긍정 쪽으로
    -- 쏠리는 반대 방향의 편향이 생김. 대신 '정부/외교 행위자(GOV)가 등장하는 모든 이벤트'로
    -- 정의해서, 그 채널이 그날 우호적이었는지 냉각됐는지를 있는 그대로 반영하게 함."""

FRESH = """    -- 외교: GOV 행위자에 더해 외교 영역 코드를 갈등 쪽까지 함께 넣는다(02x 호소 · 03x 의사 ·
    -- 04x 협의 · 05x 협력 · 082 양보 / 10x 요구 · 11x 비판 · 12x 거부 · 13x 위협 · 16x 관계축소).
    -- 01x 공개 성명은 제외했다 — 994건(12.7%)의 평균 G 가 −0.04 이고 74.2%가 정확히 0.0 인
    -- 잔여 코드라 지수를 0 쪽으로 희석시킬 뿐 영역을 가리지 않는다.
    -- 그리고 군사 주제를 뺀다(AND NOT is_military). 외교 코드는 발화 '양식'만 가리키지
    -- '주제'를 가리지 않기 때문이다 — MIL 행위자 레코드의 43.8%가 외교 코드를 달고 있고
    -- 제목은 전부 미사일·DMZ 폭발·핵무력이다."""


def main():
    src = SRC.read_text(encoding="utf-8")
    md5 = hashlib.md5(SRC.read_bytes()).hexdigest()
    print(f"대상: {SRC}  md5 {md5}  {src.count(chr(10)) + 1}줄")
    if md5 != EXPECT:
        print(f"::warning::기대한 배포본이 아닙니다 (기대 {EXPECT}). 그래도 진행합니다.")
    if "영역(sector)" in src:
        sys.exit("이미 패치된 파일입니다.")

    jobs = [("집계 쿼리 military",   OLD_A,     "    ", True,  "military"),
            ("집계 쿼리 diplomatic", OLD_A_DIP, "    ", True,  "diplomatic"),
            ("원시 쿼리 military",   OLD_B,     "  ",   False, "military"),
            ("원시 쿼리 diplomatic", OLD_B_DIP, "  ",   False, "diplomatic")]
    for label, old, ind, spaced, kind in jobs:
        n = src.count(old)
        if n != 1:
            sys.exit(f"{label} 앵커를 찾지 못했습니다 ({n}회).")
        expr = mil_expr(ind, spaced) if kind == "military" else dip_expr(ind, spaced)
        # OLD_* 는 앞 들여쓰기를 포함하므로 expr 도 그대로 쓴다(lstrip 하면 열이 0으로 밀린다).
        # 집계 쿼리의 군사 정의 앞에만 설명 머리말을 붙인다.
        head = HEAD if (kind == "military" and spaced) else ""
        src = src.replace(old, f"{head}{expr} AS is_{kind}", 1)
        print(f"  {label} 교체")

    if src.count(STALE) != 1:
        sys.exit(f"외교 쪽 기존 주석을 찾지 못했습니다 ({src.count(STALE)}회).")
    src = src.replace(STALE, FRESH, 1)
    print("  외교 쪽 기존 주석 갱신")

    SRC.write_text(src, encoding="utf-8")
    print(f"패치 완료 — {src.count(chr(10)) + 1}줄  "
          f"md5 {hashlib.md5(SRC.read_bytes()).hexdigest()}")
    print(f"  DROP_NOISY={DROP_NOISY}  DIP_NARROW={DIP_NARROW}  "
          f"DIP_EXCLUSIVE={DIP_EXCLUSIVE}")


if __name__ == "__main__":
    main()
