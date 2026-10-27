# -*- coding: utf-8 -*-
"""백필 산출물을 origin/main 위에 안전하게 얹어 커밋한다.

왜 필요한가
-----------
2026-10-27 백필은 1,202건을 코딩해 놓고 **푸시에서 통째로 날아갔다.**
도는 동안 정기 파이프라인(3시간 주기)이 llm_series.json 과 llm_coder_cache.json 을
먼저 커밋했고, `git pull --rebase` 가 충돌로 멈춰 러너가 그대로 버려졌다.
코딩은 성공했는데 결과만 사라진 것이라 크레딧이 그냥 소모됐다.

`git pull --rebase` 로는 못 푼다. 두 작업이 같은 JSON 을 **서로 다른 키**에
쓰기 때문이다 — 정기 실행은 오늘 날짜를, 백필은 과거 날짜를 넣는다. 줄 단위
병합으로는 둘 다 살릴 수 없다. 키 단위로 합쳐야 한다.

방식
----
  1. origin/main 을 받아 그 위로 리셋한다 (정기 실행의 산출물을 전부 살린다)
  2. 우리가 소유한 것만 다시 얹는다
       llm_coded/           백필 전용 — 우리 것이 맞다
       llm_series.json      **날짜 키 합집합** — 양쪽 다 살린다
       llm_coder_cache.json **URL 키 합집합** — 지문이 같을 때만
  3. 커밋하고 민다. 미는 데 실패하면 1번부터 다시 (최대 N회)

종료 코드 0 이면 밀었다는 뜻이고, 밀 것이 없을 때도 0 이다.
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time

CODED_DIR = "llm_coded"
SERIES = "llm_series.json"
CACHE = "llm_coder_cache.json"
TRIES = int(os.environ.get("PUSH_TRIES", "4"))


def sh(*args, check=True):
    r = subprocess.run(args, capture_output=True, text=True)
    if check and r.returncode:
        print(f"  ::error::{' '.join(args)}\n  {r.stderr[:400]}")
        sys.exit(1)
    return r


def theirs(path):
    """origin/main 쪽 내용. 없으면 None."""
    r = sh("git", "show", f"origin/main:{path}", check=False)
    if r.returncode:
        return None
    try:
        return json.loads(r.stdout)
    except ValueError:
        return None


def load(path):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (ValueError, OSError):
        return None


def merge_series(ours, them):
    """날짜 키 합집합. 같은 날짜가 양쪽에 있으면 **백필(우리) 쪽을 남긴다** —
    백필은 그날 전수를 코딩하고, 정기 실행은 그날 일부만 본다."""
    if not isinstance(them, dict):
        return ours
    out = dict(them)
    for k, v in (ours or {}).items():
        if k.startswith("_"):
            continue
        out[k] = v
    out["_meta"] = (ours or {}).get("_meta") or them.get("_meta") or {}
    return out


def merge_cache(ours, them):
    """URL 키 합집합. 코드북 지문이 다르면 합치지 않는다 — 다른 코드집으로
    매긴 태그를 섞으면 어느 판으로 코딩됐는지 알 수 없게 된다."""
    if not isinstance(them, dict) or not isinstance(ours, dict):
        return ours
    if them.get("fingerprint") != ours.get("fingerprint"):
        return ours
    e = dict(them.get("entries") or {})
    e.update(ours.get("entries") or {})
    out = dict(ours)
    out["entries"] = e
    return out


def main():
    for attempt in range(1, TRIES + 1):
        # 우리 산출물을 임시로 빼 둔다
        tmp = tempfile.mkdtemp(prefix="kbti_out_")
        if os.path.isdir(CODED_DIR):
            shutil.copytree(CODED_DIR, os.path.join(tmp, CODED_DIR))
        our_series, our_cache = load(SERIES), load(CACHE)

        sh("git", "fetch", "--quiet", "origin", "main")
        sh("git", "reset", "--hard", "--quiet", "origin/main")

        # 1) llm_coded/ 는 우리 것을 그대로 얹는다
        src = os.path.join(tmp, CODED_DIR)
        if os.path.isdir(src):
            os.makedirs(CODED_DIR, exist_ok=True)
            for n in os.listdir(src):
                shutil.copy2(os.path.join(src, n), os.path.join(CODED_DIR, n))
        # 2) 키 단위로 합친다
        if our_series is not None:
            with open(SERIES, "w", encoding="utf-8") as f:
                json.dump(merge_series(our_series, theirs(SERIES)), f,
                          ensure_ascii=False, indent=1)
        if our_cache is not None:
            with open(CACHE, "w", encoding="utf-8") as f:
                json.dump(merge_cache(our_cache, theirs(CACHE)), f,
                          ensure_ascii=False, indent=1)
        shutil.rmtree(tmp, ignore_errors=True)

        sh("git", "add", "-A", CODED_DIR, SERIES, CACHE, check=False)
        st = sh("git", "diff", "--cached", "--quiet", check=False)
        if st.returncode == 0:
            print("  밀 변경 없음")
            return 0
        sh("git", "commit", "--quiet", "-m",
           os.environ.get("COMMIT_MSG", "백필 코딩 결과"))
        # 목적지를 명시한다. 인자 없는 git push 는 로컬 브랜치 이름과
        # upstream 설정에 의존해서, 환경이 바뀌면 조용히 다른 ref 로 간다.
        p = sh("git", "push", "--quiet", "origin", "HEAD:main", check=False)
        if p.returncode == 0:
            n = len(os.listdir(CODED_DIR)) if os.path.isdir(CODED_DIR) else 0
            print(f"  푸시 성공 (시도 {attempt}/{TRIES}) — llm_coded {n}개")
            return 0
        print(f"  푸시 실패, 다시 시도 {attempt}/{TRIES}: {p.stderr[:200]}")
        time.sleep(5 * attempt)

    print("  ::error::푸시를 끝내 못 했습니다. 산출물이 밀리지 않았습니다.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
