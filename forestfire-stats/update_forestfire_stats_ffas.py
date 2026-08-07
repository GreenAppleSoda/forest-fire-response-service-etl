"""
국가산불정보시스템(FFAS) 산불통계 XHR → 전처리 → MariaDB forestfire_stats
+ 로컬 산불이력 JSON/CSV 저장.

GitHub Actions 배포 단위 (이 폴더만):
  - update_forestfire_stats_ffas.py
  - legal-dong-lookup.json
  - requirements.txt

동일 DB 컬럼:
  date, datetime, province, city, town, village,
  damage_area, cause, region_path, is_fire

로컬:
  python update_forestfire_stats_ffas.py --dry-run
  python update_forestfire_stats_ffas.py
  python update_forestfire_stats_ffas.py --init-schema

환경변수: DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME  (선택 DB2_*)
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import sys
from datetime import date, datetime
from functools import lru_cache
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv

KST = ZoneInfo("Asia/Seoul")

HERE = Path(__file__).resolve().parent
DEFAULT_JSON = HERE / "forestfire_stats.json"
DEFAULT_CSV = HERE / "forestfire_stats.csv"
DEFAULT_CSV_DB = HERE / "forestfire_stats_db.csv"
DEFAULT_START = "2011-01-01"
LOOKUP_JSON = HERE / "legal-dong-lookup.json"

# ---------------------------------------------------------------------------
# FFAS XHR
# ---------------------------------------------------------------------------

BASE = "https://fd.forest.go.kr"
PAGE_URL = f"{BASE}/ffas/pubConn/movePage/sub3.do"
API_URL = f"{BASE}/ffas/pubConn/stat/selectStatallList.do"
SIDO_ALL = "00"

FFAS_HEADERS = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Origin": BASE,
    "Referer": PAGE_URL,
    "X-Requested-With": "XMLHttpRequest",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/148.0.0.0 Safari/537.36"
    ),
}


def today_kst() -> date:
    return datetime.now(KST).date()


def _configure_stdout() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")


def fetch_stat_all(
    start: str,
    end: str,
    *,
    sido: str = SIDO_ALL,
    timeout: int = 180,
) -> dict[str, Any]:
    data = {"startDate": start, "endDate": end, "sidoLgdngCd": sido}
    session = requests.Session()
    session.headers.update(FFAS_HEADERS)
    session.get(PAGE_URL, timeout=60)
    resp = session.post(API_URL, data=data, timeout=timeout)
    resp.raise_for_status()
    resp.encoding = "utf-8"
    payload = resp.json()
    if not isinstance(payload, dict) or "statAllList" not in payload:
        raise RuntimeError(f"예상치 못한 FFAS 응답: {type(payload)}")
    return payload


def sigungu_code5(sgng_cd: str | None) -> str | None:
    if not sgng_cd:
        return None
    digits = re.sub(r"\D", "", str(sgng_cd))
    return digits[:5] if len(digits) >= 5 else None


def parse_place_rest(place: str, province: str, city: str, town: str) -> str:
    parts = (place or "").split()
    skip = [t for t in (province, city, town) if (t or "").strip()]
    i = 0
    for tok in skip:
        if i < len(parts) and (parts[i] == tok or parts[i].startswith(tok)):
            i += 1
    return " ".join(parts[i:]).strip()


def _split_datetime(dtm: str) -> tuple[str | None, str | None, str | None, str | None]:
    dtm = (dtm or "").strip()
    if len(dtm) < 10:
        return None, None, None, None
    try:
        y, m, d = dtm[:10].split("-")
    except ValueError:
        return None, None, None, None
    time_part = None
    if len(dtm) >= 19:
        time_part = dtm[11:19]
    elif len(dtm) >= 16:
        time_part = dtm[11:16] + ":00"
    return y, m, d, time_part


def transform_item(raw: dict[str, Any]) -> dict[str, Any]:
    province = (raw.get("ctprv_nm") or "").strip()
    city = (raw.get("sgng_nm") or "").strip()
    town = (raw.get("epmn_nm") or "").strip()
    place = (raw.get("frfr_frng_place") or "").strip()
    toks = place.split()
    if not city and len(toks) >= 2 and province and toks[0] == province:
        city = toks[1]
    rest = parse_place_rest(place, province, city, town)
    if city and " " not in city and len(toks) >= 3 and toks[0] == province:
        if len(city) >= 4 and toks[1] + toks[2] == city.replace(" ", ""):
            rest = " ".join(toks[3:]).strip()
    village = ""
    if rest:
        first = rest.split()[0]
        if not re.match(r"^(산)?\d", first):
            village = first
    dtm = (raw.get("frfr_frng_dtm") or "").strip()
    y, m, d, t = _split_datetime(dtm)
    return {
        "발생일자": dtm[:10] if len(dtm) >= 10 else None,
        "발생일시": dtm or None,
        "발생연도": y,
        "발생월": m,
        "발생일": d,
        "발생시간": t,
        "발생원인": (raw.get("frfr_occrr_caus_clssc_cd_nm") or "").strip() or None,
        "발생원인코드": (raw.get("frfr_occrr_caus_clssc_cd") or "").strip() or None,
        "발생장소": place or None,
        "발생장소_시도": province or None,
        "발생장소_시군구": city or None,
        "발생장소_읍면": town or None,
        "발생장소_동리": village or None,
        "피해면적합계": raw.get("tot_frfr_dmg_area"),
        "피해액": raw.get("tot_frfr_dmg_amt"),
        "복구액": raw.get("frfr_rcvr_amt"),
        "시도코드": (raw.get("ctprv_cd") or "").strip() or None,
        "시군구코드": (raw.get("sgng_cd") or "").strip() or None,
        "시군구코드5": sigungu_code5(raw.get("sgng_cd")),
        "_source": "ffas_selectStatallList",
    }


DB_FIELDNAMES = [
    "date", "datetime", "province", "city", "town", "village",
    "damage_area", "cause", "region_path", "is_fire",
]


def save_history_json(
    items: list[dict[str, Any]],
    path: Path,
    *,
    start: str | None = None,
    end: str | None = None,
) -> None:
    payload = {
        "결과코드": "00",
        "결과메시지": "FFAS selectStatallList",
        "조회시작일": (start or "").replace("-", "") or None,
        "조회종료일": (end or "").replace("-", "") or None,
        "페이지당건수": len(items),
        "전체건수": len(items),
        "조회건수": len(items),
        "산불이력목록": items,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"   saved JSON  {path}  n={len(items)}  ({path.stat().st_size:,} bytes)", flush=True)


def save_items_csv(items: list[dict[str, Any]], path: Path) -> None:
    keys: list[str] = []
    seen: set[str] = set()
    for it in items:
        for k in it:
            if k not in seen and not str(k).startswith("_"):
                seen.add(k)
                keys.append(k)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=keys, extrasaction="ignore")
        w.writeheader()
        for it in items:
            w.writerow({k: ("" if it.get(k) is None else it.get(k)) for k in keys})
    print(f"   saved CSV   {path}  n={len(items)}  ({path.stat().st_size:,} bytes)", flush=True)


def save_rows_csv(rows: list[dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=DB_FIELDNAMES, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            out = {k: ("" if r.get(k) is None else r.get(k)) for k in DB_FIELDNAMES}
            w.writerow(out)
    print(f"   saved CSV   {path}  n={len(rows)}  ({path.stat().st_size:,} bytes)", flush=True)


SCHEMA_DDL = """
-- 산불 발생 이력 (FFAS 산불통계 XHR → 전처리 → 전량교체 적재)
-- damage_area 단위: ha (헥타르)
-- 지역명/region_path: 법정동 정식명 기준
CREATE TABLE IF NOT EXISTS forestfire_stats (
    id              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '내부 PK',
    `date`          DATE            NOT NULL COMMENT '발생일',
    `datetime`      DATETIME        NOT NULL COMMENT '발생일시',
    province        VARCHAR(30)     NOT NULL COMMENT '시도 (정식명)',
    city            VARCHAR(40)     NOT NULL COMMENT '시군구 (정식명)',
    town            VARCHAR(40)     NULL     COMMENT '읍면동 (정식명)',
    village         VARCHAR(40)     NULL     COMMENT '리 (정식명)',
    damage_area     DECIMAL(10,2)   NOT NULL DEFAULT 0.00 COMMENT '피해면적 (ha)',
    cause           VARCHAR(50)     NOT NULL COMMENT '발생원인',
    region_path     VARCHAR(150)    NOT NULL COMMENT '시도 > 시군구 > 읍면동 > 리',
    is_fire         TINYINT         NOT NULL DEFAULT 1 COMMENT '산불 여부',
    created_at      DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP
                                           ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    KEY idx_ffs_date (`date`),
    KEY idx_ffs_datetime (`datetime`),
    KEY idx_ffs_province_date (province, `date`),
    KEY idx_ffs_city_date (city, `date`)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci
  COMMENT='산불발생통계 FFAS (정식 행정명, 전량교체)';
"""

def strip_key(name: str) -> str:
    s = re.sub(r"\s+", "", str(name).strip())
    s = re.sub(r"(특별자치시|광역시|특별시|특별자치도|자치도)$", "", s)
    s = re.sub(r"(시|군|구|읍|면|동|리|가)$", "", s)
    return s


def _is_blank(s: str) -> bool:
    t = str(s or "").strip()
    return not t or t.lower() == "unknown" or t in ("nan", "None")


@lru_cache(maxsize=1)
def load_lookup() -> dict | None:
    base = Path(__file__).resolve().parent
    for p in (base / "legal-dong-lookup.json", base / "legal_dong_lookup.json"):
        if p.exists():
            return json.loads(p.read_text(encoding="utf-8"))
    return None


def _resolve_sido(lookup: dict, raw: str) -> str:
    sido = lookup.get("sido") or {}
    if raw in sido:
        return sido[raw]
    sk = strip_key(raw)
    if sk in sido:
        return sido[sk]
    return raw


def _resolve_child(table: dict[str, str], parent: str, raw: str) -> str:
    if not raw:
        return raw
    for key in (f"{parent}|{raw}", f"{parent}|{strip_key(raw)}"):
        if key in table:
            return table[key]
    return raw


def normalize_parts(
    province: str = "",
    city: str = "",
    town: str = "",
    village: str = "",
    lookup: dict | None = None,
) -> list[str]:
    if lookup is None:
        lookup = load_lookup()
    parts_in = [province, city, town, village]
    cleaned = [p.strip() for p in parts_in if not _is_blank(p)]
    if not cleaned:
        return []
    if not lookup:
        return cleaned
    out: list[str] = []
    sig_map = lookup.get("sigungu") or {}
    emd_map = lookup.get("emd") or {}
    li_map = lookup.get("li") or {}
    sido = _resolve_sido(lookup, cleaned[0])
    out.append(sido)
    if len(cleaned) < 2:
        return out
    sig = _resolve_child(sig_map, sido, cleaned[1])
    if sig == cleaned[1]:
        for alt in {sido, cleaned[0], strip_key(sido)}:
            hit = _resolve_child(sig_map, alt, cleaned[1])
            if hit != cleaned[1]:
                sig = hit
                break
    out.append(sig)
    if len(cleaned) < 3:
        return out
    emd = _resolve_child(emd_map, f"{sido}|{sig}", cleaned[2])
    if emd == cleaned[2]:
        for sig_alt in {sig, strip_key(sig), cleaned[1]}:
            hit = _resolve_child(emd_map, f"{sido}|{sig_alt}", cleaned[2])
            if hit != cleaned[2]:
                emd = hit
                break
    out.append(emd)
    if len(cleaned) < 4:
        return out
    li = _resolve_child(li_map, f"{sido}|{sig}|{emd}", cleaned[3])
    if li == cleaned[3]:
        for emd_alt in {emd, strip_key(emd), cleaned[2]}:
            hit = _resolve_child(li_map, f"{sido}|{sig}|{emd_alt}", cleaned[3])
            if hit != cleaned[3]:
                li = hit
                break
    out.append(li)
    return out


def format_region_path(*parts: object, lookup: dict | None = None) -> str:
    vals = [str(p or "").strip() for p in parts]
    if len(vals) == 1 and ">" in vals[0]:
        vals = [p.strip() for p in vals[0].split(">")]
    while len(vals) < 4:
        vals.append("")
    return " > ".join(normalize_parts(*vals[:4], lookup=lookup))

# 인천: lookup은 2026 개편 구(서해/검단/영종/제물포), API는 옛 서·중·동 표기
# 서/중: 읍면·동이 후보 중 1곳에만 맞을 때 확정. 모르면 기본값 추정하지 않음.
# 동/동구: 제물포구로 1:1 (분할 없음).
INCHEON_OLD_GU_CANDIDATES = {
    "서": ("검단구", "서해구"),
    "서구": ("검단구", "서해구"),
    "중": ("영종구", "제물포구"),
    "중구": ("영종구", "제물포구"),
}
INCHEON_OLD_GU_DIRECT = {
    "동": "제물포구",
    "동구": "제물포구",
}
INCHEON_TOWN_TYPO = {
    "대국": "대곡",  # 대곡동 오탈자로 보임
}

# 전남광주 이상치: 광주 5구일 때만 광주로 보냄 (여수 등 오인 방지)
GWANGJU_GU_KEYS = {
    "광산",
    "광산구",
    "동",
    "동구",
    "서",
    "서구",
    "남",
    "남구",
    "북",
    "북구",
}

# 세종: 시군구 없음(날씨 admin도 세종특별자치시 1건). API는 읍면·리를 city에 넣음.
SEJONG_EUPMYEON = {
    "조치원": "조치원읍",
    "조치원읍": "조치원읍",
    "금남": "금남면",
    "금남면": "금남면",
    "연서": "연서면",
    "연서면": "연서면",
    "부강": "부강면",
    "부강면": "부강면",
    "연기": "연기면",
    "연기면": "연기면",
    "연동": "연동면",
    "연동면": "연동면",
    "소정": "소정면",
    "소정면": "소정면",
    "전동": "전동면",
    "전동면": "전동면",
    "장군": "장군면",
    "장군면": "장군면",
    "전의": "전의면",
    "전의면": "전의면",
    # 오탈자/축약
    "남": "금남면",
}
# city/village에 리명만 온 경우 → 읍면 추정 (산불 API 공출현 + 행정구역)
SEJONG_RI_TO_EUPMYEON = {
    "남곡": "금남면",
    "호탄": "금남면",
    "성덕": "금남면",
    "발산": "금남면",
    "금천": "금남면",
    "신촌": "금남면",
    "봉암": "금남면",
    "용포": "금남면",
    "대박": "금남면",
    "양화": "금남면",
    "청라": "연서면",
    "쌍류": "연서면",
    "신대": "연서면",
    "성제": "연서면",
    "고복": "연서면",
    "부동": "연서면",
    "관정": "전의면",
    "다방": "전의면",
    "금사": "전의면",
    "원성": "전의면",
    # "달전": 전의·금남 동명 → SEJONG_RI에서 제외(미확정). city에 전의/금남이 있으면 그쪽 유지.
    "석곡": "전동면",
    "노장": "전동면",
    "청람": "전동면",
    "심중": "전동면",
    "금암": "장군면",
    "하봉": "장군면",
    "대교": "장군면",
    "합강": "연동면",
    "응암": "연동면",
    "노호": "부강면",
    "운곡": "부강면",
    "수신": "연기면",
    "눌왕": "연기면",
    "신안": "조치원읍",
    "미곡": "연서면",
    "반곡": "연기면",
    "영곡": "연서면",
    # 읍면 없이 리명만 city에 온 건 (수동 확인)
    "운당": "소정면",
    "기룡": "연서면",
    "문곡": "부강면",
    "용암": "연서면",
    "산학": "장군면",
}

# 시+구 붙여쓰기 (공백 없는 API 표기)
CITY_GU_CONCAT = (
    ("창원마산합포", "창원", "마산합포"),
    ("창원마산회원", "창원", "마산회원"),
    ("창원의창", "창원", "의창"),
    ("창원성산", "창원", "성산"),
    ("창원진해", "창원", "진해"),
    ("용인처인", "용인", "처인"),
    ("용인기흥", "용인", "기흥"),
    ("용인수지", "용인", "수지"),
    ("성남분당", "성남", "분당"),
    ("성남수정", "성남", "수정"),
    ("성남중원", "성남", "중원"),
    ("고양덕양", "고양", "덕양"),
    ("고양일산동", "고양", "일산동"),
    ("고양일산서", "고양", "일산서"),
    ("안양만안", "안양", "만안"),
    ("안양동안", "안양", "동안"),
    ("안산상록", "안산", "상록"),
    ("안산단원", "안산", "단원"),
    ("수원장안", "수원", "장안"),
    ("수원권선", "수원", "권선"),
    ("수원팔달", "수원", "팔달"),
    ("수원영통", "수원", "영통"),
    ("천안동남", "천안", "동남"),
    ("천안서북", "천안", "서북"),
    ("청주상당", "청주", "상당"),
    ("청주서원", "청주", "서원"),
    ("청주흥덕", "청주", "흥덕"),
    ("청주청원", "청주", "청원"),
    ("전주완산", "전주", "완산"),
    ("전주덕진", "전주", "덕진"),
    ("포항북", "포항", "북"),
    ("포항남", "포항", "남"),
)

INSERT_SQL = """
INSERT INTO forestfire_stats (
    date, `datetime`, province, city, town, village,
    damage_area, cause, region_path, is_fire
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
)
"""

def load_env() -> None:
    load_dotenv(HERE.parent / ".env")
    load_dotenv(HERE / ".env")


def _db_config_from_prefix(prefix: str, *, required: bool) -> dict[str, Any] | None:
    load_env()
    host = os.getenv(f"{prefix}_HOST")
    user = os.getenv(f"{prefix}_USER")
    password = os.getenv(f"{prefix}_PASSWORD")
    database = os.getenv(f"{prefix}_NAME")
    port_raw = os.getenv(f"{prefix}_PORT")
    vals = [host, user, password, database]
    if not any(vals) and not port_raw:
        if required:
            raise SystemExit(
                f"DB 환경변수 필요: {prefix}_HOST, {prefix}_USER, "
                f"{prefix}_PASSWORD, {prefix}_NAME"
            )
        return None
    missing = [
        f"{prefix}_{n}"
        for n, v in [
            ("HOST", host),
            ("USER", user),
            ("PASSWORD", password),
            ("NAME", database),
        ]
        if not v
    ]
    if missing:
        raise SystemExit(f"DB 환경변수 불완전 ({prefix}): {', '.join(missing)}")
    return {
        "host": host,
        "user": user,
        "password": password,
        "database": database,
        "port": int(port_raw or "3306"),
        "charset": "utf8mb4",
        "autocommit": False,
    }


def db_config() -> dict[str, Any]:
    cfg = _db_config_from_prefix("DB", required=True)
    assert cfg is not None
    return cfg


def db_targets() -> list[tuple[str, dict[str, Any]]]:
    targets: list[tuple[str, dict[str, Any]]] = [("DB", db_config())]
    cfg2 = _db_config_from_prefix("DB2", required=False)
    if cfg2 is not None:
        targets.append(("DB2", cfg2))
    return targets


def get_conn(cfg: dict[str, Any]):
    try:
        import pymysql
    except ImportError as e:
        raise SystemExit("pip install pymysql 필요 (requirements.txt 참고)") from e
    return pymysql.connect(**cfg)


def init_schema(cfg: dict[str, Any], *, label: str = "DB") -> None:
    conn = get_conn(cfg)
    try:
        with conn.cursor() as cur:
            for stmt in SCHEMA_DDL.split(";"):
                s = stmt.strip()
                if s:
                    cur.execute(s)
        conn.commit()
        print(f"[{label}] schema ok: forestfire_stats", flush=True)
    finally:
        conn.close()


FP_KEYS = (
    "date", "datetime", "province", "city", "town", "village",
    "damage_area", "cause", "region_path", "is_fire",
)


def rows_fingerprint(rows: list[dict[str, Any]]) -> tuple[str, int]:
    """전 필드 동일 여부 판별용 지문."""
    lines: list[str] = []
    for r in sorted(
        rows,
        key=lambda x: (
            str(x.get("datetime") or ""),
            str(x.get("region_path") or ""),
            str(x.get("cause") or ""),
            str(x.get("damage_area") or ""),
        ),
    ):
        parts = []
        for k in FP_KEYS:
            v = r.get(k)
            if v is None:
                v = ""
            elif k == "damage_area":
                v = f"{float(v):.2f}"
            else:
                v = str(v)
            parts.append(v)
        lines.append("\t".join(parts))
    digest = hashlib.sha256("\n".join(lines).encode("utf-8")).hexdigest()
    return digest, len(rows)


def load_db_rows(cfg: dict[str, Any]) -> list[dict[str, Any]]:
    conn = get_conn(cfg)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT `date`, `datetime`, province, city, town, village,
                       damage_area, cause, region_path, is_fire
                FROM forestfire_stats
                """
            )
            raw = cur.fetchall()
    finally:
        conn.close()
    out: list[dict[str, Any]] = []
    for t in raw:
        d, dt = t[0], t[1]
        out.append(
            {
                "date": d.isoformat() if hasattr(d, "isoformat") else str(d)[:10],
                "datetime": (
                    dt.strftime("%Y-%m-%d %H:%M:%S")
                    if hasattr(dt, "strftime")
                    else str(dt)[:19]
                ),
                "province": t[2],
                "city": t[3],
                "town": t[4],
                "village": t[5],
                "damage_area": round(float(t[6] or 0), 2),
                "cause": t[7],
                "region_path": t[8],
                "is_fire": int(t[9]),
            }
        )
    return out


def replace_all(
    cfg: dict[str, Any],
    rows: list[dict[str, Any]],
    *,
    batch: int = 1000,
    label: str = "DB",
    force: bool = False,
) -> int:
    """내용이 동일하면 skip, 아니면 트랜잭션 DELETE+INSERT 전량교체."""
    new_fp, new_n = rows_fingerprint(rows)
    if not force:
        try:
            old_fp, old_n = rows_fingerprint(load_db_rows(cfg))
            if old_fp == new_fp and old_n == new_n:
                print(
                    f"   [{label}] unchanged skip  n={new_n}  fp={new_fp[:12]}…",
                    flush=True,
                )
                return 0
        except Exception as e:
            print(f"   [{label}] compare failed, will replace ({e})", flush=True)

    tuples = [
        (
            r["date"],
            r["datetime"],
            r["province"],
            r["city"],
            r["town"],
            r["village"],
            r["damage_area"],
            r["cause"],
            r["region_path"],
            r["is_fire"],
        )
        for r in rows
    ]
    conn = get_conn(cfg)
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM forestfire_stats")
            for i in range(0, len(tuples), batch):
                cur.executemany(INSERT_SQL, tuples[i : i + batch])
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    host = cfg.get("host", "?")
    db = cfg.get("database", "?")
    print(f"   [{label}] replaced={len(tuples)}  ({host}/{db})", flush=True)
    return len(tuples)


def replace_all_targets(
    targets: list[tuple[str, dict[str, Any]]],
    rows: list[dict[str, Any]],
    *,
    force: bool = False,
) -> dict[str, int]:
    counts: dict[str, int] = {}
    errors: list[str] = []
    for label, cfg in targets:
        try:
            counts[label] = replace_all(cfg, rows, label=label, force=force)
        except Exception as e:
            errors.append(f"[{label}] {e}")
            print(f"   [{label}] FAIL: {e}", flush=True)
    if errors:
        raise SystemExit("DB 적재 실패:\n" + "\n".join(errors))
    return counts

def _as_str(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and value != value:  # NaN
        return ""
    s = str(value).strip()
    if s.lower() in {"", "nan", "none", "unknown"}:
        return ""
    return s


def fix_jeonnam_gwangju(province: str, city: str) -> tuple[str, str]:
    """전남광주 + 광주 5구 → 광주. 그 외(여수 등)는 원문 유지."""
    if province.replace(" ", "") != "전남광주":
        return province, city
    city_key = city.replace(" ", "") if city else ""
    if city_key in GWANGJU_GU_KEYS:
        return "광주", city
    return province, city


def fix_gunwi(province: str, city: str, town: str, village: str) -> tuple[str, str, str, str]:
    """군위군: 경북→대구 편입. 날씨 admin도 대구/군위군."""
    c = city.replace(" ", "")
    if c in {"군위", "군위군"} or c.startswith("군위"):
        return "대구", "군위", town, village
    return province, city, town, village


def fix_sejong(province: str, city: str, town: str, village: str) -> tuple[str, str, str, str]:
    """
    세종: 시군구 칸에 읍면·리가 들어오는 API 표기를
    city=세종특별자치시 + town(읍면) + village(리) 로 정리.
    """
    p = province.replace(" ", "")
    if p not in {"세종", "세종시", "세종특별자치시"}:
        return province, city, town, village

    city_key = city.replace(" ", "") if city else ""
    town_key = town.replace(" ", "") if town else ""
    village_key = village.replace(" ", "") if village else ""

    new_town = ""
    new_village = village_key

    # 1) city가 읍면
    if city_key in SEJONG_EUPMYEON:
        new_town = SEJONG_EUPMYEON[city_key]
        if town_key and not village_key:
            new_village = town_key
        elif town_key and village_key:
            new_village = village_key
    # 2) town이 읍면 (city 비었거나 이상값)
    elif town_key in SEJONG_EUPMYEON:
        new_town = SEJONG_EUPMYEON[town_key]
        if city_key and city_key not in SEJONG_EUPMYEON and not village_key:
            # city에 리명만 있던 경우
            new_village = city_key
    # 3) city가 리명
    elif city_key in SEJONG_RI_TO_EUPMYEON:
        new_town = SEJONG_RI_TO_EUPMYEON[city_key]
        new_village = village_key or city_key
    # 4) village만으로 읍면 추정
    elif village_key in SEJONG_RI_TO_EUPMYEON:
        new_town = SEJONG_RI_TO_EUPMYEON[village_key]
        new_village = village_key
    # 5) town이 리명
    elif town_key in SEJONG_RI_TO_EUPMYEON:
        new_town = SEJONG_RI_TO_EUPMYEON[town_key]
        new_village = village_key or town_key
    else:
        # 읍면 미확정: 남은 텍스트를 town/village에 최대한 보존
        if city_key and not town_key:
            new_town = city_key
        else:
            new_town = town_key
        if not new_village and city_key and town_key:
            new_village = city_key

    return "세종", "세종특별자치시", new_town or "", new_village or ""


def _incheon_emd_hit(emd: dict[str, str], gu: str, town: str) -> str | None:
    """인천광역시|{gu}|{town} 계열 키로 공식 읍면동명 반환."""
    t = town.strip()
    if not t:
        return None
    parents = (f"인천광역시|{gu}", f"인천|{gu}")
    candidates = (t, f"{t}동", f"{t}읍", f"{t}면", f"{t}가")
    for parent in parents:
        for cand in candidates:
            hit = emd.get(f"{parent}|{cand}")
            if hit:
                return hit
    return None


def fix_incheon(
    province: str,
    city: str,
    town: str,
    village: str,
    lookup: dict | None = None,
) -> tuple[str, str, str, str]:
    """
    인천 옛 구 약칭 → 개편 구.

    - 동/동구 → 제물포구 (1:1)
    - 서/중 → 읍면·동이 후보 중 1곳에만 맞을 때 확정
    - 애매·읍면 없음 → 서/중 원문 유지 (억지 기본값 금지)
    확정 시 오탈자 보정값(village_fix)을 반환에 반영.
    """
    p = province.replace(" ", "")
    if p not in {"인천", "인천시", "인천광역시"}:
        return province, city, town, village

    city_key = city.replace(" ", "")
    town_fix = INCHEON_TOWN_TYPO.get(town, town) if town else town
    village_fix = INCHEON_TOWN_TYPO.get(village, village) if village else village
    emd = (lookup or {}).get("emd") or {}

    def _resolve_under_gu(gu: str) -> tuple[str, str, str, str]:
        for hint, from_village in (
            (town_fix, False),
            (village_fix, not bool(town_fix)),
        ):
            if not hint or not emd:
                continue
            formal_town = _incheon_emd_hit(emd, gu, hint)
            if formal_town:
                # village로만 맞춘 경우 town에 공식동, village 비움
                out_village = "" if from_village else (village_fix or "")
                return "인천", gu, formal_town, out_village
        return "인천", gu, town_fix or town, village_fix or village

    if city_key in INCHEON_OLD_GU_DIRECT:
        return _resolve_under_gu(INCHEON_OLD_GU_DIRECT[city_key])

    if city_key not in INCHEON_OLD_GU_CANDIDATES:
        return province, city, town, village

    candidates = INCHEON_OLD_GU_CANDIDATES[city_key]
    for hint, from_village in (
        (town_fix, False),
        (village_fix, not bool(town_fix)),
    ):
        if not hint or not emd:
            continue
        hits: dict[str, str] = {}
        for gu in candidates:
            formal_town = _incheon_emd_hit(emd, gu, hint)
            if formal_town:
                hits[gu] = formal_town
        if len(hits) == 1:
            gu, formal_town = next(iter(hits.items()))
            out_village = "" if from_village else (village_fix or "")
            return "인천", gu, formal_town, out_village
        # 0개: 다음 hint / 2개 이상: 애매 → 확정하지 않음

    # 미확정: 옛 표기 유지 (서해구 등으로 추정하지 않음)
    return "인천", city_key, town_fix or town, village_fix or village


def split_city_gu(city: str, town: str, village: str) -> tuple[str, str, str]:
    """
    '포항 북', '용인처인' → city=포항/용인, town=북/처인.

    legal-dong-lookup은 일반구를 sigungu가 아니라 emd(시 아래)로 두므로
    구는 town 슬롯이 맞음. (예: 경기|수원시|장안 → 장안구)
    기존 town(읍면)은 village가 비어 있으면 village로 이동.
    """
    c = _as_str(city)
    if not c:
        return city, town, village

    base = gu = None
    if " " in c:
        left, right = c.split(" ", 1)
        left, right = left.strip(), right.strip()
        if left and right and " " not in right:
            base, gu = left, right
    if base is None:
        compact = re.sub(r"\s+", "", c)
        for full, b, g in CITY_GU_CONCAT:
            if compact == full:
                base, gu = b, g
                break

    if base is None or gu is None:
        return city, town, village

    old_town = _as_str(town)
    old_village = _as_str(village)
    new_town = gu
    if old_town and not old_village:
        new_village = old_town
    elif old_town and old_village:
        # 구를 town에 넣고 읍면명을 village에 우선 (리는 드롭)
        new_village = old_town
    else:
        new_village = old_village
    return base, new_town, new_village


def build_datetime(year: Any, month: Any, day: Any, time_raw: Any) -> datetime | None:
    try:
        y = int(str(year).strip())
        m = int(str(month).strip())
        d = int(str(day).strip())
    except (TypeError, ValueError):
        return None
    t = _as_str(time_raw) or "00:00:00"
    # HH:MM or HH:MM:SS
    m_time = re.match(r"^(\d{1,2}):(\d{2})(?::(\d{2}))?$", t)
    if m_time:
        hh, mm, ss = (
            int(m_time.group(1)),
            int(m_time.group(2)),
            int(m_time.group(3) or 0),
        )
    else:
        hh = mm = ss = 0
    try:
        return datetime(y, m, d, hh, mm, ss)
    except ValueError:
        return None


def preprocess_items(items: list[dict]) -> list[dict[str, Any]]:
    print("2) preprocess + region normalize", flush=True)
    lookup = load_lookup()
    if lookup is None:
        print("   WARN: legal-dong-lookup.json 없음 — 약칭 그대로 저장", flush=True)
    else:
        print("   lookup loaded", flush=True)

    rows: list[dict[str, Any]] = []
    skipped = 0
    for item in items:
        province = _as_str(item.get("발생장소_시도"))
        city = _as_str(item.get("발생장소_시군구"))
        town = _as_str(item.get("발생장소_읍면"))
        village = _as_str(item.get("발생장소_동리"))

        province, city = fix_jeonnam_gwangju(province, city)
        province, city, town, village = fix_gunwi(province, city, town, village)
        province, city, town, village = fix_sejong(province, city, town, village)
        province, city, town, village = fix_incheon(
            province, city, town, village, lookup=lookup
        )
        city, town, village = split_city_gu(city, town, village)

        dt = build_datetime(
            item.get("발생연도"),
            item.get("발생월"),
            item.get("발생일"),
            item.get("발생시간"),
        )
        if dt is None:
            # 발생일자 보조
            ymd = _as_str(item.get("발생일자"))
            if ymd:
                try:
                    base = datetime.strptime(ymd[:10], "%Y-%m-%d")
                    t = _as_str(item.get("발생시간")) or "00:00:00"
                    m_time = re.match(r"^(\d{1,2}):(\d{2})(?::(\d{2}))?$", t)
                    if m_time:
                        dt = base.replace(
                            hour=int(m_time.group(1)),
                            minute=int(m_time.group(2)),
                            second=int(m_time.group(3) or 0),
                        )
                    else:
                        dt = base
                except ValueError:
                    dt = None
        if dt is None:
            skipped += 1
            continue

        parts = normalize_parts(province, city, town, village, lookup=lookup)
        province_f = parts[0] if len(parts) > 0 else (province or "Unknown")
        city_f = parts[1] if len(parts) > 1 else (city or "Unknown")
        town_f = parts[2] if len(parts) > 2 else (town or None)
        village_f = parts[3] if len(parts) > 3 else (village or None)
        if town_f is not None and _is_blank(town_f):
            town_f = None
        if village_f is not None and _is_blank(village_f):
            village_f = None

        region_path = format_region_path(
            province_f,
            city_f,
            town_f or "",
            village_f or "",
            lookup=lookup,
        )
        if not region_path:
            region_path = " > ".join(
                p for p in [province_f, city_f, town_f, village_f] if p
            )

        cause = _as_str(item.get("발생원인")) or "Unknown"
        try:
            damage = float(item.get("피해면적합계") or 0)
        except (TypeError, ValueError):
            damage = 0.0

        rows.append(
            {
                "date": dt.date().isoformat(),
                "datetime": dt.strftime("%Y-%m-%d %H:%M:%S"),
                "province": province_f,
                "city": city_f,
                "town": town_f,
                "village": village_f,
                "damage_area": round(damage, 2),
                "cause": cause,
                "region_path": region_path,
                "is_fire": 1,
            }
        )

    rows.sort(key=lambda r: r["datetime"], reverse=True)
    print(
        f"   rows={len(rows)} skipped_no_date={skipped} "
        f"provinces={len({r['province'] for r in rows})}",
        flush=True,
    )
    if rows:
        print(f"   sample: {rows[0]}", flush=True)
    return rows



# ---------------------------------------------------------------------------
# pipeline
# ---------------------------------------------------------------------------


def load_items_from_history_json(path: Path) -> list[dict[str, Any]]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict):
        items = raw.get("산불이력목록")
        if items is not None:
            return list(items)
        all_list = raw.get("statAllList")
        if all_list is not None:
            return [transform_item(x) for x in all_list]
    raise SystemExit(f"지원하지 않는 JSON: {path}")


def fetch_ffas_items(start: str, end: str, *, sido: str = SIDO_ALL) -> list[dict[str, Any]]:
    print(f"1) fetch FFAS  {start} ~ {end}  sido={sido}", flush=True)
    payload = fetch_stat_all(start, end, sido=sido)
    all_list = payload.get("statAllList") or []
    print(f"   statAllList={len(all_list)}", flush=True)
    items = [transform_item(x) for x in all_list]
    print(f"   transformed={len(items)}", flush=True)
    return items


def run(
    *,
    start: str,
    end: str,
    sido: str = SIDO_ALL,
    from_json: Path | None = None,
    save_json: Path | None = DEFAULT_JSON,
    save_csv: Path | None = DEFAULT_CSV,
    save_csv_db: Path | None = DEFAULT_CSV_DB,
    dry_run: bool = False,
    force: bool = False,
) -> list[dict[str, Any]]:
    if from_json is not None:
        print(f"1) load JSON  {from_json}", flush=True)
        items = load_items_from_history_json(from_json)
        print(f"   loaded={len(items)}", flush=True)
    else:
        items = fetch_ffas_items(start, end, sido=sido)

    if not items:
        raise SystemExit("수집/로드 결과 0건")

    if save_json is not None:
        save_history_json(
            items,
            save_json,
            start=start if from_json is None else None,
            end=end if from_json is None else None,
        )
    if save_csv is not None:
        save_items_csv(items, save_csv)

    rows = preprocess_items(items)
    if not rows:
        raise SystemExit("전처리 결과 0건")

    if save_csv_db is not None:
        save_rows_csv(rows, save_csv_db)

    if dry_run:
        print("3) dry-run (no DB)", flush=True)
        for r in rows[:5]:
            print(f"   {r}", flush=True)
        return rows

    targets = db_targets()
    print(
        f"3) replace MariaDB forestfire_stats  "
        f"targets={','.join(t[0] for t in targets)}",
        flush=True,
    )
    replace_all_targets(targets, rows, force=force)
    return rows


def main() -> None:
    _configure_stdout()
    today = today_kst().isoformat()
    p = argparse.ArgumentParser(
        description="FFAS 산불통계 → 전처리 → forestfire_stats + JSON/CSV"
    )
    p.add_argument("--start", default=DEFAULT_START, help="조회 시작일 YYYY-MM-DD")
    p.add_argument("--end", default=today, help="조회 종료일 YYYY-MM-DD")
    p.add_argument("--sido", default=SIDO_ALL, help="시도 코드 (00=전국)")
    p.add_argument("--from-json", type=Path, default=None, help="기존 JSON 사용")
    p.add_argument("--save-json", type=Path, default=DEFAULT_JSON)
    p.add_argument("--save-csv", type=Path, default=DEFAULT_CSV)
    p.add_argument("--save-csv-db", type=Path, default=DEFAULT_CSV_DB)
    p.add_argument("--no-save", action="store_true", help="JSON/CSV 저장 생략")
    p.add_argument("--dry-run", action="store_true", help="DB 적재 생략")
    p.add_argument("--init-schema", action="store_true", help="CREATE TABLE 실행")
    p.add_argument("--force", action="store_true", help="동일 내용이어도 전량교체")
    args = p.parse_args()

    if args.init_schema:
        for label, cfg in db_targets():
            init_schema(cfg, label=label)
        return

    save_json = None if args.no_save else args.save_json
    save_csv = None if args.no_save else args.save_csv
    save_csv_db = None if args.no_save else args.save_csv_db

    run(
        start=args.start,
        end=args.end,
        sido=args.sido,
        from_json=args.from_json,
        save_json=save_json,
        save_csv=save_csv,
        save_csv_db=save_csv_db,
        dry_run=args.dry_run,
        force=args.force,
    )
    print("done.", flush=True)


if __name__ == "__main__":
    main()
