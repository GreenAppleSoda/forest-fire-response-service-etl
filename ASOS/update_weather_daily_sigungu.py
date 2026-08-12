"""
시군구 일별 기상 ETL (GitHub Actions / 독립 실행용).

ASOS kma_sfcdd3 API + admin-sigungu.json -> MariaDB weather_daily_sigungu.
기존 공용모듈/asos_daily 등 로컬 스크립트에 의존하지 않는다.

환경변수:
  KMA_AUTH_KEY
  DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME
  (선택) DB2_HOST, DB2_PORT, DB2_USER, DB2_PASSWORD, DB2_NAME
        — 설정되면 2번째 DB에도 동일 upsert

예:
  python update_weather_daily_sigungu.py
  python update_weather_daily_sigungu.py --date 2026-08-02 --dry-run
  python update_weather_daily_sigungu.py --init-schema
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv

HERE = Path(__file__).resolve().parent
DEFAULT_ADMIN_JSON = HERE / "admin-sigungu.json"
SCHEMA_SQL = HERE / "schema_weather_daily_sigungu.sql"
KST = ZoneInfo("Asia/Seoul")

ASOS_API_URL = "https://apihub.kma.go.kr/api/typ01/url/kma_sfcdd3.php"
STN_INF_API_URL = "https://apihub.kma.go.kr/api/typ01/url/stn_inf.php"

# kma_sfcdd3 필드 순서 (help=0 고정폭 텍스트)
FIELD_TM = 0
FIELD_STN = 1
FIELD_WS_AVG = 2
FIELD_TA_AVG = 10
FIELD_HM_AVG = 18
FIELD_RN_DAY = 38

# ASOS 요소별 결측 표기 (평균기온 -99 / 강수·풍속·습도 -9).
# 기온에 -9 를 넣으면 영하 9℃ 실측이 NULL 로 떨어지므로 분리한다.
_MISSING_TEMP = {-99.0, -99, -999.0, -999}
_MISSING_DEFAULT = {-9.0, -9, -99.0, -99, -999.0, -999}
_HELP_FIELD_RE = re.compile(r"^#\s*\d+\.\s*([A-Za-z0-9_]+)\s*:")
_STN_KO_RE = re.compile(
    r"^\s*(\d+)\s+"  # STN_ID
    r"[-\d.]+\s+"  # LON
    r"[-\d.]+\s+"  # LAT
    r"\S+\s+"  # STN_SP
    r"[-\d.]+\s+"  # HT
    r"[-\d.]+\s+"  # HT_PA
    r"[-\d.]+\s+"  # HT_TA
    r"[-\d.]+\s+"  # HT_WD
    r"[-\d.]+\s+"  # HT_RN
    r"\d+\s+"  # STN_AD
    r"(\S+)"  # STN_KO (첫번째 토큰)
)

STATION_NAME_OVERRIDE = {
    "북춘천": "춘천",
    "북강릉": "강릉",
    "서청주": "청주",
    "북창원": "창원",
    "북부산": "부산",
    "대관령": "평창",
    "추풍령": "김천",
    "백령도": "옹진",
    "흑산도": "신안",
    "울릉도": "울릉",
    "고산": "제주",
    "성산": "서귀포",
    "고창": "고창",
    "고창군": "고창",
    "영광군": "영광",
    "김해시": "김해",
    "순창군": "순창",
    "양산시": "양산",
    "보성군": "보성",
    "강진군": "강진",
    "의령군": "의령",
    "함양군": "함양",
    "광양시": "광양",
    "진도군": "진도",
    "청송군": "청송",
    "경주시": "경주",
    "정선군": "정선",
}

UPSERT_SQL = """
INSERT INTO weather_daily_sigungu (
    obs_date, sigungu_code, sigungu_name, province,
    stn_id, stn_name, temp_avg, precip, wind_avg, humidity_avg
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
)
ON DUPLICATE KEY UPDATE
    sigungu_name = VALUES(sigungu_name),
    province     = VALUES(province),
    stn_id       = VALUES(stn_id),
    stn_name     = VALUES(stn_name),
    temp_avg     = VALUES(temp_avg),
    precip       = VALUES(precip),
    wind_avg     = VALUES(wind_avg),
    humidity_avg = VALUES(humidity_avg)
"""


# ---------------------------------------------------------------------------
# env / utils
# ---------------------------------------------------------------------------


def load_env() -> None:
    # 로컬: 상위 .env, GHA: Secrets -> env
    load_dotenv(HERE.parent / ".env")
    load_dotenv(HERE / ".env")


def auth_key() -> str:
    load_env()
    key = os.getenv("KMA_AUTH_KEY")
    if not key:
        raise SystemExit("KMA_AUTH_KEY 환경변수가 필요합니다.")
    return key


def _db_config_from_prefix(prefix: str, *, required: bool) -> dict[str, Any] | None:
    """prefix 예: 'DB' -> DB_HOST..., 'DB2' -> DB2_HOST..."""
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
    """기본(1번) DB 설정."""
    cfg = _db_config_from_prefix("DB", required=True)
    assert cfg is not None
    return cfg


def db_targets() -> list[tuple[str, dict[str, Any]]]:
    """적재 대상 DB 목록. DB2_* 가 모두 있으면 2번째도 포함."""
    targets: list[tuple[str, dict[str, Any]]] = [("DB", db_config())]
    cfg2 = _db_config_from_prefix("DB2", required=False)
    if cfg2 is not None:
        targets.append(("DB2", cfg2))
    return targets


def strip_admin(name: str) -> str:
    name = re.sub(r"\s+", "", str(name).strip())
    name = re.sub(r"(특별자치시|광역시|특별시|특별자치도)$", "", name)
    name = re.sub(r"(시|군|구)$", "", name)
    return name


def to_float(
    raw: Any, *, missing: set[float | int] | None = None
) -> float | None:
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    try:
        v = float(s)
    except ValueError:
        return None
    if v in (missing if missing is not None else _MISSING_DEFAULT):
        return None
    return v


def parse_date_arg(value: str) -> datetime:
    """yesterday/today 는 항상 KST(Asia/Seoul) 달력 기준."""
    v = value.strip().lower()
    today = datetime.now(KST).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
    if v in ("yesterday", "어제"):
        return today - timedelta(days=1)
    if v in ("today", "오늘"):
        return today
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(value.strip(), fmt)
        except ValueError:
            continue
    raise SystemExit(f"날짜 형식 오류: {value} (YYYY-MM-DD / yesterday)")


# ---------------------------------------------------------------------------
# ASOS fetch (self-contained)
# ---------------------------------------------------------------------------

# connect 짧게, read 여유. 일시 타임아웃은 재시도로 흡수.
HTTP_TIMEOUT = (20, 180)
HTTP_RETRIES = 4
HTTP_BACKOFF = (5.0, 15.0, 45.0)  # 시도 사이 대기(초), 마지막 실패 전까지만 사용
_RETRYABLE_HTTP = (
    requests.exceptions.ConnectTimeout,
    requests.exceptions.ReadTimeout,
    requests.exceptions.ConnectionError,
    requests.exceptions.ChunkedEncodingError,
)


def _http_get(url: str, *, params: dict[str, Any], label: str) -> requests.Response:
    last: BaseException | None = None
    for attempt in range(1, HTTP_RETRIES + 1):
        try:
            resp = requests.get(url, params=params, timeout=HTTP_TIMEOUT)
            # 5xx 만 재시도, 4xx 는 즉시 실패
            if resp.status_code >= 500:
                raise requests.exceptions.HTTPError(
                    f"{resp.status_code} server error", response=resp
                )
            resp.raise_for_status()
            if attempt > 1:
                print(f"    {label}: ok on attempt {attempt}/{HTTP_RETRIES}", flush=True)
            return resp
        except _RETRYABLE_HTTP as e:
            last = e
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else None
            if status is not None and status >= 500:
                last = e
            else:
                raise
        if attempt < HTTP_RETRIES:
            wait = HTTP_BACKOFF[min(attempt - 1, len(HTTP_BACKOFF) - 1)]
            print(
                f"    {label}: attempt {attempt}/{HTTP_RETRIES} failed "
                f"({type(last).__name__}: {last}); retry in {wait:.0f}s",
                flush=True,
            )
            time.sleep(wait)
    assert last is not None
    raise last


def fetch_asos_period(tm1: str, tm2: str, stn: int, key: str) -> str:
    resp = _http_get(
        ASOS_API_URL,
        params={"tm1": tm1, "tm2": tm2, "stn": stn, "help": 0, "authKey": key},
        label=f"ASOS {tm1}~{tm2}",
    )
    return resp.content.decode("euc-kr", errors="replace")


def parse_sfcdd3(text: str) -> list[list[str]]:
    rows: list[list[str]] = []
    for raw in text.splitlines():
        line = raw.rstrip()
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if len(parts) < 2:
            continue
        if len(parts[0]) != 8 or not parts[0].isdigit():
            continue
        rows.append(parts)
    return rows


def month_chunks(start: datetime, end: datetime) -> list[tuple[datetime, datetime]]:
    chunks: list[tuple[datetime, datetime]] = []
    cur = start
    while cur <= end:
        if cur.month == 12:
            next_month = datetime(cur.year + 1, 1, 1)
        else:
            next_month = datetime(cur.year, cur.month + 1, 1)
        chunk_end = min(next_month - timedelta(days=1), end)
        chunks.append((cur, chunk_end))
        cur = chunk_end + timedelta(days=1)
    return chunks


def collect_asos(start: datetime, end: datetime, stn: int, key: str) -> list[list[str]]:
    all_rows: list[list[str]] = []
    chunks = month_chunks(start, end)
    for i, (c1, c2) in enumerate(chunks, 1):
        tm1, tm2 = c1.strftime("%Y%m%d"), c2.strftime("%Y%m%d")
        print(f"  ASOS [{i}/{len(chunks)}] {tm1}~{tm2} stn={stn}", flush=True)
        text = fetch_asos_period(tm1, tm2, stn, key)
        rows = parse_sfcdd3(text)
        print(f"    -> {len(rows)} rows", flush=True)
        if not rows:
            for p in text.strip().splitlines()[:3]:
                print(f"    | {p[:100]}", flush=True)
        all_rows.extend(rows)
        if i < len(chunks):
            time.sleep(0.2)
    return all_rows


def fetch_station_names(key: str) -> dict[int, str]:
    """지점번호 -> 한글 지점명 (stn_inf)."""
    tm = datetime.now().strftime("%Y%m%d0900")
    resp = _http_get(
        STN_INF_API_URL,
        params={"inf": "SFC", "tm": tm, "stn": 0, "help": 0, "authKey": key},
        label="stn_inf",
    )
    text = resp.content.decode("euc-kr", errors="replace")
    out: dict[int, str] = {}
    for line in text.splitlines():
        if not line.strip() or line.startswith("#"):
            continue
        m = _STN_KO_RE.match(line)
        if not m:
            continue
        try:
            sid = int(m.group(1))
        except ValueError:
            continue
        name = m.group(2).strip()
        if name:
            out[sid] = name
    print(f"  stations: {len(out)}", flush=True)
    return out


def normalize_asos(
    raw_rows: list[list[str]], station_names: dict[int, str]
) -> list[dict[str, Any]]:
    need = max(FIELD_TM, FIELD_STN, FIELD_WS_AVG, FIELD_TA_AVG, FIELD_HM_AVG, FIELD_RN_DAY)
    dedup: dict[tuple[int, datetime], dict[str, Any]] = {}
    for row in raw_rows:
        if len(row) <= need:
            continue
        tm = row[FIELD_TM].strip()
        if len(tm) != 8 or not tm.isdigit():
            continue
        try:
            stn_id = int(float(row[FIELD_STN]))
            dt = datetime.strptime(tm, "%Y%m%d")
        except ValueError:
            continue
        precip = to_float(row[FIELD_RN_DAY])
        if precip is None:
            precip = 0.0
        dedup[(stn_id, dt)] = {
            "stn_id": stn_id,
            "stn_name": station_names.get(stn_id, str(stn_id)),
            "date": dt,
            "temp_avg": to_float(row[FIELD_TA_AVG], missing=_MISSING_TEMP),
            "precip": precip,
            "wind_avg": to_float(row[FIELD_WS_AVG]),
            "humidity_avg": to_float(row[FIELD_HM_AVG]),
        }
    return list(dedup.values())


# ---------------------------------------------------------------------------
# admin-sigungu mapping
# ---------------------------------------------------------------------------


def load_sigungu(path: Path) -> list[dict[str, Any]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    rows = []
    for r in data["regions"]:
        rows.append({
            "sigungu_code": str(r["code"]),
            "sigungu_name": r["name"],
            "province": r["province"],
            "x": float(r["label"][0]),
            "y": float(r["label"][1]),
            "key": strip_admin(r["name"]),
        })
    return rows


def match_station(stn_name: str, sig: list[dict[str, Any]]) -> dict[str, Any] | None:
    raw = STATION_NAME_OVERRIDE.get(stn_name, stn_name)
    key = strip_admin(raw)

    hit = [g for g in sig if g["key"] == key]
    if len(hit) == 1:
        return hit[0]
    if len(hit) > 1:
        no_gu = [g for g in hit if "구" not in g["sigungu_name"]]
        return (no_gu or hit)[0]

    partial = [g for g in sig if g["key"].startswith(key) or key in g["sigungu_name"]]
    if partial:
        exactish = [g for g in partial if g["key"].startswith(key)]
        return (exactish or partial)[0]

    metro = {
        "서울": "서울", "부산": "부산", "대구": "대구", "인천": "인천",
        "광주": "광주", "대전": "대전", "울산": "울산", "세종": "세종", "제주": "제주",
    }
    if key in metro:
        sub = [g for g in sig if g["province"] == metro[key]]
        if sub:
            cx = sum(g["x"] for g in sub) / len(sub)
            cy = sum(g["y"] for g in sub) / len(sub)
            return min(sub, key=lambda g: (g["x"] - cx) ** 2 + (g["y"] - cy) ** 2)
    return None


def build_station_map(
    stations: list[tuple[int, str]], sig: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    rows = []
    for stn_id, stn_name in sorted(stations, key=lambda x: x[0]):
        m = match_station(stn_name, sig)
        if m is None:
            rows.append({
                "stn_id": stn_id,
                "stn_name": stn_name,
                "sigungu_code": "",
                "x": math.nan,
                "y": math.nan,
            })
        else:
            rows.append({
                "stn_id": stn_id,
                "stn_name": stn_name,
                "sigungu_code": m["sigungu_code"],
                "x": m["x"],
                "y": m["y"],
            })
    return rows


def assign_nearest(
    sig: list[dict[str, Any]],
    stn_map: list[dict[str, Any]],
    available: set[int],
) -> list[dict[str, Any]]:
    anchors = [
        a for a in stn_map
        if a["sigungu_code"] and not math.isnan(a["x"]) and a["stn_id"] in available
    ]
    if not anchors:
        raise RuntimeError("당일 관측 + 시군구 매칭된 ASOS 지점이 없습니다.")

    assigned = []
    for g in sig:
        best = min(
            anchors,
            key=lambda a: (a["x"] - g["x"]) ** 2 + (a["y"] - g["y"]) ** 2,
        )
        assigned.append({
            "sigungu_code": g["sigungu_code"],
            "sigungu_name": g["sigungu_name"],
            "province": g["province"],
            "stn_id": int(best["stn_id"]),
            "stn_name": best["stn_name"],
        })
    return assigned


def build_sigungu_daily(
    asos: list[dict[str, Any]],
    sig_stn: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    by_stn: dict[int, list[dict[str, Any]]] = {}
    for r in asos:
        by_stn.setdefault(r["stn_id"], []).append(r)

    out: list[dict[str, Any]] = []
    for s in sig_stn:
        for w in by_stn.get(s["stn_id"], []):
            out.append({
                "obs_date": w["date"].strftime("%Y-%m-%d"),
                "sigungu_code": s["sigungu_code"],
                "sigungu_name": s["sigungu_name"],
                "province": s["province"],
                "stn_id": s["stn_id"],
                "stn_name": s["stn_name"],
                "temp_avg": w["temp_avg"],
                "precip": w["precip"],
                "wind_avg": w["wind_avg"],
                "humidity_avg": w["humidity_avg"],
            })
    out.sort(key=lambda r: (r["obs_date"], r["sigungu_code"]))
    return out


# ---------------------------------------------------------------------------
# MariaDB
# ---------------------------------------------------------------------------


def get_conn(cfg: dict[str, Any]):
    try:
        import pymysql
    except ImportError as e:
        raise SystemExit("pip install pymysql 필요 (이 작업 전용 requirements 참고)") from e
    return pymysql.connect(**cfg)


def init_schema(cfg: dict[str, Any], *, label: str = "DB") -> None:
    sql = SCHEMA_SQL.read_text(encoding="utf-8")
    conn = get_conn(cfg)
    try:
        with conn.cursor() as cur:
            for stmt in sql.split(";"):
                s = stmt.strip()
                if s:
                    cur.execute(s)
        conn.commit()
        print(f"[{label}] schema ok: {SCHEMA_SQL.name}", flush=True)
    finally:
        conn.close()


def truncate_weather_daily_sigungu(
    cfg: dict[str, Any], *, label: str = "DB"
) -> None:
    """테이블 비우고 AUTO_INCREMENT 를 1부터 다시 시작."""
    conn = get_conn(cfg)
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE weather_daily_sigungu")
        conn.commit()
        print(f"[{label}] TRUNCATE weather_daily_sigungu", flush=True)
    finally:
        conn.close()


def upsert(cfg: dict[str, Any], rows: list[dict[str, Any]], batch: int = 1000) -> int:
    if not rows:
        return 0
    tuples = [
        (
            r["obs_date"], r["sigungu_code"], r["sigungu_name"], r["province"],
            r["stn_id"], r["stn_name"], r["temp_avg"], r["precip"],
            r["wind_avg"], r["humidity_avg"],
        )
        for r in rows
    ]
    conn = get_conn(cfg)
    n = 0
    try:
        with conn.cursor() as cur:
            for i in range(0, len(tuples), batch):
                cur.executemany(UPSERT_SQL, tuples[i : i + batch])
                n += len(tuples[i : i + batch])
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return n


def upsert_all(
    targets: list[tuple[str, dict[str, Any]]],
    rows: list[dict[str, Any]],
    batch: int = 1000,
) -> dict[str, int]:
    """여러 DB에 순차 upsert. 실패한 대상은 모아서 마지막에 실패 처리."""
    counts: dict[str, int] = {}
    errors: list[str] = []
    for label, cfg in targets:
        try:
            n = upsert(cfg, rows, batch=batch)
            counts[label] = n
            host = cfg.get("host", "?")
            db = cfg.get("database", "?")
            print(f"   [{label}] upserted={n}  ({host}/{db})", flush=True)
        except Exception as e:
            errors.append(f"[{label}] {e}")
            print(f"   [{label}] FAIL: {e}", flush=True)
    if errors:
        raise SystemExit("DB 적재 실패:\n" + "\n".join(errors))
    return counts


# ---------------------------------------------------------------------------
# pipeline
# ---------------------------------------------------------------------------


def run(
    start: datetime,
    end: datetime,
    *,
    admin_json: Path,
    stn: int = 0,
    dry_run: bool = False,
) -> list[dict[str, Any]]:
    key = auth_key()

    print("1) load admin-sigungu", flush=True)
    sig = load_sigungu(admin_json)
    print(f"   sigungu={len(sig)}", flush=True)

    print("2) fetch ASOS daily", flush=True)
    raw = collect_asos(start, end, stn, key)
    if not raw:
        raise SystemExit("ASOS 응답 0건")

    print("3) fetch station names", flush=True)
    names = fetch_station_names(key)
    asos = normalize_asos(raw, names)
    print(
        f"   asos={len(asos)} stations={len({r['stn_id'] for r in asos})} "
        f"{min(r['date'] for r in asos).date()}~{max(r['date'] for r in asos).date()}",
        flush=True,
    )

    print("4) map station -> sigungu", flush=True)
    pool = list(names.items()) if names else sorted({(r["stn_id"], r["stn_name"]) for r in asos})
    stn_map = build_station_map(pool, sig)
    matched = sum(1 for m in stn_map if m["sigungu_code"])
    print(f"   matched stations {matched}/{len(stn_map)}", flush=True)

    available = {r["stn_id"] for r in asos}
    sig_stn = assign_nearest(sig, stn_map, available)
    daily = build_sigungu_daily(asos, sig_stn)
    print(
        f"   rows={len(daily)} "
        f"sigungu={len({r['sigungu_code'] for r in daily})} "
        f"days={len({r['obs_date'] for r in daily})}",
        flush=True,
    )

    if dry_run:
        print("5) dry-run (no DB)", flush=True)
        for r in daily[:3]:
            print(f"   {r}", flush=True)
        return daily

    targets = db_targets()
    print(f"5) upsert MariaDB  targets={','.join(t[0] for t in targets)}", flush=True)
    upsert_all(targets, daily)
    return daily


def main() -> None:
    p = argparse.ArgumentParser(description="ASOS API + admin-sigungu -> weather_daily_sigungu")
    p.add_argument("--date", default="yesterday", help="YYYY-MM-DD | yesterday | today")
    p.add_argument("--tm1", default=None, help="start YYYYMMDD")
    p.add_argument("--tm2", default=None, help="end YYYYMMDD")
    p.add_argument("--stn", type=int, default=0, help="ASOS station (0=all)")
    p.add_argument("--admin-json", type=Path, default=DEFAULT_ADMIN_JSON)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--init-schema", action="store_true")
    args = p.parse_args()

    if args.init_schema:
        for label, cfg in db_targets():
            init_schema(cfg, label=label)
        return

    if args.tm1 or args.tm2:
        if not (args.tm1 and args.tm2):
            raise SystemExit("--tm1 and --tm2 together")
        start = datetime.strptime(args.tm1, "%Y%m%d")
        end = datetime.strptime(args.tm2, "%Y%m%d")
    else:
        start = end = parse_date_arg(args.date)

    if end < start:
        raise SystemExit("end < start")
    if not args.admin_json.exists():
        raise SystemExit(f"missing: {args.admin_json}")

    run(start, end, admin_json=args.admin_json, stn=args.stn, dry_run=args.dry_run)
    print("done.", flush=True)


if __name__ == "__main__":
    main()
