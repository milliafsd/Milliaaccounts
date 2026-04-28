from __future__ import annotations

import csv
import hashlib
import io
import json
import re
import secrets
import sqlite3
import struct
from datetime import datetime
from html import escape
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse


APP_DIR = Path(__file__).resolve().parent
STATIC_DIR = APP_DIR / "static"
DB_PATH = APP_DIR / "madrasa_modern.sqlite3"
DEFAULT_LEGACY_DIR = Path(r"C:\vDos\acc")
UPLOADS_DIR = APP_DIR / "uploaded_legacy"
YEAR_PATTERN = re.compile(r"^JIID(\d{4})\.DBF$", re.IGNORECASE)
BRAND_NAME = "JAMIA MILLIA ISLAMIA AND MSJID MADRASA WALI"
PDF_LINE_LIMIT = 44
DEFAULT_LOGIN_USER = "admin"
DEFAULT_LOGIN_PASSWORD = "admin123"
SESSIONS: dict[str, str] = {}


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS accounts (
            code TEXT PRIMARY KEY,
            name TEXT NOT NULL DEFAULT '',
            atype TEXT NOT NULL DEFAULT '',
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS control_settings (
            year TEXT PRIMARY KEY,
            start_date TEXT,
            end_date TEXT,
            cash_in_hand REAL NOT NULL DEFAULT 0,
            min_cash REAL NOT NULL DEFAULT 0,
            max_cash REAL NOT NULL DEFAULT 0,
            last_jvno INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS entries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            year TEXT NOT NULL,
            entry_date TEXT,
            jv_no INTEGER,
            jv_ext INTEGER,
            branch TEXT NOT NULL DEFAULT '',
            category TEXT NOT NULL DEFAULT '',
            code TEXT NOT NULL,
            description TEXT NOT NULL DEFAULT '',
            receipt_no INTEGER,
            voucher_no INTEGER,
            entry_kind TEXT NOT NULL DEFAULT '',
            income REAL NOT NULL DEFAULT 0,
            payment REAL NOT NULL DEFAULT 0,
            checked_flag INTEGER NOT NULL DEFAULT 0,
            group_no INTEGER,
            source_file TEXT NOT NULL DEFAULT 'MODERN',
            source_row INTEGER,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(code) REFERENCES accounts(code)
        );

        CREATE TABLE IF NOT EXISTS app_users (
            username TEXT PRIMARY KEY,
            password_hash TEXT NOT NULL,
            display_name TEXT NOT NULL DEFAULT '',
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_entries_source
            ON entries(year, source_file, source_row)
            WHERE source_row IS NOT NULL;

        CREATE INDEX IF NOT EXISTS idx_entries_year_date
            ON entries(year, entry_date);

        CREATE INDEX IF NOT EXISTS idx_entries_year_code
            ON entries(year, code);
        """
    )
    seed_default_user(conn)
    conn.commit()


def password_hash(password: str) -> str:
    return hashlib.sha256(password.encode("utf-8")).hexdigest()


def seed_default_user(conn: sqlite3.Connection) -> None:
    exists = conn.execute("SELECT username FROM app_users WHERE username = ?", (DEFAULT_LOGIN_USER,)).fetchone()
    if exists:
        return
    conn.execute(
        """
        INSERT INTO app_users (username, password_hash, display_name, updated_at)
        VALUES (?, ?, ?, CURRENT_TIMESTAMP)
        """,
        (DEFAULT_LOGIN_USER, password_hash(DEFAULT_LOGIN_PASSWORD), "Administrator"),
    )


def scan_legacy_years(legacy_dir: Path = DEFAULT_LEGACY_DIR) -> list[str]:
    if not legacy_dir.exists():
        return []
    years = []
    for item in legacy_dir.iterdir():
        match = YEAR_PATTERN.match(item.name)
        if match:
            years.append(match.group(1))
    return sorted(set(years))


def parse_dbf_date(value: str) -> str | None:
    clean = value.strip()
    if not clean or clean == "00000000":
        return None
    try:
        return datetime.strptime(clean, "%Y%m%d").date().isoformat()
    except ValueError:
        return None


def parse_dbf_number(value: str, decimals: int) -> int | float | None:
    clean = value.strip()
    if not clean:
        return None
    try:
        number = float(clean)
    except ValueError:
        return None
    if decimals == 0:
        return int(number)
    return round(number, decimals)


def parse_dbf_bool(value: str) -> bool:
    return value.strip().upper() in {"Y", "T"}


def iterate_dbf(path: Path):
    with path.open("rb") as handle:
        header = handle.read(32)
        if len(header) < 32:
            return

        record_count = struct.unpack("<I", header[4:8])[0]
        header_length = struct.unpack("<H", header[8:10])[0]
        record_length = struct.unpack("<H", header[10:12])[0]

        fields: list[tuple[str, str, int, int]] = []
        while True:
            descriptor = handle.read(32)
            if not descriptor or descriptor[0] == 0x0D:
                break
            name = descriptor[:11].split(b"\x00", 1)[0].decode("ascii", errors="ignore")
            field_type = chr(descriptor[11])
            length = descriptor[16]
            decimals = descriptor[17]
            fields.append((name, field_type, length, decimals))

        handle.seek(header_length)
        for row_index in range(1, record_count + 1):
            raw_record = handle.read(record_length)
            if not raw_record:
                break
            if raw_record[0:1] == b"*":
                continue

            position = 1
            row = {}
            for name, field_type, length, decimals in fields:
                chunk = raw_record[position : position + length]
                position += length
                text = chunk.decode("cp1252", errors="ignore")
                if field_type == "D":
                    row[name] = parse_dbf_date(text)
                elif field_type == "N":
                    row[name] = parse_dbf_number(text, decimals)
                elif field_type == "L":
                    row[name] = parse_dbf_bool(text)
                else:
                    row[name] = text.rstrip()
            yield row_index, row


def choose_account_record(current: dict[str, str] | None, candidate: dict[str, str]) -> dict[str, str]:
    if current is None:
        return candidate
    score_current = (1 if current["atype"] else 0, len(current["name"].strip()))
    score_candidate = (1 if candidate["atype"] else 0, len(candidate["name"].strip()))
    return candidate if score_candidate >= score_current else current


def import_accounts(conn: sqlite3.Connection, legacy_dir: Path) -> int:
    path = legacy_dir / "JIICODED.DBF"
    if not path.exists():
        return 0

    deduped: dict[str, dict[str, str]] = {}
    for _, row in iterate_dbf(path):
        code = str(row.get("CODE") or "").strip()
        if not code:
            continue
        candidate = {
            "code": code.zfill(3),
            "name": str(row.get("NAME") or "").strip(),
            "atype": str(row.get("ATYPE") or "").strip(),
        }
        deduped[candidate["code"]] = choose_account_record(deduped.get(candidate["code"]), candidate)

    for record in deduped.values():
        conn.execute(
            """
            INSERT INTO accounts (code, name, atype, updated_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(code) DO UPDATE SET
                name = excluded.name,
                atype = CASE WHEN excluded.atype <> '' THEN excluded.atype ELSE accounts.atype END,
                updated_at = CURRENT_TIMESTAMP
            """,
            (record["code"], record["name"], record["atype"]),
        )
    conn.commit()
    return len(deduped)


def ensure_account(conn: sqlite3.Connection, code: str) -> None:
    if not code:
        return
    conn.execute(
        """
        INSERT INTO accounts (code, name, atype, updated_at)
        VALUES (?, '', '', CURRENT_TIMESTAMP)
        ON CONFLICT(code) DO NOTHING
        """,
        (code.zfill(3),),
    )


def import_control_settings(conn: sqlite3.Connection, legacy_dir: Path, year: str) -> int:
    path = legacy_dir / f"JIIC{year}.DBF"
    if not path.exists():
        return 0

    chosen = None
    for _, row in iterate_dbf(path):
        if row.get("SDATE") or row.get("EDATE") or row.get("CIH") is not None:
            chosen = row
            break

    if not chosen:
        return 0

    conn.execute(
        """
        INSERT INTO control_settings (
            year, start_date, end_date, cash_in_hand, min_cash, max_cash, last_jvno, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(year) DO UPDATE SET
            start_date = excluded.start_date,
            end_date = excluded.end_date,
            cash_in_hand = excluded.cash_in_hand,
            min_cash = excluded.min_cash,
            max_cash = excluded.max_cash,
            last_jvno = excluded.last_jvno,
            updated_at = CURRENT_TIMESTAMP
        """,
        (
            year,
            chosen.get("SDATE"),
            chosen.get("EDATE"),
            float(chosen.get("CIH") or 0),
            float(chosen.get("MINCIN") or 0),
            float(chosen.get("MAXCIN") or 0),
            int(chosen.get("JVNO") or 0),
        ),
    )
    conn.commit()
    return 1


def import_entries(conn: sqlite3.Connection, legacy_dir: Path, year: str) -> int:
    path = legacy_dir / f"JIID{year}.DBF"
    if not path.exists():
        return 0

    conn.execute("DELETE FROM entries WHERE year = ? AND source_file <> 'MODERN'", (year,))
    count = 0
    source_name = path.name.upper()

    for row_index, row in iterate_dbf(path):
        code = str(row.get("CODE") or "").strip().zfill(3)
        ensure_account(conn, code)
        conn.execute(
            """
            INSERT INTO entries (
                year, entry_date, jv_no, jv_ext, branch, category, code, description,
                receipt_no, voucher_no, entry_kind, income, payment, checked_flag,
                group_no, source_file, source_row, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            (
                year,
                row.get("DATE"),
                row.get("JVNO"),
                row.get("JVEXT"),
                str(row.get("BRANCH") or "").strip(),
                str(row.get("CATEGORY") or "").strip(),
                code,
                str(row.get("DESC1") or "").strip(),
                row.get("R_NO"),
                row.get("V_NO"),
                str(row.get("CJ") or "").strip(),
                float(row.get("INCOME") or 0),
                float(row.get("PAYMENT") or 0),
                1 if row.get("CHECKED") else 0,
                row.get("GROUP"),
                source_name,
                row_index,
            ),
        )
        count += 1

    conn.commit()
    return count


def import_year(conn: sqlite3.Connection, legacy_dir: Path, year: str) -> dict[str, int | str]:
    return {
        "year": year,
        "entries": import_entries(conn, legacy_dir, year),
        "settings": import_control_settings(conn, legacy_dir, year),
    }


def import_all_years(conn: sqlite3.Connection, legacy_dir: Path) -> dict[str, object]:
    imported_accounts = import_accounts(conn, legacy_dir)
    years = scan_legacy_years(legacy_dir)
    imported_years = [import_year(conn, legacy_dir, year) for year in years]
    return {"accounts": imported_accounts, "years": imported_years}


def bootstrap_if_empty() -> None:
    with get_connection() as conn:
        init_db(conn)
        has_entries = conn.execute("SELECT COUNT(*) FROM entries").fetchone()[0]
        if not has_entries and DEFAULT_LEGACY_DIR.exists():
            import_all_years(conn, DEFAULT_LEGACY_DIR)


def parse_multipart_uploads(handler: SimpleHTTPRequestHandler) -> list[tuple[str, bytes]]:
    content_type = handler.headers.get("Content-Type", "")
    match = re.search(r'boundary=(?:"([^"]+)"|([^;]+))', content_type)
    if not match:
        raise ValueError("Upload boundary missing.")

    boundary_value = match.group(1) or match.group(2)
    boundary = ("--" + boundary_value).encode("utf-8")
    content_length = int(handler.headers.get("Content-Length", "0"))
    body = handler.rfile.read(content_length)

    files: list[tuple[str, bytes]] = []
    for chunk in body.split(boundary):
        part = chunk.strip()
        if not part or part == b"--":
            continue
        if part.endswith(b"--"):
            part = part[:-2]
        part = part.strip(b"\r\n")
        headers_blob, separator, data = part.partition(b"\r\n\r\n")
        if not separator:
            continue
        headers_text = headers_blob.decode("utf-8", errors="ignore")
        disposition = next(
            (line for line in headers_text.split("\r\n") if line.lower().startswith("content-disposition:")),
            "",
        )
        filename_match = re.search(r'filename="([^"]+)"', disposition)
        if not filename_match:
            continue
        filename = Path(filename_match.group(1)).name
        files.append((filename, data.rstrip(b"\r\n")))

    return files


def store_uploaded_files(handler: SimpleHTTPRequestHandler) -> tuple[Path, list[str]]:
    UPLOADS_DIR.mkdir(exist_ok=True)
    batch_dir = UPLOADS_DIR / datetime.now().strftime("%Y%m%d-%H%M%S")
    batch_dir.mkdir(exist_ok=True)
    saved_files = []
    for filename, file_bytes in parse_multipart_uploads(handler):
        if not filename:
            continue
        destination = batch_dir / filename
        with destination.open("wb") as target:
            target.write(file_bytes)
        saved_files.append(filename)
    if not saved_files:
        raise ValueError("No valid files were uploaded.")
    return batch_dir, saved_files


def parse_cookie_header(cookie_header: str) -> dict[str, str]:
    cookies = {}
    if not cookie_header:
        return cookies
    for part in cookie_header.split(";"):
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        cookies[key.strip()] = value.strip()
    return cookies


def authenticate_user(conn: sqlite3.Connection, username: str, password: str) -> sqlite3.Row | None:
    row = conn.execute(
        "SELECT username, password_hash, display_name FROM app_users WHERE username = ?",
        (username.strip(),),
    ).fetchone()
    if not row:
        return None
    if row["password_hash"] != password_hash(password):
        return None
    return row


def json_body(handler: SimpleHTTPRequestHandler) -> dict[str, object]:
    length = int(handler.headers.get("Content-Length", "0"))
    raw = handler.rfile.read(length) if length else b"{}"
    if not raw:
        return {}
    return json.loads(raw.decode("utf-8"))


def send_json(handler: SimpleHTTPRequestHandler, payload: object, status: int = 200) -> None:
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(data)))
    handler.end_headers()
    handler.wfile.write(data)


def send_text(handler: SimpleHTTPRequestHandler, content: str, filename: str, content_type: str) -> None:
    data = content.encode("utf-8")
    handler.send_response(200)
    handler.send_header("Content-Type", content_type)
    handler.send_header("Content-Disposition", f'attachment; filename="{filename}"')
    handler.send_header("Content-Length", str(len(data)))
    handler.end_headers()
    handler.wfile.write(data)


def send_bytes(handler: SimpleHTTPRequestHandler, data: bytes, filename: str, content_type: str) -> None:
    handler.send_response(200)
    handler.send_header("Content-Type", content_type)
    handler.send_header("Content-Disposition", f'attachment; filename="{filename}"')
    handler.send_header("Content-Length", str(len(data)))
    handler.end_headers()
    handler.wfile.write(data)


def send_html(handler: SimpleHTTPRequestHandler, html: str) -> None:
    data = html.encode("utf-8")
    handler.send_response(200)
    handler.send_header("Content-Type", "text/html; charset=utf-8")
    handler.send_header("Content-Length", str(len(data)))
    handler.end_headers()
    handler.wfile.write(data)


def bad_request(handler: SimpleHTTPRequestHandler, message: str, status: int = 400) -> None:
    send_json(handler, {"ok": False, "error": message}, status=status)


def as_text(value: object) -> str:
    return str(value or "").strip()


def as_int(value: object) -> int | None:
    if value in ("", None):
        return None
    return int(value)


def as_float(value: object) -> float:
    if value in ("", None):
        return 0.0
    return round(float(value), 2)


def safe_year(value: str) -> str:
    year = as_text(value)
    if not re.fullmatch(r"\d{4}", year):
        raise ValueError("A valid 4-digit year is required.")
    return year


def list_years(conn: sqlite3.Connection) -> dict[str, object]:
    imported = conn.execute(
        """
        SELECT year, COUNT(*) AS entries
        FROM entries
        GROUP BY year
        ORDER BY year
        """
    ).fetchall()
    legacy_years = scan_legacy_years(DEFAULT_LEGACY_DIR)
    imported_map = {row["year"]: row["entries"] for row in imported}
    years = []
    for year in sorted(set(legacy_years) | set(imported_map)):
        years.append(
            {
                "year": year,
                "imported_entries": int(imported_map.get(year, 0)),
                "legacy_present": year in legacy_years,
            }
        )
    return {
        "brand_name": BRAND_NAME,
        "years": years,
        "legacy_dir": str(DEFAULT_LEGACY_DIR),
        "database": str(DB_PATH),
    }


def get_year_settings_row(conn: sqlite3.Connection, year: str) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT year, start_date, end_date, cash_in_hand, min_cash, max_cash, last_jvno
        FROM control_settings
        WHERE year = ?
        """,
        (year,),
    ).fetchone()


def resolve_period(conn: sqlite3.Connection, year: str, date_from: str | None, date_to: str | None) -> tuple[str | None, str | None]:
    settings = get_year_settings_row(conn, year)
    if not date_from and settings:
        date_from = settings["start_date"]
    if not date_to and settings:
        date_to = settings["end_date"]
    return date_from or None, date_to or None


def fetch_entries(
    conn: sqlite3.Connection,
    year: str,
    *,
    date_from: str | None = None,
    date_to: str | None = None,
    search: str | None = None,
    code: str | None = None,
    mode: str | None = None,
    limit: int | None = None,
    sort_ascending: bool = False,
) -> list[sqlite3.Row]:
    sql = [
        """
        SELECT
            e.id,
            e.year,
            e.entry_date,
            e.jv_no,
            e.jv_ext,
            e.branch,
            e.category,
            e.code,
            COALESCE(a.name, '') AS account_name,
            COALESCE(a.atype, '') AS account_type,
            e.description,
            e.receipt_no,
            e.voucher_no,
            e.entry_kind,
            e.income,
            e.payment,
            e.checked_flag,
            e.group_no,
            e.source_file
        FROM entries e
        LEFT JOIN accounts a ON a.code = e.code
        WHERE e.year = ?
        """
    ]
    params: list[object] = [year]

    if date_from:
        sql.append("AND COALESCE(e.entry_date, '') >= ?")
        params.append(date_from)
    if date_to:
        sql.append("AND COALESCE(e.entry_date, '') <= ?")
        params.append(date_to)
    if code:
        sql.append("AND e.code = ?")
        params.append(code.zfill(3))
    if search:
        wildcard = f"%{search}%"
        sql.append(
            """
            AND (
                e.description LIKE ?
                OR e.category LIKE ?
                OR e.code LIKE ?
                OR a.name LIKE ?
            )
            """
        )
        params.extend([wildcard, wildcard, wildcard, wildcard])
    if mode == "income":
        sql.append("AND e.income > 0")
    elif mode == "expense":
        sql.append("AND e.payment > 0")
    elif mode == "opening":
        sql.append("AND (e.entry_kind = 'B' OR UPPER(e.description) LIKE '%OPENING BALANCE%')")

    order = "ASC" if sort_ascending else "DESC"
    sql.append(f"ORDER BY COALESCE(e.entry_date, '') {order}, e.id {order}")
    if limit is not None:
        sql.append("LIMIT ?")
        params.append(limit)
    return conn.execute("\n".join(sql), params).fetchall()


def get_dashboard(conn: sqlite3.Connection, year: str) -> dict[str, object]:
    summary = conn.execute(
        """
        SELECT
            COUNT(*) AS entries_count,
            ROUND(COALESCE(SUM(income), 0), 2) AS total_income,
            ROUND(COALESCE(SUM(payment), 0), 2) AS total_payment,
            ROUND(COALESCE(SUM(income - payment), 0), 2) AS balance,
            ROUND(COALESCE(SUM(CASE WHEN entry_kind = 'B' THEN income - payment ELSE 0 END), 0), 2) AS opening_balance
        FROM entries
        WHERE year = ?
        """,
        (year,),
    ).fetchone()

    monthly = conn.execute(
        """
        SELECT
            substr(entry_date, 1, 7) AS month,
            ROUND(COALESCE(SUM(income), 0), 2) AS income,
            ROUND(COALESCE(SUM(payment), 0), 2) AS payment
        FROM entries
        WHERE year = ? AND entry_date IS NOT NULL
        GROUP BY substr(entry_date, 1, 7)
        ORDER BY month
        """
        ,
        (year,),
    ).fetchall()

    top_accounts = conn.execute(
        """
        SELECT
            e.code,
            COALESCE(a.name, '') AS name,
            COALESCE(a.atype, '') AS atype,
            ROUND(COALESCE(SUM(e.income), 0), 2) AS income,
            ROUND(COALESCE(SUM(e.payment), 0), 2) AS payment,
            ROUND(COALESCE(SUM(e.income - e.payment), 0), 2) AS balance
        FROM entries e
        LEFT JOIN accounts a ON a.code = e.code
        WHERE e.year = ?
        GROUP BY e.code, a.name, a.atype
        ORDER BY ABS(SUM(e.income - e.payment)) DESC, e.code
        LIMIT 10
        """,
        (year,),
    ).fetchall()

    recent_income = fetch_entries(conn, year, mode="income", limit=10)
    recent_expense = fetch_entries(conn, year, mode="expense", limit=10)
    settings = get_year_settings_row(conn, year)

    return {
        "summary": dict(summary),
        "monthly": [dict(row) for row in monthly],
        "top_accounts": [dict(row) for row in top_accounts],
        "recent_income": [dict(row) for row in recent_income],
        "recent_expense": [dict(row) for row in recent_expense],
        "settings": dict(settings) if settings else None,
    }


def get_entries_payload(conn: sqlite3.Connection, year: str, filters: dict[str, list[str]]) -> dict[str, object]:
    limit = min(max(int(filters.get("limit", ["250"])[0]), 1), 1000)
    date_from = filters.get("date_from", [""])[0] or None
    date_to = filters.get("date_to", [""])[0] or None
    search = filters.get("query", [""])[0].strip() or None
    code = filters.get("code", [""])[0].strip() or None
    mode = filters.get("mode", [""])[0].strip() or None
    rows = fetch_entries(
        conn,
        year,
        date_from=date_from,
        date_to=date_to,
        search=search,
        code=code,
        mode=mode,
        limit=limit,
    )
    return {"entries": [dict(row) for row in rows], "limit": limit}


def get_quick_entries(conn: sqlite3.Connection, year: str, mode: str, limit: int = 20) -> dict[str, object]:
    rows = fetch_entries(conn, year, mode=mode, limit=limit)
    return {"entries": [dict(row) for row in rows], "mode": mode}


def get_accounts(conn: sqlite3.Connection, year: str) -> dict[str, object]:
    rows = conn.execute(
        """
        SELECT
            a.code,
            a.name,
            a.atype,
            ROUND(COALESCE(SUM(e.income), 0), 2) AS income,
            ROUND(COALESCE(SUM(e.payment), 0), 2) AS payment,
            ROUND(COALESCE(SUM(e.income - e.payment), 0), 2) AS balance,
            COUNT(e.id) AS entries_count
        FROM accounts a
        LEFT JOIN entries e ON e.code = a.code AND e.year = ?
        GROUP BY a.code, a.name, a.atype
        ORDER BY a.code
        """,
        (year,),
    ).fetchall()
    return {"accounts": [dict(row) for row in rows]}


def get_settings(conn: sqlite3.Connection, year: str) -> dict[str, object]:
    row = get_year_settings_row(conn, year)
    if row:
        return {"settings": dict(row)}
    return {
        "settings": {
            "year": year,
            "start_date": None,
            "end_date": None,
            "cash_in_hand": 0,
            "min_cash": 0,
            "max_cash": 0,
            "last_jvno": 0,
        }
    }


def upsert_entry(conn: sqlite3.Connection, payload: dict[str, object], entry_id: int | None = None) -> int:
    year = safe_year(as_text(payload.get("year")))
    code = as_text(payload.get("code")).zfill(3)
    if not code or code == "000":
        raise ValueError("Account code is required.")
    ensure_account(conn, code)

    values = (
        year,
        as_text(payload.get("entry_date")) or None,
        as_int(payload.get("jv_no")),
        as_int(payload.get("jv_ext")),
        as_text(payload.get("branch")) or "G",
        as_text(payload.get("category")) or "GENERAL",
        code,
        as_text(payload.get("description")),
        as_int(payload.get("receipt_no")),
        as_int(payload.get("voucher_no")),
        as_text(payload.get("entry_kind")) or "C",
        as_float(payload.get("income")),
        as_float(payload.get("payment")),
        1 if payload.get("checked_flag") else 0,
        as_int(payload.get("group_no")),
    )

    if entry_id is None:
        cursor = conn.execute(
            """
            INSERT INTO entries (
                year, entry_date, jv_no, jv_ext, branch, category, code, description,
                receipt_no, voucher_no, entry_kind, income, payment, checked_flag,
                group_no, source_file, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'MODERN', CURRENT_TIMESTAMP)
            """,
            values,
        )
        conn.commit()
        return int(cursor.lastrowid)

    existing = conn.execute("SELECT year FROM entries WHERE id = ?", (entry_id,)).fetchone()
    if not existing:
        raise ValueError("Entry not found.")
    if existing["year"] != year:
        raise ValueError("Entry year mismatch. Please edit within the selected year.")

    conn.execute(
        """
        UPDATE entries
        SET
            year = ?, entry_date = ?, jv_no = ?, jv_ext = ?, branch = ?, category = ?,
            code = ?, description = ?, receipt_no = ?, voucher_no = ?, entry_kind = ?,
            income = ?, payment = ?, checked_flag = ?, group_no = ?, updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
        """,
        values + (entry_id,),
    )
    conn.commit()
    return entry_id


def delete_entry(conn: sqlite3.Connection, entry_id: int, year: str) -> None:
    year = safe_year(year)
    conn.execute("DELETE FROM entries WHERE id = ? AND year = ?", (entry_id, year))
    if conn.total_changes == 0:
        raise ValueError("Entry not found in the selected year.")
    conn.commit()


def upsert_account(conn: sqlite3.Connection, payload: dict[str, object]) -> str:
    code = as_text(payload.get("code")).zfill(3)
    if not code or code == "000":
        raise ValueError("Account code is required.")
    conn.execute(
        """
        INSERT INTO accounts (code, name, atype, updated_at)
        VALUES (?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(code) DO UPDATE SET
            name = excluded.name,
            atype = excluded.atype,
            updated_at = CURRENT_TIMESTAMP
        """,
        (code, as_text(payload.get("name")), as_text(payload.get("atype")).upper()),
    )
    conn.commit()
    return code


def upsert_settings(conn: sqlite3.Connection, year: str, payload: dict[str, object]) -> None:
    year = safe_year(year)
    conn.execute(
        """
        INSERT INTO control_settings (
            year, start_date, end_date, cash_in_hand, min_cash, max_cash, last_jvno, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(year) DO UPDATE SET
            start_date = excluded.start_date,
            end_date = excluded.end_date,
            cash_in_hand = excluded.cash_in_hand,
            min_cash = excluded.min_cash,
            max_cash = excluded.max_cash,
            last_jvno = excluded.last_jvno,
            updated_at = CURRENT_TIMESTAMP
        """,
        (
            year,
            as_text(payload.get("start_date")) or None,
            as_text(payload.get("end_date")) or None,
            as_float(payload.get("cash_in_hand")),
            as_float(payload.get("min_cash")),
            as_float(payload.get("max_cash")),
            as_int(payload.get("last_jvno")) or 0,
        ),
    )
    conn.commit()


def summarize_rows(rows: list[dict[str, object]]) -> dict[str, float]:
    def pick(row: dict[str, object], *keys: str) -> float:
        for key in keys:
            if key in row and row.get(key) not in (None, ""):
                return float(row.get(key) or 0)
        return 0.0

    total_income = round(sum(pick(row, "income", "Income", "Receipt") for row in rows), 2)
    total_payment = round(sum(pick(row, "payment", "Payment", "Expense") for row in rows), 2)
    return {
        "total_income": total_income,
        "total_payment": total_payment,
        "net_balance": round(total_income - total_payment, 2),
    }


def report_urls(report_type: str, year: str, date_from: str | None, date_to: str | None) -> dict[str, str]:
    params = {"type": report_type, "year": year}
    if date_from:
        params["date_from"] = date_from
    if date_to:
        params["date_to"] = date_to
    query = urlencode(params)
    return {
        "print_url": f"/report?{query}",
        "pdf_url": f"/api/report.pdf?{query}",
        "csv_url": f"/api/report.csv?{query}",
    }


def build_ledger_report(conn: sqlite3.Connection, year: str, date_from: str | None, date_to: str | None) -> dict[str, object]:
    rows = [dict(row) for row in fetch_entries(conn, year, date_from=date_from, date_to=date_to, sort_ascending=True)]
    summary = summarize_rows(rows)
    return {
        "type": "ledger",
        "title": "Ledger",
        "year": year,
        "date_from": date_from,
        "date_to": date_to,
        "columns": ["Date", "Code", "Account", "Description", "Receipt", "Voucher", "Income", "Expense"],
        "rows": [
            {
                "Date": row["entry_date"] or "",
                "Code": row["code"],
                "Account": row["account_name"],
                "Description": row["description"],
                "Receipt": row["receipt_no"] or "",
                "Voucher": row["voucher_no"] or "",
                "Income": round(float(row["income"] or 0), 2),
                "Expense": round(float(row["payment"] or 0), 2),
            }
            for row in rows
        ],
        "summary": summary,
        **report_urls("ledger", year, date_from, date_to),
    }


def build_cashbook_report(conn: sqlite3.Connection, year: str, date_from: str | None, date_to: str | None) -> dict[str, object]:
    opening_before = conn.execute(
        """
        SELECT ROUND(COALESCE(SUM(income - payment), 0), 2) AS balance
        FROM entries
        WHERE year = ? AND (? IS NOT NULL AND COALESCE(entry_date, '') < ?)
        """,
        (year, date_from, date_from or ""),
    ).fetchone()["balance"]
    rows = [dict(row) for row in fetch_entries(conn, year, date_from=date_from, date_to=date_to, sort_ascending=True)]
    running = float(opening_before or 0)
    out_rows = []
    for row in rows:
        running = round(running + float(row["income"] or 0) - float(row["payment"] or 0), 2)
        out_rows.append(
            {
                "Date": row["entry_date"] or "",
                "Code": row["code"],
                "Account": row["account_name"],
                "Description": row["description"],
                "Receipt": round(float(row["income"] or 0), 2),
                "Payment": round(float(row["payment"] or 0), 2),
                "Balance": running,
            }
        )
    summary = summarize_rows(out_rows)
    summary["opening_balance_before_period"] = round(float(opening_before or 0), 2)
    summary["closing_balance"] = running
    return {
        "type": "cashbook",
        "title": "Cash Book",
        "year": year,
        "date_from": date_from,
        "date_to": date_to,
        "columns": ["Date", "Code", "Account", "Description", "Receipt", "Payment", "Balance"],
        "rows": out_rows,
        "summary": summary,
        **report_urls("cashbook", year, date_from, date_to),
    }


def build_trial_balance_report(conn: sqlite3.Connection, year: str, date_from: str | None, date_to: str | None) -> dict[str, object]:
    rows = conn.execute(
        """
        SELECT
            e.code AS code,
            COALESCE(a.name, '') AS account_name,
            COALESCE(a.atype, '') AS account_type,
            ROUND(COALESCE(SUM(e.income), 0), 2) AS income,
            ROUND(COALESCE(SUM(e.payment), 0), 2) AS payment,
            ROUND(COALESCE(SUM(e.income - e.payment), 0), 2) AS balance
        FROM entries e
        LEFT JOIN accounts a ON a.code = e.code
        WHERE e.year = ?
          AND (? IS NULL OR COALESCE(e.entry_date, '') >= ?)
          AND (? IS NULL OR COALESCE(e.entry_date, '') <= ?)
        GROUP BY e.code, a.name, a.atype
        HAVING ROUND(COALESCE(SUM(e.income), 0), 2) <> 0
            OR ROUND(COALESCE(SUM(e.payment), 0), 2) <> 0
        ORDER BY e.code
        """,
        (year, date_from, date_from, date_to, date_to),
    ).fetchall()
    out_rows = [
        {
            "Code": row["code"],
            "Account": row["account_name"],
            "Type": row["account_type"],
            "Income": row["income"],
            "Expense": row["payment"],
            "Balance": row["balance"],
        }
        for row in rows
    ]
    summary = summarize_rows(out_rows)
    return {
        "type": "trial-balance",
        "title": "Trial Balance",
        "year": year,
        "date_from": date_from,
        "date_to": date_to,
        "columns": ["Code", "Account", "Type", "Income", "Expense", "Balance"],
        "rows": out_rows,
        "summary": summary,
        **report_urls("trial-balance", year, date_from, date_to),
    }


def build_opening_balance_report(conn: sqlite3.Connection, year: str, date_from: str | None, date_to: str | None) -> dict[str, object]:
    opening_cutoff = date_from or get_settings(conn, year)["settings"]["start_date"]
    rows = conn.execute(
        """
        SELECT
            e.code AS code,
            COALESCE(a.name, '') AS account_name,
            ROUND(COALESCE(SUM(e.income), 0), 2) AS income,
            ROUND(COALESCE(SUM(e.payment), 0), 2) AS payment,
            ROUND(COALESCE(SUM(e.income - e.payment), 0), 2) AS balance
        FROM entries e
        LEFT JOIN accounts a ON a.code = e.code
        WHERE e.year = ?
          AND (? IS NULL OR COALESCE(e.entry_date, '') <= ?)
        GROUP BY e.code, a.name
        HAVING ROUND(COALESCE(SUM(e.income - e.payment), 0), 2) <> 0
        ORDER BY e.code
        """,
        (year, opening_cutoff, opening_cutoff),
    ).fetchall()
    out_rows = [
        {
            "Code": row["code"],
            "Account": row["account_name"],
            "Income": row["income"],
            "Expense": row["payment"],
            "Balance": row["balance"],
        }
        for row in rows
    ]
    summary = summarize_rows(out_rows)
    summary["as_of_date"] = opening_cutoff
    return {
        "type": "opening-balance",
        "title": "Opening Balance",
        "year": year,
        "date_from": opening_cutoff,
        "date_to": date_to,
        "columns": ["Code", "Account", "Income", "Expense", "Balance"],
        "rows": out_rows,
        "summary": summary,
        **report_urls("opening-balance", year, opening_cutoff, date_to),
    }


def build_income_expense_report(conn: sqlite3.Connection, year: str, date_from: str | None, date_to: str | None) -> dict[str, object]:
    rows = conn.execute(
        """
        SELECT
            e.code AS code,
            COALESCE(a.name, '') AS account_name,
            COALESCE(a.atype, '') AS account_type,
            ROUND(COALESCE(SUM(e.income), 0), 2) AS income,
            ROUND(COALESCE(SUM(e.payment), 0), 2) AS payment
        FROM entries e
        LEFT JOIN accounts a ON a.code = e.code
        WHERE e.year = ?
          AND (? IS NULL OR COALESCE(e.entry_date, '') >= ?)
          AND (? IS NULL OR COALESCE(e.entry_date, '') <= ?)
        GROUP BY e.code, a.name, a.atype
        HAVING ROUND(COALESCE(SUM(e.income), 0), 2) <> 0
            OR ROUND(COALESCE(SUM(e.payment), 0), 2) <> 0
        ORDER BY e.code
        """,
        (year, date_from, date_from, date_to, date_to),
    ).fetchall()
    out_rows = [
        {
            "Code": row["code"],
            "Account": row["account_name"],
            "Type": row["account_type"],
            "Income": row["income"],
            "Expense": row["payment"],
            "Net": round(float(row["income"] or 0) - float(row["payment"] or 0), 2),
        }
        for row in rows
    ]
    summary = summarize_rows(out_rows)
    return {
        "type": "income-expense",
        "title": "Income And Expense",
        "year": year,
        "date_from": date_from,
        "date_to": date_to,
        "columns": ["Code", "Account", "Type", "Income", "Expense", "Net"],
        "rows": out_rows,
        "summary": summary,
        **report_urls("income-expense", year, date_from, date_to),
    }


def get_report(conn: sqlite3.Connection, report_type: str, year: str, date_from: str | None, date_to: str | None) -> dict[str, object]:
    year = safe_year(year)
    date_from, date_to = resolve_period(conn, year, date_from, date_to)
    builders = {
        "ledger": build_ledger_report,
        "cashbook": build_cashbook_report,
        "trial-balance": build_trial_balance_report,
        "opening-balance": build_opening_balance_report,
        "income-expense": build_income_expense_report,
    }
    if report_type not in builders:
        raise ValueError("Unknown report type.")
    return builders[report_type](conn, year, date_from, date_to)


def report_to_csv(report: dict[str, object]) -> str:
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow([BRAND_NAME])
    writer.writerow([report["title"], f"Year {report['year']}"])
    writer.writerow(["From", report.get("date_from") or "", "To", report.get("date_to") or ""])
    writer.writerow([])
    columns = list(report["columns"])
    writer.writerow(columns)
    for row in report["rows"]:
        writer.writerow([row.get(column, "") for column in columns])
    writer.writerow([])
    writer.writerow(["Total Income", report["summary"].get("total_income", 0)])
    writer.writerow(["Total Expense", report["summary"].get("total_payment", 0)])
    writer.writerow(["Net Balance", report["summary"].get("net_balance", 0)])
    return buffer.getvalue()


def money(value: object) -> str:
    return f"{float(value or 0):,.2f}"


def report_period_text(report: dict[str, object]) -> str:
    date_from = str(report.get("date_from") or "-")
    date_to = str(report.get("date_to") or "-")
    return f"{date_from} to {date_to}"


def report_to_print_html(report: dict[str, object]) -> str:
    summary = report["summary"]
    columns = report["columns"]
    rows_html = []
    for row in report["rows"]:
        cells = "".join(f"<td>{escape(str(row.get(column, '')))}</td>" for column in columns)
        rows_html.append(f"<tr>{cells}</tr>")
    rows_markup = "\n".join(rows_html) or f"<tr><td colspan='{len(columns)}'>No data</td></tr>"
    year = escape(str(report["year"]))
    title = escape(str(report["title"]))
    period = escape(report_period_text(report))
    csv_url = escape(str(report["csv_url"]))
    pdf_url = escape(str(report["pdf_url"]))
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>{title} - {year}</title>
  <style>
    @page {{ size: A4 landscape; margin: 14mm; }}
    body {{ font-family: Arial, sans-serif; margin: 24px; color: #162033; }}
    h1, h2, h3, p {{ margin: 0; }}
    .header {{ display: grid; gap: 6px; }}
    .header h1 {{ font-size: 24px; }}
    .header h2 {{ font-size: 18px; }}
    .meta {{ display: flex; gap: 18px; flex-wrap: wrap; margin-top: 10px; color: #4a5b73; }}
    .toolbar {{ display: flex; gap: 10px; margin: 18px 0 20px; flex-wrap: wrap; }}
    .button {{ padding: 10px 14px; border: 1px solid #cad2e1; border-radius: 6px; background: #fff; color: #162033; text-decoration: none; }}
    .button.primary {{ background: #106a5d; border-color: #106a5d; color: #fff; }}
    .summary {{ display: grid; grid-template-columns: repeat(3, minmax(180px, 1fr)); gap: 12px; margin: 18px 0; }}
    .summary div {{ padding: 10px 12px; border: 1px solid #d9e0ee; border-radius: 6px; background: #f8fbff; }}
    .summary strong {{ display: block; margin-top: 6px; font-size: 18px; }}
    table {{ width: 100%; border-collapse: collapse; table-layout: fixed; }}
    th, td {{ border: 1px solid #d9e0ee; padding: 7px 8px; text-align: left; font-size: 11px; vertical-align: top; word-break: break-word; }}
    th {{ background: #eef3fb; }}
    tbody tr:nth-child(even) {{ background: #fbfcfe; }}
    @media print {{
      .toolbar {{ display: none; }}
      body {{ margin: 0; }}
      thead {{ display: table-header-group; }}
    }}
  </style>
</head>
<body>
  <div class="header">
    <h1>{escape(BRAND_NAME)}</h1>
    <h2>{title}</h2>
    <div class="meta">
      <p><strong>Year:</strong> {year}</p>
      <p><strong>Period:</strong> {period}</p>
      <p><strong>Report:</strong> {title}</p>
    </div>
  </div>
  <div class="toolbar">
    <button class="button primary" onclick="window.print()">Print</button>
    <a class="button" href="{pdf_url}">Download PDF</a>
    <a class="button" href="{csv_url}">Download CSV</a>
  </div>
  <div class="summary">
    <div>Total Income<br><strong>{money(summary.get("total_income", 0))}</strong></div>
    <div>Total Expense<br><strong>{money(summary.get("total_payment", 0))}</strong></div>
    <div>Net Balance<br><strong>{money(summary.get("net_balance", 0))}</strong></div>
  </div>
  <table>
    <thead><tr>{''.join(f'<th>{escape(str(col))}</th>' for col in columns)}</tr></thead>
    <tbody>{rows_markup}</tbody>
  </table>
</body>
</html>"""


def pdf_escape(text: str) -> str:
    return text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def wrap_pdf_line(text: str, width: int = 96) -> list[str]:
    if len(text) <= width:
        return [text]
    lines = []
    remaining = text
    while len(remaining) > width:
        cut = remaining[:width]
        split = cut.rfind(" ")
        if split < 50:
            split = width
        lines.append(remaining[:split].rstrip())
        remaining = remaining[split:].lstrip()
    if remaining:
        lines.append(remaining)
    return lines


def pad_pdf_cell(value: object, width: int, align: str = "left") -> str:
    text = str(value or "")
    if len(text) > width:
        text = text[: max(width - 1, 1)].rstrip() + ("~" if width > 1 else "")
    return text.rjust(width) if align == "right" else text.ljust(width)


def report_table_lines(report: dict[str, object]) -> list[str]:
    schema_map: dict[str, list[tuple[str, int, str]]] = {
        "ledger": [
            ("Date", 10, "left"),
            ("Code", 5, "left"),
            ("Account", 22, "left"),
            ("Description", 28, "left"),
            ("Receipt", 8, "right"),
            ("Voucher", 8, "right"),
            ("Income", 12, "right"),
            ("Expense", 12, "right"),
        ],
        "cashbook": [
            ("Date", 10, "left"),
            ("Code", 5, "left"),
            ("Account", 20, "left"),
            ("Description", 26, "left"),
            ("Receipt", 12, "right"),
            ("Payment", 12, "right"),
            ("Balance", 12, "right"),
        ],
        "trial-balance": [
            ("Code", 5, "left"),
            ("Account", 28, "left"),
            ("Type", 8, "left"),
            ("Income", 12, "right"),
            ("Expense", 12, "right"),
            ("Balance", 12, "right"),
        ],
        "opening-balance": [
            ("Code", 5, "left"),
            ("Account", 30, "left"),
            ("Income", 12, "right"),
            ("Expense", 12, "right"),
            ("Balance", 12, "right"),
        ],
        "income-expense": [
            ("Code", 5, "left"),
            ("Account", 28, "left"),
            ("Type", 8, "left"),
            ("Income", 12, "right"),
            ("Expense", 12, "right"),
            ("Net", 12, "right"),
        ],
    }
    schema = schema_map.get(str(report.get("type")), [(column, 14, "left") for column in report["columns"]])
    header = " ".join(pad_pdf_cell(column, width, align) for column, width, align in schema)
    divider = "-" * len(header)
    lines = [header, divider]
    for row in report["rows"]:
        line = " ".join(pad_pdf_cell(row.get(column, ""), width, align) for column, width, align in schema)
        lines.extend(wrap_pdf_line(line, width=len(header)))
    if not report["rows"]:
        lines.append("No data")
    return lines


def build_pdf(report: dict[str, object]) -> bytes:
    wide_report = len(report["columns"]) >= 7
    page_width = 842 if wide_report else 612
    page_height = 595 if wide_report else 842
    start_y = page_height - 42
    line_limit = 30 if wide_report else PDF_LINE_LIMIT

    intro_lines = [
        BRAND_NAME,
        f"{report['title']}  |  Year {report['year']}",
        f"Period: {report_period_text(report)}",
        f"Total Income: {money(report['summary'].get('total_income', 0))}",
        f"Total Expense: {money(report['summary'].get('total_payment', 0))}",
        f"Net Balance: {money(report['summary'].get('net_balance', 0))}",
        "",
    ]
    lines = intro_lines + report_table_lines(report)

    pages = [lines[index : index + line_limit] for index in range(0, len(lines), line_limit)]
    if not pages:
        pages = [["No data"]]

    objects: list[bytes] = []

    def add_object(payload: str | bytes) -> int:
        content = payload.encode("latin1") if isinstance(payload, str) else payload
        objects.append(content)
        return len(objects)

    font_obj = add_object("<< /Type /Font /Subtype /Type1 /BaseFont /Courier >>")
    page_object_ids = []
    content_object_ids = []

    for page_number, page_lines in enumerate(pages, start=1):
        page_lines = page_lines + ["", f"Page {page_number} of {len(pages)}"]
        text_lines = ["BT", "/F1 9 Tf", f"36 {start_y} Td", "12 TL"]
        for index, line in enumerate(page_lines):
            escaped = pdf_escape(line)
            if index == 0:
                text_lines.append(f"({escaped}) Tj")
            else:
                text_lines.append(f"T* ({escaped}) Tj")
        text_lines.append("ET")
        stream = "\n".join(text_lines).encode("latin1")
        content_obj = add_object(b"<< /Length " + str(len(stream)).encode("ascii") + b" >>\nstream\n" + stream + b"\nendstream")
        content_object_ids.append(content_obj)
        page_obj = add_object("PENDING_PAGE")
        page_object_ids.append(page_obj)

    kids = " ".join(f"{page_id} 0 R" for page_id in page_object_ids)
    pages_obj = add_object(f"<< /Type /Pages /Count {len(page_object_ids)} /Kids [ {kids} ] >>")
    catalog_obj = add_object(f"<< /Type /Catalog /Pages {pages_obj} 0 R >>")

    for idx, page_obj in enumerate(page_object_ids):
        content_id = content_object_ids[idx]
        objects[page_obj - 1] = (
            f"<< /Type /Page /Parent {pages_obj} 0 R /MediaBox [0 0 {page_width} {page_height}] "
            f"/Resources << /Font << /F1 {font_obj} 0 R >> >> /Contents {content_id} 0 R >>"
        ).encode("latin1")

    pdf = io.BytesIO()
    pdf.write(b"%PDF-1.4\n")
    offsets = [0]
    for index, obj in enumerate(objects, start=1):
        offsets.append(pdf.tell())
        pdf.write(f"{index} 0 obj\n".encode("ascii"))
        pdf.write(obj)
        pdf.write(b"\nendobj\n")
    xref_offset = pdf.tell()
    pdf.write(f"xref\n0 {len(objects) + 1}\n".encode("ascii"))
    pdf.write(b"0000000000 65535 f \n")
    for offset in offsets[1:]:
        pdf.write(f"{offset:010d} 00000 n \n".encode("ascii"))
    pdf.write(
        f"trailer\n<< /Size {len(objects) + 1} /Root {catalog_obj} 0 R >>\nstartxref\n{xref_offset}\n%%EOF".encode(
            "ascii"
        )
    )
    return pdf.getvalue()


def to_filename_slug(text: str) -> str:
    return re.sub(r"[^a-z0-9-]+", "-", text.lower()).strip("-")


class ModernMadrasaHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(STATIC_DIR), **kwargs)

    def current_user(self) -> str | None:
        cookies = parse_cookie_header(self.headers.get("Cookie", ""))
        session_id = cookies.get("jamia_session")
        if not session_id:
            return None
        return SESSIONS.get(session_id)

    def is_authenticated(self) -> bool:
        return self.current_user() is not None

    def send_auth_cookie(self, session_id: str) -> None:
        self.send_header("Set-Cookie", f"jamia_session={session_id}; HttpOnly; SameSite=Lax; Path=/")

    def clear_auth_cookie(self) -> None:
        self.send_header("Set-Cookie", "jamia_session=; Max-Age=0; HttpOnly; SameSite=Lax; Path=/")

    def require_auth(self, parsed_path: str) -> bool:
        public_paths = {"/api/login", "/api/me", "/api/logout"}
        if parsed_path in public_paths:
            return False
        if self.is_authenticated():
            return False
        send_json(self, {"ok": False, "error": "Login required."}, status=401)
        return True

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/report":
            if not self.is_authenticated():
                self.path = "/index.html"
                super().do_GET()
                return
            self.handle_report_page(parsed)
            return
        if parsed.path.startswith("/api/"):
            if self.require_auth(parsed.path):
                return
            self.handle_api_get(parsed)
            return
        if parsed.path == "/":
            self.path = "/index.html"
        super().do_GET()

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        if not parsed.path.startswith("/api/"):
            bad_request(self, "Unsupported route.", 404)
            return
        if self.require_auth(parsed.path):
            return
        self.handle_api_post(parsed)

    def do_PUT(self) -> None:
        parsed = urlparse(self.path)
        if not parsed.path.startswith("/api/"):
            bad_request(self, "Unsupported route.", 404)
            return
        if self.require_auth(parsed.path):
            return
        self.handle_api_put(parsed)

    def do_DELETE(self) -> None:
        parsed = urlparse(self.path)
        if not parsed.path.startswith("/api/"):
            bad_request(self, "Unsupported route.", 404)
            return
        if self.require_auth(parsed.path):
            return
        self.handle_api_delete(parsed)

    def handle_report_page(self, parsed) -> None:
        params = parse_qs(parsed.query)
        with get_connection() as conn:
            try:
                report = get_report(
                    conn,
                    params.get("type", ["ledger"])[0],
                    params.get("year", [""])[0],
                    params.get("date_from", [""])[0] or None,
                    params.get("date_to", [""])[0] or None,
                )
                send_html(self, report_to_print_html(report))
            except Exception as exc:
                send_html(self, f"<h1>Error</h1><p>{escape(str(exc))}</p>")

    def handle_api_get(self, parsed) -> None:
        with get_connection() as conn:
            try:
                if parsed.path == "/api/me":
                    username = self.current_user()
                    if not username:
                        send_json(self, {"ok": True, "authenticated": False})
                        return
                    row = conn.execute(
                        "SELECT username, display_name FROM app_users WHERE username = ?",
                        (username,),
                    ).fetchone()
                    send_json(
                        self,
                        {
                            "ok": True,
                            "authenticated": bool(row),
                            "user": dict(row) if row else None,
                            "default_password_hint": DEFAULT_LOGIN_PASSWORD,
                        },
                    )
                    return
                if parsed.path == "/api/years":
                    send_json(self, {"ok": True, **list_years(conn)})
                    return
                if parsed.path == "/api/dashboard":
                    year = safe_year(parse_qs(parsed.query).get("year", [""])[0])
                    send_json(self, {"ok": True, **get_dashboard(conn, year)})
                    return
                if parsed.path == "/api/entries":
                    filters = parse_qs(parsed.query)
                    year = safe_year(filters.get("year", [""])[0])
                    send_json(self, {"ok": True, **get_entries_payload(conn, year, filters)})
                    return
                if parsed.path == "/api/quick-entries":
                    params = parse_qs(parsed.query)
                    year = safe_year(params.get("year", [""])[0])
                    mode = params.get("mode", ["income"])[0]
                    limit = min(max(int(params.get("limit", ["20"])[0]), 1), 100)
                    send_json(self, {"ok": True, **get_quick_entries(conn, year, mode, limit)})
                    return
                if parsed.path == "/api/accounts":
                    year = safe_year(parse_qs(parsed.query).get("year", [""])[0])
                    send_json(self, {"ok": True, **get_accounts(conn, year)})
                    return
                if parsed.path == "/api/account-head":
                    code = as_text(parse_qs(parsed.query).get("code", [""])[0]).zfill(3)
                    row = conn.execute(
                        "SELECT code, name, atype FROM accounts WHERE code = ?",
                        (code,),
                    ).fetchone()
                    send_json(self, {"ok": True, "account": dict(row) if row else None})
                    return
                if parsed.path == "/api/settings":
                    year = safe_year(parse_qs(parsed.query).get("year", [""])[0])
                    send_json(self, {"ok": True, **get_settings(conn, year)})
                    return
                if parsed.path == "/api/report-data":
                    params = parse_qs(parsed.query)
                    report = get_report(
                        conn,
                        params.get("type", ["ledger"])[0],
                        params.get("year", [""])[0],
                        params.get("date_from", [""])[0] or None,
                        params.get("date_to", [""])[0] or None,
                    )
                    send_json(self, {"ok": True, "report": report})
                    return
                if parsed.path == "/api/report.csv":
                    params = parse_qs(parsed.query)
                    report = get_report(
                        conn,
                        params.get("type", ["ledger"])[0],
                        params.get("year", [""])[0],
                        params.get("date_from", [""])[0] or None,
                        params.get("date_to", [""])[0] or None,
                    )
                    filename = f"{to_filename_slug(report['title'])}-{report['year']}.csv"
                    send_text(self, report_to_csv(report), filename, "text/csv; charset=utf-8")
                    return
                if parsed.path == "/api/report.pdf":
                    params = parse_qs(parsed.query)
                    report = get_report(
                        conn,
                        params.get("type", ["ledger"])[0],
                        params.get("year", [""])[0],
                        params.get("date_from", [""])[0] or None,
                        params.get("date_to", [""])[0] or None,
                    )
                    filename = f"{to_filename_slug(report['title'])}-{report['year']}.pdf"
                    send_bytes(self, build_pdf(report), filename, "application/pdf")
                    return
                bad_request(self, "Route not found.", 404)
            except ValueError as exc:
                bad_request(self, str(exc), 422)
            except Exception as exc:
                bad_request(self, str(exc), 500)

    def handle_api_post(self, parsed) -> None:
        with get_connection() as conn:
            try:
                if parsed.path == "/api/login":
                    payload = json_body(self)
                    row = authenticate_user(conn, as_text(payload.get("username")), as_text(payload.get("password")))
                    if not row:
                        bad_request(self, "Invalid username or password.", 401)
                        return
                    session_id = secrets.token_hex(24)
                    SESSIONS[session_id] = row["username"]
                    data = json.dumps(
                        {
                            "ok": True,
                            "authenticated": True,
                            "user": {"username": row["username"], "display_name": row["display_name"]},
                        },
                        ensure_ascii=False,
                    ).encode("utf-8")
                    self.send_response(200)
                    self.send_auth_cookie(session_id)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                    self.send_header("Content-Length", str(len(data)))
                    self.end_headers()
                    self.wfile.write(data)
                    return
                if parsed.path == "/api/logout":
                    cookies = parse_cookie_header(self.headers.get("Cookie", ""))
                    session_id = cookies.get("jamia_session")
                    if session_id and session_id in SESSIONS:
                        del SESSIONS[session_id]
                    data = json.dumps({"ok": True, "authenticated": False}).encode("utf-8")
                    self.send_response(200)
                    self.clear_auth_cookie()
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                    self.send_header("Content-Length", str(len(data)))
                    self.end_headers()
                    self.wfile.write(data)
                    return
                if parsed.path == "/api/import":
                    payload = json_body(self)
                    legacy_dir = Path(as_text(payload.get("legacy_dir")) or str(DEFAULT_LEGACY_DIR))
                    if not legacy_dir.exists():
                        bad_request(self, f"Legacy folder not found: {legacy_dir}")
                        return
                    if payload.get("all"):
                        result = import_all_years(conn, legacy_dir)
                    else:
                        year = safe_year(as_text(payload.get("year")))
                        result = {"accounts": import_accounts(conn, legacy_dir), "years": [import_year(conn, legacy_dir, year)]}
                    send_json(self, {"ok": True, "result": result}, 201)
                    return
                if parsed.path == "/api/upload-legacy":
                    upload_dir, saved_files = store_uploaded_files(self)
                    result = import_all_years(conn, upload_dir)
                    result["uploaded_dir"] = str(upload_dir)
                    result["saved_files"] = saved_files
                    send_json(self, {"ok": True, "result": result}, 201)
                    return
                if parsed.path == "/api/entries":
                    payload = json_body(self)
                    entry_id = upsert_entry(conn, payload)
                    send_json(self, {"ok": True, "id": entry_id}, 201)
                    return
                if parsed.path == "/api/accounts":
                    payload = json_body(self)
                    code = upsert_account(conn, payload)
                    send_json(self, {"ok": True, "code": code}, 201)
                    return
                bad_request(self, "Route not found.", 404)
            except ValueError as exc:
                bad_request(self, str(exc), 422)
            except Exception as exc:
                bad_request(self, str(exc), 500)

    def handle_api_put(self, parsed) -> None:
        with get_connection() as conn:
            try:
                if parsed.path.startswith("/api/entries/"):
                    entry_id = int(parsed.path.rsplit("/", 1)[-1])
                    payload = json_body(self)
                    upsert_entry(conn, payload, entry_id)
                    send_json(self, {"ok": True, "id": entry_id})
                    return
                if parsed.path.startswith("/api/settings/"):
                    year = parsed.path.rsplit("/", 1)[-1]
                    payload = json_body(self)
                    upsert_settings(conn, year, payload)
                    send_json(self, {"ok": True, "year": year})
                    return
                bad_request(self, "Route not found.", 404)
            except ValueError as exc:
                bad_request(self, str(exc), 422)
            except Exception as exc:
                bad_request(self, str(exc), 500)

    def handle_api_delete(self, parsed) -> None:
        with get_connection() as conn:
            try:
                if parsed.path.startswith("/api/entries/"):
                    entry_id = int(parsed.path.rsplit("/", 1)[-1])
                    year = parse_qs(parsed.query).get("year", [""])[0]
                    delete_entry(conn, entry_id, year)
                    send_json(self, {"ok": True, "id": entry_id})
                    return
                bad_request(self, "Route not found.", 404)
            except ValueError as exc:
                bad_request(self, str(exc), 422)
            except Exception as exc:
                bad_request(self, str(exc), 500)


def run_server() -> tuple[ThreadingHTTPServer, int]:
    host = "127.0.0.1"
    for port in range(8765, 8785):
        try:
            return ThreadingHTTPServer((host, port), ModernMadrasaHandler), port
        except OSError:
            continue
    raise OSError("Could not bind any local port.")


def main() -> None:
    STATIC_DIR.mkdir(exist_ok=True)
    bootstrap_if_empty()
    server, port = run_server()
    print(f"Modern Madrasa app running at http://127.0.0.1:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
