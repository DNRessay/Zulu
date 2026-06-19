import os
import time
import requests

_ACCOUNT = os.environ.get("CF_ACCOUNT_ID", "")
_DB_ID   = os.environ.get("CF_D1_DATABASE_ID", "")
_TOKEN   = os.environ.get("CF_API_TOKEN", "")
_BASE    = f"https://api.cloudflare.com/client/v4/accounts/{_ACCOUNT}/d1/database/{_DB_ID}"

BATCH_CHUNK = 50  # max statements per /batch call


def _headers():
    return {"Authorization": f"Bearer {_TOKEN}", "Content-Type": "application/json"}


def _post(endpoint, payload, retries=4):
    for attempt in range(retries):
        try:
            r = requests.post(
                f"{_BASE}/{endpoint}", headers=_headers(), json=payload, timeout=30
            )
            r.raise_for_status()
            data = r.json()
            if not data.get("success"):
                raise RuntimeError(f"D1: {data.get('errors')}")
            return data.get("result", [])
        except Exception as exc:
            if attempt == retries - 1:
                raise
            wait = 2 ** attempt
            print(f"  D1 retry {attempt + 1}/{retries} ({wait}s): {exc}")
            time.sleep(wait)


def _query(sql, params=None):
    result = _post("query", {"sql": sql, "params": params or []})
    return result[0] if result else {}


def _batch(statements):
    if not statements:
        return []
    out = []
    for i in range(0, len(statements), BATCH_CHUNK):
        out.extend(_post("batch", {"statements": statements[i : i + BATCH_CHUNK]}))
    return out


# ── schema ────────────────────────────────────────────────────────────────────

def init_db():
    _batch([
        {
            "sql": (
                "CREATE TABLE IF NOT EXISTS pipeline_meta "
                "(key TEXT PRIMARY KEY, value TEXT)"
            )
        },
        {
            "sql": (
                "CREATE TABLE IF NOT EXISTS ocr_pages ("
                "page_num INTEGER PRIMARY KEY, "
                "raw_text TEXT, "
                "saved_at TEXT DEFAULT (datetime('now')))"
            )
        },
    ])


# ── metadata ──────────────────────────────────────────────────────────────────

def get_meta(key):
    rows = _query("SELECT value FROM pipeline_meta WHERE key=?", [key]).get("results", [])
    return rows[0]["value"] if rows else None


def set_meta(key, value):
    _query(
        "INSERT OR REPLACE INTO pipeline_meta (key, value) VALUES (?,?)",
        [key, str(value)],
    )


# ── ocr pages ─────────────────────────────────────────────────────────────────

def get_processed_pages():
    rows = _query("SELECT page_num FROM ocr_pages").get("results", [])
    return {r["page_num"] for r in rows}


def count_processed_pages():
    rows = _query("SELECT COUNT(*) as cnt FROM ocr_pages").get("results", [])
    return rows[0]["cnt"] if rows else 0


def save_ocr_pages(results):
    """Batch-insert OCR results; silently skips pages already stored."""
    stmts = [
        {
            "sql": "INSERT OR IGNORE INTO ocr_pages (page_num, raw_text) VALUES (?,?)",
            "params": [r["page"], r["text"]],
        }
        for r in results
    ]
    _batch(stmts)


def load_all_ocr_pages():
    rows = (
        _query("SELECT page_num, raw_text FROM ocr_pages ORDER BY page_num ASC")
        .get("results", [])
    )
    return [{"page": r["page_num"], "text": r["raw_text"]} for r in rows]


# ── cleanup ───────────────────────────────────────────────────────────────────

def cleanup():
    _batch([
        {"sql": "DROP TABLE IF EXISTS ocr_pages"},
        {"sql": "DROP TABLE IF EXISTS pipeline_meta"},
    ])
