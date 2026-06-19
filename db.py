import os
import time
import psycopg2
import psycopg2.extras

_DSN = os.environ.get("DATABASE_URL", "")

# Neon free tier: 512 MB storage. Stop before we hit it.
NEON_LIMIT_MB  = 512
NEON_SAFETY_MB = 25   # stop accepting new pages this many MB before the cap

BATCH_CHUNK = 100  # rows per executemany batch


def _connect():
    return psycopg2.connect(_DSN, sslmode="require")


# ── schema ────────────────────────────────────────────────────────────────────

def init_db():
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS pipeline_meta (
                    key   TEXT PRIMARY KEY,
                    value TEXT
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS ocr_pages (
                    page_num  INTEGER PRIMARY KEY,
                    raw_text  TEXT,
                    saved_at  TIMESTAMPTZ DEFAULT NOW()
                )
            """)
        conn.commit()


# ── size monitoring ──────────────────────────────────────────────────────────

def get_db_size_mb():
    """Returns combined on-disk size of both tables in MB."""
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COALESCE(pg_total_relation_size('ocr_pages'), 0)
                     + COALESCE(pg_total_relation_size('pipeline_meta'), 0)
            """)
            bytes_used = cur.fetchone()[0] or 0
    return bytes_used / (1024 * 1024)


def near_size_limit():
    return get_db_size_mb() >= (NEON_LIMIT_MB - NEON_SAFETY_MB)


# ── metadata ──────────────────────────────────────────────────────────────────

def get_meta(key):
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT value FROM pipeline_meta WHERE key = %s", (key,))
            row = cur.fetchone()
    return row[0] if row else None


def set_meta(key, value):
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO pipeline_meta (key, value)
                VALUES (%s, %s)
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
            """, (key, str(value)))
        conn.commit()


# ── ocr pages ─────────────────────────────────────────────────────────────────

def get_processed_pages():
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT page_num FROM ocr_pages")
            rows = cur.fetchall()
    return {r[0] for r in rows}


def count_processed_pages():
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM ocr_pages")
            row = cur.fetchone()
    return row[0] if row else 0


def save_ocr_pages(results):
    """Batch-insert OCR results; silently skips pages already stored."""
    if not results:
        return
    with _connect() as conn:
        with conn.cursor() as cur:
            for i in range(0, len(results), BATCH_CHUNK):
                chunk = results[i:i + BATCH_CHUNK]
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO ocr_pages (page_num, raw_text)
                    VALUES %s
                    ON CONFLICT (page_num) DO NOTHING
                    """,
                    [(r["page"], r["text"]) for r in chunk],
                )
        conn.commit()


def load_all_ocr_pages():
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT page_num, raw_text FROM ocr_pages ORDER BY page_num ASC"
            )
            rows = cur.fetchall()
    return [{"page": r[0], "text": r[1]} for r in rows]


# ── cleanup ───────────────────────────────────────────────────────────────────

def cleanup():
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS ocr_pages")
            cur.execute("DROP TABLE IF EXISTS pipeline_meta")
        conn.commit()
