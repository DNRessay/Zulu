import os
import re
import json
import sys
import time
import concurrent.futures

import requests
from pypdf import PdfReader
from pdf2image import convert_from_path
import pytesseract

import db

# ── timing ────────────────────────────────────────────────────────────────────

_START = time.time()
TIMEOUT_SECS  = 90 * 60   # self-imposed hard limit
SAFETY_BUFFER =  3 * 60   # stop accepting new batches this many seconds before limit

def _elapsed():
    return time.time() - _START

def deadline_exceeded():
    return _elapsed() >= (TIMEOUT_SECS - SAFETY_BUFFER)

# ── paths / constants ─────────────────────────────────────────────────────────

PDF_URL    = "https://archive.org/download/zulu-bible/Zulu%20Bible.pdf"
PDF_PATH   = "data/zulu_bible.pdf"
KJV_PATH   = "data/kjv_raw.txt"
OUTPUT     = "data/zu_en_bible.jsonl"
BATCH_SIZE = 20
WORKERS    = 4

BOOK_MAP = {
    "UGENESISE": 1,  "UEKSODUSI": 2,  "ULEVI": 3,     "TINTBALO": 4,
    "UKUSHESHA": 5,  "UJOSHUA": 6,    "ABAHLULI": 7,  "URUTE": 8,
    "USAMUELI": 9,   "AMAKOSI": 11,   "IZIKRONIKE": 13, "WEZRA": 15,
    "UEZRA": 15,     "UNEHEMIA": 16,  "UESETERE": 17, "UJOBE": 18,
    "IZIHLABELELO": 19, "AMAZWI": 20, "UMSHUMAYELI": 21,
    "ISIHLABELELO": 22, "ULSAYA": 23, "UISAYA": 23,   "UJEREMIA": 24,
    "ISILILO": 25,   "UHEZEKELI": 26, "UDANYELI": 27, "UHOSEA": 28,
    "UJOELI": 29,    "UAMOSI": 30,    "UOBADIA": 31,  "UJONA": 32,
    "UMIKA": 33,     "UNAHUME": 34,   "UHABAKUKI": 35,"UZEFANIA": 36,
    "UHAGAI": 37,    "UZEKARIA": 38,  "UMALAKI": 39,
    "UMATEU": 40,    "UMARKO": 41,    "ULUKA": 42,    "UJOHANE": 43,
    "IMISEBENZI": 44,"ABASEROMA": 45, "ABASEKORINTE": 46,
    "ABASEGALATEA": 48, "ABASEEFESE": 49, "ABASEFILIPI": 50,
    "ABASEKOLOSE": 51, "ABASETERSALONIKA": 52, "UTIMOTE": 54,
    "UTUTISI": 56,   "UFILEMONE": 57, "AMAHEBERU": 58,
    "INEWADI": 59,   "UPETRO": 60,    "ISAMBULO": 66,
}

KJV_BOOK_NAMES = {
    1: "Genesis", 2: "Exodus", 3: "Leviticus", 4: "Numbers", 5: "Deuteronomy",
    6: "Joshua", 7: "Judges", 8: "Ruth", 9: "1 Samuel", 10: "2 Samuel",
    11: "1 Kings", 12: "2 Kings", 13: "1 Chronicles", 14: "2 Chronicles",
    15: "Ezra", 16: "Nehemiah", 17: "Esther", 18: "Job", 19: "Psalms",
    20: "Proverbs", 21: "Ecclesiastes", 22: "Song of Solomon", 23: "Isaiah",
    24: "Jeremiah", 25: "Lamentations", 26: "Ezekiel", 27: "Daniel",
    28: "Hosea", 29: "Joel", 30: "Amos", 31: "Obadiah", 32: "Jonah",
    33: "Micah", 34: "Nahum", 35: "Habakkuk", 36: "Zephaniah", 37: "Haggai",
    38: "Zechariah", 39: "Malachi", 40: "Matthew", 41: "Mark", 42: "Luke",
    43: "John", 44: "Acts", 45: "Romans", 46: "1 Corinthians", 47: "2 Corinthians",
    48: "Galatians", 49: "Ephesians", 50: "Philippians", 51: "Colossians",
    52: "1 Thessalonians", 53: "2 Thessalonians", 54: "1 Timothy",
    55: "2 Timothy", 56: "Titus", 57: "Philemon", 58: "Hebrews",
    59: "James", 60: "1 Peter", 61: "2 Peter", 62: "1 John", 63: "2 John",
    64: "3 John", 65: "Jude", 66: "Revelation",
}

KJV_HEADING_PATTERNS = [
    (r'\bFIRST\s+BOOK\s+OF\s+MOSES.*GENESIS\b', 1),  (r'\bGENESIS\b', 1),
    (r'\bSECOND\s+BOOK\s+OF\s+MOSES.*EXODUS\b', 2),  (r'\bEXODUS\b', 2),
    (r'\bTHIRD\s+BOOK\s+OF\s+MOSES.*LEVITICUS\b', 3),(r'\bLEVITICUS\b', 3),
    (r'\bFOURTH\s+BOOK\s+OF\s+MOSES.*NUMBERS\b', 4), (r'\bNUMBERS\b', 4),
    (r'\bFIFTH\s+BOOK\s+OF\s+MOSES.*DEUTERONOMY\b',5),(r'\bDEUTERONOMY\b', 5),
    (r'\bJOSHUA\b', 6),  (r'\bJUDGES\b', 7),  (r'\bRUTH\b', 8),
    (r'\bFIRST\s+BOOK\s+OF\s+SAMUEL\b', 9),
    (r'\bSECOND\s+BOOK\s+OF\s+SAMUEL\b', 10),
    (r'\bFIRST\s+BOOK\s+OF\s+(THE\s+)?KINGS\b', 11),
    (r'\bSECOND\s+BOOK\s+OF\s+(THE\s+)?KINGS\b', 12),
    (r'\bFIRST\s+BOOK\s+OF\s+(THE\s+)?CHRONICLES\b', 13),
    (r'\bSECOND\s+BOOK\s+OF\s+(THE\s+)?CHRONICLES\b', 14),
    (r'\bEZRA\b', 15),   (r'\bNEHEMIAH\b', 16), (r'\bESTHER\b', 17),
    (r'\bJOB\b', 18),    (r'\bPSALMS\b', 19),   (r'\bPROVERBS\b', 20),
    (r'\bECCLESIASTES\b', 21), (r'\bSONG\s+OF\s+SOLOMON\b', 22),
    (r'\bISAIAH\b', 23), (r'\bJEREMIAH\b', 24), (r'\bLAMENTATIONS\b', 25),
    (r'\bEZEKIEL\b', 26),(r'\bDANIEL\b', 27),   (r'\bHOSEA\b', 28),
    (r'\bJOEL\b', 29),   (r'\bAMOS\b', 30),     (r'\bOBADIAH\b', 31),
    (r'\bJONAH\b', 32),  (r'\bMICAH\b', 33),    (r'\bNAHUM\b', 34),
    (r'\bHABAKKUK\b', 35),(r'\bZEPHANIAH\b', 36),(r'\bHAGGAI\b', 37),
    (r'\bZECHARIAH\b', 38),(r'\bMALACHI\b', 39),
    (r'\bST\.?\s*MATTHEW\b', 40),(r'\bMATTHEW\b', 40),
    (r'\bST\.?\s*MARK\b', 41),  (r'\bMARK\b', 41),
    (r'\bST\.?\s*LUKE\b', 42),  (r'\bLUKE\b', 42),
    (r'\bST\.?\s*JOHN\b', 43),
    (r'\bACTS\b', 44),   (r'\bROMANS\b', 45),
    (r'\bFIRST\s+(EPISTLE\s+TO\s+THE\s+)?CORINTHIANS\b', 46),
    (r'\bSECOND\s+(EPISTLE\s+TO\s+THE\s+)?CORINTHIANS\b', 47),
    (r'\bGALATIANS\b', 48),(r'\bEPHESIANS\b', 49),(r'\bPHILIPPIANS\b', 50),
    (r'\bCOLOSSIANS\b', 51),
    (r'\bFIRST\s+(EPISTLE\s+TO\s+THE\s+)?THESSALONIANS\b', 52),
    (r'\bSECOND\s+(EPISTLE\s+TO\s+THE\s+)?THESSALONIANS\b', 53),
    (r'\bFIRST\s+(EPISTLE\s+TO\s+)?TIMOTHY\b', 54),
    (r'\bSECOND\s+(EPISTLE\s+TO\s+)?TIMOTHY\b', 55),
    (r'\bTITUS\b', 56),  (r'\bPHILEMON\b', 57), (r'\bHEBREWS\b', 58),
    (r'\bJAMES\b', 59),
    (r'\bFIRST\s+(EPISTLE\s+(GENERAL\s+)?OF\s+)?PETER\b', 60),
    (r'\bSECOND\s+(EPISTLE\s+(GENERAL\s+)?OF\s+)?PETER\b', 61),
    (r'\bFIRST\s+(EPISTLE\s+(GENERAL\s+)?OF\s+)?JOHN\b', 62),
    (r'\bSECOND\s+(EPISTLE\s+OF\s+)?JOHN\b', 63),
    (r'\bTHIRD\s+(EPISTLE\s+OF\s+)?JOHN\b', 64),
    (r'\bJUDE\b', 65),   (r'\bREVELATION\b', 66),
]

# ── helpers ───────────────────────────────────────────────────────────────────

def _fuzzy_book_match(text):
    upper = text.upper()
    for name, num in BOOK_MAP.items():
        if re.search(rf'\b{re.escape(name)}\b', upper):
            return num
    subs = {'O': '[O0]', 'I': '[I1]', 'S': '[S5]'}
    for name, num in BOOK_MAP.items():
        if len(name) < 6:
            continue
        pattern = ''.join(subs.get(c, re.escape(c)) for c in name)
        if re.search(pattern, upper):
            return num
    return None


def _match_kjv_heading(line):
    if not (re.match(r'^The\s', line) or line.isupper()):
        return None
    upper = line.upper()
    for pattern, num in KJV_HEADING_PATTERNS:
        if re.search(pattern, upper):
            return num
    return None

# ── parsers ───────────────────────────────────────────────────────────────────

def parse_full_bible(ocr_results):
    bible = {}
    current_book, current_chapter = 1, 1
    unmatched = []

    for r in ocr_results:
        if any(s in r["text"] for s in ["AMAGAMA EZINCWADI", "AMERICAN BIBLE SOCIETY", "NETESTAMENTE ELIDALA"]):
            continue

        raw_lines = r["text"].splitlines()
        joined = []
        for line in raw_lines:
            line = line.strip()
            if not line:
                continue
            if re.match(r'^(Zulu,|Ht;|Bib\.)', line):
                continue
            if joined and joined[-1].endswith('-'):
                joined[-1] = joined[-1][:-1] + line
            else:
                joined.append(line)

        full_text = ' '.join(joined)
        if len(full_text) < 10:
            continue

        matched_book = _fuzzy_book_match(full_text)
        if matched_book:
            current_book = matched_book
            current_chapter = 1

        ch = re.search(r'ISAHLUKO\s+(\d+)', full_text, re.IGNORECASE)
        if ch:
            current_chapter = int(ch.group(1))
        elif not matched_book and not re.search(r'(?<!\d)\d{1,3}[,\s]\s*[A-Za-zuU]', full_text):
            unmatched.append(r["page"])

        parts = re.split(r'(?<!\d)(\d{1,3})[,\s]\s*(?=[A-Za-zuU])', full_text)
        if parts:
            pre = parts[0].strip()
            pre = re.sub(r'.*ISAHLUKO\s+\d+\.?\s*', '', pre, flags=re.IGNORECASE).strip()
            pre = re.sub(r'INCWADI.*?[A-Z]{4,}\.?\s*', '', pre, flags=re.IGNORECASE).strip()
            if pre and len(pre) > 5:
                bible.setdefault(current_book, {}).setdefault(current_chapter, {})[1] = pre
            i = 1
            while i < len(parts) - 1:
                try:
                    vnum  = int(parts[i])
                    vtext = parts[i + 1].strip()
                    if 1 <= vnum <= 200 and len(vtext) > 5:
                        bible.setdefault(current_book, {}).setdefault(current_chapter, {})[vnum] = vtext
                except ValueError:
                    pass
                i += 2

    if unmatched:
        print(f"Warning: {len(unmatched)} pages had no marker: {unmatched[:20]}{'...' if len(unmatched) > 20 else ''}")
    return bible


def parse_kjv():
    with open(KJV_PATH, encoding="utf-8") as f:
        lines = f.read().splitlines()

    start_idx = next(
        (i for i, l in enumerate(lines) if re.match(r'^1:1\s+In the beginning', l.strip())),
        0,
    )

    kjv = {}
    current_book = 1
    current_ref  = None
    buf          = ""

    for line in lines[start_idx:]:
        line = line.strip()
        if not line:
            continue

        heading_book   = _match_kjv_heading(line)
        looks_heading  = re.match(r'^The\s', line) or (
            line.isupper() and len(line) > 4 and not re.match(r'^\d', line)
        )

        if looks_heading:
            if current_ref and buf:
                b, c, v = current_ref
                kjv.setdefault(b, {}).setdefault(c, {})[v] = buf.strip()
                current_ref, buf = None, ""
            if heading_book:
                current_book = heading_book
            continue

        m = re.match(r'^(\d+):(\d+)\s+(.*)', line)
        if m:
            if current_ref and buf:
                b, c, v = current_ref
                kjv.setdefault(b, {}).setdefault(c, {})[v] = buf.strip()
            c, v, text = int(m.group(1)), int(m.group(2)), m.group(3)
            current_ref, buf = (current_book, c, v), text
        elif current_ref and line:
            buf += " " + line

    if current_ref and buf:
        b, c, v = current_ref
        kjv.setdefault(b, {}).setdefault(c, {})[v] = buf.strip()

    return kjv


def build_records(zul_bible, kjv):
    records = []
    for book_num, chapters in zul_bible.items():
        book_name = KJV_BOOK_NAMES.get(book_num, f"book_{book_num}")
        for chapter, verses in chapters.items():
            kjv_ch = kjv.get(book_num, {}).get(chapter, {})
            for v in sorted(set(list(verses) + list(kjv_ch))):
                zu = verses.get(v)
                en = kjv_ch.get(v)
                if not zu or not en:
                    continue
                records.append({
                    "lang":   "zu",
                    "text":   zu,
                    "en_text": en,
                    "doc_id": f"{book_name.lower().replace(' ', '_')}_{chapter}",
                    "pid":    v,
                    "source": "zulu-bible-1883",
                })
    return records

# ── ocr ───────────────────────────────────────────────────────────────────────

def download_pdf():
    if os.path.exists(PDF_PATH):
        print("PDF already cached.")
        return
    print("Downloading PDF...")
    for attempt in range(4):
        try:
            with requests.get(PDF_URL, stream=True, timeout=300) as r:
                r.raise_for_status()
                with open(PDF_PATH, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            print("Download complete.")
            return
        except Exception as exc:
            if attempt == 3:
                raise
            wait = 2 ** attempt
            print(f"Download failed ({exc}), retry in {wait}s...")
            time.sleep(wait)


def _ocr_page(args):
    page_num, img = args
    text = pytesseract.image_to_string(img, lang="eng")
    return {"page": page_num, "text": text.strip()}


def run_ocr(total_pages):
    done = db.get_processed_pages()
    print(f"Resume: {len(done)}/{total_pages} pages already in DB.")

    for batch_start in range(1, total_pages + 1, BATCH_SIZE):
        batch_end  = min(batch_start + BATCH_SIZE - 1, total_pages)
        batch_nums = list(range(batch_start, batch_end + 1))
        pending    = [p for p in batch_nums if p not in done]

        if not pending:
            print(f"  Batch {batch_start}-{batch_end}: skip (cached).")
            continue

        if deadline_exceeded():
            saved = db.count_processed_pages()
            mins  = int(_elapsed() / 60)
            print(f"Deadline reached at {mins}min. Saved {saved}/{total_pages} pages. Exiting gracefully.")
            sys.exit(0)

        if db.near_size_limit():
            saved = db.count_processed_pages()
            size  = db.get_db_size_mb()
            print(f"Neon near {db.NEON_LIMIT_MB}MB limit ({size:.1f}MB used). Saved {saved}/{total_pages} pages. Exiting gracefully.")
            sys.exit(0)

        print(f"  OCR pages {batch_start}-{batch_end} ({len(pending)} pending)…", end=" ", flush=True)
        t0 = time.time()

        images = convert_from_path(PDF_PATH, first_page=pending[0], last_page=pending[-1], dpi=200)

        with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as ex:
            results = list(ex.map(_ocr_page, [(pending[i], img) for i, img in enumerate(images)]))

        db.save_ocr_pages(results)
        done.update(r["page"] for r in results)
        print(f"done in {time.time()-t0:.1f}s  [{len(done)}/{total_pages}]")

    return True


# ── compile ───────────────────────────────────────────────────────────────────

def compile_output():
    print("Loading OCR pages from DB…")
    ocr_results = db.load_all_ocr_pages()
    print(f"  {len(ocr_results)} pages loaded.")

    print("Parsing Zulu text…")
    zul_bible = parse_full_bible(ocr_results)

    print("Parsing KJV…")
    kjv = parse_kjv()

    zul_books = sorted(zul_bible)
    print(f"\n--- Coverage: {len(zul_books)} Zulu books ---")
    for b in zul_books:
        zv = sum(len(v) for v in zul_bible[b].values())
        kv = sum(len(v) for v in kjv.get(b, {}).values())
        print(f"  {KJV_BOOK_NAMES.get(b, b)}: zulu={zv}, kjv={kv}")
    print("--- end ---\n")

    print("Aligning verses…")
    records = build_records(zul_bible, kjv)
    print(f"  {len(records)} aligned records.")

    os.makedirs("data", exist_ok=True)
    with open(OUTPUT, "w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    print(f"Written: {OUTPUT}")

    print("Cleaning up DB…")
    db.cleanup()
    print("DB cleaned.")


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    os.makedirs("data", exist_ok=True)

    print("Initialising DB…")
    db.init_db()

    download_pdf()

    reader      = PdfReader(PDF_PATH)
    total_pages = len(reader.pages)

    stored = db.get_meta("total_pages")
    if stored is None:
        db.set_meta("total_pages", total_pages)
    else:
        total_pages = int(stored)

    done_count = db.count_processed_pages()
    print(f"Total pages: {total_pages}  |  Already done: {done_count}")

    if done_count >= total_pages:
        print("All pages cached. Compiling final output…")
        compile_output()
        return

    run_ocr(total_pages)

    done_count = db.count_processed_pages()
    if done_count >= total_pages:
        print("OCR complete. Compiling final output…")
        compile_output()
    else:
        print(f"Session ended: {done_count}/{total_pages} pages done. Re-run to continue.")


if __name__ == "__main__":
    main()