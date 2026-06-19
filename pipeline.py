import os
import re
import json
import concurrent.futures
import requests
from pypdf import PdfReader
from pdf2image import convert_from_path
import pytesseract

PDF_URL = "https://archive.org/download/zulu-bible/Zulu%20Bible.pdf"
PDF_PATH = "data/zulu_bible.pdf"
KJV_RAW_PATH = "data/kjv_raw.txt"
OUTPUT_PATH = "data/zu_en_bible.jsonl"
BATCH_SIZE = 20
WORKERS = 4

BOOK_MAP = {
    "UGENESISE": 1, "UEKSODUSI": 2, "ULEVI": 3, "TINTBALO": 4,
    "UKUSHESHA": 5, "UJOSHUA": 6, "ABAHLULI": 7, "URUTE": 8,
    "USAMUELI": 9, "AMAKOSI": 11, "IZIKRONIKE": 13, "WEZRA": 15,
    "UEZRA": 15, "UNEHEMIA": 16, "UESETERE": 17, "UJOBE": 18,
    "IZIHLABELELO": 19, "AMAZWI": 20, "UMSHUMAYELI": 21,
    "ISIHLABELELO": 22, "ULSAYA": 23, "UISAYA": 23, "UJEREMIA": 24,
    "ISILILO": 25, "UHEZEKELI": 26, "UDANYELI": 27, "UHOSEA": 28,
    "UJOELI": 29, "UAMOSI": 30, "UOBADIA": 31, "UJONA": 32,
    "UMIKA": 33, "UNAHUME": 34, "UHABAKUKI": 35, "UZEFANIA": 36,
    "UHAGAI": 37, "UZEKARIA": 38, "UMALAKI": 39,
    "UMATEU": 40, "UMARKO": 41, "ULUKA": 42, "UJOHANE": 43,
    "IMISEBENZI": 44, "ABASEROMA": 45, "ABASEKORINTE": 46,
    "ABASEGALATEA": 48, "ABASEEFESE": 49, "ABASEFILIPI": 50,
    "ABASEKOLOSE": 51, "ABASETERSALONIKA": 52, "UTIMOTE": 54,
    "UTUTISI": 56, "UFILEMONE": 57, "AMAHEBERU": 58,
    "INEWADI": 59, "UPETRO": 60, "ISAMBULO": 66,
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


def download_pdf():
    if os.path.exists(PDF_PATH):
        return
    with requests.get(PDF_URL, stream=True, timeout=300) as r:
        r.raise_for_status()
        with open(PDF_PATH, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)


def ocr_pdf():
    reader = PdfReader(PDF_PATH)
    total_pages = len(reader.pages)
    all_results = []

    def ocr_image(args):
        i, img = args
        text = pytesseract.image_to_string(img, lang="eng")
        return {"page": i, "text": text.strip()}

    for batch_start in range(1, total_pages + 1, BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE - 1, total_pages)
        images = convert_from_path(PDF_PATH, first_page=batch_start, last_page=batch_end, dpi=200)
        with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as ex:
            batch_results = list(ex.map(ocr_image, [(batch_start + i, img) for i, img in enumerate(images)]))
        all_results.extend(batch_results)
        print(f"OCR done: pages {batch_start}-{batch_end} ({len(all_results)}/{total_pages})")

    return all_results


def _fuzzy_book_match(full_text):
    """Match a Zulu book heading even with light OCR noise (e.g. O/0, I/1, S/5 confusion)."""
    upper = full_text.upper()
    for zul_name, book_num in BOOK_MAP.items():
        if re.search(rf'\b{re.escape(zul_name)}\b', upper):
            return book_num
    # fallback: tolerate common OCR digit/letter confusion on longer names
    substitutions = {'O': '[O0]', 'I': '[I1]', 'S': '[S5]'}
    for zul_name, book_num in BOOK_MAP.items():
        if len(zul_name) < 6:
            continue
        pattern = ''.join(substitutions.get(c, re.escape(c)) for c in zul_name)
        if re.search(pattern, upper):
            return book_num
    return None


def parse_full_bible(ocr_results):
    bible = {}
    current_book = 1
    current_chapter = 1
    unmatched_pages = []

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
            # no book heading, no chapter marker, no verse-like pattern: likely lost track
            unmatched_pages.append(r["page"])

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
                    vnum = int(parts[i])
                    vtext = parts[i + 1].strip()
                    if 1 <= vnum <= 200 and len(vtext) > 5:
                        bible.setdefault(current_book, {}).setdefault(current_chapter, {})[vnum] = vtext
                except ValueError:
                    pass
                i += 2

    if unmatched_pages:
        print(f"Warning: {len(unmatched_pages)} pages had no book/chapter/verse marker detected: {unmatched_pages[:20]}{'...' if len(unmatched_pages) > 20 else ''}")

    return bible


KJV_HEADING_PATTERNS = [
    # (regex, book_number) — order matters, more specific patterns first
    (r'\bFIRST\s+BOOK\s+OF\s+MOSES.*GENESIS\b', 1), (r'\bGENESIS\b', 1),
    (r'\bSECOND\s+BOOK\s+OF\s+MOSES.*EXODUS\b', 2), (r'\bEXODUS\b', 2),
    (r'\bTHIRD\s+BOOK\s+OF\s+MOSES.*LEVITICUS\b', 3), (r'\bLEVITICUS\b', 3),
    (r'\bFOURTH\s+BOOK\s+OF\s+MOSES.*NUMBERS\b', 4), (r'\bNUMBERS\b', 4),
    (r'\bFIFTH\s+BOOK\s+OF\s+MOSES.*DEUTERONOMY\b', 5), (r'\bDEUTERONOMY\b', 5),
    (r'\bJOSHUA\b', 6), (r'\bJUDGES\b', 7), (r'\bRUTH\b', 8),
    (r'\bFIRST\s+BOOK\s+OF\s+SAMUEL\b', 9), (r'\bSECOND\s+BOOK\s+OF\s+SAMUEL\b', 10),
    (r'\bFIRST\s+BOOK\s+OF\s+(THE\s+)?KINGS\b', 11), (r'\bSECOND\s+BOOK\s+OF\s+(THE\s+)?KINGS\b', 12),
    (r'\bFIRST\s+BOOK\s+OF\s+(THE\s+)?CHRONICLES\b', 13), (r'\bSECOND\s+BOOK\s+OF\s+(THE\s+)?CHRONICLES\b', 14),
    (r'\bEZRA\b', 15), (r'\bNEHEMIAH\b', 16), (r'\bESTHER\b', 17), (r'\bJOB\b', 18),
    (r'\bPSALMS\b', 19), (r'\bPROVERBS\b', 20), (r'\bECCLESIASTES\b', 21),
    (r'\bSONG\s+OF\s+SOLOMON\b', 22), (r'\bISAIAH\b', 23), (r'\bJEREMIAH\b', 24),
    (r'\bLAMENTATIONS\b', 25), (r'\bEZEKIEL\b', 26), (r'\bDANIEL\b', 27),
    (r'\bHOSEA\b', 28), (r'\bJOEL\b', 29), (r'\bAMOS\b', 30), (r'\bOBADIAH\b', 31),
    (r'\bJONAH\b', 32), (r'\bMICAH\b', 33), (r'\bNAHUM\b', 34), (r'\bHABAKKUK\b', 35),
    (r'\bZEPHANIAH\b', 36), (r'\bHAGGAI\b', 37), (r'\bZECHARIAH\b', 38), (r'\bMALACHI\b', 39),
    (r'\bST\.?\s*MATTHEW\b', 40), (r'\bMATTHEW\b', 40),
    (r'\bST\.?\s*MARK\b', 41), (r'\bMARK\b', 41),
    (r'\bST\.?\s*LUKE\b', 42), (r'\bLUKE\b', 42),
    (r'\bST\.?\s*JOHN\b', 43),
    (r'\bACTS\b', 44), (r'\bROMANS\b', 45),
    (r'\bFIRST\s+(EPISTLE\s+TO\s+THE\s+)?CORINTHIANS\b', 46), (r'\bSECOND\s+(EPISTLE\s+TO\s+THE\s+)?CORINTHIANS\b', 47),
    (r'\bGALATIANS\b', 48), (r'\bEPHESIANS\b', 49), (r'\bPHILIPPIANS\b', 50), (r'\bCOLOSSIANS\b', 51),
    (r'\bFIRST\s+(EPISTLE\s+TO\s+THE\s+)?THESSALONIANS\b', 52), (r'\bSECOND\s+(EPISTLE\s+TO\s+THE\s+)?THESSALONIANS\b', 53),
    (r'\bFIRST\s+(EPISTLE\s+TO\s+)?TIMOTHY\b', 54), (r'\bSECOND\s+(EPISTLE\s+TO\s+)?TIMOTHY\b', 55),
    (r'\bTITUS\b', 56), (r'\bPHILEMON\b', 57), (r'\bHEBREWS\b', 58), (r'\bJAMES\b', 59),
    (r'\bFIRST\s+(EPISTLE\s+(GENERAL\s+)?OF\s+)?PETER\b', 60), (r'\bSECOND\s+(EPISTLE\s+(GENERAL\s+)?OF\s+)?PETER\b', 61),
    (r'\bFIRST\s+(EPISTLE\s+(GENERAL\s+)?OF\s+)?JOHN\b', 62), (r'\bSECOND\s+(EPISTLE\s+OF\s+)?JOHN\b', 63),
    (r'\bTHIRD\s+(EPISTLE\s+OF\s+)?JOHN\b', 64), (r'\bJUDE\b', 65), (r'\bREVELATION\b', 66),
]


def _match_kjv_heading(line_s):
    """Detect a KJV book heading in either Title-case or ALL-CAPS Gutenberg style."""
    if not (re.match(r'^The\s', line_s) or line_s.isupper()):
        return None
    upper = line_s.upper()
    for pattern, book_num in KJV_HEADING_PATTERNS:
        if re.search(pattern, upper):
            return book_num
    return None


def parse_kjv_raw():
    with open(KJV_RAW_PATH, encoding="utf-8") as f:
        kjv_raw = f.read()

    kjv = {}
    lines = kjv_raw.splitlines()

    # Anchor on the first real verse line (not a TOC/heading list). Everything
    # before this point is discarded — repeated tables of contents or heading
    # lists earlier in the file must never be allowed to set current_book.
    start_idx = None
    for idx, line in enumerate(lines):
        if re.match(r'^1:1\s+In the beginning', line.strip()):
            start_idx = idx
            break

    if start_idx is None:
        print("Warning: could not find 'Genesis 1:1' anchor in KJV file; parsing from line 0")
        start_idx = 0

    current_book = 1  # the anchor line itself IS Genesis 1:1, so book is known
    current_ref = None
    full_text = ""
    unmatched_headings = []

    for line in lines[start_idx:]:
        line_s = line.strip()
        if not line_s:
            continue

        heading_book = _match_kjv_heading(line_s)
        looks_like_heading = re.match(r'^The\s', line_s) or (line_s.isupper() and len(line_s) > 4 and not re.match(r'^\d', line_s))

        if looks_like_heading:
            if current_ref and full_text:
                b, c, v = current_ref
                kjv.setdefault(b, {}).setdefault(c, {})[v] = full_text.strip()
                current_ref = None
                full_text = ""
            if heading_book:
                current_book = heading_book
            else:
                unmatched_headings.append(line_s)
            continue

        m = re.match(r'^(\d+):(\d+)\s+(.*)', line_s)
        if m:
            if current_ref and full_text:
                b, c, v = current_ref
                kjv.setdefault(b, {}).setdefault(c, {})[v] = full_text.strip()
            c, v, text = int(m.group(1)), int(m.group(2)), m.group(3)
            current_ref = (current_book, c, v)
            full_text = text
        elif current_ref and line_s:
            full_text += " " + line_s

    if current_ref and full_text:
        b, c, v = current_ref
        kjv.setdefault(b, {}).setdefault(c, {})[v] = full_text.strip()

    if unmatched_headings:
        print(f"Warning: {len(unmatched_headings)} heading-like lines didn't match a known book: {unmatched_headings[:10]}")

    return kjv


def build_records(zul_bible, kjv):
    records = []
    for book_num, chapters in zul_bible.items():
        book_name = KJV_BOOK_NAMES.get(book_num, f"book_{book_num}")
        for chapter, verses in chapters.items():
            kjv_chapter = kjv.get(book_num, {}).get(chapter, {})
            all_v = sorted(set(list(verses.keys()) + list(kjv_chapter.keys())))
            for v in all_v:
                zul_text = verses.get(v)
                eng_text = kjv_chapter.get(v)
                if not zul_text or not eng_text:
                    continue
                records.append({
                    "lang": "zu",
                    "text": zul_text,
                    "en_text": eng_text,
                    "doc_id": f"{book_name.lower().replace(' ', '_')}_{chapter}",
                    "pid": v,
                    "source": "zulu-bible-1883"
                })
    return records


def main():
    os.makedirs("data", exist_ok=True)
    print("Downloading PDF...")
    download_pdf()

    print("Running OCR...")
    ocr_results = ocr_pdf()

    print("Parsing Zulu text...")
    zul_bible = parse_full_bible(ocr_results)

    print("Parsing KJV raw text...")
    kjv = parse_kjv_raw()

    print("\n--- Coverage summary ---")
    zul_books = sorted(zul_bible.keys())
    kjv_books = sorted(kjv.keys())
    print(f"Zulu books detected: {len(zul_books)} -> {zul_books}")
    print(f"KJV books detected: {len(kjv_books)} -> {kjv_books}")
    for b in zul_books:
        zul_verses = sum(len(v) for v in zul_bible[b].values())
        kjv_verses = sum(len(v) for v in kjv.get(b, {}).values())
        name = KJV_BOOK_NAMES.get(b, f"book_{b}")
        print(f"  {name}: zulu={zul_verses} verses, kjv={kjv_verses} verses")
    print("--- end summary ---\n")

    print("Aligning verses...")
    records = build_records(zul_bible, kjv)
    print(f"Total aligned records: {len(records)}")

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    print(f"Saved {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
