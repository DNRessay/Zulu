# Zulu-English Bible Alignment Pipeline

Downloads the 1883 Zulu Bible PDF, OCRs it, parses verses, aligns them with KJV English text, and outputs a JSONL artifact.

## Setup

1. Add your KJV plain-text source as `data/kjv_raw.txt` in the repo (format: `1:1 In the beginning...`).
2. Push this repo to GitHub.

## Run

Go to **Actions → Zulu Bible Pipeline → Run workflow** (manual trigger only).

Output: `zu_en_bible.jsonl` artifact, downloadable from the workflow run page, retained for 14 days.

## Record format

```json
{"lang": "zu", "text": "...", "en_text": "...", "doc_id": "genesis_1", "pid": 1, "source": "zulu-bible-1883"}
```

## Notes

- OCR runs in batches of 20 pages, 4 threads, to keep memory in check on the GitHub runner.
- Job timeout is capped at 90 minutes.
- Re-running won't re-download the PDF if `data/zulu_bible.pdf` is already committed/cached.
- The run prints a coverage summary (verses found per book, Zulu vs KJV) before saving — check it if record count looks low. Also watch for "Warning" lines flagging unmatched pages/headings.
- `kjv_raw.txt` headings are matched in both `"The First Book of Moses"` and ALL-CAPS Gutenberg styles.
