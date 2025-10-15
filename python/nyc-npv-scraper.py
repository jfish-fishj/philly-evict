# nyc_npv_scraper.py
# pip install requests pandas pdfplumber
# %%
import re
import io
import os
import sys
import time
import random
import signal
import argparse
from pathlib import Path
from typing import Dict, Optional, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import pandas as pd
import pdfplumber
# %%
BASE_URL = "https://a836-edms.nyc.gov/dctm-rest/repositories/dofedmspts/StatementSearch"

# --------------------------- Tunables / defaults ---------------------------
TIMEOUT = 30
RETRY = 2
RETRY_SLEEP = 1.5
MAX_WORKERS = 8                # safe, bounded concurrency (I/O-bound)
SESSION_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; BBL-Scraper/1.2)"}

BATCH_SIZE = 1000              # rows buffered per append
WRITE_EVERY = 250              # flush to CSV at least this often
JITTER_RANGE = (0.05, 0.20)    # polite random delay per request

FAILURE_CHECKPOINT = "nyc_npv_failures.csv"

# ------------------------------- Regexes ----------------------------------
MONEY_RX = r"\$\s*[-\d,]+(?:\.\d{1,2})?"
PCT_RX   = r"[-+]?\d{1,2}(?:\.\d+)?\s*%"

PATTERNS = {
    "estimated_gross_income": re.compile(
        rf"Estimated\s+Gross\s+Income\s*[:\-]?\s*({MONEY_RX})",
        re.IGNORECASE
    ),
    "estimated_expenses": re.compile(
        rf"Estimated\s+Expenses\s*[:\-]?\s*({MONEY_RX})",
        re.IGNORECASE
    ),
    # YOUR fixed forms for cap rates:
    "base_cap_rate": re.compile(
        rf"capitalization rate of\s*({PCT_RX})",
        re.IGNORECASE
    ),
    "overall_cap_rate": re.compile(
        rf"capitalization rate is\s*({PCT_RX})",
        re.IGNORECASE
    ),
}

DATA_NOT_FOUND_HINTS = [
    "data not found",
    "no data found",
    "no statement found",
    "unable to locate",
]

# ---------------------------- Helpers -------------------------------------
def build_url(bbl: str, stmt_date: str, stmt_type: str = "NPV") -> str:
    return f"{BASE_URL}?bbl={bbl}&stmtDate={stmt_date}&stmtType={stmt_type}"

def parse_money(s: Optional[str]) -> Optional[float]:
    if not s:
        return None
    v = re.sub(r"[,\s$]", "", s)
    try:
        return float(v)
    except Exception:
        return None

def parse_percent(s: Optional[str]) -> Optional[float]:
    if not s:
        return None
    v = s.replace("%", "").strip()
    try:
        return float(v) / 100.0
    except Exception:
        return None

def text_looks_like_not_found(text: str) -> bool:
    t = text.lower()
    if len(t) < 200 and any(h in t for h in DATA_NOT_FOUND_HINTS):
        return True
    if len(t.strip()) < 50:
        return True
    return False

def pdf_to_text(pdf_bytes: bytes) -> str:
    chunks = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            chunks.append(page.extract_text() or "")
    return "\n".join(chunks)

def parse_statement_text(text: str) -> Dict[str, Optional[str]]:
    out = {
        "estimated_gross_income_raw": None,
        "estimated_expenses_raw": None,
        "base_cap_rate_raw": None,
        "overall_cap_rate_raw": None,
    }
    def _first(pat: re.Pattern) -> Optional[str]:
        m = pat.search(text)
        return m.group(1).strip() if m else None

    out["estimated_gross_income_raw"] = _first(PATTERNS["estimated_gross_income"])
    out["estimated_expenses_raw"]     = _first(PATTERNS["estimated_expenses"])
    out["base_cap_rate_raw"]          = _first(PATTERNS["base_cap_rate"])
    out["overall_cap_rate_raw"]       = _first(PATTERNS["overall_cap_rate"])

    out["estimated_gross_income"] = parse_money(out["estimated_gross_income_raw"])
    out["estimated_expenses"]     = parse_money(out["estimated_expenses_raw"])
    out["base_cap_rate"]          = parse_percent(out["base_cap_rate_raw"])
    out["overall_cap_rate"]       = parse_percent(out["overall_cap_rate_raw"])
    return out

# Reuse a single HTTP session for pooling
_SESSION: Optional[requests.Session] = None
def get_session() -> requests.Session:
    global _SESSION
    if _SESSION is None:
        s = requests.Session()
        s.headers.update(SESSION_HEADERS)
        _SESSION = s
    return _SESSION

def fetch_pdf_bytes(url: str) -> Optional[bytes]:
    ses = get_session()
    for attempt in range(1, RETRY + 1):
        try:
            r = ses.get(url, timeout=TIMEOUT)
            if r.status_code == 200 and r.content:
                if "pdf" in r.headers.get("Content-Type", "").lower() or r.content.startswith(b"%PDF"):
                    return r.content
                if r.content.startswith(b"%PDF"):
                    return r.content
            elif r.status_code == 404:
                return None
        except requests.RequestException:
            if attempt == RETRY:
                return None
        time.sleep(RETRY_SLEEP * attempt + random.uniform(*JITTER_RANGE))
    return None

def scrape_one(bbl: str, stmt_date: str, stmt_type: str = "NPV") -> Dict:
    url = build_url(bbl, stmt_date, stmt_type)
    rec = {
        "bbl": str(bbl),
        "stmt_date": stmt_date,
        "year": int(stmt_date[:4]) if re.match(r"^\d{8}$", stmt_date) else None,
        "stmt_type": stmt_type,
        "status": "ok",
        "source_url": url,
    }

    time.sleep(random.uniform(*JITTER_RANGE))  # be polite

    pdf_bytes = fetch_pdf_bytes(url)
    if not pdf_bytes:
        rec["status"] = "no_pdf"
        return rec

    try:
        text = pdf_to_text(pdf_bytes)
    except Exception as e:
        rec["status"] = f"pdf_parse_error: {e.__class__.__name__}"
        return rec

    if text_looks_like_not_found(text):
        rec["status"] = "not_found_pdf"
        return rec

    parsed = parse_statement_text(text)
    rec.update(parsed)

    # If all requested fields are missing, mark it explicitly
    if all(rec.get(k) is None for k in [
        "estimated_gross_income", "estimated_expenses",
        "base_cap_rate", "overall_cap_rate"
    ]):
        rec["status"] = "parsed_empty"
    return rec

# ---------------------------- Resume & append ----------------------------
def load_existing_keys(csv_path: str) -> set[Tuple[str, str, str]]:
    p = Path(csv_path)
    if not p.exists() or p.stat().st_size == 0:
        return set()
    df = pd.read_csv(p, dtype={"bbl": str, "stmt_date": str, "stmt_type": str})
    if df.empty or not {"bbl","stmt_date","stmt_type"}.issubset(df.columns):
        return set()
    return set(map(tuple, df[["bbl","stmt_date","stmt_type"]].astype(str).itertuples(index=False, name=None)))

def append_rows(rows: List[Dict], out_csv: str):
    if not rows:
        return
    df = pd.DataFrame(rows)
    preferred = [
        "bbl", "year", "stmt_date", "stmt_type", "status",
        "estimated_gross_income_raw", "estimated_gross_income",
        "estimated_expenses_raw", "estimated_expenses",
        "base_cap_rate_raw", "base_cap_rate",
        "overall_cap_rate_raw", "overall_cap_rate",
        "source_url",
    ]
    cols = [c for c in preferred if c in df.columns] + [c for c in df.columns if c not in preferred]
    df = df[cols]

    mode = "a" if Path(out_csv).exists() and Path(out_csv).stat().st_size > 0 else "w"
    df.to_csv(out_csv, index=False, mode=mode, header=(mode == "w"))

def append_failures(rows: List[Dict], path: str = FAILURE_CHECKPOINT):
    if not rows:
        return
    mode = "a" if Path(path).exists() and Path(path).stat().st_size > 0 else "w"
    pd.DataFrame(rows).to_csv(path, index=False, mode=mode, header=(mode == "w"))

# ------------------------------- Driver ----------------------------------
def scrape_streaming(
    bbls: List[str],
    stmt_date: str,
    out_csv: str,
    existing_csv: Optional[str],
    stmt_type: str = "NPV",
    max_workers: int = MAX_WORKERS,
    batch_size: int = BATCH_SIZE,
    write_every: int = WRITE_EVERY,
):
    # Load already-done keys from both existing/out CSVs
    done = set()
    if existing_csv:
        done |= load_existing_keys(existing_csv)
    if out_csv and Path(out_csv).exists():
        done |= load_existing_keys(out_csv)

    todo = [(str(b), stmt_date, stmt_type) for b in bbls if (str(b), stmt_date, stmt_type) not in done]
    print(f"[INFO] already completed: {len(done):,}")
    print(f"[INFO] to scrape now:     {len(todo):,}")

    stop_flag = {"stop": False}
    def _sigint(sig, frame):
        print("\n[WARN] Interrupt received; flushing buffers and stopping…")
        stop_flag["stop"] = True
    signal.signal(signal.SIGINT, _sigint)

    buffer: List[Dict] = []
    failures: List[Dict] = []

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(scrape_one, b, d, t): (b, d, t) for (b, d, t) in todo}
        processed = 0
        for fut in as_completed(futures):
            b, d, t = futures[fut]
            try:
                rec = fut.result()
            except Exception as e:
                rec = {
                    "bbl": b, "stmt_date": d, "year": int(d[:4]) if d and len(d)>=4 else None,
                    "stmt_type": t, "status": f"fatal_error: {e.__class__.__name__}",
                    "source_url": build_url(b, d, t),
                }
                failures.append(rec)

            buffer.append(rec)
            processed += 1

            if len(buffer) >= batch_size or (processed % write_every == 0):
                append_rows(buffer, out_csv)
                buffer.clear()

            if len(failures) >= max(50, write_every // 2):
                append_failures(failures)
                failures.clear()

            if stop_flag["stop"]:
                print("[WARN] Stop requested; flushing buffers…")
                break

    if buffer:
        append_rows(buffer, out_csv)
    if failures:
        append_failures(failures)

    print("[INFO] Done. Appended to:", out_csv)
    if Path(FAILURE_CHECKPOINT).exists():
        print("[INFO] Failures logged at:", FAILURE_CHECKPOINT)

# %%

# ------------------------------- CLI -------------------------------------
def main():
    ap = argparse.ArgumentParser(description="Scrape NYC DOF NPV statements by BBL.")
    ap.add_argument("--bbl-csv", required=True, help="Path to CSV containing the BBL list.")
    ap.add_argument("--bbl-col", default="PARID", help="Column name in --bbl-csv with BBLs (default: PARID).")
    ap.add_argument("--stmt-date", required=True, help="Statement date YYYYMMDD (e.g., 20250115).")
    ap.add_argument("--out-csv", default="nyc_npv_scrape.csv", help="Append results here (default: nyc_npv_scrape.csv).")
    ap.add_argument("--existing-csv", default=None, help="Optional existing progress CSV to skip already-done rows.")
    ap.add_argument("--stmt-type", default="NPV", help="Statement type (default: NPV).")
    ap.add_argument("--max-workers", type=int, default=MAX_WORKERS, help="Concurrency (default: 8).")
    ap.add_argument("--batch-size", type=int, default=BATCH_SIZE, help="Rows per append (default: 1000).")
    ap.add_argument("--write-every", type=int, default=WRITE_EVERY, help="Force-flush interval (default: 250).")
    args = ap.parse_args()

    # Load your REAL BBL list (no pretending)
    df_bbl = pd.read_csv(args.bbl_csv, dtype={args.bbl_col: str})
    if args.bbl_col not in df_bbl.columns:
        sys.exit(f"[ERROR] Column '{args.bbl_col}' not found in {args.bbl_csv}")
    bbls = df_bbl[args.bbl_col].astype(str).tolist()

    scrape_streaming(
        bbls=bbls,
        stmt_date=args.stmt_date,
        out_csv=args.out_csv,
        existing_csv=args.existing_csv or args.out_csv,  # resume from output by default
        stmt_type=args.stmt_type,
        max_workers=args.max_workers,
        batch_size=args.batch_size,
        write_every=args.write_every,
    )
# %%
if __name__ == "__main__":
    main()

# %%
