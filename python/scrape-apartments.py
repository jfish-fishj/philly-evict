"""
scrape-apartments.py

Fetch Google Places + Reviews data for large apartment buildings.

Inputs (from config):
  - bldg_panel_blp: Full rental panel with building characteristics

Outputs (to output_dir):
  - places_summary.csv: One row per place_id / apartment
  - reviews_raw.ndjson: One row per review (NDJSON format)
  - scraped_addresses.csv: Tracking file for already-scraped addresses

Filtering logic:
  - Parcels with num_units_imp_final >= 10, OR
  - Parcels with building_type in apartment categories

Environment:
  - GOOGLE_MAPS_API_KEY must be set (billing-enabled Places API project)
  - PHILLY_EVICTIONS_CONFIG can point to config file (defaults to config.yml)
"""

import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import requests

from config_helpers import load_config, p_output, p_product


# ------------------------
# Google Places API
# ------------------------

API_KEY = os.environ.get("GOOGLE_MAPS_API_KEY")

TEXT_SEARCH_URL = "https://maps.googleapis.com/maps/api/place/textsearch/json"
DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"

# Rate limiting
REQUEST_SLEEP_SEC = 0.25


def text_search(query: str, max_results: int = 5) -> Optional[Dict[str, Any]]:
    """
    Call Places Text Search API for a free-form query.
    Returns the top candidate (dict) or None if ZERO_RESULTS.
    """
    params = {
        "query": query,
        "key": API_KEY,
    }
    resp = requests.get(TEXT_SEARCH_URL, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    status = data.get("status")
    if status not in ("OK", "ZERO_RESULTS"):
        raise RuntimeError(f"Text Search error: {status}, query={query}")

    results = data.get("results", [])[:max_results]
    if not results:
        return None
    return results[0]


def place_details(place_id: str) -> Dict[str, Any]:
    """
    Call Place Details for a given place_id.
    Request limited fields to control costs.
    """
    fields = [
        "name",
        "formatted_address",
        "rating",
        "user_ratings_total",
        "url",
        "types",
        "reviews",
    ]
    params = {
        "placeid": place_id,
        "fields": ",".join(fields),
        "key": API_KEY,
    }
    resp = requests.get(DETAILS_URL, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    status = data.get("status")
    if status != "OK":
        raise RuntimeError(f"Place Details error: {status} for {place_id} ({data})")

    return data.get("result", {})


# ------------------------
# Data loading and filtering
# ------------------------

APARTMENT_BUILDING_TYPES = {
    "LOWRISE_MULTI",
    "MIDRISE_MULTI",
    "HIGHRISE_MULTI",
    "MULTI_BLDG_COMPLEX"
}

MIN_UNITS_THRESHOLD = 10


def load_rental_panel(cfg: dict) -> pd.DataFrame:
    """
    Load the rental panel and filter to large apartments.

    Filter criteria:
      - num_units_imp_final >= 10, OR
      - building_type in APARTMENT_BUILDING_TYPES
    """
    panel_path = p_product(cfg, "bldg_panel_blp")
    print(f"[INFO] Loading rental panel from: {panel_path}")

    # Read only needed columns to save memory
    usecols = None  # Read all, then filter
    df = pd.read_csv(panel_path, dtype=str, low_memory=False)
    print(f"[INFO] Loaded {len(df)} rows from rental panel")

    # Convert numeric columns
    if "num_units_imp_final" in df.columns:
        df["num_units_imp_final"] = pd.to_numeric(df["num_units_imp_final"], errors="coerce")

    # Apply filters
    mask_units = df["num_units_imp_final"] >= MIN_UNITS_THRESHOLD

    mask_bldg_type = pd.Series(False, index=df.index)
    if "building_type" in df.columns:
        mask_bldg_type = df["building_type"].isin(APARTMENT_BUILDING_TYPES)

    df_filtered = df[mask_units | mask_bldg_type].copy()
    print(f"[INFO] Filtered to {len(df_filtered)} rows (units >= {MIN_UNITS_THRESHOLD} or apartment building type)")

    # Deduplicate by PID (take most recent year)
    if "year" in df_filtered.columns:
        df_filtered["year"] = pd.to_numeric(df_filtered["year"], errors="coerce")
        df_filtered = df_filtered.sort_values("year", ascending=False)

    df_filtered = df_filtered.drop_duplicates(subset=["PID"], keep="first")
    print(f"[INFO] After PID dedup: {len(df_filtered)} unique parcels")

    return df_filtered


def build_address_query(row: pd.Series) -> str:
    """
    Build a Google Places query string from parcel data.

    Tries these address columns in order:
      - n_sn_ss_c (normalized street address)
      - location (full address)
      - street_address
    """
    # Try to find best address column
    address = None
    for col in ["n_sn_ss_c", "location", "street_address", "address"]:
        if col in row.index and pd.notna(row[col]) and str(row[col]).strip():
            address = str(row[col]).strip()
            break

    if address is None:
        return None

    # Get zip code
    zip_code = None
    for col in ["pm.zip", "zip_code", "zip"]:
        if col in row.index and pd.notna(row[col]):
            zip_code = str(row[col]).strip().replace("_", "")[:5]
            if zip_code and zip_code != "nan":
                break
            zip_code = None

    # Build query
    query_parts = [address, "Philadelphia", "PA"]
    if zip_code:
        query_parts.append(zip_code)
    query_parts.append("apartments")

    return ", ".join(query_parts)


# ------------------------
# Output file management
# ------------------------

def init_places_file(path: str) -> None:
    """Initialize places CSV with headers if it doesn't exist."""
    cols = [
        "PID",
        "place_id",
        "place_name",
        "formatted_address",
        "rating",
        "user_ratings_total",
        "url",
        "types",
        "text_search_query",
        "scraped_at",
    ]
    if not os.path.exists(path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        pd.DataFrame(columns=cols).to_csv(path, index=False)
        print(f"[INFO] Created new places file: {path}")
    else:
        print(f"[INFO] Places file exists, will append: {path}")


def append_place_row(path: str, row: Dict[str, Any]) -> None:
    """Append a single place row to CSV."""
    df = pd.DataFrame([row])
    df.to_csv(path, mode="a", header=False, index=False)


def reviews_to_ndjson_records(
    *,
    pid: str,
    place_id: Optional[str],
    place_name: Optional[str],
    formatted_address: Optional[str],
    reviews: Any,
    fetched_at_utc: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Flatten Google Place Details 'reviews' into JSON-serializable dicts."""
    if fetched_at_utc is None:
        fetched_at_utc = datetime.utcnow().isoformat(timespec="seconds") + "Z"

    if not place_id or not isinstance(reviews, list) or len(reviews) == 0:
        return []

    out: List[Dict[str, Any]] = []
    for r in reviews:
        out.append({
            "PID": pid,
            "place_id": place_id,
            "place_name": place_name,
            "formatted_address": formatted_address,
            "fetched_at_utc": fetched_at_utc,
            "author_name": r.get("author_name"),
            "author_url": r.get("author_url"),
            "profile_photo_url": r.get("profile_photo_url"),
            "language": r.get("language"),
            "rating": r.get("rating"),
            "text": r.get("text"),
            "time_unix": r.get("time"),
            "relative_time_description": r.get("relative_time_description"),
            "original_language": r.get("original_language"),
            "translated": r.get("translated"),
        })
    return out


def append_reviews_ndjson(
    path: str,
    *,
    pid: str,
    place_id: Optional[str],
    place_name: Optional[str],
    formatted_address: Optional[str],
    details: Dict[str, Any],
) -> int:
    """Append reviews from Place Details to NDJSON file. Returns count written."""
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)

    records = reviews_to_ndjson_records(
        pid=pid,
        place_id=place_id,
        place_name=place_name,
        formatted_address=formatted_address,
        reviews=details.get("reviews"),
    )
    if not records:
        return 0

    with open(path, "a", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    return len(records)


def load_scraped_pids(places_path: str) -> set:
    """Load set of PIDs already scraped from places file."""
    if not os.path.exists(places_path):
        return set()

    df = pd.read_csv(places_path, dtype=str, usecols=["PID"])
    return set(df["PID"].dropna().unique())


# ------------------------
# Main
# ------------------------

def main():
    # Check API key
    if not API_KEY:
        raise RuntimeError("Set GOOGLE_MAPS_API_KEY in environment before running.")

    # Load config
    cfg = load_config()
    print(f"[INFO] Loaded config from: {cfg['_config_path']}")

    # Set up paths
    output_places = p_output(cfg, "places_summary.csv")
    output_reviews = p_output(cfg, "reviews_raw.ndjson")

    # Load and filter rental panel
    addresses = load_rental_panel(cfg)

    # Build address queries
    addresses["address_query"] = addresses.apply(build_address_query, axis=1)
    addresses = addresses[addresses["address_query"].notna()]
    print(f"[INFO] {len(addresses)} addresses with valid queries")

    # Load already-scraped PIDs
    scraped_pids = load_scraped_pids(output_places)
    print(f"[INFO] {len(scraped_pids)} PIDs already scraped")

    # Filter out already-scraped
    addresses = addresses[~addresses["PID"].isin(scraped_pids)]
    print(f"[INFO] {len(addresses)} addresses remaining to scrape")

    if len(addresses) == 0:
        print("[INFO] No new addresses to scrape. Done.")
        return

    # Initialize output files
    init_places_file(output_places)

    # Process each address
    total = len(addresses)
    for idx, (_, row) in enumerate(addresses.iterrows()):
        pid = row["PID"]
        query = row["address_query"]

        print(f"[{idx+1}/{total}] PID={pid} query={query!r}")

        try:
            # Text Search
            candidate = text_search(query)
            time.sleep(REQUEST_SLEEP_SEC)

            scraped_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"

            if candidate is None:
                print(f"  -> NO RESULTS for PID={pid}")
                append_place_row(output_places, {
                    "PID": pid,
                    "place_id": None,
                    "place_name": None,
                    "formatted_address": None,
                    "rating": None,
                    "user_ratings_total": None,
                    "url": None,
                    "types": None,
                    "text_search_query": query,
                    "scraped_at": scraped_at,
                })
                continue

            place_id = candidate["place_id"]

            # Place Details
            details = place_details(place_id)
            time.sleep(REQUEST_SLEEP_SEC)

            place_row = {
                "PID": pid,
                "place_id": place_id,
                "place_name": details.get("name"),
                "formatted_address": details.get("formatted_address"),
                "rating": details.get("rating"),
                "user_ratings_total": details.get("user_ratings_total"),
                "url": details.get("url"),
                "types": "|".join(details.get("types", [])) if details.get("types") else None,
                "text_search_query": query,
                "scraped_at": scraped_at,
            }
            append_place_row(output_places, place_row)

            # Append reviews
            n_written = append_reviews_ndjson(
                output_reviews,
                pid=pid,
                place_id=place_id,
                place_name=details.get("name"),
                formatted_address=details.get("formatted_address"),
                details=details,
            )
            print(f"  -> {details.get('name')}: {n_written} reviews")

        except Exception as e:
            print(f"[ERROR] PID={pid}: {e}")
            continue

    print(f"[INFO] Done. Results written to:")
    print(f"  - {output_places}")
    print(f"  - {output_reviews}")


if __name__ == "__main__":
    main()
