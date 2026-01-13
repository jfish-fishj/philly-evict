
"""
fetch_places_and_reviews.py

Given a CSV of apartment addresses, call Google Places Text Search + Place Details
to build:

1) places_summary.csv   - one row per place_id / apartment
2) reviews_raw.csv      - one row per review

Expected input: input/addresses.csv with columns:
    - apt_id          (string/int: your internal ID)
    - address_query   (string: name/address to feed to Google)

Environment:
    - GOOGLE_MAPS_API_KEY must be set (billing-enabled Places API project).
"""
# %%
import csv
import os
import time
from typing import Dict, Any, List, Optional
import pandas as pd
import requests
# %%

# ------------------------
# Config
# ------------------------

API_KEY = os.environ.get("GOOGLE_MAPS_API_KEY")

if not API_KEY:
    raise RuntimeError("Set GOOGLE_MAPS_API_KEY in environment before running.")

TEXT_SEARCH_URL = "https://maps.googleapis.com/maps/api/place/textsearch/json"
DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"

# Paths (tweak as needed)
INPUT_ADDRESSES_CSV = "~/Desktop/data/philly-evict/processed/large_apartments.csv"
OUTPUT_PLACES_CSV = "output/places_summary.csv"
OUTPUT_REVIEWS_CSV = "output/reviews_raw.csv"
OUTPUT_REVIEWS_NDJSON = "output/reviews_raw.ndjson"

# Rate limiting: be nice to the API and avoid quota spikes
REQUEST_SLEEP_SEC = 0.25  # ~4 requests/sec; adjust based on your quota

# %%
import os
import time
from typing import Any, Dict, List, Optional

import pandas as pd
import requests

# ------------------------
# Build address_query in pandas
# ------------------------
# %%
addys = pd.read_csv(INPUT_ADDRESSES_CSV)
sample_addys = addys.sample(10).copy()

# assume n_sn_ss_c and pm.zip exist; cast to string to be safe
sample_addys["n_sn_ss_c"] = sample_addys["n_sn_ss_c"].astype(str)
sample_addys["pm.zip"] = sample_addys["pm.zip"].astype(str)

sample_addys["address_query"] = (
    sample_addys["n_sn_ss_c"]
    + ", "
    + "philadelphia"
    + ", "
    + "PA"
    + " "
    + sample_addys["pm.zip"]
    + " apartments"
)

# optionally overwrite the input CSV or write to a new one
# sample_addys.to_csv(INPUT_ADDRESSES_CSV, index=False)
# or:
# sample_addys.to_csv(ENRICHED_ADDRESSES_CSV, index=False)


# ------------------------
# API helpers
# ------------------------

def text_search(query: str, max_results: int = 5) -> Optional[Dict[str, Any]]:
    """
    Call Places Text Search API for a free-form query.
    Returns the *top* candidate (dict) or None if ZERO_RESULTS.
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
    # Use the first result as our best candidate
    return results[0]


def place_details(place_id: str) -> Dict[str, Any]:
    """
    Call Place Details for a given place_id.

    We request a limited set of fields to control costs:
      - name, formatted_address, rating, user_ratings_total, url, types, reviews
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
# ETL logic (pandas-based)
# ------------------------

def load_addresses(path: str) -> pd.DataFrame:
    """
    Load addresses as a DataFrame. Requires:
      - n_sn_ss_c (your apt_id / address ID)
      - address_query (the query string you constructed above)
    """
    df = pd.read_csv(path, dtype=str)
    required = {"n_sn_ss_c", "pm.zip"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Input must have columns: {required}, missing: {missing}")
    return df


def init_places_file(path: str):
    cols = [
        "apt_id",
        "place_id",
        "place_name",
        "formatted_address",
        "rating",
        "user_ratings_total",
        "url",
        "types",  # pipe-separated
        "text_search_query",
    ]
    if not os.path.exists(path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        pd.DataFrame(columns=cols).to_csv(path, index=False)
    else:
        print(f"[INFO] Places file {path} already exists; appending to it.")


def init_reviews_file(path: str):
    cols = [
        "apt_id",
        "place_id",
        "place_name",
        "formatted_address",
        "review_author_name",
        "review_author_url",
        "review_profile_photo_url",
        "review_language",
        "review_rating",
        "review_text",
        "review_time_unix",
        "review_relative_time_description",
    ]
    if not os.path.exists(path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        pd.DataFrame(columns=cols).to_csv(path, index=False)
    else:
        print(f"[INFO] Reviews file {path} already exists; appending to it.")


def append_place_row(path: str, row: Dict[str, Any]):
    df = pd.DataFrame([row])
    # append without header
    df.to_csv(path, mode="a", header=False, index=False)

import json
import os
from datetime import datetime
from typing import Any, Dict, List, Optional


def reviews_to_ndjson_records(
    *,
    apt_id: str,
    place_id: Optional[str],
    place_name: Optional[str],
    formatted_address: Optional[str],
    reviews: Any,
    fetched_at_utc: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Flatten Google Place Details 'reviews' into a list of JSON-serializable dicts.
    """
    if fetched_at_utc is None:
        fetched_at_utc = datetime.utcnow().isoformat(timespec="seconds") + "Z"

    if not place_id or not isinstance(reviews, list) or len(reviews) == 0:
        return []

    out: List[Dict[str, Any]] = []
    for r in reviews:
        out.append(
            {
                "apt_id": apt_id,
                "place_id": place_id,
                "place_name": place_name,
                "formatted_address": formatted_address,
                "fetched_at_utc": fetched_at_utc,
                # raw review fields (keep close to the API payload)
                "author_name": r.get("author_name"),
                "author_url": r.get("author_url"),
                "profile_photo_url": r.get("profile_photo_url"),
                "language": r.get("language"),
                "rating": r.get("rating"),
                "text": r.get("text"),
                "time_unix": r.get("time"),
                "relative_time_description": r.get("relative_time_description"),
                # if you ever request it / it appears
                "original_language": r.get("original_language"),
                "translated": r.get("translated"),
            }
        )
    return out


def append_reviews_ndjson(
    path: str,
    *,
    apt_id: str,
    place_id: Optional[str],
    place_name: Optional[str],
    formatted_address: Optional[str],
    details: Dict[str, Any],
) -> int:
    """
    Append reviews from a Place Details response to an NDJSON file.

    Returns: number of review records written.
    """
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)

    records = reviews_to_ndjson_records(
        apt_id=apt_id,
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



def append_review_rows(path: str, rows: List[Dict[str, Any]]):
    if not rows:
        return
    df = pd.DataFrame(rows)
    df.to_csv(path, mode="a", header=False, index=False)


def main():
    # if you created sample_addys above and want to use that in-memory:
    # addresses = sample_addys.copy()
    # else: load from disk
    addresses = load_addresses(INPUT_ADDRESSES_CSV)
    print(f"[INFO] Loaded {len(addresses)} addresses")

    init_places_file(OUTPUT_PLACES_CSV)
    init_reviews_file(OUTPUT_REVIEWS_CSV)

    for idx, row in addresses.iterrows():
        apt_id = row["n_sn_ss_c"]
        query = row["address_query"]

        print(f"[{idx+1}/{len(addresses)}] Resolving apt_id={apt_id} query={query!r}")

        try:
            # --- Text Search ---
            candidate = text_search(query)
            time.sleep(REQUEST_SLEEP_SEC)

            if candidate is None:
                print(f"  -> NO RESULTS for apt_id={apt_id}")
                append_place_row(
                    OUTPUT_PLACES_CSV,
                    {
                        "apt_id": apt_id,
                        "place_id": None,
                        "place_name": None,
                        "formatted_address": None,
                        "rating": None,
                        "user_ratings_total": None,
                        "url": None,
                        "types": None,
                        "text_search_query": query,
                    },
                )
                continue

            place_id = candidate["place_id"]

            # --- Details + Reviews ---
            details = place_details(place_id)
            time.sleep(REQUEST_SLEEP_SEC)

            place_row = {
                "apt_id": apt_id,
                "place_id": place_id,
                "place_name": details.get("name"),
                "formatted_address": details.get("formatted_address"),
                "rating": details.get("rating"),
                "user_ratings_total": details.get("user_ratings_total"),
                "url": details.get("url"),
                "types": "|".join(details.get("types", [])) if details.get("types") else None,
                "text_search_query": query,
            }
            append_place_row(OUTPUT_PLACES_CSV, place_row)

            # --- Reviews (up to 5 recent ones, per API) ---
            # reviews = details.get("reviews", []) or []
            # review_rows = []
            # for r in reviews:
            #     review_rows.append(
            #         {
            #             "apt_id": apt_id,
            #             "place_id": place_id,
            #             "place_name": details.get("name"),
            #             "formatted_address": details.get("formatted_address"),
            #             "review_author_name": r.get("author_name"),
            #             "review_author_url": r.get("author_url"),
            #             "review_profile_photo_url": r.get("profile_photo_url"),
            #             "review_language": r.get("language"),
            #             "review_rating": r.get("rating"),
            #             "review_text": r.get("text"),
            #             "review_time_unix": r.get("time"),
            #             "review_relative_time_description": r.get("relative_time_description"),
            #         }
            #     )
            # append_review_rows(OUTPUT_REVIEWS_CSV, review_rows)

        except Exception as e:
            print(f"[ERROR] apt_id={apt_id} {e}")
            continue


#%%
# main run
addresses = load_addresses(INPUT_ADDRESSES_CSV)
parsed_addresses = pd.read_csv(OUTPUT_PLACES_CSV, dtype=str)
addresses["address_query"] = (
    addresses["n_sn_ss_c"]
    + ", "
    + "philadelphia"
    + ", "
    + "PA"
    + " "
    + addresses["pm.zip"]
    + " apartments"
)
print(f"[INFO] Loaded {len(addresses)} addresses")
# drop already-parsed
# county number already in output places

addresses = addresses[~addresses["address_query"].isin(parsed_addresses["text_search_query"])]
init_places_file(OUTPUT_PLACES_CSV)
init_reviews_file(OUTPUT_REVIEWS_CSV)

for idx, row in addresses.iterrows():
    apt_id = row["n_sn_ss_c"]
    query = row["address_query"]

    print(f"[{idx+1}/{len(addresses)}] Resolving apt_id={apt_id} query={query!r}")

    try:
        # --- Text Search ---
        candidate = text_search(query)
        time.sleep(REQUEST_SLEEP_SEC)

        if candidate is None:
            print(f"  -> NO RESULTS for apt_id={apt_id}")
            append_place_row(
                OUTPUT_PLACES_CSV,
                {
                    "apt_id": apt_id,
                    "place_id": None,
                    "place_name": None,
                    "formatted_address": None,
                    "rating": None,
                    "user_ratings_total": None,
                    "url": None,
                    "types": None,
                    "text_search_query": query,
                },
            )
            continue

        place_id = candidate["place_id"]

        # # --- Details + Reviews ---
        details = place_details(place_id)
        time.sleep(REQUEST_SLEEP_SEC)

        place_row = {
            "apt_id": apt_id,
            "place_id": place_id,
            "place_name": details.get("name"),
            "formatted_address": details.get("formatted_address"),
            "rating": details.get("rating"),
            "user_ratings_total": details.get("user_ratings_total"),
            "url": details.get("url"),
            "types": "|".join(details.get("types", [])) if details.get("types") else None,
            "text_search_query": query,
        }
        append_place_row(OUTPUT_PLACES_CSV, place_row)
        place_name = details.get("name")
        formatted_address = details.get("formatted_address")

        # append reviews as NDJSON
        n_written = append_reviews_ndjson(
            OUTPUT_REVIEWS_NDJSON,  # e.g. "output/reviews.ndjson"
            apt_id=apt_id,
            place_id=place_id,
            place_name=place_name,
            formatted_address=formatted_address,
            details=details,
        )
        print(f"  -> wrote {n_written} reviews")
    except Exception as e:
        print(f"[ERROR] apt_id={apt_id} {e}")
    continue
# %%

