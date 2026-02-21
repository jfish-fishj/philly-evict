# Plan: process-rtt-data.R

## Goal
Create `r/process-rtt-data.R` — a single script that reads the RTT summary, cleans it, matches to parcels via `opa_account_num`, filters to arms-length deed transfers, and writes a clean product.

## Data Summary
- **Input**: `data/inputs/open-data/rtt_summary.csv` — 4.8M rows, 48 cols
- **Key ID**: `opa_account_num` (float, e.g. `162248900.0`) → maps to `parcel_number` in parcels_clean
- **Coverage**: 84% of rows have `opa_account_num`; 87% of deed-type rows
- **Date range**: 1920–2024
- **Document types**: DEED (25K/50K), MORTGAGE, MISCELLANEOUS DEED, SHERIFF'S DEED, etc.

## Implementation

### 1. Config additions (`config.yml`)
```yaml
inputs:
  rtt_summary: "open-data/rtt_summary.csv"

products:
  rtt_clean: "clean/rtt_clean.csv"
```

### 2. Script structure (following clean-parcel-addresses.R pattern)

**STEP 1: Basic cleaning**
- Parse dates: display_date, document_date, recording_date → Date
- Extract year, year_quarter from display_date
- Convert money columns to numeric (total_consideration, cash_consideration, etc.)
- Convert opa_account_num to integer, normalize via normalize_pid() → PID
- Log: row counts, date range, PID coverage

**STEP 2: Filter to relevant document types**
- Keep: DEED, SHERIFF'S DEED, DEED SHERIFF, DEED OF CONDEMNATION
- Drop: MORTGAGE, SATISFACTION, ASSIGNMENT OF MORTGAGE, RELEASE, etc.
- Log: rows kept/dropped by document_type

**STEP 3: Match to parcels_clean**
- Inner join on PID to parcels_clean (parcel_number only, for validation)
- Log: matched vs unmatched count
- Keep only matched rows

**STEP 4: Arms-length transfer filter**
- Drop $0 and very low consideration (total_consideration < $100)
- Drop where grantors ≈ grantees (self-transfers)
- Drop property_count > reasonable threshold (bulk portfolio transfers)
- Flag: is_arms_length based on above
- Log: rows kept/dropped per filter

**STEP 5: Dedup & final cleaning**
- Select final columns: PID, display_date, year, year_quarter, document_type, total_consideration, adjusted_total_consideration, cash_consideration, assessed_value, fair_market_value, grantors, grantees, street_address, zip_code, unit_num, property_count, reg_map_id, matched_regmap, document_id
- Assert uniqueness on (PID, document_id) or (record_id)
- Log final row count, year distribution

**STEP 6: Export**
- fwrite to p_product(cfg, "rtt_clean")

### 3. Key decisions
- **Primary key**: `(PID, document_id)` — one row per parcel per document
- **Arms-length filter**: Conservative — keep anything with total_consideration >= $100 and not a self-transfer
- **No address matching needed**: 87% of deeds have opa_account_num. The remaining 13% could be matched later if needed.
- **Date**: Use `display_date` as canonical date (per fields.csv: "document date when present, otherwise recording date")
- **Adjusted columns**: Keep both raw and adjusted (per-property) amounts since adjusted = raw / property_count per fields.csv

### 4. No changes to other scripts
Standalone product. Downstream scripts merge rtt_clean by PID + year as needed.
