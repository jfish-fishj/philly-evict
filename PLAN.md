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

```
r/process-rtt-data.R
├── Header comment block (purpose, inputs, outputs, primary key)
├── Load libraries (data.table, stringr)
├── Source config.R + helper-functions.R
├── Read config, set up log file
├── Load RTT data via p_input(cfg, "rtt_summary")
├── Load parcels_clean via p_product(cfg, "parcels_clean") [parcel_number only]
│
├── STEP 1: Basic cleaning
│   ├── janitor::clean_names() (already lowercase in CSV, but ensure consistency)
│   ├── Parse dates: display_date, document_date, recording_date → Date
│   ├── Extract year, year_quarter from display_date
│   ├── Convert money columns to numeric (total_consideration, cash_consideration, etc.)
│   ├── Convert opa_account_num to integer (drop NAs → log count)
│   ├── Normalize opa_account_num via normalize_pid() → PID
│   ├── Log: row counts, date range, PID coverage
│
├── STEP 2: Filter to relevant document types
│   ├── Keep: DEED, SHERIFF'S DEED, DEED SHERIFF, DEED OF CONDEMNATION
│   ├── Drop: MORTGAGE, SATISFACTION, ASSIGNMENT OF MORTGAGE, RELEASE, etc.
│   ├── Log: rows kept/dropped by document_type
│
├── STEP 3: Match to parcels_clean
│   ├── Inner join on PID to parcels_clean (parcel_number only, for validation)
│   ├── Log: matched vs unmatched count
│   ├── Keep only matched rows (PID exists in parcels_clean)
│
├── STEP 4: Arms-length transfer filter
│   ├── Drop $0 and very low consideration (e.g. total_consideration < $100)
│   │   (These are intra-family, government, tax-sale transfers)
│   ├── Drop where grantors ≈ grantees (self-transfers)
│   ├── Drop property_count > threshold (bulk portfolio transfers, not individual sales)
│   ├── Flag: is_arms_length based on above
│   ├── Log: rows kept/dropped per filter
│
├── STEP 5: Dedup & final cleaning
│   ├── Handle multi-property documents (property_count > 1):
│   │   adjusted_* columns already divide by property_count per fields.csv
│   ├── Select final columns: PID, display_date, year, year_quarter,
│   │   document_type, total_consideration, adjusted_total_consideration,
│   │   cash_consideration, assessed_value, fair_market_value,
│   │   grantors, grantees, street_address, zip_code, unit_num,
│   │   property_count, reg_map_id, matched_regmap, document_id
│   ├── assert_unique on (PID, document_id) or (record_id)
│   ├── Log final row count, year distribution
│
├── STEP 6: Export
│   ├── fwrite to p_product(cfg, "rtt_clean")
│   ├── Log output path + row count
```

### 3. Key decisions
- **Primary key**: `(PID, document_id)` — one row per parcel per document
- **Arms-length filter**: Conservative — keep anything with total_consideration >= $100 and not a self-transfer. User can tighten downstream.
- **No address matching needed**: 87% of deeds have `opa_account_num`. The remaining 13% could be matched via address later if needed, but skip for now.
- **Adjusted columns**: Keep both raw and adjusted (per-property) consideration amounts since fields.csv says adjusted = raw / property_count.
- **Date handling**: Use `display_date` as the canonical date (per fields.csv: "document date when present, otherwise recording date").

### 4. No changes to other scripts
This is a standalone product. Downstream analysis scripts can merge `rtt_clean` by PID + year as needed.
