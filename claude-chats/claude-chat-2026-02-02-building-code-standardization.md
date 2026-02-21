# Claude Chat - 2026-02-02 - Building Code Standardization

## Summary

Implemented a robust building code standardization system using both `building_code_description` (old) and `building_code_description_new` columns, plus added story extraction and refactored the building-parcel crosswalk script.

## Changes Made

### 1. New Functions in `r/helper-functions.R`

**`extract_stories_from_code(bldg_code_desc)`**
- Extracts story count from building code descriptions
- Handles patterns: "2 STY", "3.5 STY", "10-14 STY" (returns midpoint for ranges)
- Returns NA when no story info found

**`standardize_building_type(bldg_code_desc, bldg_code_desc_new, num_bldgs, num_stories)`**
- Uses BOTH old and new building code columns for better classification
- Distinguishes high-rise towers from garden-style complexes using `num_bldgs`
- Returns a **list** with two elements:
  - `building_type`: Size-based classification
  - `is_condo`: Logical flag for condo status

**Output categories for `building_type`:**
| Category | Description |
|----------|-------------|
| DETACHED | Single-family detached |
| ROW | Rowhouse/attached single-family |
| TWIN | Semi-detached/duplex |
| SMALL_MULTI_2_4 | 2-4 unit building |
| LOWRISE_MULTI | 5-19 units, ≤3 stories |
| MIDRISE_MULTI | 4-6 stories |
| HIGHRISE_MULTI | 7+ stories, single building |
| MULTI_BLDG_COMPLEX | Garden-style (>3 buildings on parcel) |
| COMMERCIAL | Commercial properties (both columns agree) |
| OTHER | Unable to classify |

### 2. Updated `r/make-rent-panel.R`

- Updated `prep_parcel_backbone()` function (~line 121) to use new standardization
- Updated unit model section (~line 595) to use new functions
- Creates new columns: `stories_from_code`, `building_type`, `is_condo`
- Preserves legacy `building_code_description_new_fixed` for backward compatibility

### 3. Updated `r/make-occupancy-vars.r`

- Added `source("r/helper-functions.R")`
- Updated imputation grouping to use `building_type` instead of old fixed column
- Updated structure_bin mapping to use direct mapping from `building_type`
- Falls back to legacy regex approach if `building_type` unavailable

### 4. Refactored `r/make_bldg_pid_xwalk.r`

Complete refactor to match project patterns:
- Added header documentation (purpose, inputs, outputs, primary keys)
- Uses `suppressPackageStartupMessages` for library loading
- Sources `config.R` and uses `read_config()`
- Replaced all hardcoded paths with `p_input()` and `p_product()` helpers
- Added comprehensive logging with `logf()` to `output/logs/make_bldg_pid_xwalk.log`
- Added QA checks and statistics logging
- Logs distribution of buildings per parcel

### 5. Config Updates

Added to `config.yml`:
```yaml
building_footprints_shp: "parcels/LI_BUILDING_FOOTPRINTS/LI_BUILDING_FOOTPRINTS.shp"
```

## Key Design Decisions

1. **Two-column approach**: Using both old and new building code columns provides better classification since the old column has structural info (unit counts, stories) while the new has style info.

2. **Multi-building complex detection**: Parcels with `num_bldgs > 3` are classified as `MULTI_BLDG_COMPLEX` to distinguish garden-style apartments from high-rise towers (important for rental market analysis).

3. **Condo as separate flag**: Condos are classified by their building size (e.g., a condo in a midrise gets `building_type = "MIDRISE_MULTI"`) with `is_condo = TRUE` as a separate flag. This allows analysis by building size while still tracking condo status.

4. **Conservative commercial detection**: Only flags as `COMMERCIAL` if **both** columns indicate commercial use. This prevents hotels, office buildings, and schools from being misclassified as residential high-rises. Commercial patterns checked:
   - Old column: HOTEL, OFFICE, WAREHOUSE, SCHOOL, HOSPITAL, HSE WORSHIP, CEMETERY, etc.
   - New column: RETAIL, WAREHOUSE, HOTEL, RELIGIOUS, COMMERCIAL, INDUSTRIAL, etc.

5. **Backward compatibility**: Legacy `building_code_description_new_fixed` column still created for existing models that depend on it.

6. **Fallback behavior**: All scripts fall back to legacy regex approach if new columns aren't available.

## Usage Example

```r
# Function returns a list
result <- standardize_building_type(
  bldg_code_desc = parcels$building_code_description,
  bldg_code_desc_new = parcels$building_code_description_new,
  num_bldgs = parcels$num_bldgs,
  num_stories = parcels$stories_from_code
)

# Extract both components
parcels[, building_type := result$building_type]
parcels[, is_condo := result$is_condo]

# Example results:
# "RES CONDO 5 STY MASONRY" + "CONDO" -> building_type="MIDRISE_MULTI", is_condo=TRUE
# "HOTEL 7 STY MASONRY" + "HOTEL" -> building_type="COMMERCIAL", is_condo=FALSE
# "APTS 5-50 UNITS MASONRY" + "APARTMENTS - LOW RISE" -> building_type="LOWRISE_MULTI", is_condo=FALSE
```

## Files Modified

- `r/helper-functions.R` - added 2 new functions (~200 lines)
- `r/make-rent-panel.R` - updated 2 locations
- `r/make-occupancy-vars.r` - updated imputation and structure_bin sections
- `r/make_bldg_pid_xwalk.r` - complete refactor
- `config.yml` - added building_footprints_shp input

## Commits

1. `78d5b20` - Add building code standardization functions and refactor make_bldg_pid_xwalk
2. `0eec887` - Refine building code standardization: split condo flag, stricter commercial detection
