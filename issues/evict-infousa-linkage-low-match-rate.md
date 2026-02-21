# Eviction ↔ InfoUSA linkage: low match-rate hypotheses (2026-02-20)

This note tracks likely reasons the case-level match rate remains modest in sample runs, with supporting evidence from current logs/QA.

## Current sample outcome (reference)
- 1000-case sample run (seed 123, tract+street fuzzy blocking, timeout 10s, default name gate):
  - final matched cases: `207`
  - unmatched: `793`
  - unmatched reasons: `no_match=780`, `ambiguous_household=8-9` (varies by run)
  - source: `output/logs/link-evictions-infousa-names.log`

## Hypotheses and evidence

1) Candidate funnel is very aggressive after cap+name gate.
- Evidence:
  - pre-cap candidates: `1,650,270`
  - after cap (`K=20`): `18,876`
  - after name gate (`first>=0.80`, `last>=0.88`): `95`
- Risk:
  - true matches can be removed before scoring/acceptance.

2) Street parsing noise still hurts blocking/coverage.
- Evidence:
  - unmatched street diagnostics include malformed street strings (e.g., `SANSOM STREET UN`, `MARKET STAKA 2040 MARKET ST`).
  - files:
    - `output/qa/evict_infousa_unmatched_street_names_1000_cases.csv`
    - `output/qa/evict_infousa_unmatched_street_names_anywhere_1000_cases.csv`
- Risk:
  - incorrect street tokens reduce same-street candidate generation.

3) fastLink timeout settings materially change outcomes.
- Evidence:
  - with `PHILLY_FASTLINK_BLOCK_TIMEOUT_SEC=1`: `fastLink_used=FALSE`, `fastLink_timeouts=18`, matched cases `206`
  - with `...=10`: `fastLink_used=TRUE`, `fastLink_timeouts=0`, matched cases `207` (or `211` after threshold relaxation)
- Risk:
  - low timeouts suppress probabilistic boosts for plausible near-matches.

4) Some unmatched cases may have no plausible InfoUSA household in sample geography.
- Evidence:
  - unmatched ZIP fuzzy diagnostic (`772` unmatched cases in that snapshot):
    - `zip_has_no_inf_candidates`: `112`
    - `no_plausible_name_in_zip`: `454`
    - `plausible_high`: `136`
    - `plausible_medium`: `70`
  - share with any plausible ZIP+name candidate: ~`26.7%`
  - file: `output/qa/evict_infousa_unmatched_zip_name_fuzzy_case_summary.csv`
- Risk:
  - observed low recall may partly be true non-overlap between eviction defendants and InfoUSA persons.

5) Case-level one-household resolution reduces case matches vs person matches.
- Evidence:
  - person-level direct matches can exceed final case matches due to rollup and ambiguity resolution.
  - example run: direct person matches `209`, final case matches `207`.
- Risk:
  - this is expected by design but lowers case-level headline match rate.

6) Missing nickname dictionary likely depresses first-name flexibility.
- Evidence:
  - log reports missing file:
    - `data/inputs/lookups/nicknames.csv`
- Risk:
  - near-name variants rely only on JW string similarity and may miss intended nickname equivalences.

## Active mitigations already implemented
- Removed direct last-name-only deterministic pass.
- Removed last-name exact blocking requirement.
- Switched to geo+street blocking with optional street-fuzzy keys.
- Restricted fastLink to KxK blocks and keyed block extraction for speed.
- Lowered size-bin acceptance thresholds (now `0.76` to `0.80` by size bin), which improved sample matches.

## Suggested next diagnostics
1) Quantify recall loss at each funnel stage by known plausible pairs.
2) Re-run unmatched street diagnostics after targeted address-cleaning fixes.
3) Add/enable nickname dictionary and compare lift.
4) Evaluate BG-only vs tract-only on a fixed 100/1000-case benchmark with same seed.
