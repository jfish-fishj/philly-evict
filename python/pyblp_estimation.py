"""
pyblp_estimation.py

Config-driven nested logit estimation module using PyBLP.

Replaces the scratch nested-logit.py with a structured, reusable pipeline.
All paths come from config.yml via config_helpers.py — no hardcoded paths.

Usage:
    # Standalone
    python python/pyblp_estimation.py

    # Interactive / Jupyter
    from pyblp_estimation import EstimationConfig, main
    out = main(est_cfg=EstimationConfig(year_range=(2016, 2019), min_units=10))
    out["results"]  # pyblp.ProblemResults
"""

import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Hashable, List, Optional, Tuple

import numpy as np
import pandas as pd
import pyblp as blp
import statsmodels.api as sm
from scipy.stats import norm

from config_helpers import load_config, p_output, p_product

pd.options.mode.chained_assignment = None


# ============================================================
# EstimationConfig: all tunable parameters in one place
# ============================================================

@dataclass
class EstimationConfig:
    """All tunable estimation parameters. Defaults match the original scratch script."""

    # --- Sample restrictions ---
    year_range: Tuple[int, int] = (2011, 2019)
    min_units: int = 5
    max_units: int = 500
    min_units_zip: int = 100
    building_type_regex: str = "Apartment|Apt|Coop|Condo|Mixed"
    max_abs_change_log_taxable_value: float = 1.25
    max_annualized_change_log_rent: float = 0.2
    require_log_market_value_positive: bool = True
    min_obs_per_pid: int = 1

    # --- Market / product / firm column definitions ---
    market_cols: Tuple[str, ...] = ("pm.zip", "year")
    market_sep: str = "-"
    firm_col: str = "owner_1"
    product_col: str = "PID"
    shares_col: str = "share_zip"  # use share_zip (denom = total housing stock in zip-year)

    # --- Nest definition ---
    nest_threshold_col: str = "filing_rate_eb_pre_covid"
    nest_threshold: float = 0.1
    nest_labels: Tuple[str, str] = ("high", "low")  # (above_threshold, below_threshold)

    # --- Formulation ---
    x1_formula: str = "0 + prices + log_market_value + log_total_area + year_built"
    absorb_formula: str = (
        "C(GEOID)*C(year) + C(building_code_description_new)"
        " + C(quality_grade_fixed_coarse)"
    )

    # --- Instruments ---
    # If non-empty, these columns are used as excluded instruments for prices.
    # When empty, demand_instruments0 = prices (no IV, just-identified logit).
    excluded_instruments: Tuple[str, ...] = ()
    # Whether to add nest sizes as an additional instrument
    include_nest_size_instrument: bool = True

    # --- Solver ---
    rho_init: float = 0.2
    solve_kwargs: Dict[str, Any] = field(default_factory=dict)

    # --- Post-estimation ---
    diversion_target_nest: str = "low"
    diversion_pct_increase: float = 0.10

    # --- Output ---
    output_subdir: str = "pyblp_summaries"

    # --- Variable names (for column compatibility) ---
    units_col: str = "total_units"  # current R output
    units_col_legacy: str = "num_units_imp"  # legacy fallback


# ============================================================
# Utility functions (FE residualizer, instrument checks, NL shares)
# ============================================================

def residualize_against_fixed_effects(
    df: pd.DataFrame,
    cols: List[str],
    fe_cols: List[str],
    max_iter: int = 100,
    tol: float = 1e-10,
) -> pd.DataFrame:
    """
    Residualize `cols` against fixed-effect columns via alternating
    within-group demeaning (Frisch-Waugh-Lovell with FE).

    Returns a new DataFrame with residualized columns named "{c}__resid".
    """
    if not fe_cols:
        return df.assign(**{f"{c}__resid": df[c] for c in cols})

    out = df[cols].astype(float).copy()
    resid = out.to_numpy()

    def demean_one(fe: pd.Series, M: np.ndarray) -> np.ndarray:
        g_means = (
            pd.DataFrame(M, index=fe)
            .groupby(level=0)
            .transform("mean")
            .to_numpy()
        )
        return M - g_means

    for _ in range(max_iter):
        prev = resid.copy()
        for fe in fe_cols:
            resid = demean_one(df[fe], resid)
        if np.nanmax(np.abs(resid - prev)) < tol:
            break

    resid_df = pd.DataFrame(
        resid, index=df.index, columns=[f"{c}__resid" for c in cols]
    )
    return pd.concat([df, resid_df], axis=1)


def instrument_relevance_after_fe(
    df: pd.DataFrame,
    endog: str,
    instruments: List[str],
    exog: Optional[List[str]] = None,
    fe_cols: Optional[List[str]] = None,
    cluster: Optional[object] = None,
    cluster_kwargs: Optional[dict] = None,
) -> Dict[str, object]:
    """
    Residualize endog + instruments + exog w.r.t. FE, then run first-stage
    diagnostics: pairwise correlations, partial R^2, and F-test on instruments.
    """
    exog = exog or []
    fe_cols = fe_cols or []

    cols_to_resid = [endog] + instruments + exog
    rdf = residualize_against_fixed_effects(df, cols_to_resid, fe_cols)

    y = rdf[f"{endog}__resid"].to_numpy()
    Z = rdf[[f"{z}__resid" for z in instruments]].to_numpy()
    X = rdf[[f"{x}__resid" for x in exog]].to_numpy() if exog else None

    corr = pd.Series(
        {z: np.corrcoef(rdf[f"{z}__resid"], y)[0, 1] for z in instruments},
        name="corr(z~, y~)",
    )

    if X is None or X.size == 0:
        RHS = Z
    else:
        RHS = np.column_stack([Z, X])

    k_z = Z.shape[1]
    model = sm.OLS(y, RHS, missing="drop")

    if cluster is None:
        res = model.fit(cov_type="HC1")
    else:
        if isinstance(cluster, str):
            groups = df.loc[rdf.index, cluster]
        else:
            groups = cluster
        cov_kw = {"groups": groups}
        cov_kw.update(cluster_kwargs or {"use_correction": True, "df_correction": True})
        res = model.fit(cov_type="cluster", cov_kwds=cov_kw)

    # Reduced model for partial R^2
    if X is None or X.size == 0:
        r2_reduced = 0.0
    else:
        red = sm.OLS(y, X, missing="drop")
        if cluster is None:
            res_red = red.fit(cov_type="HC1")
        else:
            res_red = red.fit(cov_type="cluster", cov_kwds=cov_kw)
        r2_reduced = res_red.rsquared

    # F-test on instruments
    R = np.zeros((k_z, RHS.shape[1]))
    R[np.arange(k_z), np.arange(k_z)] = 1.0
    ft = res.f_test(R)

    F_val = float(np.squeeze(np.asarray(ft.fvalue)))
    p_val = float(np.squeeze(np.asarray(ft.pvalue)))
    df_num = int(getattr(ft, "df_num", k_z))
    df_den = int(getattr(ft, "df_denom", res.df_resid))

    return {
        "corr": corr,
        "first_stage_summary": res.summary(),
        "partial_R2": float(r2_reduced),
        "F_stat": F_val,
        "df_num": df_num,
        "df_den": df_den,
        "F_pvalue": p_val,
    }


def report_instrument_checks(
    df: pd.DataFrame,
    endog: str,
    instruments: List[str],
    exog: Optional[List[str]] = None,
    fe_cols: Optional[List[str]] = None,
    cluster: Optional[object] = None,
    cluster_kwargs: Optional[dict] = None,
) -> None:
    """Print a compact first-stage diagnostics report."""
    res = instrument_relevance_after_fe(
        df, endog, instruments, exog, fe_cols, cluster, cluster_kwargs
    )
    print("=== Correlation of residualized instruments with residualized endogenous (after FE) ===")
    print(res["corr"].sort_values(ascending=False).to_string(float_format=lambda x: f"{x: .4f}"))
    print("\n=== First-stage (after FE): y~ on [Z~, X~] ===")
    print(res["first_stage_summary"])
    print(f"\nPartial R^2(Z | X): {res['partial_R2']:.4f}")
    print(
        f"F-test (Z jointly = 0): F({res['df_num']}, {res['df_den']}) "
        f"= {res['F_stat']:.2f}, p = {res['F_pvalue']:.4g}"
    )


# --- Nested logit share computation ---

def _scalar(x, name: str = "value") -> float:
    a = np.asarray(x)
    if a.size != 1:
        raise ValueError(f"{name} must be a scalar (got shape {a.shape}).")
    return float(a.reshape(()))


def _check_align(delta: np.ndarray, prices, markets, nests):
    n = int(np.asarray(delta).size)
    if not (len(prices) == len(markets) == len(nests) == n):
        raise ValueError(
            f"delta, prices, markets, nests must be same length: "
            f"{n} vs {len(prices)}, {len(markets)}, {len(nests)}"
        )


def nl_shares_from_delta_single_level(
    delta: np.ndarray,
    market_ids: np.ndarray,
    nest_ids: np.ndarray,
    rho: float,
) -> Tuple[pd.DataFrame, pd.Series]:
    """
    Single-level nested-logit product shares given mean utility delta and rho.
    Returns (shares_df, s0_by_mkt).
    """
    rho = _scalar(rho, "rho")
    if not (0.0 <= rho < 1.0):
        raise ValueError("rho must be in [0,1).")

    df = pd.DataFrame({"market": market_ids, "nest": nest_ids, "delta": delta})
    out_frames = []
    s0_map = {}

    for mkt, gdf in df.groupby("market", sort=False):
        exp_scaled = np.exp(gdf["delta"].values / (1.0 - rho))
        _, inv = np.unique(gdf["nest"].values, return_inverse=True)
        Dg = np.bincount(inv, weights=exp_scaled)
        denom = 1.0 + np.sum(Dg ** (1.0 - rho))
        shares = exp_scaled * (Dg[inv] ** (-rho)) / denom

        out_frames.append(
            pd.DataFrame(
                {"market": gdf["market"].values, "nest": gdf["nest"].values, "share": shares},
                index=gdf.index,
            )
        )
        s0_map[mkt] = 1.0 - shares.sum()

    shares_df = pd.concat(out_frames).sort_index()
    s0_by_mkt = pd.Series(s0_map)
    return shares_df, s0_by_mkt


def percent_swap_when_bumping_nests(
    results,
    data: pd.DataFrame,
    price_col: str,
    market_col: str,
    nest_col: str,
    pct_increase: float = 0.10,
    alpha_value: Optional[float] = None,
    price_beta_index: Optional[int] = None,
    target_nest: Optional[Hashable] = None,
    target_nest_by_market: Optional[Dict[Hashable, Hashable]] = None,
    include_outside_in_swap: bool = False,
    return_tables: bool = False,
) -> Dict[str, Any]:
    """
    Compute percent of consumers who switch nests after raising prices.
    swap% is 0.5 * L1 between baseline and counterfactual nest distributions.
    """
    delta = np.asarray(results.delta).reshape(-1)
    rho = _scalar(getattr(results, "rho", np.nan), "rho")
    if not (0.0 <= rho < 1.0):
        raise ValueError("results.rho must be in [0,1).")

    if alpha_value is not None:
        alpha = _scalar(alpha_value, "alpha_value")
    else:
        if price_beta_index is None:
            raise ValueError(
                "Pass alpha_value (scalar price coefficient) or price_beta_index."
            )
        beta = np.asarray(results.beta).reshape(-1)
        alpha = float(beta[price_beta_index])

    prices = np.asarray(data[price_col], dtype=float)
    markets = np.asarray(data[market_col], dtype=object)
    nests = np.asarray(data[nest_col], dtype=object)
    _check_align(delta, prices, markets, nests)

    # Build bump mask
    n = len(delta)
    bump_mask = np.zeros(n, dtype=bool)
    if target_nest_by_market is not None:
        for mkt, tgt in target_nest_by_market.items():
            bump_mask |= (markets == mkt) & (nests == tgt)
    elif target_nest is not None:
        bump_mask = nests == target_nest
    else:
        bump_mask[:] = True

    if int(bump_mask.sum()) == 0:
        uniq_nests, counts = np.unique(nests, return_counts=True)
        hint = ", ".join(f"{repr(u)}:{c}" for u, c in list(zip(uniq_nests, counts))[:12])
        raise ValueError(
            f"No rows matched bump selection. target_nest={target_nest}, "
            f"target_nest_by_market={target_nest_by_market}. Sample nests: {hint}"
        )

    # Baseline shares
    shares0_df, s0 = nl_shares_from_delta_single_level(delta, markets, nests, rho)
    shares0_df = shares0_df.rename(columns={"market": market_col, "nest": nest_col})
    base_tab = (
        shares0_df.groupby([market_col, nest_col], sort=False)["share"]
        .sum()
        .unstack(nest_col)
        .fillna(0.0)
    )
    inside0 = base_tab.sum(axis=1)

    # Counterfactual
    delta_cf = delta.copy()
    delta_cf[bump_mask] += alpha * (pct_increase * prices[bump_mask])
    shares1_df, s0_cf = nl_shares_from_delta_single_level(delta_cf, markets, nests, rho)
    shares1_df = shares1_df.rename(columns={"market": market_col, "nest": nest_col})
    cf_tab = (
        shares1_df.groupby([market_col, nest_col], sort=False)["share"]
        .sum()
        .unstack(nest_col)
        .fillna(0.0)
    )
    inside1 = cf_tab.sum(axis=1)

    if include_outside_in_swap:
        base_with_out = base_tab.copy()
        cf_with_out = cf_tab.copy()
        base_with_out["__outside__"] = 1.0 - base_with_out.sum(axis=1)
        cf_with_out["__outside__"] = 1.0 - cf_with_out.sum(axis=1)
        base_dist = base_with_out.div(base_with_out.sum(axis=1), axis=0)
        cf_dist = cf_with_out.div(cf_with_out.sum(axis=1), axis=0)
    else:
        base_dist = base_tab.div(inside0.replace(0, np.nan), axis=0).fillna(0.0)
        cf_dist = cf_tab.div(inside1.replace(0, np.nan), axis=0).fillna(0.0)

    swap_pct = 0.5 * (base_dist - cf_dist).abs().sum(axis=1)

    out: Dict[str, Any] = {
        "by_market": pd.DataFrame(
            {
                market_col: base_tab.index,
                "inside_share_baseline": inside0.values,
                "inside_share_cf": inside1.values,
                "delta_outside": (1.0 - inside1.values) - (1.0 - inside0.values),
                "swap_pct": swap_pct.values,
            }
        ).reset_index(drop=True),
        "overall_simple_mean": float(swap_pct.mean()),
        "overall_inside_weighted": float(
            np.average(swap_pct.values, weights=np.clip(inside0.values, 0, None))
        ),
    }
    if return_tables:
        out["base_tab"] = base_tab
        out["cf_tab"] = cf_tab
    return out


def diversion_ratios_for_target_nest(
    results,
    data: pd.DataFrame,
    price_col: str,
    market_col: str,
    nest_col: str,
    target_nest: Hashable,
    pct_increase: float,
    alpha_value: float,
) -> Dict[str, Any]:
    """
    Bump ONE nest by pct_increase everywhere.
    Returns per-market diversion ratios from the target nest to other nests and outside.
    """
    res = percent_swap_when_bumping_nests(
        results,
        data,
        price_col,
        market_col,
        nest_col,
        pct_increase=pct_increase,
        alpha_value=alpha_value,
        target_nest=target_nest,
        include_outside_in_swap=False,
        return_tables=True,
    )
    base_tab = res["base_tab"].copy()
    cf_tab = res["cf_tab"].copy()

    all_nests = base_tab.columns.union(cf_tab.columns)
    base_tab = base_tab.reindex(columns=all_nests, fill_value=0.0)
    cf_tab = cf_tab.reindex(columns=all_nests, fill_value=0.0)

    delta_tab = cf_tab - base_tab
    loss = (base_tab[target_nest] - cf_tab[target_nest]).clip(lower=0.0)

    div_to_nests = pd.DataFrame(index=base_tab.index)
    for k in all_nests:
        if k == target_nest:
            continue
        div_to_nests[k] = (delta_tab[k] / loss.replace(0, np.nan)).fillna(0.0).clip(lower=0.0)

    outside0 = 1.0 - base_tab.sum(axis=1)
    outside1 = 1.0 - cf_tab.sum(axis=1)
    d_outside = (outside1 - outside0).clip(lower=0.0)
    div_to_outside = (d_outside / loss.replace(0, np.nan)).fillna(0.0).clip(lower=0.0)

    rowsum = div_to_nests.sum(axis=1) + div_to_outside
    return {
        "diversion_to_nests": div_to_nests,
        "diversion_to_outside": div_to_outside,
        "rowsum_check": rowsum,
        "base_tab": base_tab,
        "cf_tab": cf_tab,
    }


# ============================================================
# Pipeline functions
# ============================================================

def load_and_prepare_data(cfg: dict, est_cfg: EstimationConfig) -> pd.DataFrame:
    """Load bldg_panel_blp from config, construct market/firm/product IDs, nesting."""
    path = p_product(cfg, "bldg_panel_blp")
    print(f"[load] Reading: {path}")
    df = pd.read_csv(path, low_memory=False)
    print(f"[load] Raw rows: {len(df):,}")

    # Column compatibility: rename legacy units column if present
    if est_cfg.units_col not in df.columns and est_cfg.units_col_legacy in df.columns:
        print(f"[load] Renaming legacy column {est_cfg.units_col_legacy} -> {est_cfg.units_col}")
        df = df.rename(columns={est_cfg.units_col_legacy: est_cfg.units_col})

    # Also handle num_units_imp as a working alias if total_units not present
    units_col = est_cfg.units_col if est_cfg.units_col in df.columns else est_cfg.units_col_legacy

    # Market IDs
    df["market_ids"] = df[list(est_cfg.market_cols)].astype(str).agg(est_cfg.market_sep.join, axis=1)
    df["firm_ids"] = df[est_cfg.firm_col]
    df["product_ids"] = df[est_cfg.product_col]

    # Prices and shares
    df["prices"] = df["log_med_rent"]

    # Use configured shares column; compute on-the-fly if missing from data
    if est_cfg.shares_col in df.columns:
        df["shares"] = df[est_cfg.shares_col]
    elif est_cfg.shares_col == "share_zip" and "occupied_units" in df.columns:
        # Compute share_zip = occupied_units / sum(total_units) by (zip, year)
        print(f"[load] share_zip not in data — computing on-the-fly from occupied_units / total_units_market")
        zip_col = est_cfg.market_cols[0] if len(est_cfg.market_cols) > 0 else "pm.zip"
        grp = [zip_col, "year"]
        df["total_units_market"] = df.groupby(grp)[units_col].transform("sum")
        df["shares"] = df["occupied_units"] / df["total_units_market"]
    elif "share_units_zip_unit" in df.columns:
        print(f"[load] WARNING: {est_cfg.shares_col} not found, falling back to share_units_zip_unit")
        df["shares"] = df["share_units_zip_unit"]
    else:
        raise KeyError(f"Neither {est_cfg.shares_col} nor share_units_zip_unit found in data")

    # Nesting
    if "filing_rate_raw" in df.columns:
        df["filing_rate_raw"] = df["filing_rate_raw"].fillna(0)
    threshold_vals = df[est_cfg.nest_threshold_col].fillna(0)
    df["nesting_ids"] = np.where(
        threshold_vals >= est_cfg.nest_threshold,
        est_cfg.nest_labels[0],
        est_cfg.nest_labels[1],
    )

    return df


def build_estimation_sample(df: pd.DataFrame, est_cfg: EstimationConfig) -> pd.DataFrame:
    """Apply all sample restrictions with logging at each step."""
    units_col = est_cfg.units_col if est_cfg.units_col in df.columns else est_cfg.units_col_legacy

    n0 = len(df)
    print(f"[sample] Starting rows: {n0:,}")

    # Year range
    df = df[df["year"].between(*est_cfg.year_range)].copy()
    print(f"[sample] After year range {est_cfg.year_range}: {len(df):,} (dropped {n0 - len(df):,})")

    # Min units
    n = len(df)
    df = df[df[units_col] > est_cfg.min_units]
    print(f"[sample] After {units_col} > {est_cfg.min_units}: {len(df):,} (dropped {n - len(df):,})")

    # Max units
    n = len(df)
    df = df[df[units_col] <= est_cfg.max_units]
    print(f"[sample] After {units_col} <= {est_cfg.max_units}: {len(df):,} (dropped {n - len(df):,})")

    # Min units in zip
    if "num_units_zip" in df.columns:
        n = len(df)
        df = df[df["num_units_zip"] >= est_cfg.min_units_zip]
        print(f"[sample] After num_units_zip >= {est_cfg.min_units_zip}: {len(df):,} (dropped {n - len(df):,})")

    # Drop missing prices/shares
    n = len(df)
    df = df.dropna(subset=["prices", "shares"])
    print(f"[sample] After dropping missing prices/shares: {len(df):,} (dropped {n - len(df):,})")

    # Share sanity: drop rows with individual share >= 1
    n = len(df)
    df = df[df["shares"] < 1.0]
    print(f"[sample] After individual share < 1 filter: {len(df):,} (dropped {n - len(df):,})")

    # Drop markets where inside shares sum to >= 1 (no outside good)
    df["market_shares_sum"] = df.groupby("market_ids")["shares"].transform("sum")
    n = len(df)
    df = df[df["market_shares_sum"] < 1.0]
    print(f"[sample] After market share sum < 1 filter: {len(df):,} (dropped {n - len(df):,})")

    # Log share distribution for diagnostics
    remaining_sums = df.groupby("market_ids")["shares"].sum()
    print(f"[sample] Inside share sums: mean={remaining_sums.mean():.3f}, "
          f"median={remaining_sums.median():.3f}, max={remaining_sums.max():.3f}")

    # Building type filter — prefer the descriptive column over coded column
    bldg_col = None
    for candidate in ["building_code_description_new_fixed", "building_code_description_new", "building_type"]:
        if candidate in df.columns:
            bldg_col = candidate
            break
    if bldg_col is not None:
        n = len(df)
        df = df[
            df[bldg_col].str.contains(
                est_cfg.building_type_regex, na=False, case=False
            )
        ]
        print(f"[sample] After building type regex on {bldg_col}: {len(df):,} (dropped {n - len(df):,})")

    # Log market value > 0
    if est_cfg.require_log_market_value_positive and "market_value" in df.columns:
        df["log_market_value"] = np.log(df["market_value"].replace(0, np.nan))
        n = len(df)
        df = df[df["log_market_value"] > 0]
        print(f"[sample] After log_market_value > 0: {len(df):,} (dropped {n - len(df):,})")

    # Max absolute change in log taxable value
    if "change_log_taxable_value" in df.columns:
        df["max_abs_change_log_taxable_value"] = df.groupby("product_ids")[
            "change_log_taxable_value"
        ].transform(lambda x: x.abs().max())
        n = len(df)
        df = df[df["max_abs_change_log_taxable_value"] < est_cfg.max_abs_change_log_taxable_value]
        print(
            f"[sample] After max abs change log taxable value < "
            f"{est_cfg.max_abs_change_log_taxable_value}: {len(df):,} (dropped {n - len(df):,})"
        )

    # Annualized change in log median rent
    df = df.sort_values(by=["product_ids", "year"])
    df["annualized_change_log_med_rent"] = (
        df.groupby("product_ids")["log_med_rent"].diff().fillna(0)
        / df.groupby("product_ids")["year"].diff().fillna(1)
    ).replace([np.inf, -np.inf], np.nan)
    n = len(df)
    df = df[df["annualized_change_log_med_rent"].abs() <= est_cfg.max_annualized_change_log_rent]
    print(
        f"[sample] After annualized rent change <= "
        f"{est_cfg.max_annualized_change_log_rent}: {len(df):,} (dropped {n - len(df):,})"
    )

    # Obs-per-PID filter
    df["num_obs_per_pid"] = df.groupby("product_ids")["year"].transform("size")
    n = len(df)
    df = df[df["num_obs_per_pid"] >= est_cfg.min_obs_per_pid].copy()
    print(f"[sample] After min obs/PID >= {est_cfg.min_obs_per_pid}: {len(df):,} (dropped {n - len(df):,})")

    # Derived columns needed for estimation
    if "total_area" in df.columns:
        df["log_total_area"] = np.log(df["total_area"].replace(0, np.nan))
        df["log_total_area_sq"] = df["log_total_area"] ** 2

    if "market_value" in df.columns and units_col in df.columns:
        df["log_market_value_per_unit"] = np.log(
            (df["market_value"] / df[units_col]).replace(0, np.nan)
        )

    if "quality_grade" in df.columns:
        df["quality_grade_fixed_coarse"] = df["quality_grade"].str.replace(
            r"[+-]", "", regex=True
        )

    df[f"{units_col}_sq"] = df[units_col] ** 2
    df["num_units_cuts"] = pd.cut(
        df[units_col],
        bins=[0, 5, 10, 20, 50, 100, 5000],
        labels=["1-5", "6-10", "11-20", "21-50", "51-100", "101+"],
    )

    # Drop rows with NaN in formulation / absorb columns (pyblp/patsy will error otherwise)
    import re
    formula_vars = [
        t.strip() for t in est_cfg.x1_formula.replace("0 +", "").replace("0+", "").split("+")
    ]
    formula_vars = [v for v in formula_vars if v in df.columns]

    # Extract bare column names from absorb formula like "C(GEOID)*C(year) + C(foo)"
    absorb_vars = re.findall(r"C\((\w+)\)", est_cfg.absorb_formula)
    absorb_vars = [v for v in absorb_vars if v in df.columns]

    all_required = list(set(formula_vars + absorb_vars))
    n = len(df)
    df = df.dropna(subset=all_required)
    if len(df) < n:
        print(f"[sample] After dropping NaN in formulation/absorb vars: {len(df):,} (dropped {n - len(df):,})")

    print(f"[sample] Final estimation sample: {len(df):,} rows, "
          f"{df['market_ids'].nunique():,} markets, {df['product_ids'].nunique():,} products")
    return df


def build_instruments(df: pd.DataFrame, est_cfg: EstimationConfig) -> pd.DataFrame:
    """
    Construct demand_instruments{k} columns.

    If est_cfg.excluded_instruments is non-empty, those columns are used as
    excluded instruments for prices (IV estimation). Otherwise falls back to
    demand_instruments0 = prices (no IV, just-identified).
    """
    # Drop any pre-existing demand instrument columns
    old_inst = [c for c in df.columns if c.startswith("demand_instruments")]
    df = df.drop(columns=old_inst)

    idx = 0
    if est_cfg.excluded_instruments:
        # IV mode: use specified columns as excluded instruments
        for inst_col in est_cfg.excluded_instruments:
            if inst_col not in df.columns:
                print(f"[instruments] WARNING: {inst_col} not in data, skipping")
                continue
            df[f"demand_instruments{idx}"] = df[inst_col]
            print(f"[instruments] demand_instruments{idx} <- {inst_col} "
                  f"(non-NA: {df[inst_col].notna().sum():,}, "
                  f"nonzero: {(df[inst_col] != 0).sum():,})")
            idx += 1
    else:
        # No IV: just-identified with prices as own instrument
        df[f"demand_instruments{idx}"] = df["prices"]
        print(f"[instruments] demand_instruments{idx} <- prices (no IV)")
        idx += 1

    # Nest sizes instrument
    if est_cfg.include_nest_size_instrument:
        nest_sizes = df.groupby(["market_ids", "nesting_ids"])["product_ids"].transform("size")
        df[f"demand_instruments{idx}"] = nest_sizes
        print(f"[instruments] demand_instruments{idx} <- nest_sizes")
        idx += 1

    # Clean up infinities and drop NAs in instrument columns
    df = df.replace([np.inf, -np.inf], np.nan)
    inst_cols = [c for c in df.columns if c.startswith("demand_instruments")]
    n = len(df)
    df = df.dropna(subset=inst_cols)
    print(f"[instruments] After cleaning instrument NAs: {len(df):,} (dropped {n - len(df):,})")
    print(f"[instruments] Total excluded instruments: {len(inst_cols)}")

    return df


def run_first_stage_diagnostics(
    df: pd.DataFrame,
    est_cfg: EstimationConfig,
) -> Optional[Dict[str, Any]]:
    """
    Run first-stage diagnostics for the IV specification.

    Residualizes prices and instruments against the absorbed FEs, then reports:
    - Pairwise correlations of residualized instruments with residualized prices
    - First-stage F-statistic (Staiger-Stock rule of thumb: F > 10)
    - Partial R^2 of instruments

    Returns None if no excluded instruments are configured.
    """
    if not est_cfg.excluded_instruments:
        print("[first-stage] No excluded instruments — skipping diagnostics")
        return None

    import re

    inst_cols = [c for c in df.columns if c.startswith("demand_instruments")]

    # Extract FE columns from absorb formula
    fe_cols = re.findall(r"C\((\w+)\)", est_cfg.absorb_formula)
    fe_cols = [c for c in fe_cols if c in df.columns]

    # For interaction terms like C(GEOID)*C(year), create a combined FE column
    # since the residualizer works with single-column groupings
    absorb_str = est_cfg.absorb_formula
    interaction_pattern = re.findall(r"C\((\w+)\)\*C\((\w+)\)", absorb_str)
    combined_fe_cols = []
    for c1, c2 in interaction_pattern:
        if c1 in df.columns and c2 in df.columns:
            combo_name = f"_fe_{c1}_{c2}"
            df[combo_name] = df[c1].astype(str) + "_" + df[c2].astype(str)
            combined_fe_cols.append(combo_name)
    # Also add any non-interacted C() terms
    simple_fe = re.findall(r"(?<!\*)C\((\w+)\)(?!\*)", absorb_str)
    for c in simple_fe:
        if c in df.columns and c not in [x for pair in interaction_pattern for x in pair]:
            combined_fe_cols.append(c)

    # If PID is in the FE set, add it
    if "PID" in fe_cols and "PID" not in combined_fe_cols:
        combined_fe_cols.append("PID")

    print(f"[first-stage] Residualizing against FEs: {combined_fe_cols}")
    print(f"[first-stage] Instruments: {inst_cols}")
    print(f"[first-stage] Endogenous variable: prices")

    # Use the existing residualizer
    try:
        result = instrument_relevance_after_fe(
            df=df,
            endog="prices",
            instruments=inst_cols,
            fe_cols=combined_fe_cols,
        )

        print("\n" + "=" * 60)
        print("  FIRST-STAGE DIAGNOSTICS")
        print("=" * 60)

        print("\n  Correlations (residualized instruments vs residualized prices):")
        for inst, corr_val in result["corr"].sort_values(ascending=False).items():
            print(f"    {inst}: {corr_val: .4f}")

        print(f"\n  Partial R^2 (instruments | FE): {result['partial_R2']:.4f}")
        print(f"  F-statistic: F({result['df_num']}, {result['df_den']}) = {result['F_stat']:.2f}")
        print(f"  F p-value: {result['F_pvalue']:.4g}")

        if result["F_stat"] < 10:
            print("\n  *** WARNING: F < 10 — instruments may be WEAK ***")
            print("  Staiger-Stock rule of thumb: F > 10 for reliable IV inference")
        elif result["F_stat"] < 20:
            print("\n  CAUTION: 10 < F < 20 — instruments are marginal")
        else:
            print(f"\n  F = {result['F_stat']:.1f} > 10 — instruments pass Staiger-Stock threshold")

        print("=" * 60 + "\n")

        # Clean up temp FE columns
        for c in combined_fe_cols:
            if c.startswith("_fe_"):
                df.drop(columns=[c], inplace=True, errors="ignore")

        return result

    except Exception as e:
        print(f"[first-stage] Error in diagnostics: {e}")
        # Clean up temp FE columns
        for c in combined_fe_cols:
            if c.startswith("_fe_"):
                df.drop(columns=[c], inplace=True, errors="ignore")
        return None


def define_formulation(est_cfg: EstimationConfig) -> blp.Formulation:
    """Create pyblp.Formulation from config."""
    return blp.Formulation(est_cfg.x1_formula, absorb=est_cfg.absorb_formula)


def estimate_model(
    X1: blp.Formulation,
    df: pd.DataFrame,
    est_cfg: EstimationConfig,
) -> Tuple[blp.Problem, Any]:
    """Set up Problem and solve. Returns (problem, results)."""
    problem = blp.Problem(X1, df)
    print(f"[estimate] Problem dimensions: {problem}")

    results = problem.solve(rho=est_cfg.rho_init, **est_cfg.solve_kwargs)
    print(results)
    return problem, results


def compute_post_estimation(
    results,
    problem,
    df: pd.DataFrame,
    est_cfg: EstimationConfig,
) -> Dict[str, Any]:
    """Elasticities, diversions, nest swaps."""
    post: Dict[str, Any] = {}

    # Elasticities
    elasticities = results.compute_elasticities()
    post["elasticities"] = elasticities

    # Own-price elasticities (diagonal of each market's JxJ matrix)
    own_elasticities = np.array([])
    for market in df["market_ids"].unique():
        market_mask = df["market_ids"] == market
        market_el = elasticities[market_mask.values]
        own_elasticities = np.concatenate([own_elasticities, np.diag(market_el)])
    post["own_elasticities"] = own_elasticities

    # Diversion ratios
    alpha_hat = float(results.beta[0])
    post["alpha"] = alpha_hat

    diversions = results.compute_diversion_ratios()
    post["diversions"] = diversions

    # Swap metrics: bump target nest
    try:
        swap_target = percent_swap_when_bumping_nests(
            results,
            df,
            "prices",
            "market_ids",
            "nesting_ids",
            pct_increase=est_cfg.diversion_pct_increase,
            alpha_value=alpha_hat,
            target_nest=est_cfg.diversion_target_nest,
            include_outside_in_swap=False,
        )
        post["swap_target"] = swap_target
    except ValueError as e:
        print(f"[post] Warning: swap for target nest failed: {e}")

    # Swap metrics: bump all nests
    try:
        swap_all = percent_swap_when_bumping_nests(
            results,
            df,
            "prices",
            "market_ids",
            "nesting_ids",
            pct_increase=est_cfg.diversion_pct_increase,
            alpha_value=alpha_hat,
            include_outside_in_swap=True,
        )
        post["swap_all"] = swap_all
    except ValueError as e:
        print(f"[post] Warning: swap all failed: {e}")

    # Diversion ratios from target nest
    try:
        div_out = diversion_ratios_for_target_nest(
            results,
            df,
            "prices",
            "market_ids",
            "nesting_ids",
            target_nest=est_cfg.diversion_target_nest,
            pct_increase=est_cfg.diversion_pct_increase,
            alpha_value=alpha_hat,
        )
        post["diversion_from_target"] = div_out
    except ValueError as e:
        print(f"[post] Warning: diversion from target nest failed: {e}")

    return post


# ============================================================
# LaTeX export
# ============================================================

def _write_latex_table(
    df: pd.DataFrame, path: Path, caption: str, label: str, index: bool = False
):
    tex = df.to_latex(escape=False, index=index, float_format=lambda x: f"{x:,.4f}")
    parts = [
        "\\begin{table}[!htbp]\n\\centering\n",
        f"\\caption{{{caption}}}\n",
        f"\\label{{{label}}}\n",
        tex,
        "\n\\end{table}\n",
    ]
    path.write_text("".join(parts), encoding="utf-8")
    print(f"[export] Wrote {path}")


def export_tables(
    results,
    problem,
    df: pd.DataFrame,
    post: Dict[str, Any],
    est_cfg: EstimationConfig,
    cfg: dict,
) -> Path:
    """Write LaTeX summary tables to output/pyblp_summaries/."""
    out_dir = Path(p_output(cfg, est_cfg.output_subdir))
    out_dir.mkdir(parents=True, exist_ok=True)

    # --- 1) Coefficient table ---
    coef = np.asarray(results.beta).reshape(-1)
    se_beta = np.asarray(results.beta_se).reshape(-1)
    tvals = coef / se_beta
    pvals = 2.0 * (1.0 - norm.cdf(np.abs(tvals)))

    # Try to parse variable names from formulation
    x1_terms = [t.strip() for t in est_cfg.x1_formula.replace("0 +", "").split("+")]
    if len(x1_terms) != len(coef):
        x1_terms = [f"x{i+1}" for i in range(len(coef))]

    coef_tab = pd.DataFrame(
        {
            "Variable": x1_terms,
            "Estimate": coef,
            "Std. Error": se_beta,
            "t-stat": tvals,
            "p-value": pvals,
        }
    )

    n_obs = df.shape[0]
    n_mkts = df["market_ids"].nunique()
    n_prods = df["product_ids"].nunique()
    rho_val = float(np.asarray(getattr(results, "rho", float("nan"))).ravel()[0])

    _write_latex_table(
        coef_tab,
        out_dir / "logit_coefficients.tex",
        caption=(
            f"Simple Logit Estimates (N={n_obs}, Markets={n_mkts}, "
            f"Products={n_prods}; $\\rho={rho_val:.3f}$)"
        ),
        label="tab:blp_logit_coef",
    )

    # --- 2) Own-price elasticities summary ---
    own_el = post.get("own_elasticities")
    if own_el is not None:
        el_df = pd.DataFrame({"own_elasticity": own_el.ravel()}, index=df.index)
        if "num_units_cuts" in df.columns:
            by_bin = (
                el_df.join(df[["num_units_cuts"]])
                .dropna(subset=["num_units_cuts"])
                .groupby("num_units_cuts")["own_elasticity"]
                .describe()[["count", "mean", "std", "min", "25%", "50%", "75%", "max"]]
            )
            _write_latex_table(
                by_bin.reset_index(),
                out_dir / "own_elasticities_summary.tex",
                caption="Own-Price Elasticities by Unit Size Bin",
                label="tab:own_elasticities",
            )

    # --- 3) Diversion ratio tables ---
    div_out = post.get("diversion_from_target")
    if div_out is not None:
        target = est_cfg.diversion_target_nest
        pct = est_cfg.diversion_pct_increase

        # To nests
        div_nests = div_out["diversion_to_nests"].reindex(
            sorted(div_out["diversion_to_nests"].columns), axis=1
        )
        div_nests_desc = div_nests.describe().T.reset_index().rename(columns={"index": "To nest"})
        _write_latex_table(
            div_nests_desc,
            out_dir / "diversion_to_nests_summary.tex",
            caption=(
                f"Diversion Ratios from Nest \\texttt{{{target}}} to Other Nests "
                f"({int(pct*100)}\\% price bump)"
            ),
            label="tab:diversion_nests",
        )

        # To outside
        div_outside = div_out["diversion_to_outside"].rename("diversion_to_outside")
        div_outside_desc = (
            div_outside.describe()[["count", "mean", "std", "min", "25%", "50%", "75%", "max"]]
            .to_frame()
            .T
        )
        div_outside_desc.insert(0, "Metric", ["Outside good"])
        _write_latex_table(
            div_outside_desc,
            out_dir / "diversion_to_outside_summary.tex",
            caption=(
                f"Diversion from Nest \\texttt{{{target}}} to Outside "
                f"({int(pct*100)}\\% price bump)"
            ),
            label="tab:diversion_outside",
        )

        # Top markets by diversion to outside
        top_out = (
            pd.DataFrame(
                {
                    "market_id": div_out["diversion_to_outside"].index,
                    "diversion_to_outside": div_out["diversion_to_outside"].values,
                }
            )
            .sort_values("diversion_to_outside", ascending=False)
            .head(20)
        )
        _write_latex_table(
            top_out,
            out_dir / "diversion_top_markets_outside.tex",
            caption=(
                f"Top 20 Markets by Diversion to Outside "
                f"(Target \\texttt{{{target}}}, +{int(pct*100)}\\% price)"
            ),
            label="tab:diversion_top_outside",
        )

    print(f"\n=== SUMMARY FILES WRITTEN to {out_dir} ===")
    for p in sorted(out_dir.glob("*.tex")):
        print(f"  - {p}")

    return out_dir


# ============================================================
# Console summary
# ============================================================

def print_summary(results, df: pd.DataFrame, post: Dict[str, Any], est_cfg: EstimationConfig):
    """Print a concise console summary of estimation results."""
    sep = "=" * 70

    # --- Header ---
    n_obs = df.shape[0]
    n_mkts = df["market_ids"].nunique()
    n_prods = df["product_ids"].nunique()
    n_firms = df["firm_ids"].nunique() if "firm_ids" in df.columns else "?"
    rho_val = float(np.asarray(getattr(results, "rho", float("nan"))).ravel()[0])
    converged = getattr(results, "converged", None)

    print(f"\n{sep}")
    print("  NESTED LOGIT ESTIMATION SUMMARY")
    print(sep)
    print(f"  Sample:  {n_obs:,} obs | {n_mkts:,} markets | {n_prods:,} products | {n_firms:,} firms")
    print(f"  Years:   {est_cfg.year_range[0]}–{est_cfg.year_range[1]}")
    print(f"  Units:   {est_cfg.min_units} < units <= {est_cfg.max_units}")
    print(f"  Nesting: '{est_cfg.nest_labels[0]}' if {est_cfg.nest_threshold_col} >= {est_cfg.nest_threshold}, else '{est_cfg.nest_labels[1]}'")
    if converged is not None:
        print(f"  Converged: {converged}")
    print()

    # --- Coefficients ---
    coef = np.asarray(results.beta).reshape(-1)
    se_beta = np.asarray(results.beta_se).reshape(-1)
    tvals = coef / se_beta
    pvals = 2.0 * (1.0 - norm.cdf(np.abs(tvals)))

    x1_terms = [t.strip() for t in est_cfg.x1_formula.replace("0 +", "").split("+")]
    if len(x1_terms) != len(coef):
        x1_terms = [f"x{i+1}" for i in range(len(coef))]

    print("  COEFFICIENTS (beta)")
    print(f"  {'Variable':<30s} {'Estimate':>10s} {'Std.Err':>10s} {'t-stat':>10s} {'p-val':>10s}")
    print("  " + "-" * 72)
    for i in range(len(coef)):
        sig = ""
        if pvals[i] < 0.001:
            sig = "***"
        elif pvals[i] < 0.01:
            sig = "**"
        elif pvals[i] < 0.05:
            sig = "*"
        elif pvals[i] < 0.1:
            sig = "."
        print(f"  {x1_terms[i]:<30s} {coef[i]:>10.4f} {se_beta[i]:>10.4f} {tvals[i]:>10.2f} {pvals[i]:>9.4f} {sig}")
    print(f"\n  rho (nesting param):  {rho_val:.4f}")
    rho_se = np.asarray(getattr(results, "rho_se", float("nan"))).ravel()
    if rho_se.size > 0 and not np.isnan(rho_se[0]):
        print(f"  rho SE:               {float(rho_se[0]):.4f}")
    print()

    # --- Own-price elasticities ---
    own_el = post.get("own_elasticities")
    if own_el is not None and len(own_el) > 0:
        el_s = pd.Series(own_el.ravel())
        print("  OWN-PRICE ELASTICITIES")
        print(f"  {'mean':>8s} {'std':>8s} {'min':>8s} {'p25':>8s} {'p50':>8s} {'p75':>8s} {'max':>8s}")
        print(f"  {el_s.mean():>8.3f} {el_s.std():>8.3f} {el_s.min():>8.3f} "
              f"{el_s.quantile(0.25):>8.3f} {el_s.quantile(0.5):>8.3f} "
              f"{el_s.quantile(0.75):>8.3f} {el_s.max():>8.3f}")

        # By unit-size bin
        if "num_units_cuts" in df.columns:
            el_df = pd.DataFrame({"own_el": own_el.ravel()}, index=df.index)
            by_bin = (
                el_df.join(df[["num_units_cuts"]])
                .dropna(subset=["num_units_cuts"])
                .groupby("num_units_cuts")["own_el"]
                .agg(["count", "mean", "std", "median"])
            )
            print(f"\n  By unit-size bin:")
            print(f"  {'Bin':<10s} {'N':>6s} {'mean':>8s} {'std':>8s} {'median':>8s}")
            print("  " + "-" * 42)
            for bin_label, row in by_bin.iterrows():
                print(f"  {str(bin_label):<10s} {int(row['count']):>6d} {row['mean']:>8.3f} {row['std']:>8.3f} {row['median']:>8.3f}")
        print()

    # --- Diversion ratios ---
    div_out = post.get("diversion_from_target")
    if div_out is not None:
        target = est_cfg.diversion_target_nest
        pct = est_cfg.diversion_pct_increase
        print(f"  DIVERSION RATIOS (from nest '{target}', +{int(pct*100)}% price bump)")

        div_nests = div_out["diversion_to_nests"]
        div_outside = div_out["diversion_to_outside"]
        rowsum = div_out["rowsum_check"]

        print(f"\n  To other nests:")
        for col in sorted(div_nests.columns):
            s = div_nests[col]
            print(f"    -> '{col}':  mean={s.mean():.4f}  std={s.std():.4f}  "
                  f"median={s.median():.4f}  [min={s.min():.4f}, max={s.max():.4f}]")

        print(f"\n  To outside good:")
        print(f"    mean={div_outside.mean():.4f}  std={div_outside.std():.4f}  "
              f"median={div_outside.median():.4f}  [min={div_outside.min():.4f}, max={div_outside.max():.4f}]")

        print(f"\n  Row-sum check (should be ~1.0):")
        print(f"    mean={rowsum.mean():.4f}  min={rowsum.min():.4f}  max={rowsum.max():.4f}")
        print()

    # --- Swap metrics ---
    swap_target = post.get("swap_target")
    if swap_target is not None:
        print(f"  NEST SWAP % (target nest '{est_cfg.diversion_target_nest}', "
              f"+{int(est_cfg.diversion_pct_increase*100)}% bump)")
        print(f"    Simple mean swap:          {swap_target['overall_simple_mean']:.4f}")
        print(f"    Inside-share weighted swap: {swap_target['overall_inside_weighted']:.4f}")

    swap_all = post.get("swap_all")
    if swap_all is not None:
        print(f"\n  NEST SWAP % (ALL nests bumped, +{int(est_cfg.diversion_pct_increase*100)}%, incl. outside)")
        print(f"    Simple mean swap:          {swap_all['overall_simple_mean']:.4f}")
        print(f"    Inside-share weighted swap: {swap_all['overall_inside_weighted']:.4f}")

    print(f"\n{sep}\n")


# ============================================================
# main() — orchestrator
# ============================================================

def main(
    config_path: Optional[str] = None,
    est_cfg: Optional[EstimationConfig] = None,
) -> Dict[str, Any]:
    """
    Run the full estimation pipeline. Returns all objects for interactive use.

    Returns dict with keys:
        cfg, est_cfg, raw_df, est_df, X1, problem, results, post_est, out_dir
    """
    cfg = load_config(config_path)
    if est_cfg is None:
        est_cfg = EstimationConfig()

    print(f"[main] Config: {cfg['_config_path']}")
    print(f"[main] EstimationConfig: year_range={est_cfg.year_range}, "
          f"min_units={est_cfg.min_units}, rho_init={est_cfg.rho_init}")
    if est_cfg.excluded_instruments:
        print(f"[main] IV mode: instruments={est_cfg.excluded_instruments}")
    else:
        print("[main] No IV (just-identified logit)")

    # 1) Load and prepare
    raw_df = load_and_prepare_data(cfg, est_cfg)

    # 2) Build estimation sample
    est_df = build_estimation_sample(raw_df, est_cfg)

    # 3) Build instruments
    est_df = build_instruments(est_df, est_cfg)

    # 4) First-stage diagnostics (if IV)
    first_stage = run_first_stage_diagnostics(est_df, est_cfg)

    # 5) Define formulation and estimate
    X1 = define_formulation(est_cfg)
    problem, results = estimate_model(X1, est_df, est_cfg)

    # 6) Post-estimation
    post_est = compute_post_estimation(results, problem, est_df, est_cfg)

    # 7) Console summary
    print_summary(results, est_df, post_est, est_cfg)

    # 8) Export tables
    out_dir = export_tables(results, problem, est_df, post_est, est_cfg, cfg)

    return {
        "cfg": cfg,
        "est_cfg": est_cfg,
        "raw_df": raw_df,
        "est_df": est_df,
        "X1": X1,
        "problem": problem,
        "results": results,
        "post_est": post_est,
        "first_stage": first_stage,
        "out_dir": out_dir,
    }


# ============================================================
# Pre-built estimation configurations
# ============================================================

def iv_config(**overrides) -> EstimationConfig:
    """
    IV nested logit with property FE + GEOID*year (full Calder-Wang spec).

    WARNING: First-stage F ≈ 0.03 — instruments are dead under this FE structure.
    The tax-value instruments capture cross-sectional and geography-by-time variation
    but have zero correlation with within-PID, within-GEOID*year residual rents.

    Use iv_config_pid_year() for a feasible specification (F ≈ 6-12).
    """
    defaults = dict(
        x1_formula="0 + prices + log_market_value + log_total_area",
        absorb_formula="C(PID) + C(GEOID)*C(year)",
        excluded_instruments=(
            "change_log_taxable_value",
            "z_sum_otherfirm_change_taxable_value",
            "z_sum_otherfirm_change_taxable_value_per_unit",
        ),
        include_nest_size_instrument=True,
        min_obs_per_pid=2,
        output_subdir="pyblp_summaries_iv",
    )
    defaults.update(overrides)
    return EstimationConfig(**defaults)


def iv_config_pid_year(**overrides) -> EstimationConfig:
    """
    IV nested logit with PID FE + year FE (additive, no GEOID interaction).

    First-stage F ≈ 6-12. Trades off GEOID*year controls for instrument power.
    PID absorbs time-invariant building quality; year absorbs aggregate trends.
    Identification: within-building over-time variation not explained by year shocks.
    """
    defaults = dict(
        x1_formula="0 + prices + log_market_value + log_total_area",
        absorb_formula="C(PID) + C(year)",
        excluded_instruments=(
            "change_log_taxable_value",
            "z_sum_otherfirm_change_taxable_value",
            "z_sum_otherfirm_change_taxable_value_per_unit",
        ),
        include_nest_size_instrument=True,
        min_obs_per_pid=2,
        output_subdir="pyblp_summaries_iv_pid_year",
    )
    defaults.update(overrides)
    return EstimationConfig(**defaults)


def iv_config_pid(**overrides) -> EstimationConfig:
    """
    IV nested logit with PID FE only (strongest first-stage, F ≈ 12).

    No time or geography*time controls — relies entirely on building FE
    to absorb unobserved quality. Suitable as a robustness check.
    """
    defaults = dict(
        x1_formula="0 + prices + log_market_value + log_total_area",
        absorb_formula="C(PID)",
        excluded_instruments=(
            "change_log_taxable_value",
            "z_sum_otherfirm_change_taxable_value",
            "z_sum_otherfirm_change_taxable_value_per_unit",
        ),
        include_nest_size_instrument=True,
        min_obs_per_pid=2,
        output_subdir="pyblp_summaries_iv_pid",
    )
    defaults.update(overrides)
    return EstimationConfig(**defaults)


IV_CONFIGS = {
    "full": iv_config,           # PID + GEOID*year (weak instruments)
    "pid_year": iv_config_pid_year,  # PID + year (feasible, F~6-12)
    "pid": iv_config_pid,        # PID only (strongest first stage)
}


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="PyBLP nested logit estimation")
    parser.add_argument(
        "--iv", nargs="?", const="pid_year", default=None,
        choices=list(IV_CONFIGS.keys()),
        help="IV specification: 'pid_year' (default, feasible), 'pid' (strongest), 'full' (Calder-Wang, weak)",
    )
    parser.add_argument("--config", type=str, default=None, help="Path to config.yml")
    args = parser.parse_args()

    if args.iv:
        est_cfg = IV_CONFIGS[args.iv]()
    else:
        est_cfg = EstimationConfig()

    main(config_path=args.config, est_cfg=est_cfg)
