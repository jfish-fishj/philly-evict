# %% [imports]
import numpy as np
import pandas as pd
import pyblp as blp
import matplotlib.pyplot as plt
import statsmodels.api as sm
from typing import List, Tuple, Optional, Dict

pd.options.mode.chained_assignment = None  # default='warn'

# %% functions
#  core: multi-way FE residualizer (alternating projections)
def residualize_against_fixed_effects(
    df: pd.DataFrame,
    cols: List[str],
    fe_cols: List[str],
    max_iter: int = 100,
    tol: float = 1e-10,
) -> pd.DataFrame:
    """
    Residualize 'cols' against one or more fixed-effect columns in 'fe_cols'
    using alternating within-group demeaning (Frisch–Waugh–Lovell with FE).
    This avoids creating huge dummy matrices.

    Returns a new DataFrame with residualized columns named f"{c}__resid".
    """
    if not fe_cols:
        return df.assign(**{f"{c}__resid": df[c] for c in cols})

    out = df[cols].astype(float).copy()

    # initialize residuals
    resid = out.to_numpy()
    n, k = resid.shape

    # helper: demean over a single FE
    def demean_one(fe: pd.Series, M: np.ndarray) -> np.ndarray:
        # compute group means for each column, subtract for each row
        g_means = (
            pd.DataFrame(M, index=fe)
            .groupby(level=0)
            .transform("mean")
            .to_numpy()
        )
        return M - g_means

    # alternating projections across FE columns
    for _ in range(max_iter):
        prev = resid.copy()
        for fe in fe_cols:
            resid = demean_one(df[fe], resid)
        if np.nanmax(np.abs(resid - prev)) < tol:
            break

    resid_df = pd.DataFrame(resid, index=df.index, columns=[f"{c}__resid" for c in cols])
    return pd.concat([df, resid_df], axis=1)

#  helper: partial R^2 and F-test for instruments after absorbing FE
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
    1) Residualizes endog, instruments, and exog w.r.t. FE.
    2) Reports pairwise correlations corr(z_tilde, y_tilde).
    3) Runs 'first-stage' OLS: y_tilde ~ Z_tilde + X_tilde (no intercept),
       returns partial R^2 for Z | X and an F-test that Z coefficients = 0.

    Returns dict with:
      - 'corr' (pd.Series)
      - 'first_stage_summary' (statsmodels summary)
      - 'partial_R2' (float)
      - 'F_stat' (float), 'df_num' (int), 'df_den' (int), 'F_pvalue' (float)
    """
    exog = exog or []
    fe_cols = fe_cols or []

    cols_to_resid = [endog] + instruments + exog
    rdf = residualize_against_fixed_effects(df, cols_to_resid, fe_cols)

    y = rdf[f"{endog}__resid"].to_numpy()
    Z = rdf[[f"{z}__resid" for z in instruments]].to_numpy()
    X = rdf[[f"{x}__resid" for x in exog]].to_numpy() if exog else None

    # 1) simple correlations: each instrument vs residualized endog
    corr = pd.Series(
        {z: np.corrcoef(rdf[f"{z}__resid"], y)[0, 1] for z in instruments},
        name="corr(z~, y~)"
    )

    # 2) first-stage regression: y~ on [Z~, X~], test Z~ jointly
    if X is None or X.size == 0:
        RHS = Z
        k_z = Z.shape[1]
        offset = 0
    else:
        RHS = np.column_stack([Z, X])
        k_z = Z.shape[1]
        offset = 0  # instruments are placed first intentionally

    model = sm.OLS(y, RHS, missing='drop')
    if cluster is None:
        res = model.fit(cov_type="HC1")
    else:
        # resolve groups
        if isinstance(cluster, str):
            groups = df.loc[rdf.index, cluster]
        else:
            groups = cluster  # array-like; can be 1D or 2D for multiway on newer statsmodels
        cov_kw = {"groups": groups}
        cov_kw.update(cluster_kwargs or {"use_correction": True, "df_correction": True})
        res = model.fit(cov_type="cluster", cov_kwds=cov_kw)

    # reduced model for partial R^2 (match the same covariance style for the summary/F-test if you like)
    if X is None or X.size == 0:
        r2_reduced = 0.0
    else:
        red = sm.OLS(y, X, missing='drop')
        if cluster is None:
            res_red = red.fit(cov_type="HC1")
        else:
            res_red = red.fit(cov_type="cluster", cov_kwds=cov_kw)
        r2_reduced = res_red.rsquared
    ...
    # F-test on instruments (works with clustered covariances too)
    R = np.zeros((k_z, RHS.shape[1])); R[np.arange(k_z), np.arange(k_z)] = 1.0
    ft = res.f_test(R)

    F_val = float(np.squeeze(np.asarray(ft.fvalue)))
    p_val = float(np.squeeze(np.asarray(ft.pvalue)))
    df_num = int(getattr(ft, "df_num", k_z))
    df_den = int(getattr(ft, "df_denom", res.df_resid))

    out = {
        "corr": corr,
        "first_stage_summary": res.summary(),
        "partial_R2": float(r2_reduced),
        "F_stat": F_val, "df_num": df_num, "df_den": df_den, "F_pvalue": p_val,
    }
    return out

#  convenience: one-liner wrapper that prints a compact report
def report_instrument_checks(
    df: pd.DataFrame,
    endog: str,
    instruments: List[str],
    exog: Optional[List[str]] = None,
    fe_cols: Optional[List[str]] = None,
    cluster: Optional[object] = None,
    cluster_kwargs: Optional[dict] = None,
) -> None:
    res = instrument_relevance_after_fe(df, endog, instruments, exog, fe_cols, cluster, cluster_kwargs)
    print("=== Correlation of residualized instruments with residualized endogenous (after FE) ===")
    print(res["corr"].sort_values(ascending=False).to_string(float_format=lambda x: f"{x: .4f}"))
    print("\n=== First-stage (after FE): y~ on [Z~, X~] ===")
    print(res["first_stage_summary"])
    print(f"\nPartial R^2(Z | X): {res['partial_R2']:.4f}")
    print(f"F-test (Z jointly = 0): F({res['df_num']}, {res['df_den']}) = {res['F_stat']:.2f}, p = {res['F_pvalue']:.4g}")


# %% [load-and-subset]
bldg_df = pd.read_csv("/Users/joefish/Desktop/data/philly-evict/processed/bldg_panel.csv")

submarket_sample = bldg_df[
    (bldg_df["year"].between(2016, 2023)) &
    (bldg_df["num_units_imp"] >= 1) &
    (bldg_df["num_units_zip"] >= 100)
].copy()

# record num_units_bins so that 51+ is a category instead of 50-100 and 101+
# otherwise, keep num_units_bins
# submarket_sample["num_units_bins"] = np.where(
#     submarket_sample["num_units_imp"] >= 51, "51+", submarket_sample["num_units_bins"]
# )

# %% [identifiers-required-vars]
# BLP identifiers
submarket_sample["market_ids"]  = (
    submarket_sample["pm.zip"].astype(str) + "-" +
    submarket_sample["year"].astype(str) + "-" +
    submarket_sample["num_units_bins"].astype(str)
)
submarket_sample["firm_ids"]    = submarket_sample["owner_grp"]
submarket_sample["product_ids"] = submarket_sample["PID"]

# Required BLP columns
submarket_sample["prices"] = submarket_sample["log_med_rent"]
submarket_sample["shares"] = submarket_sample["share_units_zip_unit"]
# Optional nest label (not estimating rho in this run)
submarket_sample["filing_rate"] = submarket_sample["filing_rate"].fillna(0)
submarket_sample["nesting_ids"] = np.where(
    submarket_sample["filing_rate_preCOVID"].fillna(0) >=0.1, "high", "low"
)


# %% [basic-cleaning]
# Drop rows without prices/shares
submarket_sample = submarket_sample.dropna(subset=["prices", "shares"])

# Market share sanity — drop markets with total share >= 1
submarket_sample["market_shares"] = submarket_sample.groupby("market_ids")["shares"].transform("sum")
submarket_sample = submarket_sample[submarket_sample["shares"] < 1.0]
submarket_sample = submarket_sample[submarket_sample["market_shares"] < 1.0]

# # Keep only markets with >1 product
# market_counts = submarket_sample["market_ids"].value_counts()
# submarket_sample = submarket_sample[submarket_sample["market_ids"].isin(market_counts[market_counts > 2].index)].copy()


# %% [build-instruments]
# drop old demand instruments if they exist
old_demand_instruments = [c for c in submarket_sample.columns if c.startswith("demand_instruments")]
submarket_sample = submarket_sample.drop(columns=old_demand_instruments)
# demand_instruments0: per-year percent change in total_tax (log of positive tax base)
# add random noise tp prevent perfect collinearity
submarket_sample["demand_instruments0"] = submarket_sample["prices"] #+ np.random.normal(0, 1, size=submarket_sample["prices"].shape)    
#submarket_sample["demand_instruments0"] = submarket_sample["z_cnt_1to3km"]
# submarket_sample["demand_instruments0"] = submarket_sample["z_mean_1to3km_log_taxable_value"]
# submarket_sample["demand_instruments2"] = submarket_sample["z_sum_1to3km_year_blt_decade"]

# # # now add polynomials of these -- continue numbering
# submarket_sample["demand_instruments3"] = submarket_sample["demand_instruments0"] ** 2
# submarket_sample["demand_instruments4"] = submarket_sample["demand_instruments1"] ** 2
# submarket_sample["demand_instruments5"] = submarket_sample["demand_instruments2"] ** 2



# demand_instruments1: nest sizes (count of products per (market, nest))
nest_sizes = submarket_sample.groupby(["market_ids", "nesting_ids"])["product_ids"].transform("size")
# dynamically make nest_instruments by max number of instrument cols 
nest_inst_col = submarket_sample.filter(like="demand_instruments").shape[1]
submarket_sample[f"demand_instruments{nest_inst_col}"] = nest_sizes

# Clean up bad instrument rows
submarket_sample = submarket_sample.replace([np.inf, -np.inf], np.nan)
submarket_sample = submarket_sample.dropna(subset=["demand_instruments0", "demand_instruments1"])

# %% # make polynomials of log_total_area
submarket_sample["log_total_area"] = np.log(submarket_sample["total_area"].replace(0, np.nan))
submarket_sample["log_total_area_sq"] = submarket_sample["log_total_area"] ** 2
submarket_sample["log_total_area_cu"] = submarket_sample["log_total_area"] ** 3

# %% [estimation-sample]
# Optional: keep products seen >1 time over the window (stability)
pid_counts = submarket_sample["product_ids"].value_counts()
est_submarket_sample = submarket_sample[submarket_sample["product_ids"].isin(pid_counts[pid_counts > 0].index)].copy()

# # balanced panel
# pid_market_counts = est_submarket_sample.groupby("product_ids")["market_ids"].transform("nunique")
# est_submarket_sample = est_submarket_sample[pid_market_counts == pid_counts.max()]

est_submarket_sample["demand_instruments0"].describe()
est_submarket_sample["prices"].describe()
# describe shares for units > 10
est_submarket_sample[est_submarket_sample["num_units_imp"] > 10]["shares"].describe()

est_submarket_sample[est_submarket_sample["year"].isin([2018,2019]) &
                          (est_submarket_sample["num_units_imp"] > 5)]["demand_instruments0"].describe()
# %% drop total market shares >= 1
est_submarket_sample = est_submarket_sample[est_submarket_sample["total_market_share_units_bins"] < 1.0]
est_submarket_sample["max_abs_change_log_taxable_value"] = est_submarket_sample.groupby("product_ids")["change_log_taxable_value"].transform(lambda x: x.abs().max())
est_submarket_sample["log_market_value"] = np.log(est_submarket_sample["market_value"].replace(0, np.nan))
est_submarket_sample["log_market_value_per_unit"] = np.log((est_submarket_sample["market_value"] / est_submarket_sample["num_units_imp"]).replace(0, np.nan))
# %% get annualized change in rents
est_submarket_sample = est_submarket_sample.sort_values(by=["product_ids", "year"])
est_submarket_sample["annualized_change_log_med_rent"] = est_submarket_sample.groupby("product_ids")["log_med_rent"].diff().fillna(0)
est_submarket_sample["annualized_change_log_med_rent"] = est_submarket_sample["annualized_change_log_med_rent"] / est_submarket_sample.groupby("product_ids")["year"].diff().fillna(1)
est_submarket_sample["annualized_change_log_med_rent"] = est_submarket_sample["annualized_change_log_med_rent"].replace([np.inf, -np.inf], np.nan)  

est_submarket_sample[est_submarket_sample["year"].isin([2018,2019]) &
                          (est_submarket_sample["num_units_imp"] > 5)]["annualized_change_log_med_rent"].describe()
# get range of adj occ rate within parcel
est_submarket_sample["range_adj_occupancy_rate"] = est_submarket_sample.groupby("product_ids")["adj_occ_rate"].transform(lambda x: x.max() - x.min())
est_submarket_sample["households_units_gap"] = est_submarket_sample["num_households"] - est_submarket_sample["num_units_imp"]
# make val psf
est_submarket_sample["log_market_value_per_sqft"] = np.log((est_submarket_sample["market_value"] / est_submarket_sample["total_area"]).replace(0, np.nan))
# %% [blp-problem]

# check if instruments are correlated with prices after residualizing on market and product FE
est_final =     (est_submarket_sample[#est_submarket_sample["year"].isin([2019]) &
                          (est_submarket_sample["num_units_imp"] >5) 
                         #&(est_submarket_sample["year_built"] <= 2010) 
                        & (est_submarket_sample.log_market_value>0)
                        # & (est_submarket_sample["source"] == "evict")
                         & (est_submarket_sample["num_units_imp"]<=500)
                      # & (est_submarket_sample["range_adj_occupancy_rate"] >= 0.2)
                         & (est_submarket_sample["max_abs_change_log_taxable_value"]<1.25)
                      & (est_submarket_sample["building_code_description_new_fixed"].str.contains("Apartment|Coop|Mixed Use", na=False,case=False))
                       & (est_submarket_sample["annualized_change_log_med_rent"].abs() <= 0.2)
                           #& (est_submarket_sample["prices"]<8.5)
                          & ((est_submarket_sample["year"] <= 2019) & (est_submarket_sample["year"] >= 2011)) 
                         ]#.drop(columns=["nesting_ids", "demand_instruments1"])
    )
# get number of obs / PID
est_final["num_units_cuts"] = pd.cut(est_final["num_units_imp"], bins=[0,5,10,20,50,100,5000], labels=["1-5","6-10","11-20","21-50","51-100","101+"])
est_final["num_obs_per_pid"] = est_final.groupby("product_ids")["year"].transform("size")
est_final = est_final[est_final.num_obs_per_pid>=1].copy()
est_final["num_units_imp_sq"] = est_final["num_units_imp"] ** 2
est_final["quality_grade_fixed_coarse"] = est_final["quality_grade"].str.replace("[+-]", "", regex=True)
# Simple logit with price, product FE, market FE
# pyblp will automatically pick up demand instruments named demand_instruments{k}
X1 = blp.Formulation(
    "0 + prices  + log_market_value + log_total_area + year_built",
    absorb="C(census_tract)*C(year)  + C(building_code_description_new_fixed)+C(quality_grade_fixed_coarse)"  # product and market fixed effects,
)
problem2 = blp.Problem(
    X1,est_final
    # filter for year in 2019,2020,2023 and > 1 units,
)

results2 = problem2.solve(rho = 0.2)
    #
print(results2)


# %% [elasticities-diversions]
elasticities = results2.compute_elasticities()
diversions   = results2.compute_diversion_ratios()

# Own elasticities summary
own_means = results2.extract_diagonal_means(elasticities)
print(pd.Series(np.asarray(own_means).ravel()).describe())

# %%
# ===========================
# Nested Logit: nest bumps, swaps, diversion
# Python 3.7+ (no union types), NumPy >=1.20
# ===========================

import numpy as np
import pandas as pd
from typing import Optional, Dict, Any, Hashable, Tuple

# ---------- small utilities ----------

def _scalar(x, name="value") -> float:
    """Return a Python float from a scalar or size-1 array."""
    a = np.asarray(x)
    if a.size != 1:
        raise ValueError(f"{name} must be a scalar (got shape {a.shape}).")
    return float(a.reshape(()))  # avoids NumPy 1.25 deprecation

def _check_align(delta: np.ndarray, prices, markets, nests):
    n = int(np.asarray(delta).size)
    if not (len(prices) == len(markets) == len(nests) == n):
        raise ValueError(
            f"delta, prices, markets, nests must be same length: "
            f"{n} vs {len(prices)}, {len(markets)}, {len(nests)}"
        )

# ---------- core: compute NL shares from delta ----------

def nl_shares_from_delta_single_level(
    delta: np.ndarray,
    market_ids: np.ndarray,
    nest_ids: np.ndarray,
    rho: float
) -> Tuple[pd.DataFrame, pd.Series]:
    """
    Single-level nested-logit product shares given mean utility delta and rho.
    Returns:
      shares_df  : DataFrame with columns [market, nest, share] aligned to input rows
      s0_by_mkt  : Series mapping market -> outside-good share
    """
    rho = _scalar(rho, "rho")
    if not (0.0 <= rho < 1.0):
        raise ValueError("rho must be in [0,1).")

    df = pd.DataFrame({"market": market_ids, "nest": nest_ids, "delta": delta})
    out_frames = []
    s0_map = {}

    for mkt, gdf in df.groupby("market", sort=False):
        exp_scaled = np.exp(gdf["delta"].values / (1.0 - rho))
        # compress nests to contiguous ids within the market
        _, inv = np.unique(gdf["nest"].values, return_inverse=True)
        Dg = np.bincount(inv, weights=exp_scaled)                      # D_g = sum exp(delta/(1-rho)) in nest g
        denom = 1.0 + np.sum(Dg ** (1.0 - rho))                        # 1 + sum_g D_g^{1-rho}
        shares = exp_scaled * (Dg[inv] ** (-rho)) / denom              # s_j

        out_frames.append(pd.DataFrame(
            {"market": gdf["market"].values, "nest": gdf["nest"].values, "share": shares},
            index=gdf.index
        ))
        s0_map[mkt] = 1.0 - shares.sum()

    shares_df = pd.concat(out_frames).sort_index()
    s0_by_mkt = pd.Series(s0_map)
    return shares_df, s0_by_mkt

# ---------- swap % when bumping prices (all or targeted nests) ----------

def percent_swap_when_bumping_nests(
    results,
    data: pd.DataFrame,
    price_col: str,
    market_col: str,
    nest_col: str,
    pct_increase: float = 0.10,
    alpha_value: Optional[float] = None,          # pass scalar alpha if you have it
    price_beta_index: Optional[int] = None,       # or pass the index of price in X1 to extract alpha
    target_nest: Optional[Hashable] = None,       # bump this nest everywhere (if None: bump ALL nests)
    target_nest_by_market: Optional[Dict[Hashable, Hashable]] = None,  # dict {market: nest}
    include_outside_in_swap: bool = False,
    return_tables: bool = False                   # if True, also return base_tab, cf_tab
) -> Dict[str, Any]:
    """
    Compute percent of consumers who switch nests after raising prices by pct_increase.
    - If target_nest/target_nest_by_market is given, only that nest is bumped; else ALL inside options bump.
    - swap% is 0.5*L1 between baseline and counterfactual distributions over nests
      (either within-inside or including outside, per include_outside_in_swap).
    """
    # pull core pieces
    delta = np.asarray(results.delta).reshape(-1)
    rho = _scalar(getattr(results, "rho", np.nan), "rho")
    if not (0.0 <= rho < 1.0):
        raise ValueError("results.rho must be in [0,1).")

    # price coefficient alpha
    if alpha_value is not None:
        alpha = _scalar(alpha_value, "alpha_value")
    else:
        # try to extract from results.beta and X1 when given an index
        if price_beta_index is None:
            raise ValueError("Pass alpha_value (scalar price coefficient) or price_beta_index to read from results.beta.")
        X1 = np.asarray(results.problem.products.X1)
        beta = np.asarray(results.beta).reshape(-1)
        if price_beta_index < 0 or price_beta_index >= X1.shape[1]:
            raise ValueError(f"price_beta_index {price_beta_index} out of bounds for X1 with {X1.shape[1]} columns.")
        alpha = float(beta[price_beta_index])

    # align data arrays
    prices  = np.asarray(data[price_col], dtype=float)
    markets = np.asarray(data[market_col], dtype=object)
    nests   = np.asarray(data[nest_col], dtype=object)
    _check_align(delta, prices, markets, nests)

    # build bump mask
    n = len(delta)
    bump_mask = np.zeros(n, dtype=bool)
    if target_nest_by_market is not None:
        for mkt, tgt in target_nest_by_market.items():
            msel = (markets == mkt)
            bump_mask |= (msel & (nests == tgt))
    elif target_nest is not None:
        bump_mask = (nests == target_nest)
    else:
        bump_mask[:] = True  # bump all inside options

    bumped_rows = int(bump_mask.sum())
    if bumped_rows == 0:
        uniq_nests, counts = np.unique(nests, return_counts=True)
        hint = ", ".join(f"{repr(u)}:{c}" for u, c in list(zip(uniq_nests, counts))[:12])
        raise ValueError(f"No rows matched bump selection. target_nest={target_nest}, target_nest_by_market={target_nest_by_market}. "
                         f"Sample nests: {hint}")

    # baseline shares
    shares0_df, s0 = nl_shares_from_delta_single_level(delta, markets, nests, rho)
    # rename shares df to originals
    shares0_df = shares0_df.rename(columns={"market": market_col, "nest": nest_col})
    base_tab = shares0_df.groupby([market_col, nest_col], sort=False)["share"].sum().unstack(nest_col).fillna(0.0)
    inside0 = base_tab.sum(axis=1)

    # counterfactual delta (only for selected rows)
    delta_cf = delta.copy()
    delta_cf[bump_mask] = delta_cf[bump_mask] + alpha * (pct_increase * prices[bump_mask])
    shares1_df, s0_cf = nl_shares_from_delta_single_level(delta_cf, markets, nests, rho)
    # rename shares df to originals
    shares1_df = shares1_df.rename(columns={"market": market_col, "nest": nest_col})
    cf_tab = shares1_df.groupby([market_col, nest_col], sort=False)["share"].sum().unstack(nest_col).fillna(0.0)
    inside1 = cf_tab.sum(axis=1)

    # choose distributions for swap metric
    if include_outside_in_swap:
        base_with_out = base_tab.copy()
        cf_with_out   = cf_tab.copy()
        base_with_out["__outside__"] = 1.0 - base_with_out.sum(axis=1)
        cf_with_out["__outside__"]   = 1.0 - cf_with_out.sum(axis=1)
        base_dist = base_with_out.div(base_with_out.sum(axis=1), axis=0)
        cf_dist   = cf_with_out.div(cf_with_out.sum(axis=1), axis=0)
    else:
        base_dist = base_tab.div(inside0.replace(0, np.nan), axis=0).fillna(0.0)
        cf_dist   = cf_tab.div(inside1.replace(0, np.nan), axis=0).fillna(0.0)

    swap_pct = 0.5 * (base_dist - cf_dist).abs().sum(axis=1)

    out = {
        "by_market": pd.DataFrame({
            market_col: base_tab.index,
            "inside_share_baseline": inside0.values,
            "inside_share_cf": inside1.values,
            "delta_outside": (1.0 - inside1.values) - (1.0 - inside0.values),
            "swap_pct": swap_pct.values
        }).reset_index(drop=True),
        "overall_simple_mean": float(swap_pct.mean()),
        "overall_inside_weighted": float(np.average(swap_pct.values, weights=np.clip(inside0.values, 0, None)))
    }
    if return_tables:
        out["base_tab"] = base_tab
        out["cf_tab"] = cf_tab
    return out

# ---------- diversion ratios when bumping a single target nest ----------

def diversion_ratios_for_target_nest(
    results,
    data: pd.DataFrame,
    price_col: str,
    market_col: str,
    nest_col: str,
    target_nest: Hashable,
    pct_increase: float,
    alpha_value: float
) -> Dict[str, Any]:
    """
    Bump ONE nest (target_nest) by pct_increase everywhere.
    Returns per-market diversion ratios from the target nest to other nests and outside.
    Diversion to nest k in market m:  Δshare_k / (-Δshare_target)  (clipped at 0 when Δshare_target≈0).
    """
    # run the bump with return_tables=True
    res = percent_swap_when_bumping_nests(
        results, data, price_col, market_col, nest_col,
        pct_increase=pct_increase, alpha_value=alpha_value,
        target_nest=target_nest, include_outside_in_swap=False, return_tables=True
    )
    base_tab = res["base_tab"].copy()
    cf_tab   = res["cf_tab"].copy()

    # identify available nests consistently across markets
    all_nests = base_tab.columns.union(cf_tab.columns)

    # ensure columns aligned
    base_tab = base_tab.reindex(columns=all_nests, fill_value=0.0)
    cf_tab   = cf_tab.reindex(columns=all_nests, fill_value=0.0)

    # compute share changes
    delta_tab = cf_tab - base_tab
    # diversion denominator: loss from target nest (positive number)
    loss = (base_tab[target_nest] - cf_tab[target_nest]).clip(lower=0.0)

    # diversion to each other nest
    div_to_nests = pd.DataFrame(index=base_tab.index)
    for k in all_nests:
        if k == target_nest:
            continue
        div_to_nests[k] = (delta_tab[k] / loss.replace(0, np.nan)).fillna(0.0).clip(lower=0.0)

    # diversion to outside = (Δoutside)/(loss)
    outside0 = 1.0 - base_tab.sum(axis=1)
    outside1 = 1.0 - cf_tab.sum(axis=1)
    d_outside = (outside1 - outside0).clip(lower=0.0)
    div_to_outside = (d_outside / loss.replace(0, np.nan)).fillna(0.0).clip(lower=0.0)

    # row sums (should be ≈ 1.0 allowing for rounding)
    rowsum = div_to_nests.sum(axis=1) + div_to_outside
    return {
        "diversion_to_nests": div_to_nests,        # DataFrame [markets x (other nests)]
        "diversion_to_outside": div_to_outside,    # Series indexed by market
        "rowsum_check": rowsum,                    # Series; should be near 1
        "base_tab": base_tab,
        "cf_tab": cf_tab
    }


# %% get 
## ===========================
# USAGE EXAMPLES
# ===========================
# Suppose you have `results` from pyblp nested-logit and a DataFrame `df`
# with columns: prices, market_ids, nesting_ids, aligned to results.delta

# alpha as scalar (index j is price column in X1):
j = 0
alpha = float(results2.beta[j])

# 1) Bump ALL nests by +10%, compute inside-only swap%
summary = percent_swap_when_bumping_nests(
    results2, est_final, "prices", "market_ids", "nesting_ids",
    pct_increase=.10, alpha_value=False,
    include_outside_in_swap=True
)

# 2) Bump ONE target nest (e.g., "Luxury") everywhere; include outside in swap metric
summary_one = percent_swap_when_bumping_nests(
    results2, est_final, "prices", "market_ids", "nesting_ids",
    pct_increase=.10, alpha_value=alpha,
    target_nest="high", include_outside_in_swap=False
)

# 3) Diversion ratios from target nest after +10%
div = diversion_ratios_for_target_nest(
    results2, est_final, "prices", "market_ids", "nesting_ids",
    target_nest="low", pct_increase=0.1, alpha_value=alpha
)
div["diversion_to_nests"].describe()
div["diversion_to_outside"].describe()
div["rowsum_check"].describe()

# %%
# --- diagnostics for "all zero" diversion ---

# 0) sanity: what alpha and pct are we applying?
alpha_value = alpha
pct_increase = 0.1
target_nest = "low"
data = est_final
price_col = "prices"
market_col = "market_ids"
nest_col = "nesting_ids"

print("alpha_value (should be NEGATIVE):", alpha_value)
print("pct_increase:", pct_increase)

# 1) does the target nest exist & have baseline share?
print("target nest label:", target_nest)
print("some nest labels:", data[nest_col].astype(object).unique()[:10])

# 2) re-run bump to get the share tables (ensures same logic as diversion fn)
res = percent_swap_when_bumping_nests(
    results2, data, price_col, market_col, nest_col,
    pct_increase=pct_increase, alpha_value=alpha_value,
    target_nest=target_nest, include_outside_in_swap=False, return_tables=True
)
base_tab, cf_tab = res["base_tab"], res["cf_tab"]

# 3) did we actually bump any rows?
markets = np.asarray(data[market_col], dtype=object)
nests    = np.asarray(data[nest_col], dtype=object)
prices   = np.asarray(data[price_col], dtype=float)
delta    = np.asarray(results2.delta).reshape(-1)

bump_mask = (nests == target_nest)
print("bumped rows:", int(bump_mask.sum()))

# 4) is the target nest present in the share tables?
print("target in base_tab columns?", target_nest in base_tab.columns)
print("baseline target share sum:", float(base_tab.get(target_nest, 0).sum()))

# 5) did the target nest actually LOSE share?
loss = (base_tab.get(target_nest, 0) - cf_tab.get(target_nest, 0))
print("loss stats (should be >0 somewhere):")
print(loss.describe())

# 6) check sign/magnitude of the delta change we’re applying
delta_change = np.zeros_like(delta)
delta_change[bump_mask] =  float(alpha_value) * (pct_increase * prices[bump_mask])
print("delta_change (bumped rows) min/median/max:",
      np.min(delta_change[bump_mask]) if bump_mask.any() else None,
      np.median(delta_change[bump_mask]) if bump_mask.any() else None,
      np.max(delta_change[bump_mask]) if bump_mask.any() else None)
#%%
# %% [summaries-and-latex]
from pathlib import Path

OUT_DIR = Path("output/pyblp_summaries")
OUT_DIR.mkdir(parents=True, exist_ok=True)

def _safe_names_from_formulation(problem_obj) -> list:
    """
    Try to recover X1 column names from pyblp internals. Fallback to generic names.
    """
    names = None
    try:
        # modern pyblp stores a "formulations" list on products; first is X1
        # and the formulation has a private _names attribute populated during construction
        names = list(getattr(problem_obj.products.X1_formulation, "_names", []))
    except Exception:
        pass
    if not names:
        # fallback: generic names based on matrix width
        k = problem_obj.products.X1.shape[1]
        names = [f"x{i+1}" for i in range(k)]
    return names

def write_latex_table(df: pd.DataFrame, path: Path, caption: str, label: str, index: bool = False):
    tex = df.to_latex(escape=False, index=index, float_format=lambda x: f"{x:,.4f}")
    parts = [
        "\\begin{table}[!htbp]\n\\centering\n",
        f"\\caption{{{caption}}}\n",
        f"\\label{{{label}}}\n",
        tex,
        "\n\\end{table}\n",
    ]
    path.write_text("".join(parts), encoding="utf-8")
    print(f"Wrote {path}")

# ---------- 1) Logistic (BLP simple logit) coefficients ----------
# Robust/asymptotic SEs

coef = np.asarray(results2.beta).reshape(-1)
se_beta = np.asarray(results2.beta_se).reshape(-1)
tvals = coef / se_beta
from scipy.stats import norm
pvals = 2.0 * (1.0 - norm.cdf(np.abs(tvals)))

x1_names = _safe_names_from_formulation(problem2)
x1_names = ["price", "log market value", "log total area", "year built"]  # manually set for clarity
coef_tab = pd.DataFrame({
    "Variable": x1_names,
    "Estimate": coef,
    "Std. Error": se_beta,
    "t-stat": tvals,
    "p-value": pvals
})

# Optional footer info
n_obs = est_final.shape[0]
n_mkts = est_final["market_ids"].nunique()
n_prods = est_final["product_ids"].nunique()
rho = getattr(results2, "rho", float("nan"))
# convert rho to float
rho = float(rho[0])
coef_tab.attrs["notes"] = f"N = {n_obs}, Markets = {n_mkts}, Products = {n_prods}, rho = {rho:.3f}"

write_latex_table(
    coef_tab,
    OUT_DIR / "logit_coefficients.tex",
    caption=f"Simple Logit Estimates (N={n_obs}, Markets={n_mkts}, Products={n_prods}; $\\rho={rho:.3f}$)",
    label="tab:blp_logit_coef",
    index=False
)

# %%
# ---------- 2) Own-price elasticities ----------
# elasticities: (J x J) per market flattened by pyblp into (rows aligned to products)
# we already have 'elasticities' and 'own_means' computed earlier
# own price elasticities: diagonal of each JxJ matrix
own_elasticities = np.array([])
for market in est_final["market_ids"].unique():
    market_mask = (est_final["market_ids"] == market)
    market_elasticities = elasticities[market_mask]
    own_elasticities = np.concatenate([own_elasticities, np.diag(market_elasticities)])

# own_el = pd.Series(np.asarray(results2.extract_diagonal_means(elasticities)).ravel(), name="own_elasticity")
# own_el_desc = own_el.describe()[["count","mean","std","min","25%","50%","75%","max"]].to_frame().T
# own_el_desc.index = ["Overall"]

# By size bin (you already created 'num_units_cuts')
el_df = pd.DataFrame({"own_elasticity": own_elasticities.ravel()})
# product df is est_final -> drop duplicates on PID
#prod_data = est_final[["product_ids", "num_units_cuts"]].drop_duplicates(subset=["product_ids"])
# Align indices in case of filtering
el_df.index = est_final.index  # ensure same alignment as products
by_bin = (
    el_df.join(est_final[["num_units_cuts", "product_ids"]], how="left")
         .dropna(subset=["num_units_cuts"])
         .groupby("num_units_cuts")["own_elasticity"]
         .describe()[["count","mean","std","min","25%","50%","75%","max"]]
)

own_by_product = (
    el_df.join(est_final[["num_units_cuts", "product_ids"]], how="left")["own_elasticity"]
          .describe()[["count","mean","std","min","25\%","50\%","75\%","max"]]
          .reset_index()
          .rename(columns={"product_ids":"Product", "own_elasticity":"Elasticity"})
)

own_el_table =own_by_product

write_latex_table(
    own_el_table,
    OUT_DIR / "own_elasticities_summary.tex",
    caption="Own-Price Elasticities",
    label="tab:own_elasticities",
    index=False
)

# ---------- 3) Diversion ratios ----------
# Choose a target nest (you computed examples for 'low' above)
target_nest_for_table = "low"
alpha_hat = float(results2.beta[0])  # first column in X1 is 'prices' per your formulation string

div_out = diversion_ratios_for_target_nest(
    results2, est_final,
    price_col="prices",
    market_col="market_ids",
    nest_col="nesting_ids",
    target_nest=target_nest_for_table,
    pct_increase=0.10,
    alpha_value=alpha_hat
)

# (a) Diversion-to-nests: summary stats (columns = other nests)
div_nests = div_out["diversion_to_nests"]
# ensure stable column order
div_nests = div_nests.reindex(sorted(div_nests.columns), axis=1)
div_nests_desc = div_nests.describe().T.reset_index().rename(columns={"index": "To nest"})
write_latex_table(
    div_nests_desc,
    OUT_DIR / "diversion_to_nests_summary.tex",
    caption=f"Diversion Ratios from Target Nest “{target_nest_for_table}” to Other Nests (10% price bump)",
    label="tab:diversion_nests",
    index=False
)

# (b) Diversion-to-outside: summary stats
div_outside = div_out["diversion_to_outside"].rename("diversion_to_outside")
div_outside_desc = div_outside.describe()[["count","mean","std","min","25%","50%","75%","max"]].to_frame().T
div_outside_desc.insert(0, "Metric", ["Outside good"])
write_latex_table(
    div_outside_desc,
    OUT_DIR / "diversion_to_outside_summary.tex",
    caption=f"Diversion Ratios from Target Nest “{target_nest_for_table}” to Outside (10% price bump)",
    label="tab:diversion_outside",
    index=False
)

# (c) (Optional) Top markets by diversion to outside
top_out = (
    pd.DataFrame({
        "market_id": div_out["diversion_to_outside"].index,
        "diversion_to_outside": div_out["diversion_to_outside"].values
    })
    .sort_values("diversion_to_outside", ascending=False)
    .head(20)
)
write_latex_table(
    top_out,
    OUT_DIR / "diversion_top_markets_outside.tex",
    caption=f"Top 20 Markets by Diversion to Outside (Target “{target_nest_for_table}”, +10% price)",
    label="tab:diversion_top_outside",
    index=False
)

# --- quick console breadcrumbs
print("\n=== SUMMARY FILES WRITTEN ===")
for p in sorted(OUT_DIR.glob("*.tex")):
    print(" -", p)

# %%
