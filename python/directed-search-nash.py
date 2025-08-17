# %% [markdown]
# Directed Search Rental Model: Landlords & Tenants with Double Loop and Mixed Strategies

# %%
import numpy as np
from scipy.optimize import minimize

# %%
# === Parameters ===
valuation = 30.0
s = 0.6                 # Share of high-type tenants
n = 0.3                 # Tenant-to-landlord ratio
h = 0.5                 # Share of high-type landlords
fc_h = 0.1
fc_l = 0.0

# %%
def g(x):
    return (1 - np.exp(-x)) / x if x > 1e-8 else 1.0

# %%
def compute_q(x):
    xHH, xHL, xLH, xLL = x
    qHH = g(xHH)
    qHL = g(xHL) * (1 - np.exp(-xHH))
    qLH = g(xLH + xLL)
    qLL = g(xLH + xLL)
    return qHH, qHL, qLH, qLL

# %%
def tenant_expected_utility(x, r_H, r_L):
    qHH, qHL, qLH, qLL = compute_q(x)
    EU_HH = qHH * (valuation - r_H)
    EU_HL = qHL * (valuation - r_H)
    EU_LH = qLH * (valuation - r_L)
    EU_LL = qLL * (valuation - r_L)
    return EU_HH, EU_HL, EU_LH, EU_LL

# %%
def tenant_mixed_strategy_probs(x, r_H, r_L):
    EU_HH, EU_HL, EU_LH, EU_LL = tenant_expected_utility(x, r_H, r_L)
    maxU_h = max(EU_HH, EU_LH)
    maxU_l = max(EU_HL, EU_LL)

    p_HH = 1.0 if EU_HH >= EU_LH else 0.0
    p_LH = 1.0 - p_HH
    p_HL = 1.0 if EU_HL >= EU_LL else 0.0
    p_LL = 1.0 - p_HL

    return p_HH, p_LH, p_HL, p_LL

# %%
def profit_high_landlord(r_H, xHH, xHL):
    total_queue = xHH + xHL
    prob_match = 1 - np.exp(-total_queue)
    return prob_match * r_H - fc_h

# %%
def profit_low_landlord(r_L, xLH, xLL):
    total_queue = xLH + xLL
    prob_match = 1 - np.exp(-total_queue)
    return prob_match * r_L - fc_l

# %%
def queue_length_system(x, r_H, r_L):
    xHH, xHL, xLH, xLL = x
    p_HH, p_LH, p_HL, p_LL = tenant_mixed_strategy_probs(x, r_H, r_L)
    eq1 = h * xHH + (1 - h) * xLH - s * n * (p_HH + p_LH)
    eq2 = h * xHL + (1 - h) * xLL - (1 - s) * n * (p_HL + p_LL)
    return 1e3 * eq1, 1e3 * eq2

# %%
def queue_objective(x, r_H, r_L):
    res = queue_length_system(x, r_H, r_L)
    return np.sum(np.square(res))

# %%
# %%
def double_loop_solver(rH_grid, rL_grid, verbose=False):
    best_profit_H = -np.inf
    best_r_H = None
    best_r_L = None
    best_x_result = None

    for r_H in rH_grid:
        for r_L in rL_grid:
            # Tenant best response (inner loop): solve for xHH, xHL, xLH, xLL
            def objective_x(x):
                return np.sum(tenant_best_response(x, r_H, r_L)**2) + \
                       np.sum(np.square(tenant_allocation_equations(x, s, n, h)))

            x0 = np.full(4, 0.5)  # initial guess for xHH, xHL, xLH, xLL
            bounds_x = [(1e-4, 10.0)] * 4
            x_result = minimize(objective_x, x0, bounds=bounds_x, method='SLSQP')

            if x_result.success:
                xHH, xHL, xLH, xLL = x_result.x
                pi_H = profit_high_landlord(r_H, xHH, xHL, fc=fc_h)

                if verbose:
                    print(f"r_H = {r_H:.2f}, r_L = {r_L:.2f}, π_H = {pi_H:.2f}")

                if pi_H > best_profit_H:
                    best_profit_H = pi_H
                    best_r_H = r_H
                    best_r_L = r_L
                    best_x_result = x_result

    if best_x_result is not None:
        return (best_r_H, best_r_L), best_x_result
    else:
        print("⚠️ No successful equilibrium found in grid search.")
        return (None, None), None


# %%
rH_grid = np.linspace(1, valuation-1, 25)
rL_grid = np.linspace(1, valuation-1, 25)
r_H_star, r_L_star = None, None
x_result = None

(r_H_star, r_L_star), x_result = double_loop_solver(rH_grid, rL_grid)

# %% [markdown]
# ## Summary Statistics

# %%
if x_result and x_result.success:
    xHH, xHL, xLH, xLL = x_result.x
    r_H, r_L = r_H_star, r_L_star

    total_high_tenants = s * n
    total_low_tenants = (1 - s) * n

    share_HH = h * xHH / total_high_tenants
    share_HL = h * xHL / total_low_tenants
    share_LH = (1 - h) * xLH / total_high_tenants
    share_LL = (1 - h) * xLL / total_low_tenants

    print(f"Share of high-type tenants applying to high landlords: {share_HH:.3f}")
    print(f"Share of high-type tenants applying to low landlords:  {share_LH:.3f}")
    print(f"Share of low-type tenants applying to high landlords: {share_HL:.3f}")
    print(f"Share of low-type tenants applying to low landlords:  {share_LL:.3f}")

    qHH, qHL, qLH, qLL = compute_q((xHH, xHL, xLH, xLL))
    mH = qHH * share_HH + qLH * share_LH
    mL = qHL * share_HL + qLL * share_LL
    print(f"Match probability for high-type tenants: {mH:.3f}")
    print(f"Match probability for low-type tenants:  {mL:.3f}")

    rT_high = share_HH * r_H + share_LH * r_L
    rT_low = share_HL * r_H + share_LL * r_L
    print(f"Expected rent paid by high-type tenants: {rT_high:.3f}")
    print(f"Expected rent paid by low-type tenants:  {rT_low:.3f}")

    EU_HH, EU_HL, EU_LH, EU_LL = tenant_expected_utility((xHH, xHL, xLH, xLL), r_H, r_L)
    U_h = max(EU_HH, EU_LH)
    U_l = max(EU_HL, EU_LL)
    print(f"High-type tenant utility: {U_h:.3f}")
    print(f"Low-type tenant utility:  {U_l:.3f}")

    pi_H = profit_high_landlord(r_H, xHH, xHL)
    pi_L = profit_low_landlord(r_L, xLH, xLL)
    print(f"High landlord profit: {pi_H:.3f}")
    print(f"Low landlord profit:  {pi_L:.3f}")
    print(f"Fixed cost (high): {fc_h}, fixed cost (low): {fc_l}")
else:
    print("Solver did not converge to a valid result.")
