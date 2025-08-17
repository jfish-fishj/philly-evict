# %% [markdown]
# # Directed Search Rental Model: Landlords & Tenants
# - Landlords post one rent level
# - High-type landlords (mean): only accept low-type if no high-type applies
# - Low-type landlords (nice): accept anyone
# - Tenants dislike rent, utility is -expected_rent

# labelling convention
# first letter is landlord type (H or L)
# second letter is tenant type (H or L)
# xHH = high landlord, high tenant
# xHL = high landlord, low tenant
# xLH = low landlord, high tenant
# xLL = low landlord, low tenant

# %%
import numpy as np
import matplotlib.pyplot as plt
from scipy.optimize import minimize
import pandas as pd

# %%
# === Parameters ===
low_landlord_low_tenant_cost = 2
low_landlord_fixed_cost = 1
low_landlord_high_tenant_cost = 2
high_landlord_high_tenant_cost = 2
high_landlord_low_tenant_cost = 10
# high landlord fixed cost
high_landlord_fixed_cost = 1
tenant_high_share = 0.7   # s
market_tightness = 0.9    # n = tenants / landlords
high_landlord_share = 0.2 # h
valuation = 20   # valuation of housing by tenants
chi_H = 1
chi_L = 0.5
# %%
def g(x):
    return (1 - np.exp(-x)) / x if x > 1e-8 else 1.0

# %%
def compute_q_rental(x):
    # given expected queue lengths x, compute the
    # matching probabilities for each type of landlord and tenant
    xHH, xHL, xLH, xLL = x
    qHH = g(xHH )  #g(xHH) * (chi_H + (1-chi_H)*np.exp(-xHL))  # high landlord, high tenant
    qHL = g(xHL) * (1 - chi_H + chi_H*np.exp(-xHH) )  # high landlord, low tenant
    qLH = g(xLH + xLL)  # low landlord, high tenant
    qLL =  g(xLH + xLL)  # low landlord, low tenant
    return qHH, qHL, qLH, qLL

compute_q_rental([1,1,1,1])
# %%
def tenant_mass_balance(x, s, n, h):
    xHH, xHL, xLH, xLL = x
    # share of high-type tenants applying to high landlords + high type tenants applying to low landlords
    eq1 = h * xHH + (1 - h) * xLH - s * n
    # share of low-type tenants applying to high landlords + low type tenants applying to low landlords
    eq2 = h * xHL + (1 - h) * xLL - (1 - s) * n
    return np.array([eq1, eq2])


# %%

def utility_gap_rental(x, r_H, r_L, U_h, U_l):
    qHH, qLH, qHL, qLL = compute_q_rental(x)
    return np.array([
        qHH * (valuation - r_H) - U_h,
        qLH * (valuation - r_L) - U_h,
        qHL * (valuation - r_H) - U_l,
        qLL * (valuation - r_L) - U_l
    ])

# %%
def profit_high_landlord(r_H, xHH, xHL, high_landlord_fixed_cost    = high_landlord_fixed_cost):
    total_queue = xHH + xHL
    prob_match = 1 - np.exp(-total_queue)
    prob_low = xHL / (total_queue)
    return prob_match * ( r_H - high_landlord_high_tenant_cost * prob_low - high_landlord_low_tenant_cost * (1 - prob_low) ) - high_landlord_fixed_cost

# %%
def profit_low_landlord(r_L, xLL, xLH, low_landlord_fixed_cost = low_landlord_fixed_cost,):
    total_queue = xLH + xLL
    prob_match = 1 - np.exp(-total_queue)
    prob_low = xLL / (total_queue)
    return prob_match * (r_L - low_landlord_high_tenant_cost * prob_low - low_landlord_low_tenant_cost * (1 - prob_low)) - low_landlord_fixed_cost

profit_high_landlord(100, 1, 1), profit_low_landlord(100, 1, 1)
# %%
def equilibrium_conditions_rental(vars):
    r_H, r_L, U_h, U_l, xHH, xHL, xLH, xLL, low_landlord_fixed_cost = vars
    x = [xHH, xHL, xLH, xLL]
    mbalance = tenant_mass_balance(x, tenant_high_share, market_tightness, high_landlord_share)
    u_gap = utility_gap_rental(x, r_H, r_L, U_h, U_l)
    profit_gap = profit_high_landlord(r_H, xHH, xHL) -\
          profit_low_landlord(r_L, xLH, xLL, low_landlord_fixed_cost)  # zero profit condition
    profit_target = 0 #profit_high_landlord(r_H, xHH, xHL)
    return np.concatenate([mbalance, u_gap, [profit_gap]])



# %%
x0 = np.array([
    valuation / 2, valuation / 2,  # r_H, r_L
    valuation / 2, valuation / 2,  # U_h, U_l
    0.5, 0.5, 0.5, 0.5    ,         # xHH, xHL, xLH, xLL
    low_landlord_fixed_cost
   # high_landlord_share_initial # landlord share
])

# %%
def objective(vars):
    resids = equilibrium_conditions_rental(vars)
    return np.sum(resids**2)


bounds = [(1e-4, None)] * 2 + [(0.0, np.inf)] * 2 + [(1e-4, None)] * 4 + [(0.0, 100.0)]
result = minimize(objective, x0, method="SLSQP", bounds=bounds, options={"ftol": 1e-9})

# %%
if result.success:
    keys = ['r_H', 'r_L', 'U_h', 'U_l', 'xHH', 'xHL', 'xLH', 'xLL','low_landlord_fixed_cost']
    r_H, r_L, U_h, U_l, xHH, xHL, xLH, xLL, low_landlord_fixed_cost = result.x


    pi_H = profit_high_landlord(r_H, xHH, xHL, high_landlord_fixed_cost=high_landlord_fixed_cost)
    pi_L = profit_low_landlord(r_L, xLH, xLL, low_landlord_fixed_cost=low_landlord_fixed_cost)

    total_high_tenants = tenant_high_share * market_tightness
    total_low_tenants = (1 - tenant_high_share) * market_tightness

    share_HH = high_landlord_share * xHH / total_high_tenants
    share_HL = (high_landlord_share) * xHL / total_low_tenants
    share_LH = (1-high_landlord_share) * xLH / total_high_tenants
    share_LL = (1 - high_landlord_share) * xLL / total_low_tenants

    # matching probabilities
    x = xHH, xHL, xLH, xLL
    #tenant_mass_balance = tenant_mass_balance(x, tenant_high_share, market_tightness, high_landlord_share)
    qHH, qHL, qLH, qLL = compute_q_rental(x)

    # multiply matching probabilitues by eq shares
    qHH *= share_HH
    qHL *= share_HL
    qLH *= share_LH
    qLL *= share_LL

    qH = qHH + qLH
    qL = qHL + qLL

    # get average rents for each tenant type
    r_H_calc = (share_HH * r_H + share_LH * r_L)
    r_L_calc = (share_HL * r_H + share_LL * r_L)

    # calcilate utilities
    U_h_calc = share_HH * (valuation - r_H) * qHH + share_LH * (valuation - r_L) * qLH
    U_l_calc = share_HL * (valuation - r_H) * qHL + share_LL * (valuation - r_L) * qLL
    
    print("=== Results ===")
    print(f"High landlord rent: {r_H:.6f}")
    print(f"Low landlord rent:  {r_L:.6f}")
    print(f"High-type tenant utility: {U_h:.6f}")
    print(f"Low-type tenant utility:  {U_l:.6f}")
    print(f"High landlord profit: {pi_H:.6f}")
    print(f"Low landlord profit:  {pi_L:.6f}")
    print(f"Total profit: {pi_H + pi_L:.6f}")
    print("Share of high landlords:", high_landlord_share)
    print(f"Share of high-type tenants applying to high landlords: {share_HH:.3f}")
    print(f"Share of high-type tenants applying to low landlords:  {share_LH:.3f}")
    print(f"Share of low-type tenants applying to high landlords: {share_HL:.3f}")
    print(f"Share of low-type tenants applying to low landlords:  {share_LL:.3f}")
    print(f"High tenant matching probability: {qH:.6f}")
    print(f"Low tenant matching probability:  {qL:.6f}")
    print(f"High tenant rent: {r_H_calc:.6f}")
    print(f"Low tenant rent:  {r_L_calc:.6f}")
    print(f"high tenant utility: {U_h_calc:.6f}")
    print(f"low tenant utility:  {U_l_calc:.6f}")
    # print(f"High landlord high tenant matching probability: {qHH:.6f}")
    # print(f"High landlord low tenant matching probability:  {qHL:.6f}")
    # print(f"Low landlord high tenant matching probability: {qLH:.6f}")
    # print(f"Low landlord low tenant matching probability:  {qLL:.6f}")


    display(pd.DataFrame([dict(zip(keys, result.x))]))
else:
    print("Solver failed.")

# %% [markdown]
# ## Grid Search over High-Type Tenant Share (s)

s_grid = np.round(np.arange(0.5, 1.3, 0.1), 2)
multi_s_results = []

for s in s_grid:
    market_tightness = s
    res = minimize(objective, x0, bounds=bounds, method="SLSQP", options={"ftol": 1e-9})
    if res.success:
        r_H, r_L, U_h, U_l, xHH, xHL, xLH, xLL, low_landlord_fixed_cost = res.x
        pi_H = profit_high_landlord(r_H, xHH, xHL, high_landlord_fixed_cost=high_landlord_fixed_cost) 
        pi_L = profit_low_landlord(r_L, xLH, xLL, low_landlord_fixed_cost=low_landlord_fixed_cost) 
        total_profit = pi_H*high_landlord_share + pi_L*(1 - high_landlord_share)

        
        total_high_tenants = tenant_high_share * market_tightness
        total_low_tenants = (1 - tenant_high_share) * market_tightness

        share_HH = high_landlord_share * xHH / total_high_tenants
        share_HL = (high_landlord_share) * xHL / total_low_tenants
        share_LH = (1-high_landlord_share) * xLH / total_high_tenants
        share_LL = (1 - high_landlord_share) * xLL / total_low_tenants

        multi_s_results.append({
            "s": s,
            "r_H": r_H,
            "r_L": r_L,
            "U_h": U_h,
            "U_l": U_l,
            "profit_H": pi_H,
            "profit_L": pi_L,
            "total_profit_H": pi_H * total_high_tenants,
            "share_HH": share_HH,
            "share_HL": share_HL,
            "share_LH": share_LH,
            "share_LL": share_LL,
            "share_high_landlords": high_landlord_share,

        })
    else:
        print(f"Solver failed for s={s:.2f}")

multi_s_df = pd.DataFrame(multi_s_results)

# Display DataFrame
#import ace_tools as tools; tools.display_dataframe_to_user(name="Grid Search over s", dataframe=multi_s_df)

# Plot rents
plt.figure()
plt.plot(multi_s_df["s"], multi_s_df["r_H"], label="High Landlord Rent")
plt.plot(multi_s_df["s"], multi_s_df["r_L"], label="Low Landlord Rent")
plt.xlabel("Market Tightness (s)")
plt.ylabel("Rent")
plt.title("Rent vs. Tenant Composition")
plt.legend()
plt.grid(True)
plt.show()

# Plot profits
plt.figure()
plt.plot(multi_s_df["s"], multi_s_df["profit_H"], label="High Landlord Profit")
plt.plot(multi_s_df["s"], multi_s_df["profit_L"], label="Low Landlord Profit")
plt.plot(multi_s_df["s"], multi_s_df["total_profit_H"], label="Total Profit")
plt.xlabel("Market Tightness (s)")
plt.ylabel("Profit")
plt.title("Profit vs. Tenant Composition")
plt.legend()
plt.grid(True)
plt.show()

# Plot shares
plt.figure(figsize=(10, 6))
plt.plot(multi_s_df["s"], multi_s_df["share_HH"], label="High-Type Tenants to High Landlords")
plt.plot(multi_s_df["s"], multi_s_df["share_LH"], label="High-Type Tenants to Low Landlords")
plt.plot(multi_s_df["s"], multi_s_df["share_HL"], label="Low-Type Tenants to High Landlords")
plt.plot(multi_s_df["s"], multi_s_df["share_LL"], label="Low-Type Tenants to Low Landlords")
plt.xlabel("Market Tightness (s)")
plt.ylabel("Share of Applications")
plt.title("Share of Applications vs. Tenant Composition")
plt.legend()
plt.grid(True)
plt.show()

# plot U_h, U_l
plt.figure()
plt.plot(multi_s_df["s"], multi_s_df["U_h"], label="High-Type Tenant Utility")
plt.plot(multi_s_df["s"], multi_s_df["U_l"], label="Low-Type Tenant Utility")
plt.xlabel("Market Tightness (s)")
plt.ylabel("Utility")
plt.title("Tenant Utility vs. Tenant Composition")
plt.legend()
plt.grid(True)
plt.show()

# plot share of high landlords
plt.figure()
plt.plot(multi_s_df["s"], multi_s_df["share_high_landlords"], label="Share of High Landlords")
plt.xlabel("Market Tightness (s)")
plt.ylabel("Share")
plt.title("Share of High Landlords vs. Tenant Composition")
plt.legend()
plt.grid(True)
plt.show()  

# %%
