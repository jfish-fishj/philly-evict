# %%
import pandas as pd
import numpy as np
import pyblp as blp
import matplotlib.pyplot as plt
pd.options.mode.chained_assignment = None  # default='warn'
# %%
# %% Data
# load cereal data


product_data = pd.read_csv(blp.data.NEVO_PRODUCTS_LOCATION)
product_data.head()

# %% add nesting_ids
# product_data["nesting_ids"] = product_data["brand_ids"]
product_data["nesting_ids"] = product_data["mushy"]

def solve_nl(df):
    groups = df.groupby(['market_ids', 'nesting_ids'])
    df['demand_instruments20'] = groups['shares'].transform(np.size)
    nl_formulation = blp.Formulation('0 + prices ')
    problem = blp.Problem(nl_formulation, df)
    return problem.solve(rho=0.7)


# %%
df2 = product_data.copy()
df2['nesting_ids'] = df2['mushy']
nl_results2 = solve_nl(df2)
nl_results2
# %%
